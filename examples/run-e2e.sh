#!/usr/bin/env bash
# run-e2e.sh — Datacat end-to-end integration test
#
# What it does:
#   1. Creates a dedicated PostgreSQL demo database (datacat_demo)
#   2. Launches datacat-ingest (backend/) on port 8090 with EdDSA token verification
#   3. Launches demo-backend on port 8091 pointing at datacat
#   4. Runs the Node e2e harness (examples/e2e/) that:
#       - emits analytics events via @datacat/sdk-web
#       - emits OTLP logs via demo-backend /api/action
#       - asserts both are in the DB and correlated on session_id
#   5. Tears down everything (kill processes, drop DB)
#
# Prerequisites:
#   - PostgreSQL reachable at postgres://datacat:datacat@localhost:55432/postgres
#   - cargo in PATH (for building/running Rust binaries)
#   - node 24+ in PATH
#
# Usage:
#   cd /path/to/datacat/examples
#   bash run-e2e.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BACKEND_DIR="$REPO_ROOT/backend"
DEMO_BACKEND_DIR="$SCRIPT_DIR/demo-backend"
E2E_DIR="$SCRIPT_DIR/e2e"
FIXTURE_PUB="$BACKEND_DIR/tests/fixtures/ed25519_public.pem"
FIXTURE_PRIV="$BACKEND_DIR/tests/fixtures/ed25519_private.pem"

# ---------------------------------------------------------------------------
# Ports & URLs
# ---------------------------------------------------------------------------
PG_ADMIN_URL="postgres://datacat:datacat@localhost:55432/postgres"
DEMO_DB="datacat_demo"
DEMO_DB_URL="postgres://datacat:datacat@localhost:55432/${DEMO_DB}"
DATACAT_PORT=8090
DEMO_PORT=8091
DATACAT_URL="http://127.0.0.1:${DATACAT_PORT}"
DEMO_URL="http://127.0.0.1:${DEMO_PORT}"

# ---------------------------------------------------------------------------
# Process tracking for cleanup
# ---------------------------------------------------------------------------
DATACAT_PID=""
DEMO_PID=""

cleanup() {
  echo ""
  echo "[teardown] Stopping processes…"
  if [[ -n "$DEMO_PID" ]]; then
    kill "$DEMO_PID" 2>/dev/null && echo "[teardown] demo-backend stopped" || true
  fi
  if [[ -n "$DATACAT_PID" ]]; then
    kill "$DATACAT_PID" 2>/dev/null && echo "[teardown] datacat-ingest stopped" || true
  fi
  # Give them a moment to die before dropping the DB
  sleep 1
  echo "[teardown] Dropping database $DEMO_DB…"
  cd "$E2E_DIR" && \
    PG_ADMIN_URL="$PG_ADMIN_URL" \
    DEMO_DB="$DEMO_DB" \
    node --experimental-strip-types db-admin.ts drop 2>/dev/null && \
    echo "[teardown] DB dropped" || echo "[teardown] DB drop skipped"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Create demo database
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 1: Create demo database ==="
cd "$E2E_DIR"
PG_ADMIN_URL="$PG_ADMIN_URL" \
DEMO_DB="$DEMO_DB" \
node --experimental-strip-types db-admin.ts create
echo "[ok] Database '${DEMO_DB}' ready"

# ---------------------------------------------------------------------------
# Step 2: Build Rust binaries
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: Build Rust binaries ==="
echo "[build] datacat-ingest…"
(cd "$BACKEND_DIR" && cargo build --bin datacat-ingest 2>&1) | grep -E '(Compiling|Finished|error|warning.*error)' | tail -5
echo "[build] demo-backend…"
(cd "$DEMO_BACKEND_DIR" && cargo build --bin demo-backend 2>&1) | grep -E '(Compiling|Finished|error|warning.*error)' | tail -5
echo "[ok] Rust binaries built"

# ---------------------------------------------------------------------------
# Step 3: Launch datacat-ingest
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Launch datacat-ingest on port ${DATACAT_PORT} ==="

DATABASE_URL="$DEMO_DB_URL" \
BIND_ADDR="0.0.0.0:${DATACAT_PORT}" \
TOKEN_ALG="EdDSA" \
TOKEN_PUBLIC_KEY_FILE="$FIXTURE_PUB" \
TOKEN_KID="2026-06-key-1" \
CORS_ALLOWED_ORIGINS="*" \
RUST_LOG="datacat_ingest=info,tower_http=warn" \
"$REPO_ROOT/target/debug/datacat-ingest" > /tmp/datacat-ingest.log 2>&1 &
DATACAT_PID=$!

# Wait for /readyz
echo "[wait] Waiting for datacat-ingest /readyz…"
for i in $(seq 1 60); do
  if curl -sf "${DATACAT_URL}/readyz" >/dev/null 2>&1; then
    echo "[ok] datacat-ingest is ready (attempt ${i})"
    break
  fi
  if ! kill -0 "$DATACAT_PID" 2>/dev/null; then
    echo "[error] datacat-ingest exited unexpectedly. Log:"
    cat /tmp/datacat-ingest.log
    exit 1
  fi
  if [[ $i -eq 60 ]]; then
    echo "[error] datacat-ingest did not become ready after 60s. Log:"
    cat /tmp/datacat-ingest.log
    exit 1
  fi
  sleep 1
done

# ---------------------------------------------------------------------------
# Step 4: Launch demo-backend
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Launch demo-backend on port ${DEMO_PORT} ==="

DATACAT_URL="$DATACAT_URL" \
PORT="$DEMO_PORT" \
SIGNING_KEY_FILE="$FIXTURE_PRIV" \
RUST_LOG="demo_backend=info,tower_http=warn" \
"$DEMO_BACKEND_DIR/target/debug/demo-backend" > /tmp/demo-backend.log 2>&1 &
DEMO_PID=$!

# Wait for demo-backend (poll /api/analytics-token)
echo "[wait] Waiting for demo-backend…"
for i in $(seq 1 30); do
  if curl -sf "${DEMO_URL}/api/analytics-token" >/dev/null 2>&1; then
    echo "[ok] demo-backend is ready (attempt ${i})"
    break
  fi
  if ! kill -0 "$DEMO_PID" 2>/dev/null; then
    echo "[error] demo-backend exited unexpectedly. Log:"
    cat /tmp/demo-backend.log
    exit 1
  fi
  if [[ $i -eq 30 ]]; then
    echo "[error] demo-backend did not become ready after 30s. Log:"
    cat /tmp/demo-backend.log
    exit 1
  fi
  sleep 1
done

# ---------------------------------------------------------------------------
# Step 5: Run e2e harness
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 5: Run e2e harness ==="
cd "$E2E_DIR"
DATACAT_URL="${DATACAT_URL}" \
DEMO_BACKEND_URL="${DEMO_URL}" \
DATABASE_URL="${DEMO_DB_URL}" \
node --experimental-strip-types harness.ts

echo ""
echo "========================================================"
echo "  E2E PASSED — events + logs correlated end-to-end!"
echo "========================================================"
