#!/usr/bin/env bash
# run-tests.sh — Orchestrate the e2e test for datacat-exporter.
#
# What this script does:
#   1. Starts a MinIO container (datacat-minio-test) on ports 9100/9101.
#   2. Waits for MinIO to be healthy.
#   3. Runs `cargo test -- --nocapture` (which manages its own PG DB internally).
#   4. Tears down the MinIO container regardless of test outcome.
#
# Prerequisites:
#   - Docker daemon running
#   - PostgreSQL available at localhost:55432 (user/pass/db = datacat)
#     (same as the project dev DB — the test creates/drops its own DB datacat_export_test)
#
# Usage:
#   cd exporter && ./run-tests.sh

set -euo pipefail

CONTAINER="datacat-minio-test"
MINIO_API_PORT=9100
MINIO_CONSOLE_PORT=9101
MINIO_IMAGE="minio/minio:latest"

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[0;33m"
NC="\033[0m"

info()    { echo -e "${GREEN}[run-tests]${NC} $*"; }
warn()    { echo -e "${YELLOW}[run-tests]${NC} $*"; }
error()   { echo -e "${RED}[run-tests]${NC} $*" >&2; }

# ── Cleanup on exit ───────────────────────────────────────────────────────────
cleanup() {
    info "Stopping MinIO container…"
    docker rm -f "$CONTAINER" 2>/dev/null || true
    info "Done."
}
trap cleanup EXIT

# ── Start MinIO ───────────────────────────────────────────────────────────────
info "Starting MinIO container '$CONTAINER'…"

# Remove stale container if it exists
docker rm -f "$CONTAINER" 2>/dev/null || true

docker run -d \
    --name "$CONTAINER" \
    -p "${MINIO_API_PORT}:9000" \
    -p "${MINIO_CONSOLE_PORT}:9001" \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    "$MINIO_IMAGE" \
    server /data --console-address ":9001"

info "Waiting for MinIO to become healthy…"
MAX_WAIT=30
WAITED=0
until curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; do
    if [ "$WAITED" -ge "$MAX_WAIT" ]; then
        error "MinIO did not become healthy within ${MAX_WAIT}s"
        docker logs "$CONTAINER"
        exit 1
    fi
    sleep 1
    WAITED=$((WAITED + 1))
done
info "MinIO is healthy (waited ${WAITED}s)."

# ── Run tests ─────────────────────────────────────────────────────────────────
info "Running cargo test (e2e + unit)…"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$SCRIPT_DIR"

DATABASE_URL="postgres://datacat:datacat@localhost:55432/datacat" \
S3_ENDPOINT="http://localhost:${MINIO_API_PORT}" \
S3_REGION="us-east-1" \
S3_BUCKET="datacat-test" \
AWS_ACCESS_KEY_ID="minioadmin" \
AWS_SECRET_ACCESS_KEY="minioadmin" \
S3_ALLOW_HTTP="true" \
cargo test -- --nocapture

TEST_EXIT=$?

if [ "$TEST_EXIT" -eq 0 ]; then
    info "All tests PASSED."
else
    error "Tests FAILED (exit code $TEST_EXIT)."
    exit "$TEST_EXIT"
fi
