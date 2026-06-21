# Datacat Integration Example

This directory contains a complete end-to-end demonstration of the Datacat analytics
platform: a React web app emitting product events, a Rust consumer backend signing tokens
and emitting OTLP logs, and an automated test harness that proves both are stored and
correlated in the database.

## Architecture

```
┌────────────────────────┐        POST /v1/events (JWT)        ┌───────────────────┐
│   web-app (React/Vite) │ ─────────────────────────────────► │                   │
│   port 5173            │                                      │  datacat-ingest   │
│                        │  GET /api/analytics-token           │  (backend/)       │
│                        │ ──────────────┐                     │  port 8090        │
└────────────────────────┘              ▼                      │                   │
                               ┌────────────────────┐          │  /v1/events  ─►  │
                               │  demo-backend       │          │  /v1/logs    ─►  │
                               │  (Rust/Axum)        │          │                   │
                               │  port 8091          │          │  PostgreSQL       │
                               │                     │ POST /v1/logs (OTLP, JWT)   │
                               │  /api/action ───────────────────────────────────► │
                               └────────────────────┘          └───────────────────┘
```

**Correlation key: `session_id`**

The same `session_id` flows through:
- Every analytics event emitted by the SDK (`events.session_id`)
- Every OTLP log emitted by the backend (`logs.session_id`)

This enables the correlation join:

```sql
SELECT e.event_name, l.body, l.severity_text
FROM events e
JOIN logs l ON e.session_id = l.session_id
WHERE e.session_id = 'my-session';
```

## Components

### `demo-backend/` — Rust/Axum consumer backend

A standalone Rust crate (its own `[workspace]`) that acts as the "real application
backend" in this demo:

- `GET /api/analytics-token` — signs a short-lived EdDSA JWT (10 min) using the shared
  fixture private key. The React SDK calls this to obtain a token for event ingestion.
- `POST /api/action` — receives a business action, signs a service JWT, and **emits an
  OTLP log** to `datacat-ingest /v1/logs` with `session_id`, `actor_id`, and
  `tenant_id` attributes for correlation.

Config via env: `PORT` (8091), `DATACAT_URL` (http://127.0.0.1:8090),
`SIGNING_KEY_FILE` (path to Ed25519 PKCS#8 PEM).

### `web-app/` — React + Vite frontend

Demonstrates SDK integration in a real browser app:

- Initialises `createDatacatClient` with `getToken` pointing at the demo-backend.
- Calls `identify()` on mount.
- "Valider le planning" button: tracks `validate_planning` via the SDK **and** calls
  `/api/action` to generate a correlated OTLP log — both share the same `session_id`.
- Displays the current `session_id` for visual confirmation.

### `e2e/` — Node.js test harness

A headless integration test (Node 24, `--experimental-strip-types`) that exercises the
full stack without a browser:

1. Fetches a token from demo-backend.
2. Creates a `DatacatClient` with injectable `fetchImpl` (Node's global `fetch`) and an
   in-memory `StorageAdapter`, pre-seeded with a known `session_id`.
3. Tracks 3 events and flushes them.
4. Calls `/api/action` twice to generate OTLP logs with the same `session_id`.
5. Queries PostgreSQL directly and **asserts**:
   - `events` table has ≥ 3 rows for the session.
   - `logs` table has ≥ 2 rows for the session.
   - A `JOIN events ↔ logs ON session_id` returns ≥ 1 row.

## Quick start — automated e2e

Prerequisites:
- PostgreSQL reachable at `postgres://datacat:datacat@localhost:55432`
- `cargo` in PATH (Rust 1.80+)
- `node` 24+ in PATH

```bash
cd examples
bash run-e2e.sh
```

The script creates a fresh `datacat_demo` database, starts both services, runs the
harness, then tears everything down regardless of outcome.

## Manual run (web app)

```bash
# 1. Start datacat-ingest (from repo root)
DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat \
TOKEN_ALG=EdDSA \
TOKEN_PUBLIC_KEY_FILE=backend/tests/fixtures/ed25519_public.pem \
TOKEN_KID=2026-06-key-1 \
CORS_ALLOWED_ORIGINS='*' \
BIND_ADDR=0.0.0.0:8090 \
cargo run --bin datacat-ingest

# 2. Start demo-backend
cd examples/demo-backend
SIGNING_KEY_FILE=../../backend/tests/fixtures/ed25519_private.pem \
DATACAT_URL=http://127.0.0.1:8090 \
cargo run

# 3. Start the web app
cd examples/web-app
npm install
npm run dev
# Open http://localhost:5173
```

## What this proves

| Concern | Proof |
|---|---|
| JWT signing (EdDSA) works | demo-backend issues tokens accepted by datacat-ingest (no 401) |
| SDK event ingestion | `events` rows inserted for the session |
| OTLP log ingestion | `logs` rows inserted for the session |
| Correlation by `session_id` | `JOIN events ↔ logs ON session_id` returns ≥ 1 row |
| Token reuse (single key pair) | Both SDK events and OTLP logs use the same Ed25519 fixture key |

## Key files

```
examples/
  run-e2e.sh              # Orchestration + automated e2e
  demo-backend/
    src/main.rs           # Axum server: /api/analytics-token + /api/action
    Cargo.toml            # Standalone workspace
  web-app/
    src/App.tsx           # React UI, SDK integration
    vite.config.ts        # Vite config with env var pass-through
  e2e/
    harness.ts            # Node.js e2e test harness (no browser)
    db-admin.ts           # Helper: create/drop the demo database
    package.json
```
