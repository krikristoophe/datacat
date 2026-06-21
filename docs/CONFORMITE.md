# Conformance matrix — acceptance criteria (spec §12)

Each criterion is linked to its implementation and to its test proof.

| # | Criterion (§12) | Implementation | Proof |
|---|---|---|---|
| 1 | Write spike without loss beyond tolerance and **without duplicates**; idempotence verified (same `event_id` n×→ 1) | COPY + UNLOGGED staging + `ON CONFLICT DO NOTHING` merge, key `(timestamp_client, event_id)` (`ingest.rs`, `migrations/`) | Tests `write_spike_no_duplicates` (2000 uniques × 2 concurrent sends → 2000 rows), `same_event_id_counts_once`; binary smoke test (3 sends → `inserted=1, deduplicated=2`) |
| 2 | Writes via `COPY`, **partitioned** table, purge via `DROP PARTITION` without impacting writes | `copy_in_raw` CSV, `PARTITION BY RANGE (timestamp_client)`, `datacat_drop_partitions_before` | Tests `copy_persists_distinct_events`, `purge_drops_old_partition` (partition DROP, events purged) |
| 3 | Two SDKs, same contract: batching, idempotent retry, `tenant`+`actor`+`session`, token never hard-coded | `sdks/typescript/`, `sdks/flutter/` conform to `CONTRACT.md` | 26 vitest tests (TS) + 28 dart tests (Flutter), including idempotence with frozen event_id/timestamp, token renewal |
| 4 | Rate limiting at **both levels**: an abusive session is throttled without impacting the IP's other sessions; an IP cannot create an unreasonable number of sessions | `ratelimit.rs` (per-session token bucket + sliding window of sessions/IP + global safety net) | Unit tests (5) + integration `session_rate_limit_isolates_sessions`, `ip_session_cap_blocks_fake_session_flood` |
| 5 | Token verified by **asymmetric** signature (public key only); ingestion cannot forge it; issuance contract documented | `token.rs` (EdDSA/RS256, PEM/JWKS, `none`/symmetric rejected), `docs/token-contract.md` | Tests `token_is_required_and_verified`, `rs256_token_accepted`, `token_in_body_works_for_beacon` (401 if missing/invalid/expired) |
| 6 | Migrations present, schema rebuilt **reproducibly** | `backend/migrations/0001_schema.sql`, `0002_functions.sql`, applied at startup (`sqlx::migrate!`) | Binary smoke test + Docker container (migrations applied, `/readyz` ok), each integration test starts from a fresh database |
| 7 | Deployment **documented and simple** to reproduce | `docs/deployment.md`, `backend/Dockerfile`, `docker-compose.yml`, `.env.example` | Docker image built and **started** (boot → `/readyz` ready → POST 202) |
| 8 | Code **tested**, standards respected, **no boilerplate** | `#![forbid(unsafe_code)]`, typed errors, cohesive modules | `cargo test` (32), `cargo clippy --all-targets -- -D warnings` (0), `cargo fmt --check`; multi-job CI |
| 9 | No obvious flaw (HDS review): validation, public endpoint protection, traceability, TLS | Strict validation (`model.rs`), rate limit + ban (`security.rs`), CORS, JSON logs + request-id, rustls | `docs/security.md` (threat model + controls); tests `rejects_invalid_and_oversized`, `out_of_skew_event_dropped_not_rejected` |
| 10 | Ingestion / storage / read boundaries **decoupled** | Separate `ingest` / `db` modules, no dependency on a read layer; no read index in v1 | `docs/architecture.md` §7 (extension map without rewrite) |

## Verification commands

```bash
# Database
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

# Backend: standards + tests (unit + PostgreSQL integration)
cd backend
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test                      # 32 tests

# TypeScript SDK
cd ../sdks/typescript && npm install && npm run typecheck && npm test && npm run build

# Flutter/Dart SDK
cd ../sdks/flutter && dart pub get && dart analyze && dart test   # 28 tests

# Deployment
docker build -f backend/Dockerfile -t datacat-ingest .
```
