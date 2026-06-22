# Datacat

A **lightweight, self-hosted analytics & observability** platform built on **PostgreSQL alone**.
Datacat ingests product **events** and OpenTelemetry **logs, traces and metrics**, stores them
idempotently, and exposes them through a hot read layer, a cold Parquet export, a modular alerting
engine and an embedded MCP server — with a strong, auditable (HDS-grade) security posture.

📖 **Documentation: https://krikristoophe.github.io/datacat/** (bilingual EN/FR) — source in
[`docs/`](docs/).

## Principles

- **PostgreSQL only** as the central store — no Kafka, ClickHouse or Zookeeper.
- **Strict idempotence** — a given `event_id` (or log/span/metric identity) counts exactly once,
  even across retries (`ON CONFLICT DO NOTHING`).
- **Write throughput first** — `COPY` into an `UNLOGGED` staging table, micro-batching,
  time-partitioned tables, partition-drop purging.
- **Light now, scalable later** — clean boundaries between ingestion, storage and reads.
- **Auditable (HDS)** — strict input validation, a public endpoint defended server-side,
  asymmetric token verification (public key only), TLS (rustls), secrets kept in the environment.
- **Tolerant to losing a tiny fraction of events — never to duplicates.**

## Components

| Folder | Description |
|---|---|
| [`backend/`](backend/) | **Axum** (Rust) ingestion API: events + OTLP telemetry (logs/traces/metrics, HTTP + gRPC), hot read layer, alerting, embedded **MCP** server (`/mcp`), scheduled cold export, `sqlx` migrations + tests |
| [`sdks/typescript/`](sdks/typescript/) | Web analytics SDK (TypeScript) |
| [`sdks/flutter/`](sdks/flutter/) | Mobile analytics SDK (Dart / Flutter) |
| [`exporter/`](exporter/) | Cold export PostgreSQL → **Parquet** on S3 (standalone crate; also embedded & scheduled in the backend) |
| [`reader/`](reader/) | Cold analytical reads with **DataFusion** over Parquet on S3 (standalone crate) |
| [`website/`](website/) | Bilingual documentation site (Astro Starlight) |
| [`examples/`](examples/) | Integration mini-project: a demo Rust backend + a React app |

## Capabilities

- **Ingestion** — product events (`POST /v1/events`) and OTLP logs/traces/metrics over HTTP and
  gRPC, all idempotent and time-partitioned, correlated by `session_id` / `actor_id` / `tenant_id`
  / `trace_id`.
- **Read layer** — hot queries over PostgreSQL (`/v1/query/*`: logs, events, traces, journeys,
  metrics) and cold analytical queries over Parquet via the `reader` crate.
- **Alerting** — declarative, **per-project** rules: log/error-rate thresholds, latency percentiles
  (p95/p99), heartbeats, spikes, composite (AND/OR) conditions, new-error detection and statistical
  anomalies — routed to Slack, e-mail or generic webhooks.
- **Cold export** — scheduled PostgreSQL → Parquet/S3 export, idempotent per day.
- **MCP** — an embedded HTTP MCP server exposing the read layer to an agent (e.g. Claude) for
  debugging and test generation.

## Configuration

Everything is configured through a single **TOML file** (`datacat.toml`, template
[`datacat.example.toml`](datacat.example.toml)) plus **one file per project** under
[`projects/`](projects/) (alerting rules + notification channels + a default service/tenant filter).
**Secrets are referenced from the environment** with `${VAR}` / `${VAR:-default}` and never written
in clear text. See [docs/configuration.md](docs/configuration.md).

## Quick start

```bash
# 1. PostgreSQL
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

# 2. Configure
cp datacat.example.toml datacat.toml   # then edit; secrets come from the environment

# 3. Backend (migrations applied on startup)
cd backend
cargo run            # listens on :8080 by default
```

Application integration: [`docs/integration.md`](docs/integration.md) ·
Deployment: [`docs/deployment.md`](docs/deployment.md) ·
Token contract: [`docs/token-contract.md`](docs/token-contract.md) ·
Security posture: [`docs/security.md`](docs/security.md) ·
Security review: [`docs/security-review.md`](docs/security-review.md) ·
Changelog: [`CHANGELOG.md`](CHANGELOG.md).

## Development

```bash
cd backend && cargo test && cargo clippy --all-targets --all-features -- -D warnings && cargo fmt --check
cd sdks/typescript && npm install && npm test && npm run build
cd sdks/flutter && dart pub get && dart test
cd website && npm install && npm run build
```

Contribution guide (for agents & humans): [`CLAUDE.md`](CLAUDE.md).

## License

Proprietary — internal use.
