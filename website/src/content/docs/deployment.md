---
title: "Deployment"
description: "Deploying and operating Datacat in production."
---

Goal: a **simple and reproducible** deployment. The only v1 dependency is **PostgreSQL**.
Migrations are **embedded in the binary** and applied automatically at startup.

## 1. Prerequisites

- PostgreSQL **14+** (tested on 17), reachable via the database URL.
- A reverse proxy terminating **TLS** (nginx, Caddy, Traefik, ALB…) in front of the service.
- The token verification **public key** (PEM or JWKS) — see [token](../token/).

## 2. Configuration

The whole deployment is described by a single **TOML file**, `datacat.toml`. It is resolved at
startup in this order:

1. `$DATACAT_CONFIG` (explicit path),
2. `./datacat.toml` (current directory),
3. `/etc/datacat/datacat.toml`.

Copy `datacat.example.toml` and adjust it. Only `[database].url` is **required**; every other
section is optional and falls back to safe defaults. See [configuration](../configuration/) for
the full reference.

Secrets are **never** written in clear text: any string value can reference an environment
variable with `${VAR}` (or `${VAR:-default}`), resolved at startup. A required `${VAR}` with no
default makes the service refuse to start (fail-closed) — an HDS requirement.

> **Development fallback.** If **no** `datacat.toml` is found, Datacat falls back to the legacy
> environment-variable configuration (`BIND_ADDR`, `DATABASE_URL`, …; see `.env.example`). This
> path is meant for development and the test suite; production deployments should use the TOML
> file.

The main sections:

| Section | Role |
|---|---|
| `[server]` | `bind_addr` (default `0.0.0.0:8080`), `request_timeout`, `trust_forwarded_for`; `[server.grpc]` (OTLP/gRPC), `[server.cors]` (origin allow-list — do not leave `["*"]` in production) |
| `[database]` | `url` (**required**), `max_connections` |
| `[ingest]` | micro-batch (`flush_interval`, `flush_batch_size`, `channel_capacity`), `retention_days`, `partition_future_days`; `[ingest.limits]`, `[ingest.rate_limit]`, `[ingest.anomaly]` |
| `[token]` | asymmetric token verification (public key only): `enabled`, `algorithms`, key source (`jwks_url` \| `public_key_pem` \| `public_key_file`), `alg`, `issuer`, `audience` |
| `[auth.logs]` / `[auth.query]` | service-to-service auth for telemetry ingestion and read endpoints: `mode` (`auto`\|`static`\|`jwt`\|`none`) + `static_token` |
| `[mcp]` | embedded MCP HTTP server (`enabled`) |
| `[export]` | embedded scheduled cold export (see §10) |
| `[notifications]` | global default Slack / e-mail channels (fallback for projects) |
| `[projects]` | where to load per-project files (`dir` and/or `files`) |

Essential keys in production:

```toml
[server]
bind_addr = "0.0.0.0:8080"
trust_forwarded_for = false          # true ONLY behind a single trusted proxy

[server.cors]
allowed_origins = ["https://app.example.com"]   # never ["*"] in production

[database]
url = "${DATABASE_URL}"
max_connections = 10

[ingest]
retention_days = 90                  # retention window (purge via DROP PARTITION)

[token]
enabled = true
algorithms = ["EdDSA", "RS256"]
alg = "EdDSA"
public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"
# or: public_key_file = "/etc/datacat/ingest_pub.pem"
# or: jwks_url = "https://issuer.example.com/.well-known/jwks.json"
```

Exactly one public-key source is used, in priority order: `jwks_url`, then `public_key_pem`, then
`public_key_file`. With `[token].enabled = true` and no source, startup fails.

## 3. Build & run

### With Docker (recommended)

```bash
# Build the image (from the repository root)
docker build -f backend/Dockerfile -t datacat-ingest:latest .

# Run, mounting the config and passing secrets via the environment
docker run --rm -p 8080:8080 \
  -v /etc/datacat/datacat.toml:/etc/datacat/datacat.toml:ro \
  -e DATABASE_URL='postgres://datacat:datacat@db:5432/datacat' \
  -e TOKEN_PUBLIC_KEY_PEM="$(cat ingest_pub.pem)" \
  datacat-ingest:latest
```

The runtime image is minimal (`debian:bookworm-slim` + binary + CA certs) and runs as a non-root
user. TLS is provided by rustls (no OpenSSL).

### Without Docker

```bash
export DATACAT_CONFIG=/etc/datacat/datacat.toml
export DATABASE_URL='postgres://…'
export TOKEN_PUBLIC_KEY_PEM="$(cat /etc/datacat/ingest_pub.pem)"
cargo run --release --bin datacat-ingest
```

## 4. Migrations

No manual step: at startup the service applies the versioned migrations from
`backend/migrations/` (embedded via `sqlx::migrate!`). The schema therefore rebuilds
**reproducibly** from a clean checkout. To apply/inspect them manually:

```bash
cd backend
export DATABASE_URL='postgres://…'
sqlx migrate run        # requires sqlx-cli
sqlx migrate info
```

## 5. Health & observability

| Endpoint | Use |
|---|---|
| `GET /healthz` | **liveness** (the process responds) |
| `GET /readyz` | **readiness** (process ready + database reachable) — for k8s/LB probes |
| `GET /stats` | counters: received, inserted, deduplicated, dropped (skew/saturation), bans, etc. |

Structured **JSON logs** (`LOG_FORMAT=json`), level via `RUST_LOG`, with `x-request-id` propagated
(traceability §7.4).

## 6. Retention

At startup and then every hour, the service:
- creates the upcoming daily partitions (plus those covering the past skew window);
- purges partitions older than `[ingest].retention_days` via `DROP PARTITION` (instantaneous).

## 7. Sizing & PostgreSQL

- Set `[database].max_connections` according to PostgreSQL's `max_connections`.
- `synchronous_commit=off` (cf. `docker-compose.yml`) increases write throughput by accepting the
  loss of the very latest transactions on a crash — consistent with the §2 tolerance.
- The staging table is `UNLOGGED` (no WAL).

## 8. Graceful shutdown

On `SIGTERM`/`Ctrl-C`, the service stops accepting, **flushes** the in-flight micro-batch, then
closes the pool. Allow a sufficient `terminationGracePeriod` (a few seconds).

## 9. Scalability (beyond v1)

Adding cold storage, analytical reads, or a distributed write buffer (Citus/Redpanda) later does
not impact the ingestion core (decoupled boundaries — cf. architecture §7). None of these building
blocks are deployed in v1.

## 10. Embedded scheduled export

When the `export` Cargo feature is compiled (on by default) and `[export].enabled = true`, the
backend runs a background task that exports the **previous UTC day** to Parquet on
S3-compatible storage on each tick. Secrets (S3 credentials, endpoint) are passed via `${ENV}`:

```toml
[export]
enabled = true
schedule = "24h"
bucket = "datacat-cold"
region = "eu-west-1"
endpoint = "${S3_ENDPOINT:-}"           # empty = AWS S3; set for MinIO/compatible
access_key_id = "${AWS_ACCESS_KEY_ID:-}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY:-}"
allow_http = false
tables = ["events", "logs"]
```

The export is idempotent (re-running a day overwrites its object). The same logic is also
available as a standalone CLI — see [cold storage](../cold-storage/).
