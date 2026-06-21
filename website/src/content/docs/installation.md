---
title: "Installation"
description: "Build the Datacat backend, resolve its config file, and run it behind TLS in production."
---

This page covers building and installing the ingestion backend for a real deployment. For a quick
local run, see [quickstart](../quickstart/); for the operational details (health probes, retention,
graceful shutdown), see [deployment](../deployment/).

## 1. Build the backend

The backend is a single Rust crate that produces the `datacat-ingest` binary. Build it in release
mode from the repository root:

```bash
cd backend
cargo build --release          # target/release/datacat-ingest
```

Migrations are **embedded** in the binary (`sqlx::migrate!`) and applied automatically at startup,
so the schema rebuilds reproducibly from a clean checkout — there is no separate migration step to
run before first boot.

The crate also builds a dev-only `mint-dev-token` binary used to forge ingestion tokens for tests;
it is **never** deployed in production (token issuance is out of scope — see [token](../token/)).

## 2. Config file resolution

The whole deployment is described by a single `datacat.toml`. At startup it is looked up in this
order:

1. `$DATACAT_CONFIG` — an explicit path (recommended in production);
2. `./datacat.toml` — the current directory;
3. `/etc/datacat/datacat.toml`.

If **no** file is found, Datacat falls back to the legacy environment-variable configuration
(`BIND_ADDR`, `DATABASE_URL`, …), which is convenient for development and the test suite but not
intended for production. Pin the path explicitly:

```bash
export DATACAT_CONFIG=/etc/datacat/datacat.toml
export DATABASE_URL='postgres://…'
export TOKEN_PUBLIC_KEY_PEM="$(cat /etc/datacat/ingest_pub.pem)"
./target/release/datacat-ingest
```

Only `[database].url` is required; every other section falls back to safe defaults. Secrets are
referenced from the environment with `${VAR}` (or `${VAR:-default}`) and resolved at startup — a
required `${VAR}` with no default makes the service refuse to start (fail-closed). See
[configuration](../configuration/) for the full reference.

## 3. TLS and reverse proxy

Datacat is a public, exposed endpoint and **must** sit behind TLS. Terminate TLS at a reverse proxy
(nginx, Caddy, Traefik, an ALB…) in front of the service.

- Bind the backend to an internal address (default `0.0.0.0:8080`) and let the proxy front it.
- Set `[server].trust_forwarded_for = true` **only** when behind a single trusted proxy, so the
  real client IP is read from `X-Forwarded-For` (rate limiting and IP bans depend on it).
- Restrict CORS to your real origins in `[server.cors].allowed_origins` — never leave `["*"]` (see
  §4).

The runtime image (`backend/Dockerfile`) is minimal (`debian:bookworm-slim` + binary + CA certs),
runs as non-root, and uses rustls (no OpenSSL). See [deployment](../deployment/) for the Docker
build/run commands and the `/healthz`, `/readyz`, `/stats` probes.

## 4. The `dev` Cargo feature (production guardrails)

Two relaxations are dangerous in production and are therefore **refused** unless the binary is built
with the `dev` Cargo feature (which is off by default):

| Relaxation | Without `dev` | With `--features dev` |
|---|---|---|
| `[server.cors].allowed_origins = ["*"]` (wildcard CORS) | startup **fails** | allowed |
| `[token].enabled = false` (no token verification) | startup **fails** | allowed |

```bash
# Production build: guardrails active (default features).
cargo build --release

# Local/dev build: wildcard CORS and disabled token are permitted.
cargo run --features dev
```

This makes an unsafe production configuration **impossible to start by accident**: a release binary
will not boot with an open CORS policy or token verification turned off.

## 5. The optional `export` feature

The cold-export pipeline (PostgreSQL → Parquet on S3) is gated behind the `export` Cargo feature,
which is **on by default** (`default = ["export"]`). When compiled and `[export].enabled = true`,
the backend runs a scheduled background task that exports the previous UTC day on each tick.

```bash
# Default build includes export.
cargo build --release

# Slim build without the Arrow/Parquet/S3 dependencies:
cargo build --release --no-default-features
```

The same logic is also available as a standalone CLI (the `exporter/` crate). See
[configuration](../configuration/) §5 and [cold storage](../cold-storage/) for the on-disk layout.

## Next steps

- [Deployment](../deployment/) — Docker, migrations, health probes, retention, graceful shutdown.
- [Configuration](../configuration/) — the full `datacat.toml` reference.
- [Security](../security/) — the HDS audit posture and controls.
