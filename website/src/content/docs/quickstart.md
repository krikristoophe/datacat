---
title: "Quickstart"
description: "Run Datacat locally end to end: PostgreSQL, the backend, a test event and a test OTLP log."
---

This guide takes you from an empty checkout to a running ingestion service that accepts a
product **event** and an OTLP **log** — in a few minutes, on a dev box. The only dependency is
PostgreSQL. For a production setup, see [installation](../installation/) and
[deployment](../deployment/).

## 1. Start PostgreSQL

The repository ships a `docker-compose.yml` with a single `postgres` service tuned for write
throughput (it is the only required dependency of the v1 ingestion service).

```bash
docker compose up -d postgres

# The compose file maps PostgreSQL to host port 55432.
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat
```

## 2. Create a config file

Copy the template and adjust it. Every secret is referenced from the environment with `${VAR}`,
so nothing sensitive is written in clear text.

```bash
cp datacat.example.toml datacat.toml
```

For a frictionless first run, relax token verification — but note this is **only** accepted by a
binary built with the `dev` Cargo feature (see [installation](../installation/)). In `datacat.toml`:

```toml
[database]
url = "${DATABASE_URL}"

[token]
enabled = false          # dev only — requires `--features dev`

[auth.logs]
mode = "static"
static_token = "${LOGS_STATIC_TOKEN:-dev-logs-token}"
```

:::note
If you skip the config file entirely, Datacat falls back to its legacy environment-variable
configuration (`BIND_ADDR`, `DATABASE_URL`, …), which is enough for development. See
[configuration](../configuration/) for the resolution order (`$DATACAT_CONFIG`, then
`./datacat.toml`, then `/etc/datacat/datacat.toml`).
:::

## 3. Run the backend

Migrations are embedded in the binary and applied automatically at startup, so there is no manual
schema step.

```bash
cd backend
cargo run --features dev          # listens on :8080 by default
```

Check it is alive:

```bash
curl -s http://localhost:8080/healthz        # liveness
curl -s http://localhost:8080/readyz         # readiness (DB reachable)
```

## 4. Send a test event

Events go to `POST /v1/events` as a **batch** (`{ "events": [ ... ] }`), with the ingestion JWT in
the `Authorization` header. With `[token].enabled = false` (dev), any bearer is accepted, so you
can send a placeholder:

```bash
curl -s -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d '{
    "events": [
      {
        "event_id":         "550e8400-e29b-41d4-a716-446655440000",
        "event_name":       "validate_planning",
        "tenant_id":        "clinic-42",
        "actor_id":         "user-123",
        "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f",
        "timestamp_client": "2026-06-21T10:15:30.123Z",
        "properties":       { "planning_id": 42 }
      }
    ]
  }'
```

The API acknowledges immediately:

```json
202 Accepted
{ "received": 1 }
```

`received` is the number of events accepted for asynchronous writing — **not** the number
inserted. Deduplication happens in the database: re-sending the same `event_id` is silently
ignored (`ON CONFLICT DO NOTHING`). See the [contract](../contract/) for the full wire format.

In production the JWT is **not** a placeholder: it is a short-lived token signed by your
authenticated backend and verified by Datacat with the public key only. See
[token](../token/).

## 5. Send a test OTLP log

Technical logs use the standard **OTLP/HTTP (JSON)** format on `POST /v1/logs`, authenticated with
the **static service token** (`[auth.logs]`). The body is an OTLP `ExportLogsServiceRequest`:

```bash
curl -s -X POST http://localhost:8080/v1/logs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceLogs": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "demo-api" } }
      ]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1718900000000000000",
          "severityText": "INFO",
          "body": { "stringValue": "hello from quickstart" }
        }]
      }]
    }]
  }'
```

The response is an OTLP `ExportLogsServiceResponse` (empty on full success). The same endpoint
accepts logs from any OpenTelemetry SDK or Collector — see [OTLP logs](../otel-logs/) and, for
shipping container logs and metrics, [Logs & metrics with Docker](../docker-telemetry/).

## 6. Inspect what landed

`GET /stats` exposes per-domain counters (received, inserted, deduplicated, dropped):

```bash
curl -s http://localhost:8080/stats
```

## Next steps

- [Installation](../installation/) — release builds, the `dev`/`export` features, TLS.
- [SDKs](../sdks/) — send events from a web (TypeScript) or mobile (Flutter) app.
- [Logs & metrics with Docker](../docker-telemetry/) — ship container telemetry with an OTel Collector.
- [Token](../token/) — issue real, short-lived ingestion tokens from your backend.
