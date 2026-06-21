---
title: "OTLP Logs"
description: "Ingesting OpenTelemetry technical logs into Datacat."
---

Datacat ingests **technical logs** in the **OpenTelemetry / OTLP-HTTP (JSON)** format, on the
same foundation as product events: a table partitioned by day, idempotent, written with `COPY`.
Goal (spec §4.2, §9): **link** product events and technical logs via `tenant_id` / `actor_id` /
`session_id`, and to traces via `trace_id` / `span_id`.

## 1. Transports

Datacat accepts logs over **two transports**, both standard (drop-in for an OpenTelemetry SDK or
a Collector):

| Transport | Endpoint | Activation |
|---|---|---|
| **OTLP/HTTP (JSON)** | `POST /v1/logs` | always on (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`) |
| **OTLP/gRPC** | `LogsService/Export` service on `:4317` | `[server.grpc].enabled = true` (port `[server.grpc].bind_addr`) |

Body: an OTLP `ExportLogsServiceRequest`. Response: `ExportLogsServiceResponse` (empty, or
`partialSuccess` if some records were dropped under back-pressure). Both transports share
**exactly** the same admission logic (auth, rate limit, correlation, dedup) and produce the same
`log_id` for identical content.

## 1.1 Authentication (service token, fixed)

Unlike events (web/mobile front end, short-lived per-session JWT because a client cannot hold a
secret), logs are emitted **service-to-service**: a trusted backend **can** hold a secret. Log
auth is therefore, by default, a **fixed service token**.

Modes (`[auth.logs].mode`):

| Mode | Behaviour |
|---|---|
| `static` (**recommended**) | `Authorization: Bearer <static_token>` header/metadata, compared in **constant time**. The token is fixed (config of the emitting service), rotated by changing its value. |
| `jwt` | JWT verification by public key (a **long-lived** service token signed asymmetrically) — useful to share the events' key infrastructure. |
| `none` | no auth (endpoint on an internal network / mTLS terminated at the proxy). |
| `auto` (default) | `static` if `static_token` is set, otherwise `jwt` if token verification is enabled, otherwise `none`. |

The static token is configured as `[auth.logs].static_token = "${LOGS_STATIC_TOKEN:-}"`.

The token (static or JWT) also serves indirectly as a filter; log rate limiting, on the other
hand, is keyed on `service.name` (the trusted source for service-to-service logs).

## 2. Data model

Each OTLP `LogRecord` is flattened into the `logs` table:

| Column | OTLP source |
|---|---|
| `log_id` | **deterministic hash** of the content (dedup of resends — OTLP has no native id) |
| `log_time` | `timeUnixNano` (or `observedTimeUnixNano`, or reception time). **Partition key.** |
| `observed_time` | `observedTimeUnixNano` |
| `severity_number` / `severity_text` | `severityNumber` / `severityText` |
| `body` | `body` (flattened to text) |
| `service_name` | resource attribute `service.name` |
| `scope_name` | `scope.name` |
| `trace_id` / `span_id` | `traceId` / `spanId` (correlation to traces) |
| `tenant_id` / `actor_id` / `session_id` | attributes (log then resource) — **correlation to events** |
| `resource_attributes` / `log_attributes` | full attributes (JSONB) |

### Idempotency

As for events, idempotency relies on `(log_time, log_id)`:
- `log_time` is carried by the record (stable across two identical exports) → the partition key
  is stable;
- `log_id` = `SHA-256(log_time, service, body, trace_id, span_id, severity, attributes)`
  truncated to a UUID → two identical exports (an OTLP exporter retry) produce the same id, hence
  a single row (`ON CONFLICT DO NOTHING`).

### Correlation keys

The keys looked up in the attributes (log then resource), to link logs and events:
- tenant: `tenant_id`, `tenant.id`, `tenant`
- actor: `actor_id`, `actor.id`, `user.id`, `enduser.id`, `user_id`
- session: `session_id`, `session.id`, `session`

Example join (the heart of the future debugging need):

```sql
SELECT e.event_name, l.body, l.severity_text
FROM events e
JOIN logs l ON e.session_id = l.session_id
WHERE e.session_id = 'sess-abc'
ORDER BY e.timestamp_client;
```

## 3. Emitting logs to Datacat

### From an OpenTelemetry-instrumented backend

Configure the OTLP exporter to point at Datacat, add the correlation attributes (`session_id`,
`actor_id`, `tenant_id`) on the logs (or the resource), and attach the **fixed service token**
via the `Authorization` header/metadata.

HTTP/JSON:
```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.example.com/v1/logs
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<LOGS_STATIC_TOKEN>
```

gRPC (port 4317, if `[server.grpc].enabled = true` on the Datacat side):
```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.example.com:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<LOGS_STATIC_TOKEN>
```

A full example (Rust backend + React app) is provided under `examples/` (see its README).

## 4. Bounds & security

- `MAX_LOGS_RECORDS` (default 2048): max number of `LogRecord` per request.
- `MAX_LOGS_PAYLOAD_BYTES` (default 4 MiB): max size of the OTLP body (dedicated route).
- Same guardrails as events: token (public key), rate limiting, IP banning, skew window (logs
  outside the window are dropped, tolerated loss), JSON validation.

## 5. Related streams

OTLP **traces** ([traces](../traces/)) and **metrics** ([metrics](../otel-metrics/)) are ingested by
the same generic mechanism (`Ingestable` + an idempotent partitioned table), correlated with logs
through the shared `trace_id` / `service_name`. Cold analytical reads over Parquet are described in
[cold reads](../read-cold/).
