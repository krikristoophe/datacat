---
title: "OTLP traces"
description: "Ingesting OpenTelemetry traces and correlating them with logs."
---

Datacat ingests distributed traces in the **OpenTelemetry (OTLP)** format, over **HTTP** (`POST
/v1/traces`) and **gRPC** (`TraceService`). Spans are stored in PostgreSQL alongside logs, events
and metrics, and **correlated** with logs through the shared `trace_id`.

Like every other Datacat stream, trace ingestion is **strictly idempotent**: a span is keyed by
`(start_time, trace_id, span_id)` and merged with `ON CONFLICT DO NOTHING`, so retries never create
duplicates.

## Endpoints

| Transport | Endpoint | Notes |
|---|---|---|
| HTTP | `POST /v1/traces` | OTLP/JSON or OTLP/protobuf body (`ExportTraceServiceRequest`) |
| gRPC | `TraceService/Export` | enabled with `[server.grpc].enabled = true` |

Both are authenticated **service-to-service** by `[auth.logs]` (the telemetry auth shared by logs,
traces and metrics) — see [configuration](../configuration/). A fixed service token (`mode =
"static"`) is recommended for trusted backends.

## Storage model

Spans live in the partitioned `spans` table (one partition per day on `start_time`):

| Column | Meaning |
|---|---|
| `trace_id`, `span_id`, `parent_span_id` | OTel identifiers (hex) |
| `start_time`, `end_time`, `duration_ms` | span timing (`start_time` is the partition key) |
| `name`, `kind` | operation name and OTel span kind |
| `service_name`, `scope_name` | resource / instrumentation scope |
| `status_code`, `status_message` | OTel status (`2` = error) |
| `tenant_id`, `actor_id`, `session_id` | correlation keys (shared with events/logs) |
| `resource_attributes`, `span_attributes` | JSONB attribute bags |
| `events`, `links` | JSONB span events and links |

## Correlation with logs

A log record carrying a `trace_id` (and optionally `span_id`) is linked to its span, so you can
pivot from a log line to the full trace, or list every log emitted during a request:

- `GET /v1/query/traces/{trace_id}` returns all spans of a trace, ordered by `start_time`.
- `GET /v1/query/logs?trace_id=…` returns the logs attached to a trace.

See [read (hot)](../read-hot/) for the read endpoints and [MCP](../mcp/) for the same data exposed
to an agent through the `get_trace` tool.

## Alerting on traces

The alerting engine can target spans directly (`source = "spans"`):

- `span_duration` — latency aggregate (`avg`/`max`/`p50`…`p99`) of `duration_ms`, filterable by
  `service`, `operation` and `error_only`. Example: "p99 of `checkout` > 2 s".
- `error_ratio` with `source = "spans"` — fraction of spans whose `status_code = 2` (error).

See [alerting](../alerting/) for the full rule schema.
