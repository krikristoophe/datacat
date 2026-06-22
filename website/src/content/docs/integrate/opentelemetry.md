---
title: Integrate OpenTelemetry
description: Point your existing OpenTelemetry SDK or Collector at Datacat — logs, traces and metrics over OTLP, no re-instrumentation.
---

Datacat speaks **OTLP** natively, over HTTP (JSON) and gRPC, on `/v1/logs`, `/v1/traces` and
`/v1/metrics`. If your services are already instrumented with OpenTelemetry, you don't change a
line of instrumentation — you just add Datacat as an export target. Telemetry authenticates with the
static **service token** (`[auth.logs]`).

## Option A — from an OpenTelemetry SDK

Set the standard OTLP environment variables and your app exports straight to Datacat:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.example.com
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer ${DATACAT_SERVICE_TOKEN}
OTEL_EXPORTER_OTLP_PROTOCOL=http/json   # or grpc
```

The SDK appends `/v1/logs`, `/v1/traces`, `/v1/metrics` to the endpoint automatically.

## Option B — from the OpenTelemetry Collector

Add an `otlphttp` (or `otlp` for gRPC) exporter pointing at Datacat:

```yaml
exporters:
  otlphttp/datacat:
    endpoint: https://ingest.example.com
    headers:
      authorization: "Bearer ${DATACAT_SERVICE_TOKEN}"

service:
  pipelines:
    logs:    { exporters: [otlphttp/datacat] }
    traces:  { exporters: [otlphttp/datacat] }
    metrics: { exporters: [otlphttp/datacat] }
```

This is the recommended path for shipping **container logs and host metrics** — see
[Logs & metrics with Docker](../../docker-telemetry/) for Compose and Swarm setups.

## Correlate with product events

Datacat lifts a few attributes off your resource/records to correlate telemetry with product
events and across signals. Set them where you can:

- `service.name` — groups telemetry by service.
- `tenant_id`, `actor_id`, `session_id` — link a log/span/metric to a tenant, user and session.
- `trace_id` / `span_id` — already standard on spans; logs carrying the same `trace_id` are linked.

## What is ingested

Logs, spans, and metric **gauges / sums / histograms** are stored. `summary` and
`exponentialHistogram` metric points are not ingested (documented). Each record is bounded by a
per-record size cap; oversized records are dropped (tolerated loss), never the whole request.

## Next steps

- [Logs & metrics with Docker](../../docker-telemetry/) — Collector on Compose & Swarm.
- Reference: [OTLP logs](../../otel-logs/) · [metrics](../../otel-metrics/) · [traces](../../traces/).
