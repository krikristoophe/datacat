---
title: "Tutorial: instrument a service with OTLP"
description: "Send OpenTelemetry logs, traces and metrics to Datacat over OTLP/HTTP, and correlate them with product events."
---

Datacat ingests the three OpenTelemetry signals — **logs**, **traces** and **metrics** — on the
same service, over OTLP/HTTP (JSON) and OTLP/gRPC. This tutorial ships one of each by hand so you
see the exact wire format, then points you at the real OpenTelemetry SDKs and Collector.

It assumes a running backend from [Track your first event](../first-event/). Telemetry endpoints
authenticate with the **static service token** (`[auth.logs]`), not the product JWT — here it's
`dev-logs-token` (or any string when running `--features dev`).

## 1. Ship a log

```bash
curl -s -X POST http://localhost:8080/v1/logs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceLogs": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } },
        { "key": "tenant_id",    "value": { "stringValue": "clinic-7" } }
      ]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1750586130000000000",
          "severityText": "ERROR",
          "body": { "stringValue": "payment gateway timeout" },
          "traceId": "5b8efff798038103d269b633813fc60c",
          "attributes": [
            { "key": "session_id", "value": { "stringValue": "sess-1" } }
          ]
        }]
      }]
    }]
  }'
```

The response is an OTLP `ExportLogsServiceResponse` (empty on full success). Datacat flattens each
record, extracts `service.name`, and lifts `tenant_id` / `actor_id` / `session_id` / `trace_id`
from attributes for correlation.

## 2. Ship a trace

Spans use `POST /v1/traces`. The shared `traceId` is what later links this span to the log above.

```bash
curl -s -X POST http://localhost:8080/v1/traces \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceSpans": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } }
      ]},
      "scopeSpans": [{
        "spans": [{
          "traceId": "5b8efff798038103d269b633813fc60c",
          "spanId":  "eee19b7ec3c1b174",
          "name":    "POST /checkout",
          "kind": 2,
          "startTimeUnixNano": "1750586129000000000",
          "endTimeUnixNano":   "1750586130000000000",
          "status": { "code": 2, "message": "gateway timeout" }
        }]
      }]
    }]
  }'
```

## 3. Ship a metric

Gauges and sums use `NumberDataPoint`; histograms carry buckets. Post to `POST /v1/metrics`:

```bash
curl -s -X POST http://localhost:8080/v1/metrics \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceMetrics": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } }
      ]},
      "scopeMetrics": [{
        "metrics": [{
          "name": "http.server.duration",
          "unit": "ms",
          "histogram": { "dataPoints": [{
            "timeUnixNano": "1750586130000000000",
            "count": "3", "sum": 1850.0,
            "bucketCounts": ["1","1","1"],
            "explicitBounds": [100.0, 500.0]
          }] }
        }]
      }]
    }]
  }'
```

## 4. Correlate

Because the log and the span share `traceId`, you can join them. The same holds across product
events and telemetry via `session_id` / `actor_id` / `tenant_id`:

```sql
SELECT l.body, s.name AS span, s.status_code
FROM   logs l
JOIN   spans s ON s.trace_id = l.trace_id
WHERE  l.trace_id = '5b8efff798038103d269b633813fc60c';
```

## Use a real OpenTelemetry SDK or Collector

You rarely hand-write OTLP. Point any OpenTelemetry exporter at Datacat's endpoints:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:8080
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer dev-logs-token
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
```

For shipping container logs and host metrics through the OpenTelemetry Collector on Docker Compose
and Swarm, follow [Logs & metrics with Docker](../../docker-telemetry/). gRPC is available too —
see [OTLP logs](../../otel-logs/), [metrics](../../otel-metrics/) and [traces](../../traces/).

## Next steps

- [Alert to Slack](../alert-to-slack/) on the errors you just sent.
