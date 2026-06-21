---
title: "OTLP Metrics"
description: "Ingesting OpenTelemetry metrics into Datacat."
---

Datacat ingests **metrics** in the **OpenTelemetry / OTLP-HTTP (JSON)** and **OTLP/gRPC**
formats, on the same foundation as events, logs and traces: a table partitioned by day,
idempotent, written with `COPY`. Goal: round out observability (APM) by linking metrics to
events / logs / traces via `tenant_id` / `actor_id` / `session_id`.

## 1. Transports

| Transport | Endpoint | Activation |
|---|---|---|
| **OTLP/HTTP (JSON)** | `POST /v1/metrics` | always on (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`) |
| **OTLP/gRPC** | `MetricsService/Export` service on `:4317` | `[server.grpc].enabled = true` (port `[server.grpc].bind_addr`) |

Body: an OTLP `ExportMetricsServiceRequest`. Response: `ExportMetricsServiceResponse` (empty, or
`partialSuccess` with `rejectedDataPoints` if some points were dropped under back-pressure). Both
transports share **exactly** the same admission logic (auth, rate limit, correlation, dedup) and
produce the same `point_id` for identical content.

## 1.1 Authentication

Auth is **identical to that of logs/traces**: a service token (`[auth.logs].mode`,
`[auth.logs].static_token`). Metrics are emitted service-to-service; see
[OTLP logs](../otel-logs/) §1.1. Rate limiting is keyed on `service.name` (falling back to the
IP).

## 2. Data model

Each **data point** of a metric is flattened into a row of the `metric_points` table. The OTLP
hierarchy is:
`resourceMetrics → scopeMetrics → metrics → {gauge|sum|histogram}.dataPoints`.

| Column | OTLP source |
|---|---|
| `point_id` | **deterministic hash** of the content (dedup of resends — OTLP has no native id) |
| `time` | `timeUnixNano` of the data point (or reception time if absent). **Partition key.** |
| `metric_name` | `metric.name` |
| `metric_type` | `gauge` \| `sum` \| `histogram` |
| `unit` | `metric.unit` |
| `value_double` / `value_int` | `NumberDataPoint.asDouble` / `asInt` (gauge, sum) |
| `count` / `sum` | `HistogramDataPoint.count` / `sum` (histogram) |
| `buckets` | `{ "bounds": [...explicitBounds], "counts": [...bucketCounts] }` (histogram, JSONB) |
| `service_name` | resource attribute `service.name` |
| `scope_name` | `scope.name` |
| `tenant_id` / `actor_id` / `session_id` | attributes (data point then resource) — **correlation** |
| `resource_attributes` / `attributes` | full attributes (JSONB) |

### Supported types

| OTLP type | Ingested? | Flattened to |
|---|---|---|
| **Gauge** | ✅ | one row per `NumberDataPoint` (`value_double` or `value_int`) |
| **Sum** | ✅ | same as gauge (`metric_type = sum`) |
| **Histogram** | ✅ | one row per `HistogramDataPoint` (`count`, `sum`, `buckets`) |
| **Summary** | ❌ ignored | — (legacy Prometheus type, not cleanly mergeable) |
| **ExponentialHistogram** | ❌ ignored | — (out of scope; explicit histogram recommended) |

Points of type `summary` and `exponentialHistogram` are **silently ignored** (neither stored nor
counted as a rejection). The aggregation granularity (delta/cumulative temporality) and the
monotonic nature of a `sum` are not interpreted: each point is stored as-is.

### Idempotency

As with the other domains, idempotency relies on `(time, point_id)`. `point_id` is a SHA-256 hash
(truncated to 128 bits → UUID) of the point's normalized content:
`time + metric_name + metric_type + service_name + value(s) + buckets + attributes`.
Resending the **same** point N times creates only one row (dedup in the database,
`ON CONFLICT DO NOTHING`).

## 3. Partitioning, retention, staging

Identical to the other domains:
- the `metric_points` table is partitioned by day on `time` (RANGE);
- `UNLOGGED` staging (`metric_points_staging`) + `COPY` → idempotent merge;
- SQL functions `datacat_ensure_metric_partition(date)`,
  `datacat_ensure_metric_partitions_for_staging()`, `datacat_merge_metric_staging()`,
  `datacat_drop_metric_partitions_before(date)` (see `migrations/0006_metrics.sql`);
- retention via `[ingest].retention_days`, future partitions via `[ingest].partition_future_days`;
- timestamp skew bounded by `MAX_PAST_SKEW` / `MAX_FUTURE_SKEW` (points outside the window are
  dropped, counted in `dropped_skew_total`).

Read indexes: `(metric_name, time)` and `(service_name, time)`.

## 4. Reads

`GET /v1/query/metrics?name=&service=&from=&to=&limit=` returns the matching points (descending
`time` order). Authenticated via `[auth.query]` (the read token), like the other `/v1/query/*`
endpoints.

## 5. Observability

`GET /stats` exposes the domain's ingestion counters under the `metrics` key (`received_total`,
`inserted_total`, `deduplicated_total`, `dropped_skew_total`, …).

## 6. Example (OTLP/HTTP JSON)

```json
{
  "resourceMetrics": [{
    "resource": { "attributes": [
      { "key": "service.name", "value": { "stringValue": "api" } }
    ]},
    "scopeMetrics": [{
      "metrics": [
        {
          "name": "process.cpu.utilization", "unit": "1",
          "gauge": { "dataPoints": [
            { "timeUnixNano": "1718900000000000000", "asDouble": 0.42 }
          ]}
        },
        {
          "name": "http.server.duration", "unit": "ms",
          "histogram": { "dataPoints": [
            { "timeUnixNano": "1718900000000000000",
              "count": "3", "sum": 600.0,
              "bucketCounts": ["1", "2", "0"], "explicitBounds": [100.0, 500.0] }
          ]}
        }
      ]
    }]
  }]
}
```
