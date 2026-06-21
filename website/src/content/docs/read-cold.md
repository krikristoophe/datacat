---
title: "Cold Reads"
description: "Analytical SQL over cold storage with Apache DataFusion: query the exported Parquet files on S3-compatible storage directly, bypassing PostgreSQL."
---

## Overview

The `datacat-reader` crate provides an analytical SQL query engine over Datacat's
cold storage. It relies on [Apache DataFusion](https://datafusion.apache.org/)
to run arbitrary SQL directly on the Parquet files exported to
S3-compatible storage (AWS S3, MinIO), without going through the PostgreSQL database.

```
         ┌────────────┐      daily export        ┌──────────────────────┐
         │ PostgreSQL │ ─────────────────────────▶│  S3 / MinIO          │
         │ (hot)      │    datacat-exporter       │  Parquet + Hive      │
         └────────────┘                           │  layout              │
                                                  └──────────┬───────────┘
                                                             │
                                              ┌──────────────▼──────────────┐
                                              │   datacat-reader             │
                                              │   DataFusion (SQL)           │
                                              │   object_store (S3/MinIO)    │
                                              └──────────────────────────────┘
```

**Positioning within the Datacat architecture:**

| Layer              | Technology                   | Latency     | Use                                |
|--------------------|------------------------------|-------------|------------------------------------|
| Hot read           | PostgreSQL (REST backend)    | < 50 ms     | Real-time API, live dashboards     |
| Analytical read    | DataFusion on Parquet/S3     | seconds     | Historical queries, exports, BI    |
| Cold storage       | Parquet zstd on S3           | —           | Long-term archive, open format     |

Slow reads are **accepted and documented**: DataFusion scans the Parquet files
from S3 on every query. This is not a replacement for the backend's real-time
queries — it is the analytical layer over cold storage.

---

## S3 layout (Hive partitioning)

```
<bucket>/
  events/
    date=2024-06-15/
      part-0000.parquet
    date=2024-06-16/
      part-0000.parquet
    ...
  logs/
    date=2024-06-15/
      part-0000.parquet
    ...
```

This layout is **Iceberg/Spark-compatible**: the `date=YYYY-MM-DD` partition
is a standard Hive partition that Data ecosystem tools recognize natively.

---

## Parquet schemas

### `events` table

| Column           | Arrow type                       | Nullable |
|------------------|----------------------------------|----------|
| event_id         | Utf8                             | no       |
| event_name       | Utf8                             | no       |
| tenant_id        | Utf8                             | yes      |
| actor_id         | Utf8                             | no       |
| session_id       | Utf8                             | no       |
| timestamp_client | Timestamp(Microsecond, UTC)      | no       |
| received_at      | Timestamp(Microsecond, UTC)      | no       |
| properties       | Utf8 (serialized JSON)           | no       |

### `logs` table

| Column               | Arrow type                  | Nullable |
|----------------------|-----------------------------|----------|
| log_id               | Utf8                        | no       |
| log_time             | Timestamp(Microsecond, UTC) | no       |
| observed_time        | Timestamp(Microsecond, UTC) | yes      |
| received_at          | Timestamp(Microsecond, UTC) | no       |
| severity_number      | Int16                       | yes      |
| severity_text        | Utf8                        | yes      |
| body                 | Utf8                        | yes      |
| service_name         | Utf8                        | yes      |
| scope_name           | Utf8                        | yes      |
| trace_id             | Utf8                        | yes      |
| span_id              | Utf8                        | yes      |
| tenant_id            | Utf8                        | yes      |
| actor_id             | Utf8                        | yes      |
| session_id           | Utf8                        | yes      |
| resource_attributes  | Utf8 (serialized JSON)      | no       |
| log_attributes       | Utf8 (serialized JSON)      | no       |

---

## Configuration

The environment variables are identical to those of `datacat-exporter`:

| Variable              | Required | Default    | Description                              |
|-----------------------|----------|------------|------------------------------------------|
| `S3_ENDPOINT`         | no       | AWS S3     | Endpoint URL (e.g. `http://localhost:9200`) |
| `S3_REGION`           | no       | `eu-west-1`| AWS / MinIO region                       |
| `S3_BUCKET`           | yes      | —          | Bucket name                              |
| `AWS_ACCESS_KEY_ID`   | yes      | —          | Access key                               |
| `AWS_SECRET_ACCESS_KEY`| yes     | —          | Secret key                               |
| `S3_ALLOW_HTTP`       | no       | `false`    | `true` for local MinIO (no TLS)          |
| `S3_PREFIX`           | no       | root       | Prefix within the bucket (e.g. `prod/`)  |

---

## CLI: `datacat-query-cold`

```bash
# Global count
datacat-query-cold --table events \
  --sql "SELECT count(*) FROM events"

# TOP 10 most frequent events
datacat-query-cold --table events \
  --sql "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC LIMIT 10"

# Filter on a specific date
datacat-query-cold --table events --date 2024-06-15 \
  --sql "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC"

# JSON format
datacat-query-cold --table events --date 2024-06-15 \
  --sql "SELECT actor_id, count(*) AS n FROM events GROUP BY actor_id" \
  --format json
```

### Environment variables for local MinIO

```bash
export S3_ENDPOINT=http://localhost:9200
export S3_REGION=us-east-1
export S3_BUCKET=datacat
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_ALLOW_HTTP=true
```

---

## Analytical query examples

### 1. Most frequent events

```sql
SELECT event_name, count(*) AS n
FROM events
GROUP BY event_name
ORDER BY n DESC
LIMIT 20
```

### 2. Activity per actor

```sql
SELECT actor_id, count(*) AS n_events, count(DISTINCT session_id) AS n_sessions
FROM events
GROUP BY actor_id
ORDER BY n_events DESC
```

### 3. Event sequences per session (user journeys)

Useful for **E2E test generation**: retrieves event sequences in chronological
order per session.

```sql
-- Overview: number of events and duration per session
SELECT
    session_id,
    count(*) AS n_events,
    min(timestamp_client) AS first_event_at,
    max(timestamp_client) AS last_event_at,
    max(timestamp_client) - min(timestamp_client) AS session_duration
FROM events
GROUP BY session_id
ORDER BY first_event_at
```

To get the full event sequence per session (chronological order):

```sql
-- Event sequence for a given session
SELECT event_name, timestamp_client, actor_id, properties
FROM events
WHERE session_id = 'session-42'
ORDER BY timestamp_client
```

> **DataFusion note**: `array_agg(event_name ORDER BY timestamp_client)` is
> supported in DataFusion 54, but the results are `List`-typed columns.
> The recommended approach for CI integration is to use the GROUP BY +
> ORDER BY above to inspect the sequences, then generate the test scripts
> in post-processing.

### 4. Temporal distribution (by hour)

```sql
SELECT
    date_trunc('hour', timestamp_client) AS hour,
    count(*) AS n_events
FROM events
GROUP BY date_trunc('hour', timestamp_client)
ORDER BY hour
```

### 5. Log analysis by severity

```sql
SELECT
    severity_text,
    count(*) AS n,
    count(DISTINCT trace_id) AS n_traces
FROM logs
WHERE severity_number >= 9  -- WARNING and above
GROUP BY severity_text
ORDER BY n DESC
```

### 6. Event/log correlation per session

```sql
-- Cross-table query: sessions with both events and error logs
SELECT e.session_id, count(DISTINCT e.event_name) AS n_event_types
FROM events e
WHERE e.session_id IN (
    SELECT DISTINCT session_id FROM logs WHERE severity_number >= 17
)
GROUP BY e.session_id
ORDER BY n_event_types DESC
```

---

## Technical architecture

### Data flow

```
S3 (Parquet zstd)
    │
    ├── object_store 0.13 (AWS SigV4, HTTP/HTTPS)
    │       └── ListingTable (scan Hive partitions)
    │
    └── DataFusion 54 (SQL → physical plan → Arrow RecordBatch)
            ├── Projection / Filter / Aggregation
            ├── Parquet reader (predicate pushdown, column pruning)
            └── Arrow RecordBatch → output (ASCII table / JSON)
```

### Iceberg compatibility

The Hive layout `table/date=YYYY-MM-DD/part-*.parquet` is the natural substrate
for an Iceberg v2 table. Migrating to Iceberg is possible without rewriting
the data: simply add an Iceberg catalog (e.g. a REST catalog) that points
at the same Parquet files. DataFusion has an Iceberg connector
(`datafusion-iceberg`) that can be added later.

### Performance

- **Slow reads accepted**: each query scans the Parquet files
  from S3. For short date ranges (`--date YYYY-MM-DD`), latency is
  on the order of a second. For full month-wide scans,
  expect several seconds to tens of seconds depending on volume.
- **Predicate pushdown**: DataFusion pushes filters down onto the Parquet
  columns (min/max statistics, bloom filters) to reduce I/O.
- **Parallelism**: DataFusion executes plans in parallel across multiple
  threads (configurable via `SessionConfig`).

---

## End-to-end tests

The e2e test is launched via `reader/run-tests.sh`:

```bash
cd reader && ./run-tests.sh
```

This script:
1. Starts a MinIO container on ports 9200/9201.
2. Runs `cargo test -- --nocapture`.
3. The test generates synthetic event Parquet files, uploads them to MinIO,
   then runs DataFusion queries and verifies the results.
4. Removes the MinIO container.

The e2e tests are **not run in CI** (they require Docker + MinIO).
The `reader` CI job is limited to `cargo build --release`, `cargo clippy`,
`cargo fmt --check`.

---

## Relationship with the other components

| Component              | Role                                                        |
|------------------------|-------------------------------------------------------------|
| `exporter/`            | Exports PostgreSQL → Parquet/S3 (produces the files read). A standalone crate, also embedded & scheduled in the backend. |
| `backend/`             | Real-time REST API over PostgreSQL (hot reads)              |
| `reader/` (this crate) | Analytical SQL queries over Parquet/S3 (cold reads)         |

The `reader` is **read-only**: it never modifies the data on S3.
