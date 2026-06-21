---
title: "Cold Storage"
description: "Exporting PostgreSQL to Parquet on S3 for cold storage."
---

## Overview

```
PostgreSQL (events / logs)
        │
        │  SELECT … WHERE timestamp_client IN [day, day+1)
        │  in batches of 10,000 rows
        ▼
  Arrow RecordBatch (typed columns)
        │
        │  ArrowWriter (Parquet zstd level 3)
        ▼
  In-memory Parquet file
        │
        │  object_store PUT
        ▼
  S3-compatible (MinIO / Scaleway / Cloudflare R2 / AWS S3 EU)
        │
        ▼  (future) DataFusion / DuckDB read directly
```

The `exporter/` crate is a standalone binary (`datacat-export`) with no dependency on the
ingestion core (`backend/`). The boundaries are clean: ingestion writes to the database, export
reads from the database and writes to S3. The two components have no mutual dependency
(spec §9, architecture §7).

The same export logic can also run **embedded** in the backend as a scheduled task via the
`[export]` section of `datacat.toml` — see [configuration](../configuration/) §5. This page
describes the standalone CLI and the on-disk layout shared by both paths.

---

## S3 layout — Hive / Iceberg partitioning

```
s3://<bucket>/
  events/
    date=2024-06-15/
      part-0000.parquet
    date=2024-06-16/
      part-0000.parquet
  logs/
    date=2024-06-15/
      part-0000.parquet
```

- Partition key: `date=YYYY-MM-DD` (Hive / Apache Iceberg convention).
- Each file covers one full **UTC day**.
- The optional prefix (`--prefix` / `S3_PREFIX`) lets you organize several environments in the
  same bucket (e.g. `prod/events/date=…`, `staging/events/date=…`).

---

## Parquet schema

### `events` table

| Column            | Arrow/Parquet type                    | Nullable | Notes |
|---|---|---|---|
| `event_id`        | `Utf8`                                | no       | Text UUID (36 chars) |
| `event_name`      | `Utf8`                                | no       | |
| `tenant_id`       | `Utf8`                                | yes      | |
| `actor_id`        | `Utf8`                                | no       | |
| `session_id`      | `Utf8`                                | no       | |
| `timestamp_client`| `Timestamp(Microsecond, UTC)`         | no       | Partitioned / sorted |
| `received_at`     | `Timestamp(Microsecond, UTC)`         | no       | |
| `properties`      | `Utf8`                                | no       | Serialized JSON |

**Mapping decisions:**
- `uuid` → `Utf8`: open format, readable by DuckDB/DataFusion without an extension.
- `timestamptz` → `Timestamp(Microsecond, UTC)`: microsecond precision, UTC timezone explicit in
  the Arrow metadata (compatible with Iceberg / Spark / DuckDB).
- `jsonb` → `Utf8` (JSON string): Parquet has no native JSON type; storing the JSON text
  representation lets any read engine parse it.

### `logs` table

| Column                | Arrow/Parquet type            | Nullable |
|---|---|---|
| `log_id`              | `Utf8`                        | no       |
| `log_time`            | `Timestamp(Microsecond, UTC)` | no       |
| `observed_time`       | `Timestamp(Microsecond, UTC)` | yes      |
| `received_at`         | `Timestamp(Microsecond, UTC)` | no       |
| `severity_number`     | `Int16`                       | yes      |
| `severity_text`       | `Utf8`                        | yes      |
| `body`                | `Utf8`                        | yes      |
| `service_name`        | `Utf8`                        | yes      |
| `scope_name`          | `Utf8`                        | yes      |
| `trace_id`            | `Utf8`                        | yes      |
| `span_id`             | `Utf8`                        | yes      |
| `tenant_id`           | `Utf8`                        | yes      |
| `actor_id`            | `Utf8`                        | yes      |
| `session_id`          | `Utf8`                        | yes      |
| `resource_attributes` | `Utf8`                        | no       |
| `log_attributes`      | `Utf8`                        | no       |

---

## Compression

**zstd level 3** (high ratio, fast decompression). Alternatives are available via the
`writer_properties()` constant in `src/export.rs`: snappy (faster decompression), lz4
(a trade-off). The choice is centralized and can be changed without touching the rest of the code.

---

## Idempotence

**Strategy: unconditional PUT (overwrite).**

Re-running the export for a given day *replaces* the existing S3 object. The result is
deterministic because:
1. The PostgreSQL query sorts on `(timestamp_client, event_id)` (stable ORDER BY).
2. Arrow/Parquet serialization is deterministic for an identical input RecordBatch.
3. The same batch of data therefore produces the same Parquet file, bit for bit.

Consequence: a re-run after a partial failure or a data correction produces exactly the expected
final file. No deduplication logic is needed on the S3 side.

**Correction case:** if data is corrected in the database (e.g. replaying a missing batch), simply
re-run the export for the same day; the S3 file is overwritten.

---

## Scheduling (cron)

Daily export of day D-1 (once the day is complete):

```
# /etc/cron.d/datacat-export or a systemd timer
0 1 * * * datacat-export \
    --table events \
    --date $(date -d yesterday +%Y-%m-%d) \
    --bucket <bucket> 2>&1 | logger -t datacat-export

0 2 * * * datacat-export \
    --table logs \
    --date $(date -d yesterday +%Y-%m-%d) \
    --bucket <bucket> 2>&1 | logger -t datacat-export
```

Alternative: a Kubernetes CronJob or an AWS EventBridge Scheduler → ECS Task. For an
always-on deployment, prefer the embedded scheduled export (`[export]` in `datacat.toml`,
see [configuration](../configuration/) §5), which runs the same logic on a tick.

Required environment variables (standalone CLI):

| Variable                | Example                              |
|---|---|
| `DATABASE_URL`          | `postgres://datacat:…@host:5432/db`  |
| `S3_ENDPOINT`           | (empty for AWS S3, or a MinIO URL)   |
| `S3_REGION`             | `eu-west-3`                          |
| `S3_BUCKET`             | `my-datacat-cold`                    |
| `AWS_ACCESS_KEY_ID`     | `AKIA…`                              |
| `AWS_SECRET_ACCESS_KEY` | `…`                                  |
| `S3_ALLOW_HTTP`         | `true` (MinIO dev without TLS only)  |

---

## Batch streaming

Data is read in **batches of 10,000 rows** (`BATCH_SIZE` in `src/export.rs`). Each batch produces
a separate Parquet **row group** in the same file. This:
- Bounds memory consumption to ~10,000 rows × (the size of one row).
- Lets DataFusion/DuckDB read by row group (predicate pushdown).

For very large volumes (> 100M rows/day), several `part-NNNN.parquet` files can be produced by
changing the part counter in `hive_path()` — the batching logic is already in place; only the
file split remains to be wired up if needed.

---

## Future analytical reads (DataFusion / DuckDB)

The Hive layout is directly consumable:

```sql
-- DuckDB
SELECT date_trunc('day', timestamp_client), event_name, count(*)
FROM read_parquet('s3://bucket/events/date=*/*.parquet', hive_partitioning=true)
WHERE date = '2024-06-15'
GROUP BY 1, 2;
```

```rust
// DataFusion
let ctx = SessionContext::new();
ctx.register_listing_table(
    "events",
    "s3://bucket/events/",
    ListingTableConfig::new(…).with_listing_options(
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![("date".to_string(), DataType::Utf8)]),
    ),
    None,
).await?;
```

Correlation between events ↔ logs is done via the shared columns `tenant_id`, `actor_id`,
`session_id` (cf. spec §4.2).
