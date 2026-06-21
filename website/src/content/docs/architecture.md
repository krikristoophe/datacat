---
title: "Architecture"
description: "Design decisions of the ingestion-first v1: idempotence × partitioning, the write path, and how the architecture prepares future extensions."
---

This document explains the structuring choices of v1 and how the architecture **prepares**
for the out-of-scope extensions (spec §9) without deploying them.

## 1. Overview

```
Events (web / mobile / backend)
        │  POST /v1/events   (JSON batch, Bearer <jwt>)
        ▼
┌─────────────────────────────────────────────────────────────┐
│ Ingestion API (Axum)                                         │
│  • guardrails: CORS, size limit, timeout, IP ban             │
│  • token verification (public key)  • 2-level rate limiting  │
│  • strict validation → non-blocking enqueue (immediate 202)  │
│         │ mpsc (bounded back-pressure)                       │
│         ▼                                                     │
│  Batcher (single task): in-memory micro-batch                │
└─────────┬───────────────────────────────────────────────────┘
          │ COPY (CSV) → events_staging (UNLOGGED)
          ▼
   datacat_merge_staging()  →  INSERT … ON CONFLICT DO NOTHING
          ▼
   events  (table partitioned by day on timestamp_client)
          │
          ▼  (out of v1) Parquet/Iceberg export · DataFusion/DuckDB reads
```

Clean boundaries: **ingestion** (`ingest`), **storage** (`db` + migrations), **reads**
(absent in v1). The ingestion core has no dependency on any read layer, which makes it
possible to add cold storage / reads / a write buffer **without a rewrite** (spec §9).

## 2. Idempotence × partitioning: the central decision

The system must be **partitioned by time** AND guarantee that a given `event_id` is stored
only once (strict idempotence). In PostgreSQL, these two requirements collide:

> A `UNIQUE` constraint on a partitioned table **must include the partition key**.

So we cannot have a simple global `UNIQUE(event_id)` on a partitioned table. The uniqueness
key must contain the partition column. Which time column should we choose?

| Candidate | Stable across two sends of the same event? | Consequence |
|---|---|---|
| `received_at` (server) | **No** — each reception ⇒ a new timestamp | Two retries would have two different keys ⇒ **duplicates**. Unusable for dedup. |
| `timestamp_client` (client) | **Yes** — frozen at creation, reused identically on retry | Any duplicate falls back into the **same partition** ⇒ `ON CONFLICT` dedups globally. ✅ |

**Decision: partition by `timestamp_client`**, idempotence key `(timestamp_client, event_id)`.
This is the only choice that reconciles time partitioning with native idempotence.

This imposes an **SDK contract** (see [Contract](../contract/) §2.2): `event_id` **and**
`timestamp_client` are frozen at creation and never regenerated on retry. Both SDKs honor this.

### Associated guardrails

- Since `timestamp_client` is provided by the client (hence forgeable / wrong clock), it is
  **bounded** by validation: rejected outside `[received_at - MAX_PAST_SKEW, received_at + MAX_FUTURE_SKEW]`
  (default: 31 d / 24 h). This prevents the **creation of arbitrary partitions** (poisoning):
  at most ~33 daily partitions can exist for the allowed window.
- `received_at` is still stored as a column (for later analysis: reliable server clock).

## 3. Optimized write path

1. **Immediate acknowledgement**: the handler validates, then enqueues into a bounded `mpsc`
   channel and replies `202`. Write latency is never on the request path.
2. **Micro-batch**: a **single** task (one writer ⇒ zero contention on staging) accumulates
   events and flushes when (a) the `FLUSH_BATCH_SIZE` size is reached or (b) the
   `FLUSH_INTERVAL` interval elapses.
3. **COPY**: bulk write in CSV format into `events_staging`, an **`UNLOGGED`** table
   (no WAL ⇒ maximum throughput). Losing recent staging on a crash is acceptable
   (tolerance §2); on restart, any residue is merged (`drain_staging`).
4. **Idempotent merge**: `datacat_merge_staging()` performs
   `INSERT … SELECT DISTINCT ON (timestamp_client, event_id) … ON CONFLICT DO NOTHING`
   (intra-batch collapse via `DISTINCT ON`, inter-batch via `ON CONFLICT`), then `TRUNCATE`s
   the staging table. The function returns the number of rows **actually** inserted (post-dedup).

`COPY` (rather than row-by-row `INSERT`s) is what delivers the throughput; the merge is
WAL-logged and durable.

## 4. Retention via DROP PARTITION

Purging is done by `datacat_drop_partitions_before(day)`, which runs `DROP TABLE` on partitions
older than `RETENTION_DAYS`. Dropping a partition's `DROP TABLE` is **instant** (file release),
unlike a massive `DELETE` (rewrite + VACUUM). No impact on the write path (writes target recent
partitions).

## 5. Back-pressure & loss tolerance

The `mpsc` channel is bounded (`CHANNEL_CAPACITY`). Under extreme overload, `try_enqueue`
fails: the event is **dropped** (`dropped_channel_full_total` counter) and the response stays
`202` with `received` = the number actually enqueued. This is the concrete application of
*unbiased loss tolerance* (§2): we do not return `5xx`, which would trigger retries that worsen
the overload. **Never a duplicate**, however: idempotence is guaranteed in the database.

## 6. Security (summary; details in [Security](../security/))

Public endpoint, not strongly authenticated. Defenses are **100% server-side**:
token verification by **asymmetric** signature (public key only — ingestion cannot forge a
token), two-level rate limiting + a global safety net, strict validation, size bounds, CORS,
banning of anomalous IPs, traceable structured logs, TLS at deployment.

## 7. Preparing the out-of-v1 extensions (spec §9)

| Extension | How v1 accommodates it without a rewrite |
|---|---|
| **Cold storage** (Parquet/Iceberg on S3 EU) | `events` is already partitioned by day: an export job reads partition by partition into Parquet, without touching ingestion. |
| **Analytical reads** (DataFusion/DuckDB) | A separate read layer plugged onto the cold tier (and/or the hot one). The ingestion core references no reads. No read index in v1 to preserve write throughput; they will be created on the cold side. |
| **Technical logs** | **Ingested in v1** via `POST /v1/logs` (OTLP/HTTP), on the same generic partitioned/idempotent base as events, correlated via `tenant_id` / `actor_id` / `session_id` and `trace_id`. See [OTLP logs](../otel-logs/). |
| **Cold S3 storage** | Date-partitioned Parquet export via the cold exporter — a standalone crate, also embedded & scheduled in the backend (outside the ingestion core). See [Cold storage](../cold-storage/). |
| **Write scale-out** (Citus / Redpanda) | The writer is isolated behind a channel: a distributed buffer can be inserted in front, or sharding via Citus, without changing the ingestion contract. |
| **Read scale-out** (Ballista) | Guaranteed by the open format (Iceberg) on the cold side. |

## 8. Configuration & multi-project

Configuration is a single **TOML file** (`datacat.toml`, template `datacat.example.toml`)
that describes the whole deployment, plus **one TOML file per project** under `projects/*.toml`.
Every string value can reference an environment variable with `${VAR}` (or `${VAR:-default}`),
resolved at startup, so no secret is committed. A legacy environment-variable fallback remains
for development and the test suite. See [Configuration](../configuration/) for the full reference.

Datacat is **multi-project at the configuration level**: each project carries its own alerting
rules and notification channels, and the backend runs **one alerting evaluator per project**.
The ingestion pipeline and the stored data are **shared** — project isolation is at the
configuration level, not at the data level.

The cold export is driven from the same config: an `[export]` TOML section schedules the
embedded exporter (the cold export logic is a standalone crate, also embedded & scheduled in
the backend).

## 9. Module map (backend)

Split into coherent submodules (domains on top of a shared infrastructure):

| Module | Role |
|---|---|
| `config` | Configuration from the TOML file (`datacat.toml`) with `${ENV}` secret expansion and per-project files (`projects/*.toml`), safe defaults, startup validation; legacy environment-variable fallback. |
| `error`, `telemetry` | Typed errors (→ HTTP responses); structured logs. |
| `events/model` | Event wire format + strict validation; `Ingestable` impl. |
| `logs/model` | OTLP log wire format + flattening/correlation/dedup; `Ingestable` impl. |
| `ingest` | **Generic**: `Ingestable` trait, channel, batcher, `COPY`, idempotent merge, metrics (shared by events + logs). |
| `db` (+ `db/partitions`) | Pool, migrations, partition management/purge (events & logs), staging drain. |
| `security` (`token`, `ratelimit`, `anomaly`) | Asymmetric JWT verification (PEM/JWKS, `kid`); token buckets + per-session/IP cap; IP resolution + anomaly banning. |
| `api` (+ `api/routes`) | Router assembly + guardrails (CORS, size, timeout, tracing); handlers (`/v1/events`, `/v1/logs`, `/healthz`, `/readyz`, `/stats`). |
| `lib` | `AppState` (events & logs ingestors) + module declarations. |
