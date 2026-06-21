---
title: "Hot Reads"
description: "Read-only query endpoints over PostgreSQL for recent (hot) data: logs, events, metrics, traces, journeys, and ad-hoc SQL."
---

**Read-only** endpoints over PostgreSQL (hot data). Decoupled from ingestion. For bulk
analysis, see [Cold reads](../read-cold/) (DataFusion/Parquet).

Authentication: configurable via `QUERY_AUTH` (`auto`|`static`|`jwt`|`none`) + `QUERY_TOKEN`,
`Authorization: Bearer` header.

## Endpoints

| Endpoint | Parameters | Response |
|---|---|---|
| `GET /v1/query/logs` | `service`, `session`, `trace_id`, `severity_min`, `q` (body substring), `from`, `to`, `limit` | `{ logs: [...] }` |
| `GET /v1/query/events` | `actor`, `session`, `tenant`, `name`, `from`, `to`, `limit` | `{ events: [...] }` |
| `GET /v1/query/metrics` | `name`, `service`, `from`, `to`, `limit` | `{ metrics: [...] }` |
| `GET /v1/query/traces/{trace_id}` | — | `{ trace_id, span_count, spans: [...] }` (ordered by start) |
| `GET /v1/query/journeys` | `actor`, `tenant`, `limit` | `{ journeys: [ { path: [...], occurrences } ] }` |
| `POST /v1/query/sql` | body `{ sql, limit? }` | `{ row_count, truncated, rows: [...] }` |

`from`/`to` are in RFC3339 format. `limit` is capped (default 100, max 1000; journeys 20/200).

## Ad-hoc read-only SQL (`/v1/query/sql`)

For exploratory analysis (aggregates, correlation joins). **Disabled by default**
(`QUERY_SQL_ENABLED=true` to enable it). Defenses:

- only `SELECT` / `WITH` queries are accepted; **single statement** (`;` forbidden);
- executed in a **`READ ONLY`** transaction (blocks any `INSERT`/`UPDATE`/`DELETE`/DDL,
  defense in depth) with a **`statement_timeout`** (`QUERY_SQL_TIMEOUT`, default 10s);
- wrapped as `SELECT to_jsonb(t) FROM (<sql>) AS t LIMIT n` → JSON result, with rows capped
  (`QUERY_SQL_MAX_ROWS`, default 1000, `truncated` flag);
- protected by `QUERY_AUTH`.

Example:
```bash
curl -s -X POST "$DATACAT/v1/query/sql" -H 'content-type: application/json' \
  -d '{"sql":"SELECT service_name, count(*) FROM logs WHERE severity_number>=17 GROUP BY 1 ORDER BY 2 DESC"}'
```

Queryable tables: `events`, `logs`, `spans`, `metric_points` (correlatable via
`session_id` / `actor_id` / `tenant_id` / `trace_id`).

## Agent access (MCP)

The embedded [MCP HTTP server](../mcp/) (route `/mcp`) exposes these queries as tools for Claude.
