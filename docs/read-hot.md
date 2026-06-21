# Hot read layer (`/v1/query/*`)

**Read-only** endpoints over PostgreSQL (hot data). Decoupled from ingestion. For bulk
analysis, see cold reads ([read-cold.md](read-cold.md), DataFusion/Parquet).

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

`from`/`to` are in RFC3339 format. `limit` is capped (default 100, max 1000; journeys 20/200).

## Agent access (MCP)

The embedded [MCP HTTP server](mcp.md) (route `/mcp`) exposes these queries as tools for Claude.
