---
title: "MCP Server"
description: "The embedded HTTP MCP server gives an agent (Claude) read access to logs, traces, events, metrics, and journeys for debugging, analysis, and test generation."
---

The Datacat backend exposes an **MCP** (Model Context Protocol) server over **HTTP** on the
**`/mcp`** route (*streamable HTTP* transport). It gives an agent (Claude) **read** access to the
data — logs, traces, events, metrics, journeys — to **debug, analyze real usage, verify
hypotheses, and generate tests**.

> **Embedded, nothing to install.** The MCP server is part of the ingestion binary (Rust crate
> `rmcp`); its tools hit the [read layer](../read-hot/) **in-process** (no HTTP hop).
> An agent connects to it simply via a URL — no process or package to install.

## Activation & security

- Enabled by default (`MCP_ENABLED=true`).
- Protected by `query_auth` (`auto`|`static`|`jwt`|`none`) + `QUERY_TOKEN`: the read token
  is expected in the `Authorization: Bearer` header (the same token as `/v1/query/*`).
- Mounted **outside the global HTTP timeout** (the SSE stream is long-lived).
- Read-only end to end; the ad-hoc SQL tool is bounded and can be disabled (`QUERY_SQL_ENABLED`).

## Exposed tools

| Tool | Role |
|---|---|
| `search_logs` | Log search (service, session, trace_id, severity, substring, time). |
| `get_trace` | All spans of a trace (by `trace_id`), ordered. |
| `search_events` | Product event search (actor, session, tenant, name, time). |
| `frequent_journeys` | Frequent journey sequences per session (E2E test generation). |
| `search_metrics` | Metric points (name, service, time). |
| `run_read_sql` | Ad-hoc **read-only** SQL (SELECT/WITH) over events/logs/spans/metric_points. |
| `ingest_stats` | Volumes and deduplication per domain, drops. |

## Wiring it into Claude Code

```bash
claude mcp add --transport http datacat https://datacat.example.com/mcp \
  --header "Authorization: Bearer <read-token>"
```

or via a project `.mcp.json`:

```json
{
  "mcpServers": {
    "datacat": {
      "type": "http",
      "url": "https://datacat.example.com/mcp",
      "headers": { "Authorization": "Bearer <read-token>" }
    }
  }
}
```

(Locally: `http://localhost:8080/mcp`. If `QUERY_AUTH=none`, the header is unnecessary.)

## Usage examples (prompts)

- **Diagnose an incident**: "Find the ERROR logs of the `billing` service from the last
  hour, then fetch the trace of the first one to see where it breaks."
  → `search_logs(service=billing, severity_min=17)` then `get_trace(trace_id=…)`.
- **Understand a user**: "What events did actor `user-123` produce today?"
  → `search_events(actor=user-123, from=…)`.
- **Generate an E2E test**: "What are the 5 most frequent journeys? Write a Playwright
  test for the most common one." → `frequent_journeys(limit=5)` then generation.
- **Correlate**: "For session `sess-abc`, show the events and logs side by side,
  ordered in time."
  → `run_read_sql("SELECT 'event' AS k, timestamp_client AS t, event_name AS d FROM events WHERE session_id='sess-abc' UNION ALL SELECT 'log', log_time, body FROM logs WHERE session_id='sess-abc' ORDER BY t")`.
- **Monitor ingestion**: "How many events/logs ingested and deduplicated?" → `ingest_stats`.
