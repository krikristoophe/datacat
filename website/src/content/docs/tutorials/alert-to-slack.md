---
title: "Tutorial: alert to Slack"
description: "Define a project alerting rule that watches your logs and posts to Slack via the Bot API when an error rate crosses a threshold."
---

Datacat evaluates **alerting rules per project** on a fixed interval and routes firing alerts to
channels — Slack, e-mail or webhooks. This tutorial wires an error-rate rule on the `checkout`
service to a Slack channel.

It builds on the telemetry from [Instrument a service](../instrument-a-service/).

## 1. Create a Slack bot token

Slack notifications use the **Web API** (`chat.postMessage`), not legacy incoming webhooks. In your
Slack workspace, create an app, give it the `chat:write` scope, install it, and copy the **bot
token** (`xoxb-…`). Invite the bot to the target channel (e.g. `#alerts`).

## 2. Reference the token from the environment

Secrets never live in the TOML in clear text — reference them with `${VAR}`:

```bash
export CHECKOUT_SLACK_BOT_TOKEN=xoxb-your-real-token
```

## 3. Define the project and rule

Create `projects/checkout.toml`. The `[project]` block scopes every rule to `service = "checkout"`
by default; `[notifications.slack]` is the channel; the rule fires when more than 5% of at least 50
log records in a 5-minute window are errors (`severity_min = 17` is OTLP ERROR).

```toml
[project]
id = "checkout"
name = "Checkout"
service = "checkout"          # default service filter for this project's rules

[alerting]
eval_interval = "60s"

[notifications.slack]
bot_token = "${CHECKOUT_SLACK_BOT_TOKEN}"
channel = "#alerts"

[[alerting.rules]]
name = "High error rate"
kind = "error_ratio"
source = "logs"
severity_min = 17            # OTLP ERROR and above
min_count = 50               # ignore low-traffic noise
window_secs = 300
comparator = "gt"
threshold = 0.05             # 5%
cooldown_secs = 300          # don't re-fire for 5 min
severity = "critical"
```

Point `datacat.toml` at the projects directory (it loads every `projects/*.toml`):

```toml
[projects]
dir = "projects"
```

## 4. Restart and watch it fire

Restart the backend so it picks up the project. Datacat starts one alerting evaluator for
`checkout`. Now generate errors — replay the error log from the previous tutorial in a loop until
you cross 50 records with >5% errors in the window:

```bash
for i in $(seq 1 60); do
  curl -s -X POST http://localhost:8080/v1/logs \
    -H 'Authorization: Bearer dev-logs-token' -H 'Content-Type: application/json' \
    -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeLogs":[{"logRecords":[{"severityText":"ERROR","severityNumber":17,"body":{"stringValue":"payment gateway timeout"}}]}]}]}' \
    > /dev/null
done
```

Within one evaluation interval, a message lands in `#alerts`. When the rate falls back below the
threshold, Datacat posts a **resolved** follow-up.

## Going further

- Other rule kinds: latency percentiles (`metric_threshold` + `agg = "p95"`), heartbeats
  (`telemetry_count`), spikes (`relative_change`), first-seen errors (`log_new_signature`),
  statistical anomalies (`anomaly`), and `composite` (AND/OR) — see [Alerting](../../alerting/).
- Route the same rule to e-mail or a webhook by adding `[[alerting.rules.actions]]` blocks.
- Keep main and remote nodes honest with the [Companion heartbeat](../../companion/).
