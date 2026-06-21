# Alerting engine (pluggable actions: Slack, e-mail, webhook)

Datacat ships a lightweight alerting engine: **declarative rules** are evaluated periodically over
the ingested data (`logs`, `metric_points`, …), and each threshold crossing triggers one or more
pluggable **actions** (Slack, e-mail, generic HTTP webhook). The engine is **entirely optional**:
with no rules, or with no action or channel configured, it stays disabled (a no-op at startup).

Alerting is configured **per project**. Each project is a TOML file under `projects/*.toml` that
carries its alerting rules (`[[alerting.rules]]`) and, optionally, its own notification channels
(`[notifications.slack]` / `[notifications.email]`). Datacat runs **one evaluator per project**.
See [configuration.md](configuration.md) for how projects are loaded.

## 1. Activation

For a given project the evaluator starts if the project declares at least one rule **and** there is
at least one notification target, that is:
- a **channel** configured for the project (or, by fallback, the global `[notifications]` in
  `datacat.toml`), **or**
- at least one rule carrying its own `[[alerting.rules.actions]]` (a webhook alone is enough, with
  no global config).

Otherwise an info/warning message is logged and the evaluator is not started for that project.

### Project scope (default `service` / `tenant` filter)

A project may declare a default `service` and/or `tenant`. These are applied as **defaults** to
every rule (and every composite sub-condition) that does not set its own — so a project scoped to
`service = "billing"` implicitly targets `service=billing` on all its rules. A rule can still
override the filter explicitly.

```toml
[project]
id = "billing"
name = "Billing"
service = "billing"     # default `service` filter for this project's rules
# tenant = "acme"       # default `tenant` filter (logs / spans / events)

[alerting]
eval_interval = "60s"
```

## 2. Rule schema (`[[alerting.rules]]`)

Each rule is a `[[alerting.rules]]` table in the project file:

| Field | Required | Description |
|---|---|---|
| `name` | ✅ | human-readable name; identifies the state (ok↔firing state machine) and appears in the alert |
| `kind` | ✅ | condition type (see table below) |
| `source` | — | `logs` (default) \| `events` \| `spans` \| `metrics` — for `telemetry_count` / `error_ratio` / `relative_change` |
| `service` | — | `service.name` filter (logs/spans/metrics); defaults to the project's `service` |
| `window_secs` | ✅ | sliding window (seconds) over which the value is computed |
| `comparator` | ✅ | `gt` \| `gte` \| `lt` \| `lte` (compares the value to the threshold) |
| `threshold` | ✅ | numeric threshold (count, fraction 0..1 for ratios, ms for `span_duration`, multiplier for `relative_change`) |
| `cooldown_secs` | — | minimum duration between two notifications (default 0) |
| `severity` | — | severity of the emitted alert (free-form: `info`/`warning`/`critical`, default `warning`) |
| `severity_min` | — | minimum OTLP severity of logs (e.g. `17` = ERROR) |
| `metric_name` | (metric_threshold) | name of the evaluated metric |
| `agg` | (metric_threshold / span_duration) | `avg`\|`max`\|`min`\|`sum`\|`count`\|`last`\|`p50`\|`p90`\|`p95`\|`p99` |
| `event_name` | — | `event_name` filter (source `events`) |
| `operation` | — | filters the span's operation name (spans) |
| `error_only` | — | restricts to errors (spans: status=error; logs: severity ≥ `severity_min`/17) |
| `min_count` | — | minimum sample/baseline (`error_ratio` / `relative_change` / `anomaly`) below which it does not fire |
| `baseline_secs` | (log_new_signature / anomaly) | reference window: "known" lookback (default 24 h) / bucket duration (default 30×`window_secs`) |
| `group_by` | (log_group_count / log_new_signature) | grouping key (default `body`) — see below |
| `op` | (composite) | `all` (AND, default) \| `any` (OR) |
| `conditions` | (composite) | list of sub-conditions (scalar rules, no `name`) |
| `actions` | — | actions to trigger (slack/email/webhook). Empty ⇒ the project/global channels by default |

### Kinds (standard use cases)

| `kind` | Computes | Typical use case |
|---|---|---|
| `log_count` | log count (service, `severity_min`) over the window | "> 10 ERROR billing logs in 5 min" |
| `log_group_count` | log count **grouped by signature** (`group_by`) — one state per group | "5 **identical** errors → webhook" |
| `metric_threshold` | aggregate of a metric (`avg`/`max`/`p95`/`p99`/…) | "**p95** `http.server.duration` > 800 ms" |
| `telemetry_count` | count of rows on a `source` | **heartbeat/no-data** (`lte` 0), traffic drop (`lt`), volume spike (`gt`) |
| `error_ratio` | fraction of errors over `logs` or `spans` (`min_count` guard) | "**error rate** > 5% over ≥ 50 requests" |
| `span_duration` | aggregate of span latency (`duration_ms`, ms) | "**p99** of the `checkout` operation > 2 s" |
| `relative_change` | volume ratio current-window / previous-window | "errors **× 3** vs the previous period" |
| `composite` | combines sub-conditions via `op` (`all`=AND, `any`=OR) | "high error rate **AND** degraded p95 latency" |
| `log_new_signature` | log signature absent from the `baseline` window | "**new error** never seen in 24 h" |
| `anomaly` | z-score of volume vs sliding baseline (μ ± σ) | "**abnormal** volume (+3σ) vs history" |

Useful details:

- **`log_group_count`** — `group_by` is **allow-listed** (anti-injection): `body`, `service_name`,
  `severity_text`, `trace_id`, or `attr:<key>` (a log attribute, e.g. `attr:error.code`). One alert
  per signature, carrying its `group_key`.
- **`telemetry_count`** — covers three needs via the comparator: `lte 0` = *dead man's switch* (no
  data received = a silent/down service), `lt N` = traffic drop, `gt N` = spike. The `source`
  selects the table (logs/events/spans/metrics).
- **`error_ratio`** — value ∈ [0, 1]. Numerator = rows in error (logs: severity ≥
  `severity_min`/17; spans: status=error), denominator = total. If the total < `max(min_count, 1)`,
  the value is 0 (we don't alert on 1 error / 1 request).
- **`span_duration`** — default `agg` is `p95`. Filterable by `service`, `operation`, `error_only`.
- **`relative_change`** — the previous window is `[now-2w, now-w]`. The baseline is capped by
  `max(min_count, 1)` to avoid false spikes when there is no history.
- **percentiles** (`p50`/`p90`/`p95`/`p99`) — available for `metric_threshold` **and**
  `span_duration` (via `percentile_cont`).
- **`composite`** — each sub-condition is a scalar rule (`error_ratio`, `span_duration`,
  `telemetry_count`, `metric_threshold`, `relative_change`, `anomaly`, `log_count`) with its own
  window/threshold. `op=all` (AND) fires when **all** are crossed, `op=any` (OR) as soon as **one**
  is. Grouped kinds (`log_group_count`, `log_new_signature`) and nested composites are forbidden as
  a sub-condition. The alert value = the number of sub-conditions crossed.
- **`log_new_signature`** — detects the **first appearance** of a signature (`group_by`): present
  in the current window but **absent** from `[now-baseline_secs, now-window]`. One state per
  signature (like `log_group_count`); `threshold`/`comparator` set a minimum number of recent
  occurrences (typically `gte 1`). When the signature ages into the baseline, the alert resolves.
- **`anomaly`** — splits `[now-baseline_secs, now-window]` into buckets of `window_secs` (zeros
  included), computes the mean μ and standard deviation σ of the volume, then the **z-score** of the
  current volume `(current-μ)/σ`. `comparator gt 3` = a +3σ spike; `lt -3` = a drop. Returns 0 (no
  alert) if the history has < 3 buckets, if μ < `min_count`, or if σ ≈ 0 (zero variance =
  undecidable).

### Example (project file)

```toml
[project]
id = "api"
name = "API"
service = "api"           # default service filter for the rules below

[alerting]
eval_interval = "60s"

# Channel for this project (else fall back to the global [notifications]).
[notifications.slack]
bot_token = "${API_SLACK_BOT_TOKEN}"
channel = "#alerts"

# 5 identical errors (grouped by message) -> webhook + Slack.
[[alerting.rules]]
name = "repeated identical errors"
kind = "log_group_count"
severity_min = 17
group_by = "body"
window_secs = 300
comparator = "gte"
threshold = 5
cooldown_secs = 300
severity = "critical"

[[alerting.rules.actions]]
type = "webhook"
url = "https://hooks.internal/alert"
headers = { Authorization = "Bearer ${INTERNAL_HOOK_TOKEN}" }

[[alerting.rules.actions]]
type = "slack"

# Error rate > 5% over ≥ 50 requests (spans).
[[alerting.rules]]
name = "api error rate"
kind = "error_ratio"
source = "spans"
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05
cooldown_secs = 300
severity = "critical"

# p95 latency of the checkout operation.
[[alerting.rules]]
name = "checkout p95 latency"
kind = "span_duration"
agg = "p95"
operation = "checkout"
window_secs = 300
comparator = "gt"
threshold = 2000
cooldown_secs = 600
severity = "warning"

# Ingestion heartbeat (dead man's switch).
[[alerting.rules]]
name = "ingestion heartbeat"
kind = "telemetry_count"
source = "metrics"
window_secs = 300
comparator = "lte"
threshold = 0
cooldown_secs = 600
severity = "critical"

# Error spike vs the previous period.
[[alerting.rules]]
name = "error spike"
kind = "relative_change"
source = "logs"
severity_min = 17
min_count = 20
window_secs = 300
comparator = "gt"
threshold = 3
cooldown_secs = 600
severity = "warning"

# p99 latency (metric).
[[alerting.rules]]
name = "p99 latency (metric)"
kind = "metric_threshold"
metric_name = "http.server.duration"
agg = "p99"
window_secs = 300
comparator = "gt"
threshold = 900
cooldown_secs = 600

# api incident (error rate AND latency).
[[alerting.rules]]
name = "api incident (error rate AND latency)"
kind = "composite"
op = "all"
severity = "critical"
cooldown_secs = 600

[[alerting.rules.conditions]]
kind = "error_ratio"
source = "spans"
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05

[[alerting.rules.conditions]]
kind = "span_duration"
agg = "p95"
window_secs = 300
comparator = "gt"
threshold = 2000

# New error (never seen in 24 h).
[[alerting.rules]]
name = "new error (never seen in 24h)"
kind = "log_new_signature"
severity_min = 17
group_by = "body"
baseline_secs = 86400
window_secs = 600
comparator = "gte"
threshold = 1
cooldown_secs = 0

# Abnormal log volume.
[[alerting.rules]]
name = "abnormal log volume"
kind = "anomaly"
source = "logs"
severity_min = 17
baseline_secs = 18000
min_count = 5
window_secs = 300
comparator = "gt"
threshold = 3
cooldown_secs = 600
```

The rules above inherit `service = "api"` from the project; none of them needs to repeat it.

## 3. Evaluation, state machine and cooldown

A background task evaluates all of a project's rules every `[alerting].eval_interval` (default
`60s`). For each rule the engine maintains a **state** (`ok` ↔ `firing`):

- **ok → firing** (the threshold has just been crossed): a `[FIRING]` alert is notified;
- **firing → ok** (the condition no longer holds): a `[RESOLVED]` alert is notified;
- no transition: nothing is sent (no spam on every evaluation).

The **cooldown** (`cooldown_secs`) bounds the frequency: after a notification, no new notification
is emitted for that state key until the cooldown expires — including a resolution. Thus two close
evaluations of the same rule in `firing` notify **only once**.

For `log_group_count`, the state (and the cooldown) is maintained **per group** (internal key
`<rule>::<group_key>`): each signature has its own ok↔firing state machine. A group previously in
alert but gone from the window is resolved (its count fell back to 0).

The function `evaluate_once(&pool, &rules, &mut state, &dispatcher, now)` is exposed and
**testable** (the clock `now` is injected), which makes the threshold and cooldown logic
deterministic. The `Dispatcher` resolves, for each rule, the notifiers to trigger (its `actions`,
otherwise the project's default channels).

## 4. Actions (pluggable, per rule)

Each transition triggers a set of **notifiers**. The textual content is a single-line message
`[FIRING] <name> (<severity>) [<group_key>] — <condition> (value=…, threshold=…)`; webhooks
additionally receive a structured JSON payload:

```json
{ "rule": "...", "severity": "...", "state": "FIRING|RESOLVED", "value": 5.0,
  "threshold": 5.0, "description": "...", "group_key": "...", "summary": "..." }
```

A rule's `actions` field **explicitly** declares its targets. If `actions` is empty, the rule falls
back to the **default channels** (Slack and/or e-mail configured for the project, otherwise the
global `[notifications]`). Three action types:

| Type | Fields | Behavior |
|---|---|---|
| `slack` | `channel` (optional) | Posts via the configured bot to `chat.postMessage`; `channel` overrides the configured channel. Reuses the project/global Slack bot token |
| `email` | `to` (optional) | SMTP e-mail; `to` overrides the default recipients. Reuses the project/global SMTP config |
| `webhook` | `url` (required), `headers` (optional) | POST of the JSON payload above, with arbitrary headers (e.g. `Authorization`) |

A single rule may combine several actions (e.g. an internal webhook **and** Slack). A misconfigured
action (e.g. `email` with no SMTP config, `slack` with no bot token) is logged and then skipped,
without failing the others.

### Notification channels (`[notifications.*]`)

Channels are resolved per project: a project uses its own `[notifications.*]` if present, otherwise
the global `[notifications]` from `datacat.toml`. They are used by rules without `actions`, and they
serve as the fallback/base config for actions. Secrets are passed via `${ENV}` references.

```toml
# In a project file (projects/api.toml) — or globally in datacat.toml.
[notifications.slack]
bot_token = "${SLACK_BOT_TOKEN}"               # Slack bot token (xoxb-…)
channel = "#alerts"                            # target channel

[notifications.email]
smtp_host = "smtp.example.com"
smtp_port = 587                                # STARTTLS
username = "${SMTP_USERNAME:-}"
password = "${SMTP_PASSWORD:-}"
from = "Datacat <alerts@example.com>"
to = ["ops@example.com"]
```

Datacat sends Slack notifications through the **Slack Web API** (`chat.postMessage`), not legacy
incoming webhooks. It POSTs to `https://slack.com/api/chat.postMessage` with
`Authorization: Bearer <bot_token>` and a body `{ "channel": …, "text": … }`, then checks the `ok`
field of the JSON response. The Slack bot needs the `chat:write` scope and must be **invited to the
target channel**. The Slack channel is only enabled if both `bot_token` and `channel` are provided.

The SMTP transport uses **STARTTLS via rustls** (no OpenSSL). The e-mail channel is only enabled if
`smtp_host`, `from` and at least one `to` recipient are provided.

## 5. Configuration (summary)

| Setting | Where | Role |
|---|---|---|
| `[[alerting.rules]]` | `projects/*.toml` | the project's rules (enables its evaluator) |
| `[alerting].eval_interval` | `projects/*.toml` | evaluation period (default `60s`) |
| `[project].service` / `[project].tenant` | `projects/*.toml` | default rule filter for the project |
| `[notifications.slack]` | project, else `datacat.toml` | Slack bot token + channel |
| `[notifications.email]` | project, else `datacat.toml` | SMTP relay + sender/recipients |
| `[projects].dir` / `[projects].files` | `datacat.toml` | which project files to load |

All settings are optional; the engine disables itself cleanly for a project whose configuration is
incomplete. See [configuration.md](configuration.md) for the full configuration model and secret
expansion.
