# Configuration

Datacat is configured through a single **TOML file** (`datacat.toml`). It describes the whole
deployment (server, database, ingestion, security, query layer, MCP, cold export) and points at
**one TOML file per project** under `projects/*.toml`, each carrying that project's alerting rules
and notification channels.

Secrets are never written in clear text: every string value may reference an environment variable
with `${VAR}` (or `${VAR:-default}`), resolved at startup. A required `${VAR}` with no default makes
the service refuse to start (fail-closed) — an HDS requirement.

A ready-to-copy template lives at [`datacat.example.toml`](../datacat.example.toml), with an example
project in [`projects/example.toml`](../projects/example.toml).

## 1. File resolution

At startup the file is looked up in this order:

1. `$DATACAT_CONFIG` (explicit path),
2. `./datacat.toml` (current directory),
3. `/etc/datacat/datacat.toml`.

If **no** file is found, Datacat falls back to the legacy **environment-variable** configuration
(`BIND_ADDR`, `DATABASE_URL`, …), which is convenient for development and the test suite. In that
mode, a single project named `default` is derived from the `ALERT_*` variables.

## 2. Secret expansion

Any string in the TOML — top-level config **and** project files — is scanned for `${...}`:

| Form | Behaviour |
|---|---|
| `${VAR}` | replaced by the value of `VAR`; **error** if unset (fail-closed) |
| `${VAR:-default}` | replaced by `VAR`, or `default` if unset |
| `prefix-${VAR}-suffix` | partial expansion is supported |

```toml
[database]
url = "${DATABASE_URL}"

[notifications.slack]
bot_token = "${SLACK_BOT_TOKEN}"         # Slack bot token (Web API, chat.postMessage)
```

This keeps every secret out of version control. Real `datacat.toml` and `projects/*.toml` files are
git-ignored; only the `*.example.toml` templates are committed.

## 3. Global configuration (`datacat.toml`)

All sections are optional except `[database].url`; omitted values use safe defaults.

| Section | Purpose |
|---|---|
| `[server]` | `bind_addr`, `request_timeout`, `trust_forwarded_for`; `[server.grpc]` (OTLP/gRPC), `[server.cors]` |
| `[database]` | `url` (required), `max_connections` |
| `[ingest]` | micro-batch (`flush_interval`, `flush_batch_size`, `channel_capacity`), `retention_days`, `partition_future_days`; sub-sections `[ingest.limits]`, `[ingest.rate_limit]`, `[ingest.anomaly]` |
| `[token]` | asymmetric token verification (public key only) — `enabled`, `algorithms`, key source (`jwks_url` \| `public_key_pem` \| `public_key_file`), `issuer`, `audience` |
| `[auth.logs]` / `[auth.query]` | service-to-service auth for telemetry ingestion and for the read endpoints — `mode` (`auto`\|`static`\|`jwt`\|`none`) + `static_token` |
| `[mcp]` | embedded MCP HTTP server (`enabled`) |
| `[export]` | scheduled cold export (see §5) |
| `[notifications]` | global default Slack / e-mail channels (fallback for projects without their own) |
| `[projects]` | where to load project files (`dir` and/or `files`) |

### Token key source

Exactly one public-key source is used, in priority order: `jwks_url`, then `public_key_pem`, then
`public_key_file`. With `enabled = true` and no source, startup fails.

```toml
[token]
enabled = true
algorithms = ["EdDSA", "RS256"]
public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"
alg = "EdDSA"
# or: jwks_url = "https://issuer.example.com/.well-known/jwks.json"
```

### Ingestion limits (`[ingest.limits]`)

All fields are optional and default to safe values.

| Field | Default | Purpose |
|---|---|---|
| `max_batch_events` | `500` | events per `/v1/events` request |
| `max_payload_bytes` | `1048576` | max body size of an events request |
| `max_properties_bytes` | `16384` | max size of an event's `properties` JSON |
| `max_string_len` | `200` | max length of an event string field |
| `max_json_depth` | `16` | max nesting depth of event payloads |
| `max_past_skew` | `"31d"` | oldest accepted timestamp |
| `max_future_skew` | `"24h"` | furthest-ahead accepted timestamp |
| `max_otlp_record_bytes` | `65536` | **per-record** size cap for OTLP logs/spans/metric points |

`max_otlp_record_bytes` is a defence-in-depth guard: the request body is already bounded by
`max_logs_payload_bytes`, but a single oversized record (a huge log body, a span with thousands of
events, an attribute blob) is dropped individually rather than letting one record dominate a batch.
Dropped records are counted in `dropped_oversized_total` (exposed on `/stats`) and logged at
`warn`. This is tolerated loss (§2) — never a duplicate, never a hard failure of the whole request.

## 4. Projects (`projects/*.toml`)

A **project** groups alerting rules and notification channels, optionally scoped by a default
`service` / `tenant` filter. Datacat runs **one alerting evaluator per project**. The ingestion
pipeline and stored data are shared (project isolation is at the configuration level, not the data
level).

`[projects]` selects which files to load:

```toml
[projects]
dir = "projects"                       # load every *.toml in this directory
# files = ["projects/billing.toml"]    # and/or explicit files
```

A project file:

```toml
[project]
id = "billing"
name = "Billing"
service = "billing"     # default `service` filter for this project's rules
# tenant = "acme"       # default `tenant` filter (logs / spans / events)

[alerting]
eval_interval = "60s"

# Channels for this project (else fall back to the global [notifications]).
[notifications.slack]
bot_token = "${BILLING_SLACK_BOT_TOKEN}"
channel = "#billing-alerts"

[[alerting.rules]]
name = "High error rate"
kind = "error_ratio"
severity_min = 17
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05
cooldown_secs = 300
severity = "critical"

[[alerting.rules.actions]]
type = "slack"
```

- The project's `service` / `tenant` are applied as **defaults** to every rule (and composite
  sub-condition) that does not set its own — so the rules above implicitly target `service=billing`.
- Notification resolution: a project uses its own `[notifications.*]` if present, otherwise the
  global `[notifications]` from `datacat.toml`.
- Rule schema (kinds, comparators, actions) is documented in [alerting.md](alerting.md).

## 5. Scheduled cold export

When the `export` Cargo feature is compiled (on by default) and `[export].enabled = true`, the
backend runs a background task that exports the **previous UTC day** to Parquet on S3-compatible
storage on each tick.

```toml
[export]
enabled = true
schedule = "24h"
bucket = "datacat-cold"
prefix = ""
region = "eu-west-1"
endpoint = "${S3_ENDPOINT:-}"           # empty = AWS S3; set for MinIO/compatible
access_key_id = "${AWS_ACCESS_KEY_ID:-}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY:-}"
allow_http = false
tables = ["events", "logs"]
```

The export is idempotent: re-running a day overwrites its object. See
[cold-storage.md](cold-storage.md) for the on-disk layout and the standalone exporter CLI.

## 6. Environment-variable fallback (legacy)

Without a `datacat.toml`, every setting comes from environment variables (see
[`.env.example`](../.env.example) for the full list). This path is intended for development and
tests; production deployments should use the TOML file with `${ENV}` secret references.
