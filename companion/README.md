# datacat-companion

Remote companion agent — the remote half of a bidirectional dead-man's-switch.

This binary runs on a remote node and periodically POSTs a heartbeat to a Datacat **main**
instance. If it cannot reach main for several consecutive attempts, it raises **its own** alert
(Slack or webhook) — because a down main cannot alert about itself. The main instance separately
alerts when a companion goes silent (that half lives in the backend, `backend/src/companion/`).

## Protocol

```
POST {main_url}/v1/heartbeat
Authorization: Bearer {token}
Content-Type: application/json

{"id": "<companion_id>"}
```

Success is any HTTP 2xx (main returns 204). A per-request timeout of ~10s applies.

## Usage

```bash
# Config path: $DATACAT_COMPANION_CONFIG, then ./companion.toml
cp companion.example.toml companion.toml   # then edit
export DATACAT_HEARTBEAT_TOKEN=...          # secret referenced by the default token = "${...}"
datacat-companion
```

The agent runs until SIGINT (Ctrl-C) or SIGTERM.

## Configuration (`companion.toml`)

| Field               | Required | Default                       | Description |
|---|---|---|---|
| `main_url`          | yes      | —                             | Base URL of the Datacat main instance |
| `id`                | yes      | —                             | Stable companion id (matches `[[companions.expected]].id` on main) |
| `token`             | yes      | `"${DATACAT_HEARTBEAT_TOKEN}"`| Bearer heartbeat token (use `${ENV}`) |
| `interval`          | no       | `"30s"`                       | Heartbeat interval (`ms`/`s`/`m`/`h`/`d`) |
| `failure_threshold` | no       | `3`                           | Consecutive failures before self-alert |

Exactly one self-alert channel must be configured:

- `[alert.slack]` — `bot_token` + `channel`. POSTs to `https://slack.com/api/chat.postMessage`
  with `Authorization: Bearer <bot_token>` and `{channel, text}`; checks the `ok` field.
- `[alert.webhook]` — `url` + optional `[alert.webhook.headers]`. POSTs JSON
  `{"id", "state": "firing"|"resolved", "text"}`.

Every string value supports `${VAR}` / `${VAR:-default}` expansion; required variables fail
closed (the agent refuses to start with an unresolved secret).

## Tests

```bash
cd companion
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```
