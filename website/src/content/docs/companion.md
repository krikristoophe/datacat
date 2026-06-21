---
title: "Companion liveness"
description: "A multi-node dead-man's switch: the main instance and remote companions watch each other."
---

A **companion** is a lightweight agent deployed on a *separate* node (another host, region or
cloud). It and the Datacat **main** instance watch each other's liveness, so a network partition or
a down node is turned into an alert. Any number of companions are supported, each identified by a
stable `id`.

The mechanism is **bidirectional** on purpose — a down main cannot alert about itself:

- **Companion → main:** the companion POSTs a heartbeat to `POST /v1/heartbeat` every `interval`.
  The main instance records the last-seen time per `id`; a background monitor raises an alert when a
  companion goes silent longer than its `timeout`, and resolves it when the companion returns.
- **Main → companion:** if the companion cannot reach the main instance for `failure_threshold`
  consecutive attempts, it raises **its own** alert (through its own Slack/webhook channel), and a
  recovery alert when connectivity returns.

## 1. Heartbeat endpoint

| Method | Endpoint | Auth | Body |
|---|---|---|---|
| `POST` | `/v1/heartbeat` | service-to-service (`[auth.logs]`) | `{ "id": "<companion_id>" }` |

Returns `204 No Content`. The companion id is recorded in an in-memory registry (soft state: after a
main restart, companions re-check-in within one `timeout` window). The current registry is visible in
the authenticated `/stats` response under `companions`.

## 2. Main-side configuration (`datacat.toml`)

```toml
[companions]
check_interval = "30s"          # how often the monitor checks liveness

[[companions.expected]]
id = "edge-eu"
timeout = "90s"                 # silence longer than this ⇒ alert
severity = "critical"

[[companions.expected]]
id = "edge-us"
timeout = "90s"
```

Companion-down alerts are sent through the **global** `[notifications]` channels (Slack / e-mail).
If no expected companion is configured, or no global channel exists, the monitor stays disabled.

## 3. The companion agent (`datacat-companion`)

The standalone `companion/` crate runs on the remote node. Its config (`companion.toml`, template
`companion.example.toml`) sets the main URL, its `id`, the heartbeat token (from the environment),
the send `interval`, the `failure_threshold`, and its own self-alert channel (Slack bot or generic
webhook) used when it cannot reach main. Secrets are referenced from the environment with `${VAR}`,
like the main config.

```toml
main_url = "https://datacat.example.com"
id = "edge-eu"
token = "${DATACAT_HEARTBEAT_TOKEN}"
interval = "30s"
failure_threshold = 3

[alert.slack]                   # or [alert.webhook] url = "..."
bot_token = "${SLACK_BOT_TOKEN}"
channel = "#alerts"
```

## 4. Why bidirectional

If a region hosting a companion goes dark, the **main** monitor fires (no heartbeats). If the
**main** instance (or the network to it) goes dark, every companion fires locally. Either failure
mode surfaces an alert from the side that is still up — there is no single point whose death silences
the alarm.
