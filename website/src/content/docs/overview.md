---
title: What is Datacat?
description: Datacat in a nutshell — what you can do with it, and where to go next depending on what you want to ship.
---

Datacat is a **self-hosted ingestion platform** for product analytics and observability. You run it
on your own infrastructure (PostgreSQL is the only dependency), point your apps and services at it,
and keep full control of your users' data — useful when that data is sensitive or regulated
(health, HDS, GDPR).

## What you can do with it

- **Capture product events** — what users do in your app — from web and mobile, with strict
  idempotency so retries never double-count.
- **Collect observability** — logs, traces and metrics over OpenTelemetry (OTLP) — from your
  services, correlated with those product events by tenant, user and session.
- **Read your data** — query recent data from PostgreSQL (hot) or long-term data exported to
  Parquet on S3 (cold), or let an AI agent explore it through the MCP server.
- **Get alerted** — per-project rules on error rates, latency, anomalies and more, routed to Slack,
  e-mail or webhooks.

Everything runs on a database you already know, with no Kafka, ClickHouse or Zookeeper to operate.

## Where to start

- **Just trying it?** Run it locally in minutes with the [Quickstart](../quickstart/), then
  [track your first event](../tutorials/first-event/).
- **Adding it to your product?** Pick your surface in **Integrate**:
  [web app](../integrate/web-app/), [backend](../integrate/backend/),
  [Flutter](../integrate/flutter/), or an existing
  [OpenTelemetry](../integrate/opentelemetry/) setup.
- **Putting it in production?** See [Installation](../installation/), [Configuration](../configuration/)
  and [Deployment](../deployment/).

Looking for the exact wire format, token rules or internals? Those live under **Reference**
([contract](../contract/), [architecture](../architecture/), [security](../security/)).
