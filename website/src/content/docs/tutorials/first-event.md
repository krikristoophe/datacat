---
title: "Tutorial: track your first event"
description: "End to end — stand up Datacat locally, send a product event from the web SDK, and confirm it landed in PostgreSQL."
---

By the end of this tutorial you will have a running Datacat instance, an event sent from the
TypeScript web SDK, and a SQL query proving it was stored — in about ten minutes, on a dev box.

You'll need Docker (for PostgreSQL) and a recent Rust toolchain. Node is optional (only for the
SDK step).

## 1. Start PostgreSQL and the backend

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

cd backend
cargo run --features dev          # listens on :8080
```

The `dev` feature lets you run with token verification disabled — never use it in production. See
[Quickstart](../../quickstart/) for the full config-file walkthrough.

Confirm it's up:

```bash
curl -s http://localhost:8080/readyz     # "ok" once the DB is reachable
```

## 2. Send an event with curl

Events are sent as a **batch** to `POST /v1/events`. Each event carries its own `event_id` — the
same id is counted exactly once, so retries are safe.

```bash
curl -s -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d '{
    "events": [{
      "event_id":         "550e8400-e29b-41d4-a716-446655440000",
      "event_name":       "appointment_booked",
      "tenant_id":        "clinic-7",
      "actor_id":         "user-123",
      "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f",
      "timestamp_client": "2026-06-22T10:15:30.123Z",
      "properties":       { "duration_ms": 412 }
    }]
  }'
# → 202 Accepted  { "received": 1 }
```

`received` is the number of events **accepted for asynchronous writing**, not the number inserted.
Send the exact same body again: you still get `received: 1`, but the database keeps a single row
(`ON CONFLICT DO NOTHING`). That is idempotence in action.

## 3. Send the same event from the web SDK

In a real app you don't hand-write `event_id`s — the SDK does. Install it and wire a token
endpoint (in production the token is a short-lived JWT minted by your authenticated backend; in
this dev run any string works because verification is off).

```bash
npm install @datacat/sdk-web
```

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const datacat = createDatacatClient({
  endpoint: "http://localhost:8080/v1/events",
  getToken: () => Promise.resolve("dev"),   // prod: fetch a real JWT from your backend
});

// actor_id is required — identify once, then track as often as you like.
datacat.identify({ actorId: "user-123", tenantId: "clinic-7" });
datacat.track("appointment_booked", { duration_ms: 412 });

// The SDK batches and flushes automatically; force it before exit in a script:
await datacat.flush();
```

The SDK freezes `event_id` and `timestamp_client` at creation, retries with backoff, and falls
back to a page-unload beacon — so events survive flaky networks and tab closes.

## 4. Confirm it landed

```bash
docker compose exec postgres \
  psql -U datacat -d datacat -c \
  "SELECT event_name, actor_id, properties FROM events ORDER BY received_at DESC LIMIT 5;"
```

You should see your `appointment_booked` row. One row — even though you sent it from both curl and
the SDK with different ids, and even if you re-ran the curl.

## Next steps

- [Instrument a service with OTLP](../instrument-a-service/) — logs, traces and metrics.
- [Alert to Slack](../alert-to-slack/) — get notified when something breaks.
- [Event contract](../../contract/) — the full wire format and token rules.
