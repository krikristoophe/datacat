---
title: Integrate a backend service
description: Send product events and OpenTelemetry logs, traces and metrics to Datacat from any server-side language over HTTP.
---

From a backend you typically send two things to Datacat:

1. **Product events** — to `POST /v1/events`, authenticated with a short-lived **JWT** your service
   mints for the acting user.
2. **Telemetry** (logs, traces, metrics) — to the OTLP endpoints, authenticated with a **static
   service token**.

No SDK is required: it is plain HTTP/JSON, so any language works.

## Send product events

Events are sent as a **batch**. Each carries its own `event_id` — reuse the same id on a retry and
it is counted exactly once (`ON CONFLICT DO NOTHING`).

```ts
// Node — any HTTP client works the same way.
await fetch("https://ingest.example.com/v1/events", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: `Bearer ${await mintAnalyticsToken(user)}`,
  },
  body: JSON.stringify({
    events: [
      {
        event_id: crypto.randomUUID(),
        event_name: "invoice_paid",
        tenant_id: "clinic-7",
        actor_id: user.id,
        session_id: sessionId,
        timestamp_client: new Date().toISOString(),
        properties: { amount_eur: 42 },
      },
    ],
  }),
});
// → 202 Accepted { "received": 1 }
```

The JWT is signed by your backend and verified by Datacat with the **public key only**. See
[token](../../token/) for the expected claims and algorithms.

## Send telemetry (logs, traces, metrics)

Telemetry uses the standard OTLP endpoints (`/v1/logs`, `/v1/traces`, `/v1/metrics`) and the static
service token from `[auth.logs]`. The simplest path is to point your existing OpenTelemetry SDK at
Datacat — see [Integrate OpenTelemetry](../opentelemetry/). To send by hand:

```bash
curl -X POST https://ingest.example.com/v1/logs \
  -H "Authorization: Bearer $DATACAT_SERVICE_TOKEN" \
  -H "Content-Type: application/json" \
  -d @log.otlp.json
```

Lift `tenant_id`, `actor_id` and `session_id` into resource or record attributes so your telemetry
correlates with product events.

## Choosing the token

| Surface | Endpoint | Auth |
|---|---|---|
| Product events | `/v1/events` | short-lived **JWT** per user (asymmetric) |
| Logs / traces / metrics | `/v1/logs`, `/v1/traces`, `/v1/metrics` | **static service token** |

## Next steps

- [Integrate OpenTelemetry](../opentelemetry/) — reuse your existing instrumentation.
- [Event contract](../../contract/) — the exact wire format and limits.
- [Tutorial: instrument a service](../../tutorials/instrument-a-service/).
