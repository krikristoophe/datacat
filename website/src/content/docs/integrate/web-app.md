---
title: Integrate a web app
description: Add Datacat product analytics to a web front-end with the TypeScript SDK — token handling, identify/track, and best practices.
---

Use the **`@datacat/sdk-web`** client to send product events from a browser app (React, Vue,
Svelte, or vanilla JS). The SDK batches events, retries on flaky networks, and flushes on page
unload — you just call `identify()` and `track()`.

## 1. Install

```bash
npm install @datacat/sdk-web
```

## 2. Create the client

The SDK never holds a long-lived secret. It calls your **`getToken`** callback to fetch a
short-lived JWT from *your* backend, which mints it for the logged-in user and signs it with a key
Datacat verifies by public key only (see [token](../../token/)).

```ts
import { createDatacatClient } from "@datacat/sdk-web";

export const datacat = createDatacatClient({
  // Full URL of the ingestion endpoint, including /v1/events.
  endpoint: "https://ingest.example.com/v1/events",
  // Fetch a fresh token from your backend; the SDK renews it ~30s before expiry and on 401.
  getToken: () => fetch("/api/analytics-token").then((r) => r.text()),
  // Optional: strip sensitive fields before anything leaves the browser.
  redact: (props) => ({ ...props, email: undefined }),
});
```

## 3. Identify, then track

`actor_id` is required, so call `identify()` once you know who the user is (typically right after
login). Events tracked before identify — with no actor — are dropped and reported via the error
callback.

```ts
// After authentication:
datacat.identify({ actorId: user.id, tenantId: user.clinicId });

// Anywhere in your UI:
datacat.track("appointment_booked", { duration_ms: 412, channel: "web" });
```

The SDK assigns each event a frozen `event_id` and `timestamp_client`, batches them, and flushes on
a timer and on page unload (via `navigator.sendBeacon`). Resends of the same `event_id` are
deduplicated server-side, so retries never inflate your numbers.

## 4. Framework notes

- **React / SPA**: create the client once (module scope or a context provider), call `identify()`
  in your auth effect, and `track()` from event handlers. Don't recreate the client per render.
- **SSR / Next.js**: only instantiate in the browser (guard with `typeof window !== "undefined"`),
  or inject a no-op `StorageAdapter` on the server.
- **Manual flush**: `await datacat.flush()` before a hard navigation you control (e.g. a full-page
  redirect after checkout).

## Best practices

- **Never put secrets or PII in `properties`** (passwords, tokens, full names, emails). Use the
  `redact` hook to enforce it centrally.
- Keep `event_name` a stable, low-cardinality verb (`appointment_booked`, not
  `appointment_booked_42`); put the variable parts in `properties`.
- The token endpoint is yours to build: authenticate the user, then return a short-lived JWT. See
  [token](../../token/) for the claims Datacat expects.

## Next steps

- [Integrate a backend](../backend/) to send server-side events and telemetry.
- [Tutorial: track your first event](../../tutorials/first-event/) for an end-to-end run.
- [SDKs reference](../../sdks/) for every option and the Flutter client.
