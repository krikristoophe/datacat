---
title: "SDKs"
description: "Send product events from the web (TypeScript) and mobile (Flutter/Dart) SDKs."
---

Datacat ships two client SDKs that emit product **events** conforming to the same wire format: a
web SDK in TypeScript and a mobile SDK in Dart (Flutter-compatible). Both freeze `event_id` and
`timestamp_client` at call time, batch automatically, retry idempotently, and renew the ingestion
token on their own.

## The token, not a secret

Neither SDK contains a secret. At runtime each one calls a `getToken` callback you provide, which
fetches a **short-lived JWT** from your already-authenticated backend; the SDK attaches it as
`Authorization: Bearer`. Datacat verifies the signature with the **public key only**, so the
exposed ingestion endpoint can never forge a token. The JWT carries `actor_id` and `session_id`
(required) plus an optional `tenant_id`.

The SDKs cache the token, refresh it ~30 s before expiry (by decoding `exp`), and refresh again on
a `401`. Read the [token](../token/) issuance specification and the [contract](../contract/) for the
authoritative claim and wire-format details.

## TypeScript (web)

Package: `@datacat/sdk-web` (source under `sdks/typescript/`). Zero runtime dependencies; targets
modern browsers and Node 24+.

```bash
npm install @datacat/sdk-web
```

Minimal init and a `track()` call:

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const analytics = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  // getToken MUST fetch a JWT from YOUR backend — never embed a token in source.
  getToken: () =>
    fetch("/api/analytics-token", { credentials: "include" })
      .then((r) => r.json())
      .then((d) => d.token),
  actorId: "user-123",     // or set later via identify()
  tenantId: "clinic-42",   // optional (B2B multi-tenant)
});

// Once the user is authenticated, (re)set the identity:
analytics.identify({ actorId: "user-123", tenantId: "clinic-42" });

// Emit a business event (queued and sent in batches):
analytics.track("validate_planning", { planning_id: 42, count: 3 });

// Force a send if needed; flush + teardown on app exit:
await analytics.flush();
await analytics.shutdown();
```

The end-of-session flush is handled for you via `visibilitychange` / `pagehide` / `beforeunload`,
preferring `fetch(..., { keepalive: true })` and falling back to `navigator.sendBeacon`. A `redact`
hook lets you strip sensitive fields from `properties` before they ever leave the client. See
`sdks/typescript/README.md` for the full option table and a React provider example.

## Flutter / Dart (mobile)

Package: `datacat_sdk` (source under `sdks/flutter/`). Pure-Dart core with optional Flutter
lifecycle integration — `dart pub get` and `dart test` work without the Flutter SDK.

```yaml
# pubspec.yaml
dependencies:
  datacat_sdk: ^0.1.0
```

Minimal init and a `track()` call:

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    // NEVER hard-code the token. Fetch it from your backend at runtime.
    getToken: () => myBackend.getAnalyticsToken(),
    actorId: currentUser?.id,        // optional — or call identify() after login
    tenantId: currentUser?.tenantId, // optional
  ),
);

// After authentication (B2B apps often log in after the SDK is created):
analytics.identify(actorId: user.id, tenantId: user.tenantId);

// Track an event with properties:
analytics.track('button_tapped', { 'button_id': 'validate_planning', 'planning_id': 42 });
```

Events tracked with **no** actor (no `actorId` in config and no `identify()` call) are dropped and
the `onError` callback is invoked — `actor_id` is required by the contract. In a Flutter app, flush
when the app is backgrounded so the OS does not kill the process mid-batch:

```dart
@override
void didChangeAppLifecycleState(AppLifecycleState state) {
  if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
    analytics.flush();
  }
}
```

The default in-memory session storage does not survive restarts; provide a
`shared_preferences`-backed `DatacatStorage` to persist the `session_id`. See
`sdks/flutter/README.md` for the storage adapter and configuration reference.

## Common contract

Both SDKs produce events conforming to the same [contract](../contract/): a batch
`{ "events": [ ... ] }` to `POST /v1/events`, frozen `event_id` / `timestamp_client`, idempotent
retry (same `event_id` on resend), and the [token](../token/) handling described above. `properties`
are free-form but must **never** contain sensitive data (passwords, PII, secrets) — both SDKs expose
a redaction hook for this.
