---
title: Integrate a Flutter app
description: Add Datacat product analytics to a Flutter / Dart mobile app with the datacat_sdk package.
---

Use the **`datacat_sdk`** package (pure Dart, Flutter-friendly) to send product events from a mobile
app. Like the web SDK, it batches, retries and never holds a long-lived secret — it fetches a
short-lived token from your backend.

## 1. Add the dependency

```yaml
# pubspec.yaml
dependencies:
  datacat_sdk: ^0.1.0
```

## 2. Create the client

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    // NEVER hard-code the token — fetch it from your backend at runtime.
    getToken: () => myBackend.getAnalyticsToken(),
    actorId: currentUser?.id,        // optional here — or call identify() after login
    tenantId: currentUser?.tenantId,
  ),
);
```

## 3. Identify and track

```dart
// B2B apps usually log in after the client is created:
analytics.identify(actorId: user.id, tenantId: user.tenantId);

analytics.track('button_tapped', {
  'button_id': 'validate_planning',
  'planning_id': 42,
});
```

`actor_id` is required by the [contract](../../contract/): events tracked with no actor are dropped
and the `onError` callback fires.

## 4. Flush on app lifecycle

Flush when the app is backgrounded so the OS does not kill the process mid-batch:

```dart
@override
void didChangeAppLifecycleState(AppLifecycleState state) {
  if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
    analytics.flush();
  }
}
```

## 5. Persist the session

The default `InMemoryStorage` does not survive restarts. Provide a `shared_preferences`-backed
`DatacatStorage` so the `session_id` persists across launches — see `sdks/flutter/README.md` for the
adapter.

## Best practices

- Keep secrets and PII out of `properties`; both SDKs expose a redaction hook.
- Stable, low-cardinality `event_name`s; variable data goes in `properties`.

## Next steps

- [Integrate a backend](../backend/) to mint the analytics token and send server-side events.
- [SDKs reference](../../sdks/) · [Event contract](../../contract/).
