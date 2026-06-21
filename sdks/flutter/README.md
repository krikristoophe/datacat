# datacat_sdk

Dart/Flutter analytics SDK for [Datacat](https://github.com/datacat) — pure-Dart core, optional Flutter lifecycle integration.

## Features

- UUID v4 `event_id` generated once at creation, frozen on retry (idempotent)
- `timestamp_client` frozen at creation and reused verbatim on every retry
- Automatic batching (configurable size + interval)
- Exponential back-off retry for network errors, 5xx, and 429 (respects `Retry-After`)
- Permanent drop of events on 400 Bad Request (with `onError` callback)
- JWT token caching with proactive renewal (30 s before expiry) and reactive renewal on 401
- Injectable HTTP client and storage — testable without Flutter
- Session ID generated and persisted via pluggable storage interface

## Installation

Add to `pubspec.yaml`:

```yaml
dependencies:
  datacat_sdk: ^0.1.0
```

The package is pure Dart: `dart pub get` and `dart test` work without the Flutter SDK.

## Usage

### Minimal example (Flutter)

```dart
import 'package:datacat_sdk/datacat_sdk.dart';
import 'package:flutter/widgets.dart';

class _AppState extends State<MyApp> with WidgetsBindingObserver {
  late final DatacatClient _analytics;

  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    _analytics = DatacatClient(
      config: DatacatConfig(
        endpoint: 'https://ingest.example.com/v1/events',
        // NEVER hard-code the token. Fetch it from your backend at runtime.
        getToken: () => myBackend.getAnalyticsToken(),
        // actorId is optional here — in B2B apps the user often logs in AFTER
        // the SDK is created. Set it now if known, or call identify() later.
        actorId: currentUser?.id,
        tenantId: currentUser?.tenantId, // optional
        // storage: SharedPrefsStorage(), // see below for persistent session
      ),
    );
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.paused ||
        state == AppLifecycleState.detached) {
      // Flush remaining events before the OS may kill the process.
      _analytics.flush();
    }
  }

  @override
  void dispose() {
    WidgetsBinding.instance.removeObserver(this);
    _analytics.dispose(); // also flushes
    super.dispose();
  }
}
```

### Identifying the actor

In B2B apps the user usually authenticates *after* the SDK is created. Call
`identify()` once the actor is known — every event tracked afterwards carries
the new identity:

```dart
// e.g. inside your login success handler
await auth.signIn(...);
_analytics.identify(actorId: user.id, tenantId: user.tenantId);
```

Events tracked **before** any actor is set (no `actorId` in config and no
`identify()` call) are **dropped**, and the `onError` callback is invoked —
`actor_id` is required by the ingestion contract. The SDK never crashes and
never sends an event without an actor.

### Tracking events

```dart
// Simple event
_analytics.track('page_viewed');

// Event with properties
// WARNING: never include sensitive data (PII, credentials, health data) in
// properties — they are stored and transmitted in plain text.
_analytics.track('button_tapped', {
  'button_id': 'validate_planning',
  'planning_id': 42,
});
```

## Token retrieval

The token is obtained at runtime via the `getToken` callback — it is never
stored in the SDK or shipped in the binary. Your backend should sign a
short-lived JWT (5–15 min) after the user authenticates, then expose an
endpoint that the app can call. Example:

```dart
Future<String> getAnalyticsToken() async {
  final response = await http.get(
    Uri.parse('https://api.example.com/analytics-token'),
    headers: {'Authorization': 'Bearer ${auth.accessToken}'},
  );
  return jsonDecode(response.body)['token'] as String;
}
```

See `docs/CONTRACT.md` §4 for the full JWT contract (algorithm, claims, expiry).

## Persistent session storage (Flutter)

The default `InMemoryStorage` does not survive app restarts. For persistent
sessions, provide a `shared_preferences`-backed implementation:

```dart
import 'package:shared_preferences/shared_preferences.dart';
import 'package:datacat_sdk/datacat_sdk.dart';

class SharedPrefsStorage implements DatacatStorage {
  SharedPrefsStorage(this._prefs);
  final SharedPreferences _prefs;

  @override
  Future<String?> read(String key) async => _prefs.getString(key);

  @override
  Future<void> write(String key, String value) async {
    await _prefs.setString(key, value);
  }
}

// Then pass it to the client:
final prefs = await SharedPreferences.getInstance();
final client = DatacatClient(
  config: DatacatConfig(
    // ...
    storage: SharedPrefsStorage(prefs),
  ),
);
```

## Testing

```bash
dart pub get
dart test
dart analyze
```

All tests run with pure Dart — no Flutter SDK required.

## Security notes

- **Never put sensitive data in `properties`** (PII, credentials, session tokens,
  health data). Properties are stored in plain text in the analytics database and
  may appear in logs.
- The `getToken` callback must return a fresh JWT obtained from your backend after
  the user is authenticated. Never hard-code or bundle a token.
- The SDK provides an optional `onError` callback for monitoring dropped events.

## Configuration reference

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `String` | required | Ingestion URL, e.g. `https://ingest.example.com/v1/events` |
| `getToken` | `Future<String> Function()` | required | Returns a fresh JWT |
| `actorId` | `String?` | `null` | Persistent actor identifier. Optional — set later via `identify()`. Events tracked with no actor are dropped. |
| `tenantId` | `String?` | `null` | Optional tenant identifier |
| `sessionId` | `String?` | auto-generated | Override session ID |
| `batchSize` | `int` | `20` | Auto-flush when queue reaches this size |
| `flushInterval` | `Duration` | `5s` | Timer-based flush interval |
| `maxQueueSize` | `int` | `1000` | Oldest events dropped when full |
| `maxRetries` | `int` | `5` | Max retry attempts per batch |
| `httpClient` | `http.Client?` | default | Injectable for testing |
| `storage` | `DatacatStorage?` | `InMemoryStorage` | Injectable session storage |
| `onError` | `void Function(Object, List<DatacatEvent>)?` | `null` | Called on permanent drops |
