# Quick integration guide

Goal: wire Datacat into an existing application **with no friction**. Two steps:
(1) expose a token endpoint on the consumer backend, (2) initialize the SDK on the client side.

## 1. On the consumer backend: token endpoint

The SDK contains **no secret**. At runtime it retrieves a short-lived token signed by
your (already authenticated) backend. Expose an **authenticated** endpoint that returns this token.

See [`token-contract.md`](token-contract.md) for the full specification and issuance examples
(Node `jose`, Python `PyJWT`). Minimal example (Express):

```ts
app.get("/api/analytics-token", requireAuth, async (req, res) => {
  const token = await issueIngestToken(
    { id: req.user.id, tenantId: req.user.tenantId },
    req.sessionId,            // the session identifier you propagate
  );
  res.json({ token });
});
```

On the Datacat side (ingestion), configure the corresponding **public key** (`TOKEN_PUBLIC_KEY_FILE`
or `TOKEN_JWKS_URL`) — see [`deployment.md`](deployment.md).

## 2. Web SDK (TypeScript / React)

Installation: the `@datacat/sdk-web` package (folder [`sdks/typescript/`](../sdks/typescript/)).

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const analytics = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  // Retrieves the token via YOUR authenticated endpoint; renewed automatically.
  getToken: () =>
    fetch("/api/analytics-token", { credentials: "include" })
      .then((r) => r.json())
      .then((d) => d.token),
  // actor_id can be set here, or later via identify() after authentication.
  actorId: currentUser?.id,
  tenantId: currentUser?.tenantId,
  // optional redaction (never any sensitive data in properties)
  redact: (props) => ({ ...props, password: undefined }),
});

// After the user authenticates:
analytics.identify({ actorId: user.id, tenantId: user.tenantId });

// Emit a business event:
analytics.track("validate_planning", { planningId: 42, count: 3 });

// The SDK sends in batches automatically; end-of-page flush is handled (sendBeacon/keepalive).
// To force it: await analytics.flush();
```

The SDK handles: `event_id` generation (frozen), `timestamp_client` (frozen), batching,
idempotent retry (same `event_id` on resend), `session_id` persistence (sessionStorage),
token renewal, and end-of-session flush via `navigator.sendBeacon`/`fetch keepalive`.

## 3. Mobile SDK (Flutter / Dart)

`datacat_sdk` package (folder [`sdks/flutter/`](../sdks/flutter/)).

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    getToken: () async {
      final res = await http.get(Uri.parse('https://app.example.com/api/analytics-token'));
      return (jsonDecode(res.body) as Map)['token'] as String;
    },
    actorId: currentUser?.id, // or via identify() after login
    tenantId: currentUser?.tenantId,
  ),
);

// After authentication:
analytics.identify(actorId: user.id, tenantId: user.tenantId);

analytics.track('validate_planning', {'planningId': 42, 'count': 3});
```

Lifecycle integration (flush when the app goes to the background — equivalent to `sendBeacon`):

```dart
class _AppState extends State<App> with WidgetsBindingObserver {
  @override
  void initState() { super.initState(); WidgetsBinding.instance.addObserver(this); }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
      analytics.flush();
    }
  }

  @override
  void dispose() { WidgetsBinding.instance.removeObserver(this); analytics.dispose(); super.dispose(); }
}
```

To persist the `session_id` across launches, provide a `DatacatStorage` implementation
based on `shared_preferences` (see the SDK README).

## 4. Common contract (both SDKs)

Both SDKs produce events **conforming to the same wire format** ([`CONTRACT.md`](CONTRACT.md)),
with the same logic for frozen `event_id`/`timestamp_client`, batching, idempotent retry, and
token handling. `tenant_id` (if available) + `actor_id` + `session_id` are attached to each event.

## 5. Sensitive data

`properties` are **free-form** but must **never** contain sensitive data
(passwords, unnecessary PII, tokens). Both SDKs expose a redaction hook and
document it. This responsibility lies with the emitter (see `security.md`).
