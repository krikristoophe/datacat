# @datacat/sdk-web

TypeScript SDK for [Datacat](https://github.com/yourorg/datacat) — a self-hosted, production-grade analytics event ingestion system.

Targets modern browsers and Node 24+. Zero runtime dependencies; uses only native browser/Node APIs (`crypto.randomUUID`, `fetch`, `navigator.sendBeacon`).

---

## Installation

```bash
npm install @datacat/sdk-web
```

---

## Quick Start

### Vanilla TypeScript / JavaScript

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const analytics = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  // getToken MUST fetch a JWT from your backend — never embed a token in source code.
  getToken: () =>
    fetch("/api/analytics-token").then((r) => r.json()).then((d) => d.token),
  actorId: "user-123",
  tenantId: "clinic-42", // optional (B2B multi-tenant)
});

// Track an event
analytics.track("validate_planning", { planning_id: 42, count: 3 });

// Force-send the queue
await analytics.flush();

// Clean up on app teardown
await analytics.shutdown();
```

### React Integration

```tsx
import { createContext, useContext, useEffect, useRef } from "react";
import { createDatacatClient, type DatacatClient } from "@datacat/sdk-web";

const AnalyticsContext = createContext<DatacatClient | null>(null);

export function AnalyticsProvider({ children }: { children: React.ReactNode }) {
  const clientRef = useRef<DatacatClient | null>(null);

  useEffect(() => {
    const client = createDatacatClient({
      endpoint: import.meta.env.VITE_ANALYTICS_ENDPOINT,
      getToken: () =>
        fetch("/api/analytics-token").then((r) => r.json()).then((d) => d.token),
    });
    clientRef.current = client;

    return () => {
      void client.shutdown();
    };
  }, []);

  return (
    <AnalyticsContext.Provider value={clientRef.current}>
      {children}
    </AnalyticsContext.Provider>
  );
}

export function useAnalytics(): DatacatClient | null {
  return useContext(AnalyticsContext);
}

// In a component:
function PlanningPage() {
  const analytics = useAnalytics();

  function handleValidate(planningId: number) {
    analytics?.identify({ actorId: currentUser.id, tenantId: currentUser.org });
    analytics?.track("validate_planning", { planning_id: planningId });
  }

  return <button onClick={() => handleValidate(42)}>Validate</button>;
}
```

---

## API Reference

### `createDatacatClient(options)`

Returns a `DatacatClient` instance.

#### Options

| Option | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `string` | **required** | Full URL of the ingestion endpoint (e.g. `https://ingest.example.com/v1/events`) |
| `getToken` | `() => Promise<string>` | **required** | Async callback returning a JWT. Fetched from your backend, never hardcoded. |
| `actorId` | `string` | `undefined` | Initial actor ID. Can be set later with `identify()`. |
| `tenantId` | `string` | `undefined` | Initial tenant ID (B2B multi-tenant). |
| `batchSize` | `number` | `20` | Events per batch request. |
| `flushIntervalMs` | `number` | `5000` | Auto-flush interval (ms). |
| `maxQueueSize` | `number` | `1000` | Max queued events. Oldest events are dropped when exceeded. |
| `maxRetries` | `number` | `5` | Max retry attempts per batch. |
| `sessionId` | `string` | auto-generated | Override the session ID. Normally generated and persisted in `sessionStorage`. |
| `onError` | `(err, events?) => void` | `undefined` | Called on non-retryable errors or event drops. |
| `redact` | `(props) => props` | `undefined` | Hook to sanitize event properties before transmission. |
| `fetchImpl` | `typeof fetch` | `globalThis.fetch` | Injectable fetch for testing or custom environments. |
| `storage` | `StorageAdapter` | `sessionStorage` | Injectable storage for session ID persistence. |

#### `client.identify({ actorId, tenantId? })`

Sets the current actor identity. Must be called before `track()` (or `actorId` must be set in options).

#### `client.track(eventName, properties?)`

Queues an event. The `event_id` and `timestamp_client` are **frozen at call time** and never regenerated on retry — this guarantees idempotency (CONTRACT.md §2.2).

#### `client.flush(): Promise<void>`

Immediately sends all queued events.

#### `client.shutdown(): Promise<void>`

Performs a final flush then removes all timers and event listeners. Call this in framework cleanup hooks (React `useEffect` return, Vue `onUnmounted`, etc.).

---

## Token Integration

The SDK **never** embeds a token in source code. You provide a `getToken` callback that fetches a short-lived JWT from your backend at runtime:

```ts
const client = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  getToken: async () => {
    const res = await fetch("/api/analytics-token", { credentials: "include" });
    const { token } = await res.json();
    return token;
  },
});
```

The SDK:
- Caches the token in memory.
- Refreshes it proactively ~30 seconds before expiry (reads `exp` from the JWT payload — no signature verification client-side).
- Refreshes immediately on a `401 Unauthorized` response from the ingestion endpoint.

The token contract (algorithm, claims, key rotation) is specified in `docs/CONTRACT.md §4`.

---

## End-of-Session Flush (Beacon)

The SDK registers `visibilitychange`, `pagehide`, and `beforeunload` listeners to flush the queue when the page is closing.

**Primary path**: `fetch(..., { keepalive: true, headers: { Authorization: "Bearer <jwt>" } })` — preserves the `Authorization` header.

**Fallback path** (if keepalive fetch fails): `navigator.sendBeacon(url, blob)` where the blob body is `{ "token": "<jwt>", "events": [...] }`. The token goes in the **body**, never in the URL query string (per CONTRACT.md §1.1).

---

## Security: Sensitive Data in Properties

> **The `properties` object MUST NOT contain sensitive data** (passwords, session tokens, PII, health data, payment information, secrets of any kind).

`properties` are stored as free-form JSONB and may appear in analytics queries, logs, or exports. Once sent, they cannot be reliably purged.

Use the `redact` option to enforce sanitization at the SDK level:

```ts
const client = createDatacatClient({
  endpoint: "...",
  getToken: () => fetch("/api/token").then(r => r.json()).then(d => d.token),
  redact: (properties) => {
    // Remove any field that might be sensitive
    const { password, token, secret, ssn, ...safe } = properties as Record<string, unknown>;
    return safe;
  },
});
```

This hook runs synchronously before every event is queued.

---

## Retry & Error Handling

| HTTP Status | Behavior |
|---|---|
| `202 Accepted` | Success — events removed from queue |
| `400 Bad Request` | Abandoned — events dropped, `onError` called |
| `401 Unauthorized` | Token refreshed, one retry; if still failing, event requeued |
| `413 Payload Too Large` | Abandoned — events dropped, `onError` called |
| `429 Too Many Requests` | Retry after `Retry-After` header delay (or exponential backoff) |
| `5xx` | Retry with exponential backoff (base 200 ms, max 30 s) |
| Network error | Same as 5xx |

Retried events always reuse their original `event_id` and `timestamp_client` — this ensures **idempotent delivery**: the server deduplicates duplicates via `ON CONFLICT DO NOTHING`.

---

## Build

```bash
npm install
npm run build      # produces dist/index.js (ESM), dist/index.cjs (CJS), dist/index.d.ts
npm test           # run vitest
npm run typecheck  # strict tsc --noEmit
```

---

## Wire Format

See `docs/CONTRACT.md` for the authoritative specification. A batch request looks like:

```
POST /v1/events
Content-Type: application/json
Authorization: Bearer <jwt>

{
  "events": [
    {
      "event_id":         "550e8400-e29b-41d4-a716-446655440000",
      "event_name":       "validate_planning",
      "tenant_id":        "clinic-42",
      "actor_id":         "user-123",
      "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f",
      "timestamp_client": "2026-06-21T10:15:30.123Z",
      "properties":       { "planning_id": 42 }
    }
  ]
}
```
