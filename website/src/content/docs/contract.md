---
title: "Ingestion Contract"
description: "The wire format for events — the source of truth shared by backend and SDKs."
---

This document is the **single source of truth** for the ingestion contract. The backend
(`backend/`) and both SDKs (`sdks/typescript/`, `sdks/flutter/`) MUST conform to it exactly.
Any change is made here first.

## 1. Ingestion endpoint

```
POST /v1/events
Content-Type: application/json
Authorization: Bearer <ingestion-jwt>         # see §4
Origin: https://app.example.com               # validated by CORS (web)
```

Success response:

```
202 Accepted
{ "received": 12 }          # number of events accepted for asynchronous writing
```

> The API **acknowledges immediately** (202) and then writes to the database behind the
> scenes (micro-batch). `received` is NOT the number of events actually inserted:
> deduplication (idempotency) happens in the database, asynchronously. An `event_id` that
> is already known is silently ignored (`ON CONFLICT DO NOTHING`).

### 1.1 Token transport (header vs `sendBeacon`)

The token is transmitted via the `Authorization: Bearer <jwt>` header **by default**.

`navigator.sendBeacon` (used at end of page/session on the web) **does not allow** adding an
`Authorization` header. For that case — and **only** that fallback case — the SDK may place
the token in a top-level property of the JSON body:

```jsonc
{ "token": "<jwt>", "events": [ ... ] }   // beacon fallback: token in the body
```

On the ingestion side, token resolution follows this order: (1) the `Authorization: Bearer`
header; (2) failing that, the `token` field of the body. The token is **never** transmitted
in a *query string* (to avoid logging it in access logs). The nominal path remains the
header; the SDK prefers `fetch(..., { keepalive: true })` with a header on unload, and falls
back to `sendBeacon` (token in the body) only as a last resort.

Error codes:

| Code | Meaning | Body |
|---|---|---|
| `400 Bad Request` | malformed payload / structural validation failed | `{ "error": "...", "details": [...] }` |
| `401 Unauthorized` | token missing, invalid, expired, or claims missing | `{ "error": "..." }` |
| `413 Payload Too Large` | payload or batch beyond the bounds | `{ "error": "..." }` |
| `429 Too Many Requests` | rate limit reached (one of the three levels) | `{ "error": "...", "scope": "session\|ip_sessions\|global" }` + `Retry-After` header |
| `503 Service Unavailable` | shutting down / not ready | `{ "error": "..." }` |

## 2. Event format (wire format)

The body is an object `{ "events": [ <event>, ... ] }` (always a batch, never a single event).

```jsonc
{
  "events": [
    {
      "event_id":         "550e8400-e29b-41d4-a716-446655440000", // UUID, generated CLIENT-side, idempotency key
      "event_name":       "validate_planning",                    // free-form, 1..=200 chars
      "tenant_id":        "clinic-42",                            // optional (string|null|absent), <=200
      "actor_id":         "user-123",                             // required, 1..=200
      "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f", // required, 1..=200
      "timestamp_client": "2026-06-21T10:15:30.123Z",            // RFC3339/ISO-8601 UTC, FROZEN at creation
      "properties":       { "planning_id": 42, "count": 3 }       // optional, JSON object, default {}
    }
  ]
}
```

### 2.1 Fields

| Field | Wire type | Required | Server-side validation constraint |
|---|---|---|---|
| `event_id` | string (UUID) | yes | valid UUID. **Idempotency key.** |
| `event_name` | string | yes | 1..=200 characters (non-empty after trim) |
| `tenant_id` | string\|null | no | if present: 1..=200 characters |
| `actor_id` | string | yes | 1..=200 characters |
| `session_id` | string | yes | 1..=200 characters |
| `timestamp_client` | RFC3339 string | yes | parsable; within `[received_at - MAX_PAST_SKEW, received_at + MAX_FUTURE_SKEW]` |
| `properties` | object | no | JSON object; serialized size <= `MAX_PROPERTIES_BYTES`; depth <= `MAX_JSON_DEPTH` |

`received_at` (server timestamp) is **never sent by the client**: it is filled in by the API.

### 2.2 The golden rule of idempotency (SDK imperative)

> **`event_id` AND `timestamp_client` are frozen at the event's *creation* and reused
> *unchanged* on every resend (retry).** NEVER regenerate them on a retry.

Technical reason (see [`architecture`](../architecture/)): the table is partitioned by
`timestamp_client` and the idempotency key is `(timestamp_client, event_id)`. It is the only
stable timestamp across two sends of the same event; it guarantees that a duplicate always
lands in the same partition and is therefore deduplicated globally.

### 2.3 Bounds (default values, configurable server-side)

| Constant | Default | Role |
|---|---|---|
| `MAX_BATCH_EVENTS` | 500 | max number of events per request |
| `MAX_PAYLOAD_BYTES` | 1,048,576 (1 MiB) | max HTTP body size |
| `MAX_PROPERTIES_BYTES` | 16,384 (16 KiB) | max serialized size of `properties` |
| `MAX_STRING_LEN` | 200 | max length of text fields (name/ids) |
| `MAX_JSON_DEPTH` | 16 | max depth of `properties` |
| `MAX_PAST_SKEW` | 31 days | reject if `timestamp_client` is too old |
| `MAX_FUTURE_SKEW` | 24 hours | reject if `timestamp_client` is too far in the future |

Validation policy:
- **Structural errors** (invalid JSON, missing required field, wrong type, empty or
  oversized batch, oversized payload) → **the entire request is rejected** (`400`/`413`).
- **Per-event semantic filters** (`timestamp_client` outside the skew window) → the
  offending event is **dropped** (tolerated loss, counter incremented), the other events in
  the batch are accepted. `received` reflects the number of events retained.

## 3. Identity & correlation

- `actor_id`: the persistent identity of an actor (provided by the application).
- `session_id`: a **structuring identifier**. Generated and persisted by the SDK for the
  duration of a session. Used (a) for fine-grained per-session rate limiting and (b) as a
  **future correlation key** between product events and technical logs.
- `tenant_id`: B2B multi-tenant, optional.

All three are **text**, attached to every event.

## 4. Ingestion token contract (JWT, asymmetric signature)

> Token **issuance** is **out of scope** for this project: it belongs to each consuming
> backend (Swappy, etc.). This document specifies the contract so that any backend
> implements it identically. On the ingestion side (in scope): **verification only**, using
> the **public key alone**. On the SDK side (in scope): fetching, attaching and renewing the
> token, **never hard-coded**.

### 4.1 Algorithm

- **Asymmetric only.** Recommended: **EdDSA (Ed25519)**. Alternative: **RS256**.
- The consuming backend signs with the **private key**. Ingestion verifies with the **public
  key only** → the public endpoint holds no secret capable of *forging* a token, only of
  *verifying* one.

### 4.2 JWT header

```jsonc
{ "alg": "EdDSA", "typ": "JWT", "kid": "2026-06-key-1" }
```

- `kid` (recommended): identifies the key to enable **rotation** (several public keys active
  on the ingestion side, selected by `kid`).

### 4.3 Claims (payload)

| Claim | Type | Required | Description |
|---|---|---|---|
| `iss` | string | recommended | issuer (consuming backend). Verified if `[token].issuer` is configured. |
| `aud` | string | recommended | audience, expected value `datacat-ingest`. Verified if `[token].audience` is configured. |
| `sub` | string | recommended | = `actor_id` (standard subject) |
| `actor_id` | string | **yes** | authenticated actor |
| `session_id` | string | **yes** | authenticated session — **key for fine-grained rate limiting** |
| `tenant_id` | string | no | tenant (where applicable) |
| `iat` | number (epoch s) | **yes** | issued-at |
| `exp` | number (epoch s) | **yes** | expiration (**short-lived**, see §4.5) |
| `jti` | string | no | token identifier (optional application-level anti-replay) |

### 4.4 Verification rules (ingestion side)

In order, failure → `401`:

1. The `Authorization: Bearer <jwt>` header is present and well-formed.
2. `alg` ∈ allowed algorithms (`EdDSA`/`RS256`) — **never `none`**, never a symmetric
   algorithm. Key selection via `kid` if provided.
3. Valid signature against the **public** key.
4. `exp` not exceeded (with a clock tolerance `[token].leeway`, default 60 s).
5. `iat` present and not aberrant (not in the future beyond the leeway).
6. Required claims present and non-empty: `actor_id`, `session_id`.
7. If configured: `iss == [token].issuer`, `aud == [token].audience`.

The token authenticates the **quality of the traffic** (sessions originating from the main
system), not the **content** of the events (always forgeable). The token's `session_id` /
`actor_id` are the trusted source for rate limiting; the same fields in the event body are
stored as-is but **not presumed reliable**.

### 4.5 Lifetime & renewal (SDK side)

- **Short-lived**: `exp - iat` recommended **5 to 15 minutes**.
- The SDK retrieves the token via a callback provided by the application (`getToken`), caches
  it, and **renews** it: (a) before expiration (~30 s margin), and (b) on a `401`.
- The token is **never** hard-coded into the SDK or into the delivered application code.

### 4.6 Public-key provisioning (ingestion side)

Two modes, configured in the `[token]` section of `datacat.toml`:

- **PEM in configuration** (`public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"` or
  `public_key_file`): public key(s) provided at deployment. Rotation = adding a new key then
  removing the old one.
- **JWKS** (`jwks_url`): ingestion fetches and caches the consuming backend's public key set,
  refreshed periodically; selection by `kid`. Enables rotation without redeploying ingestion.

> In dev/test, an environment-variable fallback exists (`TOKEN_*`) when no `datacat.toml` is
> present. See [configuration](../configuration/).

The full issuance specification (consumer-side generation examples) is in the
[token contract](../token/).

## 5. SDK behavior (common to TS & Flutter)

1. `track(name, properties)` creates an event: `event_id = uuid v4`, `timestamp_client = now()`
   (both frozen), + the current `actor_id`/`session_id`/`tenant_id`.
2. Events are **queued** and sent in **batches** (triggers: batch size reached, flush
   interval, or explicit flush / end of session).
3. **Idempotent retry**: on a network/5xx failure, events stay in the queue and are
   **resent with the same `event_id`/`timestamp_client`**. Bounded exponential backoff.
4. Web: end-of-page/session `flush` via `navigator.sendBeacon` (fallback `fetch keepalive`).
5. Token attached to every request (`Authorization: Bearer`), retrieved/renewed via `getToken`.
6. `properties` must **never** contain sensitive data (documented; the SDK exposes an
   optional redaction hook).

## 6. Contract versioning

The URL prefix `/v1/` carries the major version. Any breaking change to the wire format or to
the claims requires `/v2/`. Backward-compatible additions (a new optional field) stay in `/v1/`.
