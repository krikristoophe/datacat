---
title: "Security"
description: "Security model, hardening and HDS-grade auditability."
---

The system must be able to pass a **rigorous HDS-type security audit** without reservations. This
document describes the threat model, the controls, and what is — honestly — guaranteed or not.

## 1. Threat model (spec §7.1)

Founding assumption: **any incoming request can be forged**. The ingestion endpoint is public and
not strongly authenticated. `actor_id`, `session_id`, the event content, and even the token can be
extracted from a client (web or mobile) and replayed. **No client-side defense is treated as a
guarantee.** Real security is **entirely server-side**.

## 2. What is guaranteed / what is not

| Guaranteed | Not guaranteed (assumed) |
|---|---|
| Traffic originates from **sessions authenticated** by the main system (signed token). | **Content unforgeability**: a legitimate user can emit fake but "valid" events. |
| Ingestion **cannot forge** a token (public key only). | — |
| **Strict idempotence** (no duplicates). | — |
| Revocation is possible (key rotation). | — |

This is the **appropriate** level of guarantee for noise-tolerant analytics, and it is defensible
in an audit **provided it is presented honestly**: the token is a *traffic-quality filter*, not an
authentication of the content.

## 3. Implemented controls (mapping to spec §7)

### 3.1 Ingestion token — asymmetric signature (§7.3)
- **EdDSA / RS256** verification, **public key only**. `none` and symmetric algorithms are
  **rejected**. Key selection by `kid`, rotation via multiple PEMs or JWKS.
- No hard-coded secret in the SDKs: the token is fetched at runtime, renewed, never embedded.
- The configured public key is supplied via `[token]` in `datacat.toml`
  (`public_key_pem` / `public_key_file` / `jwks_url`), with the PEM passed as a `${ENV}` reference
  rather than written in clear text — see [configuration](../configuration/).
- Checks: signature, `exp` (+ leeway), `iat`, required claims (`actor_id`, `session_id`),
  `iss`/`aud` if configured. Details: [token](../token/) §7.

### 3.2 Two-level rate limiting + global safety net (§7.2)
- **Per `session_id`** (token bucket): prevents one session from impacting its peers —
  indispensable in B2B (sites behind a single NAT IP).
- **Cap on distinct sessions per IP** (sliding window): closes the "generate thousands of fake
  sessions" workaround without penalizing a legitimate site.
- **Global safety net** (token bucket): protects the infrastructure from a massive multi-source
  flood.
- Memory-bounded structures (caps + periodic purge) → no DoS on the limiter itself.

### 3.3 Strict input validation (§7.4)
- Bounds: payload size (`MAX_PAYLOAD_BYTES`, → `413`), batch size (`MAX_BATCH_EVENTS`), field
  lengths, size **and depth** of `properties` (anti-JSON-bomb), valid `event_id` UUID, parsable and
  **bounded** `timestamp_client` (anti-partition-poisoning).
- Structural error → the whole request is rejected (`400`). Semantic filter (skew) → the event is
  dropped (tolerated loss), never a duplicate.

### 3.4 Anomaly detection (§7.4)
- Counting of "bad" requests (400/401/429) per IP over a window; beyond a threshold, **temporary
  ban** of the IP (immediate `429` response).

### 3.5 CORS (§7.4)
- Origin allow-list (`[server.cors].allowed_origins` in `datacat.toml`). `["*"]` is reserved for
  development (documented).

### 3.6 IP resolution
- By default, the TCP peer IP (not network-forgeable). `X-Forwarded-For` is taken into account only
  if `[server].trust_forwarded_for = true`, to be enabled **only behind a single trusted proxy**
  (otherwise the header is forgeable). The entry added by the proxy is then used.

### 3.7 Traceability (§7.4)
- Structured **JSON logs**, `x-request-id` generated/propagated, internal errors logged but
  **never returned** to the client (no information leak).

### 3.8 Encryption in transit (§7.4)
- TLS terminated at the reverse proxy; the binary uses **rustls** (no OpenSSL) for its outbound
  calls (JWKS). Controlled hosting expected (HDS).

### 3.9 Sensitive data (§7.4)
- `properties` are free-form but documented as **not meant to** contain sensitive data; the SDKs
  expose a **redaction** hook. Responsibility lies with the emitter, technically supported.

### 3.10 Controlled dependencies (§7.4)
- Minimal surface, maintained crates, up-to-date versions. `#![forbid(unsafe_code)]` on the
  backend. Auditable via `cargo audit` / `cargo deny` (cf. CI).

### 3.11 Secret hygiene (configuration)
- No secret is written in clear text in `datacat.toml` or in `projects/*.toml`: every string can
  reference an environment variable via `${VAR}` (or `${VAR:-default}`), expanded at startup. A
  required `${VAR}` with no default makes the service **refuse to start** (fail-closed). Real
  config files are git-ignored; only `*.example.toml` templates are committed. This keeps database
  URLs, token keys, S3 credentials, and notification webhooks out of version control.

## 4. Exposed surface

| Endpoint | Auth | Data |
|---|---|---|
| `POST /v1/events` | token (quality filter) | ingestion |
| `GET /healthz`, `/readyz` | none | status, no business data |
| `GET /stats` | none | aggregated counters (no event data). Place it behind the internal network / ingress if desired. |

> Deployment recommendation: restrict `/stats` (and `/readyz`) to the internal network via the
> reverse proxy/ingress.

## 5. Points of attention for the audit

- The token **does not protect** content integrity (assumed, cf. §2). Any strong content-integrity
  requirement would fall outside the analytics scope and require a different mechanism.
- Loss tolerance (under overload) is **intentional** and bounded; **never** at the cost of a
  duplicate.
