---
title: "Token"
description: "The ingestion token contract and asymmetric signature verification."
---

> **Scope.** Token **issuance** is **out of scope** for Datacat (spec §7.3.1): it belongs to
> each **consuming backend** (Swappy, or any other project using the analytics). This document
> is the **deliverable**: a specification precise enough for a consuming backend to implement
> issuance **without ambiguity**. On the Datacat side, only **verification** is implemented
> (public key only), in accordance with this contract.

See also the [ingestion contract](../contract/) §4 (summary integrated into the shared contract).

## 1. Principle

At the moment the user is **already authenticated** on the application, the consuming backend
signs a **short-lived JWT** with its **private key**. The SDK retrieves this token (via the
`getToken` callback) and attaches it to ingestion requests (`Authorization: Bearer`). Datacat
verifies the signature using the **public key only**.

Guarantee: the ingestion service (the public, more exposed endpoint) holds **no secret capable
of forging** a token. If ingestion is compromised, an attacker cannot create valid tokens. The
token is a **traffic-quality filter**, not a proof of content integrity (event content remains
forgeable — see [security](../security/)).

## 2. Signature algorithm

- **Asymmetric required.** Recommended: **EdDSA (Ed25519)**. Alternative: **RS256**.
- **Forbidden**: `none`, and any symmetric algorithm (`HS256`, …). Datacat rejects them.
- The JWT header **should** carry a `kid` to enable rotation:

```json
{ "alg": "EdDSA", "typ": "JWT", "kid": "2026-06-key-1" }
```

## 3. Claims (payload)

| Claim | Type | Required | Description |
|---|---|---|---|
| `iss` | string | recommended | issuer (the consuming backend). Verified by Datacat if `[token].issuer` is configured. |
| `aud` | string | recommended | expected audience, e.g. `datacat-ingest`. Verified if `[token].audience` is configured. |
| `sub` | string | recommended | = `actor_id`. |
| `actor_id` | string | **yes** | identity of the authenticated actor. |
| `session_id` | string | **yes** | authenticated session. **Key for fine-grained rate limiting** on the Datacat side. |
| `tenant_id` | string | no | tenant (B2B multi-tenant). |
| `iat` | number (epoch s) | **yes** | issued-at instant. |
| `exp` | number (epoch s) | **yes** | expiration. **Short-lived** (see §5). |
| `jti` | string | no | token identifier (optional application-level anti-replay). |

`actor_id` and `session_id` must be **non-empty**. `session_id` must be the session identifier
that the SDK propagates (consistency of correlation and rate limiting).

## 4. Issuance example (consuming backend)

### 4.1 Node.js (EdDSA, `jose`)

```ts
import { SignJWT, importPKCS8 } from "jose";

// PRIVATE key, kept ONLY on the consuming backend (never exposed).
const privateKey = await importPKCS8(process.env.DATACAT_SIGNING_KEY_PEM!, "EdDSA");

export async function issueIngestToken(user: { id: string; tenantId?: string }, sessionId: string) {
  return await new SignJWT({
    actor_id: user.id,
    session_id: sessionId,
    ...(user.tenantId ? { tenant_id: user.tenantId } : {}),
  })
    .setProtectedHeader({ alg: "EdDSA", kid: "2026-06-key-1" })
    .setSubject(user.id)
    .setIssuer("swappy-backend")
    .setAudience("datacat-ingest")
    .setIssuedAt()
    .setExpirationTime("10m")          // short-lived
    .sign(privateKey);
}
```

The application exposes an **authenticated** endpoint (e.g. `GET /analytics/ingest-token`) that
returns this token to the logged-in user; the SDK calls it via `getToken`.

### 4.2 Python (RS256, `PyJWT`)

```python
import jwt, time

def issue_ingest_token(actor_id: str, session_id: str, tenant_id: str | None) -> str:
    now = int(time.time())
    claims = {
        "iss": "swappy-backend", "aud": "datacat-ingest", "sub": actor_id,
        "actor_id": actor_id, "session_id": session_id,
        "iat": now, "exp": now + 600,
    }
    if tenant_id:
        claims["tenant_id"] = tenant_id
    return jwt.encode(claims, PRIVATE_KEY_PEM, algorithm="RS256",
                      headers={"kid": "2026-06-key-1"})
```

### 4.3 Dev tool provided by Datacat

To test ingestion without a consuming backend, Datacat ships a **dev** binary:

```bash
cargo run --bin mint-dev-token -- \
  --key keys/ed25519_private.pem --alg EdDSA \
  --actor user-123 --session sess-abc --tenant clinic-42 \
  --ttl 600 --iss swappy-backend --aud datacat-ingest --kid 2026-06-key-1
```

⚠️ This tool is **never** deployed in production; it illustrates the contract and feeds the tests.

## 5. Lifetime & renewal (SDK side)

- **Short-lived**: `exp - iat` recommended **5 to 15 minutes**.
- The SDK caches the token and **renews** it: before expiration (~30 s margin, by decoding
  `exp`) and on a `401` response. Implemented in both SDKs.

## 6. Public-key provisioning (Datacat side)

The public-key source is configured in the `[token]` section of `datacat.toml`. Two modes,
chosen at deployment.

### 6.1 PEM key in configuration

```toml
[token]
enabled = true
alg = "EdDSA"
public_key_file = "/etc/datacat/ingest_pub.pem"
# or: public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"   # "-----BEGIN PUBLIC KEY----- …"
```

Rotation: deploy the new key, switch issuance over, then remove the old one (overlap window).
For rotation without redeployment, prefer JWKS.

### 6.2 JWKS (rotation without redeployment)

```toml
[token]
enabled = true
jwks_url = "https://swappy-backend.example.com/.well-known/jwks.json"
```

The consuming backend publishes a standard JWKS endpoint exposing its **public** keys:

```json
{
  "keys": [
    { "kty": "OKP", "crv": "Ed25519", "kid": "2026-06-key-1", "x": "…", "alg": "EdDSA", "use": "sig" }
  ]
}
```

Datacat fetches and caches the JWKS, refreshes it periodically, and selects the key by `kid`.
Rotation = publish the new key in the JWKS (keep the old one while tokens in circulation
expire), then remove it.

> In dev/test, an environment-variable fallback exists for these settings (`TOKEN_ALG`,
> `TOKEN_PUBLIC_KEY_FILE` / `TOKEN_PUBLIC_KEY_PEM`, `TOKEN_JWKS_URL`, …) when no `datacat.toml`
> is present. See [configuration](../configuration/).

## 7. Verification rules applied by Datacat

Failure → `401`. In order:

1. The `Authorization: Bearer <jwt>` header is present and well-formed (or the body's `token`
   field in the `sendBeacon` fallback, see the [ingestion contract](../contract/) §1.1).
2. `alg` ∈ {`EdDSA`, `RS256`} (configurable via `[token].algorithms`) — **never
   `none`/symmetric**. Key selection via `kid` (exact match if provided).
3. Valid signature against the **public key**.
4. `exp` not exceeded (tolerance `[token].leeway`, default 60 s).
5. `iat` present.
6. Required claims present and non-empty: `actor_id`, `session_id`.
7. If configured: `iss == [token].issuer`, `aud == [token].audience`.

## 8. Acknowledged limitations (to present honestly in an audit)

This mechanism guarantees that the traffic originates from **sessions authenticated** by the
main system and enables **revocation** (key rotation). It does **not** guarantee that the
content is unforgeable: a legitimate user can emit fake "valid" events. This is the appropriate
level of guarantee for noise-tolerant analytics. See [security](../security/).
