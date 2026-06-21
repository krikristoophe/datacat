# Security review (HDS posture)

Point-in-time security review of the Datacat ingestion service, against the HDS-grade requirements
in [CLAUDE.md](../CLAUDE.md) and the controls described in [security.md](security.md). This document
records what was verified and the findings that were fixed.

> Scope: the `backend` ingestion service (HTTP + gRPC), its configuration, alerting and read layers,
> and the dependency supply chain. The standalone `reader` cold-query tool is out of scope here.

## 1. Controls verified ✓

### Authentication & token verification (`security/token.rs`)
- **Asymmetric only.** EdDSA / RS256; the service holds **public keys only** and can verify but
  never forge a token. `none` and symmetric algorithms are rejected.
- **No algorithm confusion.** The `alg` from the JWT header is checked against an explicit allow-list
  (`algorithms`) *before* a key is selected; the selected key must match that algorithm. An attacker
  cannot downgrade RS256 → HS256 (HS256 is not allow-listed, and the key is an asymmetric
  `DecodingKey`).
- **Expiry & claims.** `validate_exp = true`, `exp` is a required claim; `iat` presence is enforced
  by deserialization; `actor_id` / `session_id` must be non-empty. Configurable `leeway`.
- **Issuer / audience.** Validated when configured.
- **Key id (`kid`).** When the token carries a `kid`, an exact match is required (no silent
  fallback to another key).
- **Rotation.** JWKS keys are refreshed by a background task without redeployment.

### Service-to-service auth (`security/mod.rs`)
- Static service tokens are compared in **constant time** (only the length is revealed, which is
  standard). The telemetry ingestion streams (logs/traces/metrics) and the read endpoints share this
  check; the MCP endpoint (`/mcp`) is gated by `query_auth`.

### Input validation & DoS guards
- Strict structural validation (batch size, payload size, property size, string length, JSON depth,
  timestamp skew) — see [CONTRACT.md](CONTRACT.md).
- Per-route **body-size limits** (`DefaultBodyLimit`), a **request timeout** layer, two-level
  **rate limiting** + a global net, and an **anomaly guard** that temporarily bans abusive IPs.

### Read-only SQL endpoint (`query/engine.rs::run_read_sql`)
- **Disabled by default** (`[query.sql].enabled = false`).
- Accepts only `SELECT` / `WITH`, **rejects `;`** (single statement, no chaining), and executes
  inside a **`SET TRANSACTION READ ONLY`** transaction with a **`statement_timeout`**. Even a CTE
  attempting a write is blocked by the read-only transaction (defense in depth). Results are wrapped
  in `to_jsonb(...)` and bounded by `LIMIT`.

### SQL injection surface (alerting engine)
- Dynamic SQL is built with `QueryBuilder`; only **allow-listed identifiers** (table names, time
  columns, `group_by` keys) are ever interpolated — every user/operator value is **bound** as a
  parameter. The `group_by` allow-list (`body`, `service_name`, `severity_text`, `trace_id`,
  `attr:<key>`) prevents injection through grouping.

### Error handling (`error.rs`)
- Internal errors are logged server-side and returned as a **generic 500** — no internals leak to
  clients. 401 carries `WWW-Authenticate: Bearer`; 429 carries `Retry-After`.

### Secret hygiene (`settings.rs`)
- Configuration is TOML; secrets are referenced via `${VAR}` / `${VAR:-default}` and resolved from
  the environment. A required `${VAR}` with no value makes startup **fail closed**. Real
  `datacat.toml` / `projects/*.toml` are git-ignored — **no secrets in version control**. No secret
  values are written to logs.

### Transport & supply chain
- TLS via **rustls** throughout (no OpenSSL): JWKS fetch, Slack/webhook POSTs, SMTP (STARTTLS),
  PostgreSQL, S3. `#![forbid(unsafe_code)]`.
- CI runs `cargo-audit` on every push.

## 2. Findings fixed in this review

### F-1 — Availability: a handler panic could crash the whole process (medium)
The release profile used `panic = "abort"`, so **any** panic reachable from a request handler would
abort the entire process (dropping in-flight batches and all connections) — and the `tower-http`
`catch-panic` feature, though enabled, was never wired. There were also two `.expect()` calls on a
`RwLock` in the token-verification request path (panic on lock poisoning).

**Fix:** switched the release profile to `panic = "unwind"`, wired `CatchPanicLayer` (outermost) on
both the main and MCP routers so a handler panic becomes a logged **500** instead of a crash, and
replaced the request-path `.expect()`s with poison-tolerant guard recovery
(`unwrap_or_else(|e| e.into_inner())`). Covered by a regression test
(`api::tests::handler_panic_becomes_500`).

### F-2 — CI: RustSec audit false positive (low / process)
`RUSTSEC-2023-0071` (`rsa`, Marvin timing attack) was failing CI. The `rsa` crate is an **uncompiled
optional dependency** of the `sqlx` MySQL backend, which Datacat does not enable (`cargo tree -i rsa`
is empty); Datacat performs **public-key verification only** and never an RSA private-key operation,
so the timing sidechannel does not apply. Ignored with this justification in CI, to be revisited if
`rsa` ever enters the build graph.

## 3. Accepted risks / notes
- **Wildcard CORS** (`allowed_origins = ["*"]`) is supported for development. It is **not** combined
  with `allow_credentials`, and the bearer token is an explicit app-held credential (not an ambient
  cookie), so there is no CSRF-style exposure. Production deployments should pin an origin allow-list.
- **Webhook / Slack action URLs** are operator-configured (from TOML), not user-supplied, so the
  alerting egress is not an SSRF vector for untrusted input.
- **Ad-hoc SQL endpoint**: even with all the guards above, it is a powerful debugging tool — keep it
  disabled in production unless gated behind a strong `[auth.query]` token.

## 4. Recommended next steps (not blocking)
- Run the cloud multi-agent review (`/code-review`) for an independent pass.
- Extend `cargo-audit` to the `reader` / `exporter` lockfiles in CI.
- Consider a `cargo-deny` policy (licenses + sources + advisories) for stricter supply-chain control.
