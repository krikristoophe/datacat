# Security review (HDS posture)

Full-codebase security review of Datacat against the HDS-grade requirements in
[CLAUDE.md](../CLAUDE.md) and the controls in [security.md](security.md). Per project policy, the
review covers the **whole codebase** (backend ingestion/read/alerting/security, the `exporter` and
`reader` crates), not just a diff. This document records what was verified, the findings, and their
remediation status.

## 1. Controls verified ✓

- **Token verification** (`security/token.rs`): asymmetric only (EdDSA/RS256, public key only);
  `alg` checked against an allow-list before key selection (no algorithm confusion); `exp` required
  and validated; `iat` presence enforced; `kid` exact-match; issuer/audience validated when set;
  JWKS rotation; poison-tolerant key lock (no request-path panic).
- **Service-to-service auth** (`security/mod.rs`): static tokens compared in **constant time**;
  telemetry ingestion (logs/traces/metrics) and the read endpoints share the check; `/mcp` and
  `/stats` are gated.
- **SQL injection**: migrations build dynamic SQL with `format('%I'/%L', …)` over `to_char`-derived
  partition names (no user input). The alerting engine interpolates only **allow-listed**
  identifiers (tables, time columns, `group_by` keys) and **binds** every value. The COPY encoder
  doubles quotes / keeps newlines inside quoted fields (no row injection).
- **Idempotence**: every stream is keyed by `(partition_timestamp, id)` with a PK + `DISTINCT ON`
  + `ON CONFLICT DO NOTHING`; the skew window clamps attacker timestamps so partition creation is
  bounded.
- **Availability**: `panic = "unwind"` + an outermost `CatchPanicLayer` turn a handler panic into a
  logged 500 instead of a process crash; request-path locks (token, rate limiter) are
  poison-tolerant.
- **Error handling** (`error.rs`): internal errors → generic 500, logged server-side; parse-error
  detail is no longer reflected to clients (potential PII).
- **Secret hygiene** (`settings.rs`): TOML config with `${VAR}` env references, fail-closed on a
  missing required secret; real config files git-ignored; no secrets logged. Wildcard CORS and
  disabled token verification require the explicit `--features dev` build (`enforce_runtime_guards`).
- **Transport / supply chain**: rustls throughout (no OpenSSL); `#![forbid(unsafe_code)]`; CI runs
  `cargo-audit` (backend/exporter/reader) and a `cargo-deny` policy (advisories + licenses + sources).

## 2. Findings fixed in this review

| ID | Sev | Area | Fix |
|---|---|---|---|
| S-1 | High | `security/ratelimit.rs` | Mutex `.expect()` on the global/session/IP locks → a single panic while held would poison the lock and **permanently 500 the whole ingestion path**. Now poison-tolerant (`unwrap_or_else(\|e\| e.into_inner())`), matching the token verifier. |
| S-2 | High | `api/routes.rs` `/stats` | Endpoint was **unauthenticated**, leaking tracked-session/IP counts, banned-IP count and ingestion volumes (lets an attacker tune a flood / confirm a ban). Now gated by `query_auth`. |
| S-3 | Medium | OTLP intake (logs/traces/metrics) | Rate limiter charged **1 token regardless of record count** (up to 2048), so a packed request bypassed the real write-rate ceiling. Cost is now the record count. |
| S-4 | Medium | `grpc.rs` | No gRPC decode-size limit (HTTP had per-route body limits) — silently relied on tonic's 4 MB default. Now `max_decoding_message_size` is set from `max_logs_payload_bytes`. |
| S-5 | Low | `api/routes.rs` | serde parse errors echoed a fragment of the (potentially PII) body back to the client. Now logged server-side only; client gets a generic 400. |
| S-6 | High | `reader/src/engine.rs` + `reader/src/sandbox.rs` | Arbitrary SQL on a default DataFusion `SessionContext` exposed `read_csv` / `read_parquet` / `read_json` (and `CREATE EXTERNAL TABLE … LOCATION`, `COPY … TO`) over the local filesystem — `SELECT * FROM read_csv('/etc/passwd')` would exfiltrate host files. Two layers: (1) the `SessionContext` uses a custom `S3OnlyObjectStoreRegistry` that registers **no** local `file://` store and resolves **only** the configured S3 bucket, so all file access (table functions, `LOCATION`, schema inference) is denied at one point; (2) SQL runs via `sql_with_options` with DDL/DML/`COPY`/statements disabled, so `verify_plan` rejects them **recursively, before execution** — important because `SessionContext::sql` executes DDL and `SET`/`DROP`/`CREATE` *eagerly*, so a post-hoc plan check would run too late. |
| S-7 | Medium | OTLP intake (logs/traces/metrics) | No **per-record** size cap on OTLP — a single multi-MB log body / span with tens of thousands of events was stored verbatim (bounded only by record count + total body size). Each record's variable content is now measured (`approx_content_bytes()`); records over `max_otlp_record_bytes` (default 64 KiB) are dropped (tolerant-loss), counted in `dropped_oversized_total`, and logged at `warn`. |
| (prev) | Med | release profile | `panic = "abort"` → handler panic crashed the process; switched to `unwind` + `CatchPanicLayer`. |

## 3. Findings documented — remediation planned

These are real but require larger / feature-sized changes or carry lower exploitability; they are
tracked for follow-up.

- **S-8 (Medium) — `max_logs_records` checked after flattening.** The count cap is enforced after
  the request is fully expanded into `Vec<Stored*>`, allowing transient memory amplification.
  **Planned**: short-circuit the flatten loop at the limit.
- **S-9 (Medium) — gRPC `request_ip` falls back to `0.0.0.0`.** Behind a proxy/socket where
  `remote_addr` is unavailable, all gRPC clients collapse onto one IP (shared-fate ban / meaningless
  per-IP cap). **Planned**: skip IP-scoped ban/limit when the peer IP is unknown, or require a
  trusted proxy protocol. Note: gRPC is typically deployed with direct connections.
- **S-10 (Low) — rate-limiter micro-races / ordering.** The per-session first-insert is
  get-then-insert (not atomic) and the global bucket is debited before the session/IP checks; both
  are backstopped by the per-IP session cap and the global net. **Planned**: DashMap `entry` API +
  check session/IP before debiting global.
- **S-11 (Low) — `anomaly.rs`.** Rightmost-parseable `X-Forwarded-For` token can skip a malformed
  entry; a saturated banned-map can fail-open for brand-new IPs. Bounded impact. **Planned**:
  strict rightmost-hop parse; reserve headroom in the banned map.
- **S-12 (Low) — exporter `prefix` / timestamp coercion.** Operator-supplied S3 `prefix` is not
  validated for `..`; extreme OTLP timestamps are silently coerced to `received_at` rather than
  rejected. Operator-config / data-integrity, not external RCE. **Planned**: validate prefix; reject
  out-of-range timestamps.

## 4. Accepted risks / notes

- **Wildcard CORS** is dev-only (compile-time `--features dev`); not combined with credentials; the
  bearer token is app-held, not an ambient cookie → no CSRF-style exposure.
- **Webhook / Slack egress** targets are operator-configured (TOML), not user input → not an SSRF
  vector for untrusted data.
- **RUSTSEC-2023-0071** (`rsa`) is ignored with justification: `rsa` is an uncompiled optional dep
  of the sqlx MySQL backend (not enabled); Datacat does only public-key verification.

## 5. Recommended next steps
- Run the cloud multi-agent review (`/code-review`) for an independent pass.
- Work through the remaining lower-severity documented findings (S-8 to S-12) as follow-ups.
