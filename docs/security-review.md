# Security review (HDS posture)

Full-codebase security review of Datacat against the HDS-grade requirements in
[CLAUDE.md](../CLAUDE.md) and the controls in [security.md](security.md). Per project policy the
review covers the **whole codebase** (backend ingestion/read/alerting/security, the `exporter` and
`reader` crates), not just a diff.

This document records only the **current state**: controls that are verified, and points that are
**not a risk** (by design or not applicable), with justification. There is no "planned" or
"accepted risk" section — under the HDS posture, anything found is fixed in-tree, not deferred. The
history of fixed findings lives in the root [CHANGELOG.md](../CHANGELOG.md) (see its *Security*
entries, IDs `S-1`…`S-12`).

## 1. Controls verified ✓

### Authentication & authorization
- **Token verification** (`security/token.rs`): asymmetric only (EdDSA/RS256, public key only);
  `alg` checked against an allow-list before key selection (no algorithm confusion); `exp` required
  and validated; `iat` presence enforced; `kid` exact-match; issuer/audience validated when set;
  JWKS rotation; poison-tolerant key lock (no request-path panic).
- **Service-to-service auth** (`security/mod.rs`): static tokens compared in **constant time**;
  telemetry ingestion (logs/traces/metrics) and the read endpoints share the check; `/mcp` and
  `/stats` are gated by `query_auth`.

### Input handling & injection
- **SQL injection**: migrations build dynamic SQL with `format('%I'/%L', …)` over `to_char`-derived
  partition names (no user input). The alerting engine interpolates only **allow-listed**
  identifiers (tables, time columns, `group_by` keys) and **binds** every value. The COPY encoder
  doubles quotes / keeps newlines inside quoted fields (no row injection).
- **Cold-reader SQL sandbox** (`reader/src/sandbox.rs`): arbitrary operator SQL runs on a
  `SessionContext` whose object-store registry resolves **only** the configured S3 bucket (no local
  `file://` store), so `read_csv`/`read_parquet`/`LOCATION`/schema-inference cannot touch the host
  filesystem; SQL is run through `sql_with_options` with DDL/DML/`COPY`/statements disabled, so
  `verify_plan` rejects them **before** DataFusion executes them (it executes DDL/statements
  eagerly).
- **Per-record OTLP size cap** (`ingest::drop_oversized`): each log/span/metric point is bounded by
  `max_otlp_record_bytes`; oversized records are dropped (tolerant-loss) and counted.
- **Bounded flattening**: OTLP flatten loops stop at `max_logs_records + 1`, so a request cannot be
  expanded into an unbounded `Vec` before the count cap rejects it (peak-memory bound).

### Rate limiting, anomaly & abuse
- **Three-tier rate limiter** (`security/ratelimit.rs`): per-session and per-IP-distinct-session
  buckets plus a global net. The OTLP cost is the **submitted** record count (a packed or
  all-oversized request cannot bypass the write-rate ceiling). Session buckets are created
  atomically via the `entry` API; the global bucket is debited **last** so a request denied at the
  finer tiers cannot cheaply drain it. All request-path locks are poison-tolerant.
- **Unknown peer IP**: an `UNSPECIFIED` peer (e.g. gRPC without `remote_addr`) is exempt from
  per-IP scoping and from banning, so such clients never collapse onto a shared `0.0.0.0` identity.
- **Anomaly guard** (`security/anomaly.rs`): trusted `X-Forwarded-For` parsing uses **strictly** the
  rightmost hop (a malformed last hop is ignored rather than falling back to a client-controlled
  token); the banned map keeps headroom (evicts the soonest-to-expire ban) so a new abuser can
  always be banned — no fail-open under saturation.

### Idempotence & availability
- **Idempotence**: every stream is keyed by `(partition_timestamp, id)` with a PK + `DISTINCT ON`
  + `ON CONFLICT DO NOTHING`; the skew window clamps attacker timestamps so partition creation is
  bounded. Out-of-range or absent OTLP timestamps resolve within `DateTime` range and are then
  bounded by the same skew window (no silent mis-stamping past the window).
- **Availability**: `panic = "unwind"` + an outermost `CatchPanicLayer` turn a handler panic into a
  logged 500 instead of a process crash; request-path **and** background-prune locks are
  poison-tolerant.

### Transport, secrets & supply chain
- **Error handling** (`error.rs`): internal errors → generic 500, logged server-side; parse-error
  detail is not reflected to clients (potential PII).
- **Secret hygiene** (`settings.rs`): TOML config with `${VAR}` env references, fail-closed on a
  missing required secret; real config files git-ignored; no secrets logged. Wildcard CORS and
  disabled token verification require the explicit `--features dev` build (`enforce_runtime_guards`).
- **Export prefix** (`exporter`): the operator-supplied S3 key prefix is validated (no `..`
  segments, no leading `/`, no backslashes or control characters) so a misconfiguration fails
  closed.
- **Transport / supply chain**: rustls throughout (no OpenSSL); `#![forbid(unsafe_code)]`; CI runs
  `cargo-audit` (backend/exporter/reader) and a `cargo-deny` policy (advisories + licenses + sources).

## 2. Not a risk / by design

- **Wildcard CORS** is dev-only (compile-time `--features dev`); not combined with credentials; the
  bearer token is app-held, not an ambient cookie → no CSRF-style exposure.
- **Webhook / Slack egress** targets are operator-configured (TOML), not user input → not an SSRF
  vector for untrusted data.
- **Cold reader is operator-only** (a CLI with no network endpoint); the SQL sandbox above is
  defence-in-depth, not the only boundary.
- **RUSTSEC-2023-0071** (`rsa`) is ignored with justification: `rsa` is an uncompiled optional dep
  of the sqlx MySQL backend (not enabled); Datacat does only public-key verification.
