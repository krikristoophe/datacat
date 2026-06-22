# Changelog

All notable changes to Datacat are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This file is the history of fixed
security findings; [docs/security-review.md](docs/security-review.md) only records the current
state (verified controls and justified non-risks).

## [Unreleased]

### Added
- Per-record OTLP size cap `max_otlp_record_bytes` (default 64 KiB) for logs/traces/metrics;
  oversized records are dropped (tolerant-loss), counted in `dropped_oversized_total`, logged.
- Cold-reader DataFusion sandbox: `S3OnlyObjectStoreRegistry` (resolves only the configured S3
  bucket, no local `file://`) plus read-only `sql_with_options` gate.
- Companion API (bidirectional heartbeat dead-man's switch) with a standalone `datacat-companion`
  agent; N companions supported, alerting on disconnect.
- Marketing landing page for the documentation site (bilingual EN/FR) and usage guides
  (Quickstart, Installation, SDKs, Docker telemetry, Companion).

### Changed
- Unified TOML configuration (global `datacat.toml` + one file per project) with `${VAR}` env
  expansion; embedded scheduled cold export; multi-project alerting.
- Slack notifications use the Web API (`chat.postMessage`) instead of incoming webhooks.

### Security
Findings from the HDS security review, all fixed in-tree (IDs match the review history):

- **S-1** — Rate-limiter mutexes were poison-panicking (`.expect`); a single panic would 500 the
  whole ingestion path. Now poison-tolerant.
- **S-2** — `/stats` was unauthenticated (leaked tracked-session/IP and ban counts); now gated by
  `query_auth`.
- **S-3** — OTLP rate-limit charged 1 token regardless of record count; cost is now the record count.
- **S-4** — gRPC had no decode-size limit; now set from `max_logs_payload_bytes`.
- **S-5** — serde parse errors echoed body fragments (potential PII); now logged server-side only.
- **S-6** — Cold reader ran arbitrary SQL on a default DataFusion context, exposing
  `read_csv`/`read_parquet` over the local filesystem. Sandboxed via an S3-only object-store
  registry and a pre-execution read-only gate (`sql_with_options`, which rejects DDL/DML/COPY/
  statements before DataFusion executes them eagerly).
- **S-7** — No per-record OTLP size cap; added `max_otlp_record_bytes`.
- **S-8** — `max_logs_records` was checked only after fully flattening the request; flattening now
  stops at the cap (`max_records + 1`), bounding peak memory.
- **S-9** — gRPC peer IP fell back to `0.0.0.0`, collapsing all such clients onto one IP
  (shared-fate ban / per-IP cap). An unspecified IP is now exempt from per-IP scoping and banning.
- **S-10** — Per-session bucket creation was a non-atomic get-then-insert, and the global bucket
  was debited before the finer checks. Now uses the `entry` API and debits global last.
- **S-11** — `X-Forwarded-For` parsing could skip a malformed rightmost hop to a client-controlled
  token (now strict rightmost only); a saturated banned-map could fail open (now evicts the
  soonest-to-expire ban so a new abuser is always bannable).
- **S-12** — Operator-supplied export `prefix` is now validated (rejects `..`, absolute paths,
  backslashes, control characters) so a misconfiguration fails closed.
