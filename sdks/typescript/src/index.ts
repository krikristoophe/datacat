/**
 * @datacat/sdk-web — Datacat analytics SDK for web
 *
 * Implements the Datacat event ingestion contract (CONTRACT.md):
 * - Wire format: `{ "events": [...] }` batch POST
 * - JWT token via `Authorization: Bearer` header (never in query string)
 * - Token renewal before expiry (~30s margin) and on 401
 * - Retry with exponential backoff; event_id/timestamp_client frozen on creation
 * - Beacon fallback with token in body (CONTRACT.md §1.1)
 * - session_id persisted in sessionStorage
 *
 * Security: `properties` must not contain sensitive data (passwords, PII, tokens).
 * Use the `redact` option to sanitize properties before transmission.
 */

export { createDatacatClient } from "./client/index.js";
export type {
  DatacatClient,
  DatacatClientOptions,
  DatacatEvent,
  DatacatIdentity,
  DatacatBatchPayload,
  DatacatBeaconPayload,
  StorageAdapter,
} from "./types/index.js";
