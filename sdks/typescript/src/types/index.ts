/**
 * Wire-format event as specified in CONTRACT.md §2.
 * All fields except `properties` are required by the server.
 */
export interface DatacatEvent {
  /** UUID v4, generated client-side at event creation. Idempotency key. NEVER regenerated on retry. */
  event_id: string;
  /** Business action name, 1–200 chars. */
  event_name: string;
  /** Tenant identifier (optional, B2B multi-tenant). */
  tenant_id?: string;
  /** Persistent actor identity, required. */
  actor_id: string;
  /** Session identifier, required. Structural key for rate-limiting and correlation. */
  session_id: string;
  /** ISO-8601 UTC timestamp frozen at event creation. NEVER regenerated on retry. */
  timestamp_client: string;
  /** Arbitrary business context. Must NOT contain sensitive data. */
  properties: Record<string, unknown>;
}

/**
 * Batch payload sent to the ingestion endpoint.
 * Wire format: `{ "events": [...] }` (CONTRACT.md §2)
 */
export interface DatacatBatchPayload {
  events: DatacatEvent[];
}

/**
 * Beacon payload used as fallback when fetch keepalive is unavailable.
 * Token is placed in the body because sendBeacon cannot set Authorization headers.
 * See CONTRACT.md §1.1.
 */
export interface DatacatBeaconPayload {
  token: string;
  events: DatacatEvent[];
}

/** Identity context set via `identify()`. */
export interface DatacatIdentity {
  actorId: string;
  tenantId?: string;
}

/**
 * Optional injectable storage interface (for testing or SSR environments
 * where sessionStorage is unavailable).
 */
export interface StorageAdapter {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

/**
 * Configuration options for `createDatacatClient`.
 *
 * @example
 * ```ts
 * const client = createDatacatClient({
 *   endpoint: "https://api.example.com/v1/events",
 *   getToken: () => fetch("/api/analytics-token").then(r => r.text()),
 * });
 * ```
 */
export interface DatacatClientOptions {
  /**
   * Full URL of the ingestion endpoint (e.g. "https://ingest.example.com/v1/events").
   */
  endpoint: string;

  /**
   * Async callback that returns a fresh JWT.
   * Called by the SDK on first use and before token expiry (~30s margin).
   * NEVER embed a token in this callback—fetch it from your backend at runtime.
   *
   * @example
   * ```ts
   * getToken: () => fetch("/api/analytics-token").then(r => r.json()).then(d => d.token)
   * ```
   */
  getToken: () => Promise<string>;

  /** Initial actor identity. Can be set later via `identify()`. */
  actorId?: string;

  /** Initial tenant identity. Can be set later via `identify()`. */
  tenantId?: string;

  /** Maximum number of events per batch request. Default: 20. */
  batchSize?: number;

  /** Interval in milliseconds between automatic flushes. Default: 5000. */
  flushIntervalMs?: number;

  /** Maximum events held in the in-memory queue. Oldest are dropped when exceeded. Default: 1000. */
  maxQueueSize?: number;

  /** Maximum number of retry attempts per batch before events are abandoned. Default: 5. */
  maxRetries?: number;

  /**
   * Session ID. If not provided, one is generated and persisted in sessionStorage.
   * Reused across the entire session.
   */
  sessionId?: string;

  /** Called when a non-retryable error occurs or events are dropped. */
  onError?: (error: Error, events?: DatacatEvent[]) => void;

  /**
   * Optional hook to redact sensitive data from event properties before sending.
   * Called for every event. Return the sanitized properties object.
   *
   * @example
   * ```ts
   * redact: (props) => {
   *   const { password, ...safe } = props as any;
   *   return safe;
   * }
   * ```
   */
  redact?: (properties: Record<string, unknown>) => Record<string, unknown>;

  /**
   * Injectable fetch implementation. Defaults to global `fetch`.
   * Useful for testing or Node environments without native fetch.
   */
  fetchImpl?: typeof fetch;

  /**
   * Injectable storage adapter. Defaults to `sessionStorage`.
   * Falls back to an in-memory map when sessionStorage is unavailable.
   */
  storage?: StorageAdapter;
}

/** Public API returned by `createDatacatClient`. */
export interface DatacatClient {
  /**
   * Set (or update) the current user identity.
   * Subsequent `track()` calls will use these values.
   */
  identify(identity: DatacatIdentity): void;

  /**
   * Queue an event for batched delivery.
   * `event_id` and `timestamp_client` are frozen at call time and never regenerated on retry.
   *
   * @param eventName - Business action name (1–200 chars). Must NOT identify as sensitive data.
   * @param properties - Arbitrary metadata. MUST NOT contain passwords, PII, tokens, or secrets.
   */
  track(eventName: string, properties?: Record<string, unknown>): void;

  /**
   * Immediately send all queued events.
   * Resolves when the batch has been successfully delivered (or permanently failed).
   */
  flush(): Promise<void>;

  /**
   * Perform a final flush and clean up all timers and event listeners.
   * Call this when the client is no longer needed (e.g. in framework cleanup hooks).
   */
  shutdown(): Promise<void>;
}
