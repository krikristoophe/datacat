/**
 * DatacatClient — core implementation.
 *
 * Wire format, token handling, batching, retry, and beacon fallback
 * are all specified in CONTRACT.md. This file implements those contracts.
 *
 * Security note: `properties` MUST NOT contain sensitive data (passwords, PII,
 * tokens, secrets). The optional `redact` hook provides a safety net, but
 * responsibility lies with the caller. See README.md for details.
 */

import type {
  DatacatClient,
  DatacatClientOptions,
  DatacatEvent,
  StorageAdapter,
} from "./types.js";
import { TokenManager } from "./token.js";
import { resolveSessionId, newEventId, buildStorageAdapter } from "./session.js";
import { EventQueue, type QueuedEvent } from "./queue.js";

const DEFAULT_BATCH_SIZE = 20;
const DEFAULT_FLUSH_INTERVAL_MS = 5000;
const DEFAULT_MAX_QUEUE_SIZE = 1000;
const DEFAULT_MAX_RETRIES = 5;

/** Base delay for exponential backoff (ms). */
const BACKOFF_BASE_MS = 200;
const BACKOFF_MAX_MS = 30_000;

function backoffMs(attempt: number): number {
  return Math.min(BACKOFF_BASE_MS * Math.pow(2, attempt), BACKOFF_MAX_MS);
}

export function createDatacatClient(options: DatacatClientOptions): DatacatClient {
  const {
    endpoint,
    getToken,
    batchSize = DEFAULT_BATCH_SIZE,
    flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS,
    maxQueueSize = DEFAULT_MAX_QUEUE_SIZE,
    maxRetries = DEFAULT_MAX_RETRIES,
    onError,
    redact,
  } = options;

  // ── Identity ────────────────────────────────────────────────────────────────
  let currentActorId: string | undefined = options.actorId;
  let currentTenantId: string | undefined = options.tenantId;

  // ── Session ID ──────────────────────────────────────────────────────────────
  let storage: StorageAdapter;
  if (options.storage !== undefined) {
    storage = options.storage;
  } else {
    storage = buildStorageAdapter();
  }
  const sessionId = resolveSessionId(storage, options.sessionId);

  // ── Token manager ────────────────────────────────────────────────────────────
  const tokenManager = new TokenManager(getToken);

  // ── Fetch implementation ─────────────────────────────────────────────────────
  const fetchImpl: typeof fetch =
    options.fetchImpl ??
    (typeof fetch !== "undefined"
      ? fetch
      : (() => {
          throw new Error("fetch is not available");
        }) as unknown as typeof fetch);

  // ── Queue ────────────────────────────────────────────────────────────────────
  const queue = new EventQueue(maxQueueSize, (dropped) => {
    onError?.(
      new Error(
        `Datacat: dropped ${dropped.length} event(s) — queue exceeded maxQueueSize (${maxQueueSize})`
      ),
      dropped
    );
  });

  // ── Retry schedule: events with a future "not-before" timestamp ──────────────
  // Instead of sleeping inside sendBatch (which breaks fake timers in tests),
  // failed events are requeued with a retryNotBefore timestamp.
  // The flush loop skips events that are not yet due.

  interface RetryItem {
    items: QueuedEvent[];
    notBefore: number; // Date.now() epoch ms
  }

  const retrySchedule: RetryItem[] = [];

  function drainDueRetries(): void {
    const now = Date.now();
    const due: RetryItem[] = [];
    const pending: RetryItem[] = [];
    for (const item of retrySchedule) {
      if (item.notBefore <= now) {
        due.push(item);
      } else {
        pending.push(item);
      }
    }
    retrySchedule.length = 0;
    for (const p of pending) retrySchedule.push(p);
    for (const d of due) {
      queue.requeue(d.items);
    }
  }

  // ── Flush lock (prevent concurrent flushes) ──────────────────────────────────
  let flushPromise: Promise<void> | null = null;

  // ── Page-unload listener refs (for cleanup) ──────────────────────────────────
  let visibilityHandler: (() => void) | null = null;
  let pagehideHandler: (() => void) | null = null;
  let beforeunloadHandler: (() => void) | null = null;

  // ── Interval timer ──────────────────────────────────────────────────────────
  let flushTimer: ReturnType<typeof setInterval> | null = null;

  // ── Internal: send a batch ───────────────────────────────────────────────────

  /**
   * Attempt to POST a batch to the ingestion endpoint.
   * Returns { ok: true } on success.
   * Returns { ok: false; retryAfterMs?: number } on retryable failure.
   * Throws on non-retryable failure (400/413).
   */
  async function attemptSend(
    events: DatacatEvent[],
    token: string
  ): Promise<{ ok: true } | { ok: false; retryAfterMs?: number }> {
    const body = JSON.stringify({ events });
    let response: Response;
    try {
      response = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body,
        keepalive: true,
      });
    } catch {
      // Network error — retryable
      return { ok: false };
    }

    if (response.ok) {
      return { ok: true };
    }

    if (response.status === 401) {
      // Token expired — invalidate cache, refresh, retry once
      tokenManager.invalidate();
      const newToken = await tokenManager.refresh();
      let retryResp: Response;
      try {
        retryResp = await fetchImpl(endpoint, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${newToken}`,
          },
          body,
          keepalive: true,
        });
      } catch {
        return { ok: false };
      }
      if (retryResp.ok) return { ok: true };
      // Still failing after token refresh — retryable
      return { ok: false };
    }

    if (response.status === 400 || response.status === 413) {
      // Non-retryable: abandon events
      const errText = await response.text().catch(() => "(unreadable body)");
      throw new Error(
        `Datacat: ingestion rejected batch (${response.status}): ${errText}`
      );
    }

    if (response.status === 429) {
      const retryAfterHeader = response.headers.get("Retry-After");
      if (retryAfterHeader !== null) {
        return { ok: false, retryAfterMs: parseFloat(retryAfterHeader) * 1000 };
      }
      return { ok: false };
    }

    // 5xx and other codes — retryable
    return { ok: false };
  }

  /**
   * Send one batch. On failure, schedule a retry (via retrySchedule) rather
   * than sleeping inline. This keeps the async path non-blocking and
   * compatible with fake timer environments.
   */
  async function sendBatch(items: QueuedEvent[]): Promise<void> {
    if (items.length === 0) return;

    const token = await tokenManager.get();
    const events = items.map((i) => i.event);

    try {
      const result = await attemptSend(events, token);

      if (result.ok) return;

      // Retryable failure — split into retry vs abandoned
      const nextItems: QueuedEvent[] = [];
      const abandoned: DatacatEvent[] = [];

      for (const item of items) {
        const next = item.retryCount + 1;
        if (next >= maxRetries) {
          abandoned.push(item.event);
        } else {
          nextItems.push({ event: item.event, retryCount: next });
        }
      }

      if (abandoned.length > 0) {
        onError?.(
          new Error(
            `Datacat: abandoned ${abandoned.length} event(s) after ${maxRetries} retries`
          ),
          abandoned
        );
      }

      if (nextItems.length > 0) {
        // Schedule retry with backoff — not a blocking sleep
        const minRetry = Math.min(...nextItems.map((i) => i.retryCount));
        const waitMs =
          (result as { ok: false; retryAfterMs?: number }).retryAfterMs ??
          backoffMs(minRetry - 1);
        retrySchedule.push({ items: nextItems, notBefore: Date.now() + waitMs });
      }
    } catch (err) {
      // Non-retryable (400/413)
      onError?.(
        err instanceof Error ? err : new Error(String(err)),
        events
      );
    }
  }

  // ── Internal: flush the queue ────────────────────────────────────────────────

  async function doFlush(): Promise<void> {
    drainDueRetries();
    if (queue.isEmpty()) return;

    // Send all current batches synchronously (no sleeping between them)
    while (!queue.isEmpty()) {
      const batch = queue.dequeue(batchSize);
      await sendBatch(batch);
    }
  }

  function scheduleFlush(): void {
    // Deduplicate: if already flushing, don't stack
    if (flushPromise !== null) return;
    flushPromise = doFlush().finally(() => {
      flushPromise = null;
    });
  }

  // ── Page-unload beacon flush ─────────────────────────────────────────────────

  /**
   * On page hide/unload, attempt to send remaining events.
   *
   * Preferred path: fetch with keepalive=true (can set Authorization header).
   * Fallback: navigator.sendBeacon with token in the JSON body (CONTRACT.md §1.1).
   *
   * The token is NEVER placed in a query string.
   */
  async function beaconFlush(): Promise<void> {
    // Drain retries that are due
    drainDueRetries();
    if (queue.isEmpty()) return;

    const items = queue.drain();
    const events = items.map((i) => i.event);
    if (events.length === 0) return;

    let token: string;
    try {
      token = await tokenManager.get();
    } catch {
      onError?.(
        new Error(
          "Datacat: could not obtain token for beacon flush — events dropped"
        ),
        events
      );
      return;
    }

    const body = JSON.stringify({ events });

    // Primary: fetch with keepalive (preserves Authorization header)
    try {
      const ok = await fetchImpl(endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body,
        keepalive: true,
      })
        .then((r) => r.ok)
        .catch(() => false);

      if (ok) return;
    } catch {
      // Fall through to beacon
    }

    // Fallback: sendBeacon — token in body per CONTRACT.md §1.1
    if (
      typeof navigator !== "undefined" &&
      typeof navigator.sendBeacon === "function"
    ) {
      const beaconBody = JSON.stringify({ token, events });
      const sent = navigator.sendBeacon(
        endpoint,
        new Blob([beaconBody], { type: "application/json" })
      );
      if (!sent) {
        onError?.(
          new Error("Datacat: sendBeacon returned false — events may be lost"),
          events
        );
      }
    } else {
      onError?.(
        new Error(
          "Datacat: beacon flush failed — neither keepalive fetch nor sendBeacon available"
        ),
        events
      );
    }
  }

  // ── Register page-unload listeners ──────────────────────────────────────────

  if (typeof document !== "undefined") {
    visibilityHandler = () => {
      if (document.visibilityState === "hidden") {
        void beaconFlush();
      }
    };
    document.addEventListener("visibilitychange", visibilityHandler);
  }

  if (typeof window !== "undefined") {
    pagehideHandler = () => {
      void beaconFlush();
    };
    window.addEventListener("pagehide", pagehideHandler);

    beforeunloadHandler = () => {
      void beaconFlush();
    };
    window.addEventListener("beforeunload", beforeunloadHandler);
  }

  // ── Start periodic flush timer ───────────────────────────────────────────────
  flushTimer = setInterval(() => {
    scheduleFlush();
  }, flushIntervalMs);
  // Allow Node.js to exit even if the timer is active
  if (
    typeof flushTimer === "object" &&
    flushTimer !== null &&
    "unref" in flushTimer
  ) {
    (flushTimer as { unref(): void }).unref();
  }

  // ── Public API ───────────────────────────────────────────────────────────────

  function identify(identity: { actorId: string; tenantId?: string }): void {
    currentActorId = identity.actorId;
    currentTenantId = identity.tenantId;
  }

  function track(
    eventName: string,
    properties: Record<string, unknown> = {}
  ): void {
    if (currentActorId === undefined) {
      onError?.(
        new Error(
          "Datacat: track() called before identify() — event dropped (actor_id is required)"
        )
      );
      return;
    }

    const sanitizedProps =
      redact !== undefined ? redact(properties) : properties;

    const event: DatacatEvent = {
      event_id: newEventId(), // frozen at creation, never regenerated on retry
      event_name: eventName,
      actor_id: currentActorId,
      session_id: sessionId,
      timestamp_client: new Date().toISOString(), // frozen at creation, never regenerated
      properties: sanitizedProps,
    };

    if (currentTenantId !== undefined) {
      event.tenant_id = currentTenantId;
    }

    queue.enqueue(event);

    // Trigger flush if batch size reached
    if (queue.size >= batchSize) {
      scheduleFlush();
    }
  }

  async function flush(): Promise<void> {
    if (flushPromise !== null) {
      return flushPromise;
    }
    return doFlush();
  }

  async function shutdown(): Promise<void> {
    // Stop periodic timer
    if (flushTimer !== null) {
      clearInterval(flushTimer);
      flushTimer = null;
    }

    // Remove page-unload listeners
    if (typeof document !== "undefined" && visibilityHandler !== null) {
      document.removeEventListener("visibilitychange", visibilityHandler);
      visibilityHandler = null;
    }
    if (typeof window !== "undefined") {
      if (pagehideHandler !== null) {
        window.removeEventListener("pagehide", pagehideHandler);
        pagehideHandler = null;
      }
      if (beforeunloadHandler !== null) {
        window.removeEventListener("beforeunload", beforeunloadHandler);
        beforeunloadHandler = null;
      }
    }

    // Final flush
    await doFlush();
  }

  return { identify, track, flush, shutdown };
}
