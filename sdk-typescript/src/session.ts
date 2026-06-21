/**
 * Session ID management.
 *
 * The session_id is a structural identifier (CONTRACT.md §3):
 * - Used for fine-grained rate limiting (per session_id)
 * - Serves as a future correlation key between product events and technical logs
 *
 * It is generated once per browser session and persisted in sessionStorage.
 * Falls back to an in-memory value if sessionStorage is unavailable (SSR, private mode, etc.).
 */

import type { StorageAdapter } from "./types.js";

const SESSION_KEY = "datacat_session_id";

function generateUUID(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  // Fallback for environments without crypto.randomUUID (very old Node)
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/**
 * Resolve or generate the session ID.
 *
 * Resolution order:
 * 1. Explicit `sessionId` passed in options (takes precedence)
 * 2. Value stored in the provided storage adapter
 * 3. Newly generated UUID, persisted to storage
 */
export function resolveSessionId(
  storage: StorageAdapter,
  explicitId?: string
): string {
  if (explicitId !== undefined && explicitId.length > 0) {
    return explicitId;
  }

  const stored = storage.getItem(SESSION_KEY);
  if (stored !== null && stored.length > 0) {
    return stored;
  }

  const id = generateUUID();
  try {
    storage.setItem(SESSION_KEY, id);
  } catch {
    // Storage quota exceeded or blocked — use in-memory value only
  }
  return id;
}

/** Generate a new UUID for use as an event_id. */
export function newEventId(): string {
  return generateUUID();
}

/**
 * Build a storage adapter backed by sessionStorage.
 * Falls back to an in-memory Map if sessionStorage throws (SSR / private browsing).
 */
export function buildStorageAdapter(): StorageAdapter {
  try {
    // Test that sessionStorage is actually accessible
    sessionStorage.setItem("__datacat_test__", "1");
    sessionStorage.removeItem("__datacat_test__");
    return sessionStorage;
  } catch {
    const map = new Map<string, string>();
    return {
      getItem: (key) => map.get(key) ?? null,
      setItem: (key, value) => { map.set(key, value); },
    };
  }
}
