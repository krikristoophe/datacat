/**
 * Test helpers: mock factories and utilities.
 */

import type { StorageAdapter } from "../types.js";

/** In-memory storage adapter for tests. */
export function makeMemoryStorage(): StorageAdapter {
  const map = new Map<string, string>();
  return {
    getItem: (key) => map.get(key) ?? null,
    setItem: (key, value) => { map.set(key, value); },
  };
}

/** Build a simple JWT-shaped string with given `exp` (epoch seconds). */
export function makeJwt(exp: number): string {
  const header = btoa(JSON.stringify({ alg: "EdDSA", typ: "JWT" }))
    .replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  const payload = btoa(JSON.stringify({ exp, actor_id: "user-1", session_id: "sess-1" }))
    .replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
  return `${header}.${payload}.fakesignature`;
}

/** A token that expires far in the future. */
export function validToken(): string {
  return makeJwt(Math.floor(Date.now() / 1000) + 3600);
}

/** A token that has already expired. */
export function expiredToken(): string {
  return makeJwt(Math.floor(Date.now() / 1000) - 60);
}

/** A token expiring in 10 seconds (within the 30s renewal margin). */
export function almostExpiredToken(): string {
  return makeJwt(Math.floor(Date.now() / 1000) + 10);
}
