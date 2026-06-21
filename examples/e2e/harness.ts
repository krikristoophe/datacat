/**
 * Datacat end-to-end integration harness.
 *
 * Runs without a browser. Uses the @datacat/sdk-web with an injectable
 * fetchImpl (global fetch from Node 24) and an in-memory StorageAdapter.
 *
 * Scenario:
 *   1. Fetch an ingestion token from demo-backend /api/analytics-token
 *   2. Create a Datacat client (with the demo-backend as token provider)
 *   3. identify() the demo user, track() several events, flush()
 *   4. Call demo-backend /api/action to generate correlated OTLP logs
 *   5. Wait for the Datacat ingest micro-batch to flush (FLUSH_INTERVAL default 200ms)
 *   6. Query PostgreSQL directly to assert events AND logs were inserted,
 *      and that they share the same session_id (correlation join ≥ 1 row)
 */

import { createDatacatClient } from "@datacat/sdk-web";
import type { StorageAdapter } from "@datacat/sdk-web";
import pg from "pg";

const { Client: PgClient } = pg;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const DATACAT_URL = process.env.DATACAT_URL ?? "http://127.0.0.1:8090";
const DEMO_URL = process.env.DEMO_BACKEND_URL ?? "http://127.0.0.1:8091";
const DATABASE_URL = process.env.DATABASE_URL ?? "postgres://datacat:datacat@localhost:55432/datacat_demo";

// The session_id we inject — fixed so we can assert the correlation join
const SESSION_ID = `e2e-session-${Date.now()}`;
const ACTOR_ID = "e2e-actor-1";
const TENANT_ID = "demo-tenant";

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

function log(msg: string) {
  console.log(`[e2e] ${msg}`);
}

function fail(msg: string): never {
  console.error(`\n[e2e] FAIL: ${msg}`);
  process.exit(1);
}

function assert(condition: boolean, msg: string) {
  if (!condition) fail(msg);
}

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// In-memory StorageAdapter (replaces sessionStorage in Node env)
// ---------------------------------------------------------------------------

class MemoryStorage implements StorageAdapter {
  private store = new Map<string, string>();
  getItem(key: string) { return this.store.get(key) ?? null; }
  setItem(key: string, value: string) { this.store.set(key, value); }
}

// ---------------------------------------------------------------------------
// Step 1: Verify demo-backend is up and token works
// ---------------------------------------------------------------------------

async function checkDemoBackend() {
  log(`Checking demo-backend at ${DEMO_URL}…`);
  const resp = await fetch(`${DEMO_URL}/api/analytics-token`);
  assert(resp.ok, `demo-backend /api/analytics-token returned ${resp.status}`);
  const data = await resp.json() as { token: string };
  assert(typeof data.token === "string" && data.token.length > 10, "token missing or too short");
  log(`Token obtained (${data.token.substring(0, 30)}…)`);
}

// ---------------------------------------------------------------------------
// Step 2: Create SDK client and emit events
// ---------------------------------------------------------------------------

async function emitEvents(): Promise<void> {
  log("Creating Datacat SDK client…");

  const storage = new MemoryStorage();
  // Pre-seed the session_id so it matches what we put in the OTLP log
  storage.setItem("datacat_session_id", SESSION_ID);

  const client = createDatacatClient({
    endpoint: `${DATACAT_URL}/v1/events`,
    getToken: () =>
      fetch(`${DEMO_URL}/api/analytics-token`)
        .then((r) => r.json())
        .then((d: { token: string }) => d.token),
    actorId: ACTOR_ID,
    tenantId: TENANT_ID,
    sessionId: SESSION_ID,
    storage,
    fetchImpl: fetch as typeof globalThis.fetch,
    onError: (err) => { throw err; },
    // Flush quickly so we don't have to wait long
    flushIntervalMs: 99999, // disable auto-flush; we flush manually
    batchSize: 10,
  });

  client.identify({ actorId: ACTOR_ID, tenantId: TENANT_ID });

  // Track several events
  const eventNames = ["page_view", "validate_planning", "confirm_action"];
  for (const name of eventNames) {
    client.track(name, { source: "e2e-harness", session_id: SESSION_ID });
    log(`Tracked event '${name}'`);
  }

  log("Flushing events to Datacat…");
  await client.flush();
  log("Events flushed");
  await client.shutdown();
}

// ---------------------------------------------------------------------------
// Step 3: Emit OTLP logs via demo-backend /api/action
// ---------------------------------------------------------------------------

async function emitLogs(): Promise<void> {
  log("Calling demo-backend /api/action to generate OTLP logs…");

  const actions = ["validate_planning", "confirm_action"];
  for (const name of actions) {
    const resp = await fetch(`${DEMO_URL}/api/action`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        sessionId: SESSION_ID,
        actorId: ACTOR_ID,
        name,
      }),
    });
    assert(resp.ok, `/api/action returned ${resp.status} for action '${name}'`);
    const data = await resp.json() as { ok: boolean; message: string };
    assert(data.ok, `action '${name}' returned ok=false: ${data.message}`);
    log(`Action '${name}' confirmed: ${data.message}`);
  }
}

// ---------------------------------------------------------------------------
// Step 4: Wait for the Datacat micro-batch flush (default interval is 200ms)
// ---------------------------------------------------------------------------

async function waitForIngest() {
  log("Waiting for Datacat micro-batch flush (1s)…");
  await sleep(1000);
}

// ---------------------------------------------------------------------------
// Step 5: Assert via direct PostgreSQL queries
// ---------------------------------------------------------------------------

async function assertCorrelation() {
  log(`Connecting to PostgreSQL at ${DATABASE_URL}…`);
  const db = new PgClient({ connectionString: DATABASE_URL });
  await db.connect();

  try {
    // Assert events were inserted
    const eventsResult = await db.query(
      "SELECT COUNT(*) AS cnt FROM events WHERE session_id = $1 AND actor_id = $2",
      [SESSION_ID, ACTOR_ID]
    );
    const eventCount = parseInt(eventsResult.rows[0].cnt, 10);
    log(`Events in DB for session '${SESSION_ID}': ${eventCount}`);
    assert(eventCount >= 3, `Expected ≥ 3 events in DB, got ${eventCount}`);

    // Assert logs were inserted
    const logsResult = await db.query(
      "SELECT COUNT(*) AS cnt FROM logs WHERE session_id = $1 AND actor_id = $2",
      [SESSION_ID, ACTOR_ID]
    );
    const logCount = parseInt(logsResult.rows[0].cnt, 10);
    log(`Logs in DB for session '${SESSION_ID}': ${logCount}`);
    assert(logCount >= 2, `Expected ≥ 2 logs in DB, got ${logCount}`);

    // Assert correlation join: events ↔ logs on session_id (≥ 1 row)
    const joinResult = await db.query(
      `SELECT e.event_name, l.body, l.severity_text
       FROM events e
       JOIN logs l ON e.session_id = l.session_id
       WHERE e.session_id = $1
       LIMIT 5`,
      [SESSION_ID]
    );
    const joinCount = joinResult.rows.length;
    log(`Correlation join (events ↔ logs on session_id='${SESSION_ID}'): ${joinCount} row(s)`);
    if (joinCount > 0) {
      for (const row of joinResult.rows) {
        log(`  event='${row.event_name}' | log body='${row.body}' | severity='${row.severity_text}'`);
      }
    }
    assert(joinCount >= 1, `Correlation join returned 0 rows — events and logs not correlated!`);

  } finally {
    await db.end();
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log("=".repeat(60));
  console.log("Datacat E2E Integration Harness");
  console.log("=".repeat(60));
  console.log(`  Datacat URL:    ${DATACAT_URL}`);
  console.log(`  Demo backend:   ${DEMO_URL}`);
  console.log(`  Database:       ${DATABASE_URL}`);
  console.log(`  Session ID:     ${SESSION_ID}`);
  console.log("=".repeat(60));

  await checkDemoBackend();
  await emitEvents();
  await emitLogs();
  await waitForIngest();
  await assertCorrelation();

  console.log("");
  console.log("=".repeat(60));
  console.log("  SUCCESS — events + logs inserted and correlated in DB!");
  console.log("=".repeat(60));
}

main().catch((err) => {
  console.error("[e2e] Unexpected error:", err);
  process.exit(1);
});
