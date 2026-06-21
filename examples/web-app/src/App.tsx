import { useEffect, useRef, useState } from "react";
import { createDatacatClient } from "@datacat/sdk-web";
import type { DatacatClient } from "@datacat/sdk-web";

const DATACAT_URL = import.meta.env.VITE_DATACAT_URL as string;
const DEMO_BACKEND_URL = import.meta.env.VITE_DEMO_BACKEND_URL as string;

const DEMO_ACTOR_ID = "demo-user-1";
const DEMO_TENANT_ID = "demo-tenant";

export default function App() {
  const analyticsRef = useRef<DatacatClient | null>(null);
  const [sessionId, setSessionId] = useState<string>("");
  const [logs, setLogs] = useState<Array<{ text: string; isError: boolean }>>([]);
  const [busy, setBusy] = useState(false);

  function addLog(text: string, isError = false) {
    setLogs((prev) => [...prev, { text, isError }]);
  }

  useEffect(() => {
    // Create the Datacat analytics client on mount
    const client = createDatacatClient({
      endpoint: `${DATACAT_URL}/v1/events`,
      getToken: () =>
        fetch(`${DEMO_BACKEND_URL}/api/analytics-token`)
          .then((r) => r.json())
          .then((d: { token: string }) => d.token),
      actorId: DEMO_ACTOR_ID,
      tenantId: DEMO_TENANT_ID,
      onError: (err) => addLog(`[SDK error] ${err.message}`, true),
    });

    // Identify the demo user
    client.identify({ actorId: DEMO_ACTOR_ID, tenantId: DEMO_TENANT_ID });

    // Expose the session_id for display (via the SDK's internal session storage)
    // We use a small trick: the SDK exposes sessionId via the storage adapter default
    const storedSession = sessionStorage.getItem("datacat_session_id") ?? "(generated internally)";
    setSessionId(storedSession);

    analyticsRef.current = client;

    return () => {
      client.shutdown().catch(() => {});
    };
  }, []);

  async function handleValidatePlanning() {
    if (!analyticsRef.current) return;
    setBusy(true);
    try {
      // (a) Track the analytics event
      analyticsRef.current.track("validate_planning", {
        planning_id: 42,
        count: 3,
        source: "web-app-example",
      });
      await analyticsRef.current.flush();
      addLog("[analytics] event 'validate_planning' tracked and flushed");

      // (b) Call demo-backend to generate a correlated OTLP log
      const resp = await fetch(`${DEMO_BACKEND_URL}/api/action`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          sessionId: sessionStorage.getItem("datacat_session_id") ?? "demo-session-fixed",
          actorId: DEMO_ACTOR_ID,
          name: "validate_planning",
        }),
      });
      const data = (await resp.json()) as { ok: boolean; message: string };
      addLog(`[backend] ${data.message}`);
    } catch (e) {
      addLog(`[error] ${e instanceof Error ? e.message : String(e)}`, true);
    } finally {
      setBusy(false);
    }
  }

  return (
    <div>
      <h1>Datacat Integration Demo</h1>
      <p>
        This page demonstrates end-to-end event + log correlation. Click the
        button to emit an analytics event via the SDK <em>and</em> trigger an
        OTLP log from the demo backend, both tagged with the same{" "}
        <code>session_id</code>.
      </p>

      <p>
        <strong>Session ID:</strong>{" "}
        <code>{sessionId || "initializing…"}</code>
      </p>
      <p>
        <strong>Actor:</strong> <code>{DEMO_ACTOR_ID}</code> &nbsp;|&nbsp;
        <strong>Tenant:</strong> <code>{DEMO_TENANT_ID}</code>
      </p>
      <p>
        <strong>Datacat ingest:</strong> <code>{DATACAT_URL}</code>
        &nbsp;|&nbsp;
        <strong>Demo backend:</strong> <code>{DEMO_BACKEND_URL}</code>
      </p>

      <button onClick={handleValidatePlanning} disabled={busy}>
        {busy ? "Sending…" : "Valider le planning"}
      </button>

      {logs.map((entry, i) => (
        <div key={i} className={`log${entry.isError ? " err" : ""}`}>
          {entry.text}
        </div>
      ))}
    </div>
  );
}
