import { describe, it, expect } from "vitest";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { DatacatClient } from "./datacat.js";
import { buildServer, TOOL_NAMES } from "./server.js";

interface Captured {
  method: string;
  path: string;
  search: string;
  headers: Record<string, string>;
  body?: string;
}

function mockFetch(
  routes: Record<string, unknown>,
  captured: Captured[],
): typeof fetch {
  return (async (url: string | URL, init?: RequestInit) => {
    const u = new URL(url.toString());
    captured.push({
      method: init?.method ?? "GET",
      path: u.pathname,
      search: u.search,
      headers: (init?.headers as Record<string, string>) ?? {},
      body: init?.body as string | undefined,
    });
    const data = routes[`${init?.method ?? "GET"} ${u.pathname}`];
    if (data === undefined) return new Response("not found", { status: 404 });
    return new Response(JSON.stringify(data), {
      status: 200,
      headers: { "content-type": "application/json" },
    });
  }) as unknown as typeof fetch;
}

async function connect(client: DatacatClient): Promise<Client> {
  const server = buildServer(client);
  const [ct, st] = InMemoryTransport.createLinkedPair();
  await server.connect(st);
  const mcp = new Client({ name: "test", version: "1.0.0" });
  await mcp.connect(ct);
  return mcp;
}

// deno-lint-ignore no-explicit-any
const textOf = (res: any) => JSON.parse(res.content[0].text);

describe("serveur MCP Datacat", () => {
  it("expose tous les outils", async () => {
    const dc = new DatacatClient({ baseUrl: "http://dc", fetchImpl: mockFetch({}, []) });
    const mcp = await connect(dc);
    const { tools } = await mcp.listTools();
    expect(tools.map((t) => t.name).sort()).toEqual([...TOOL_NAMES].sort());
  });

  it("search_logs transmet les filtres + le token et renvoie les données", async () => {
    const captured: Captured[] = [];
    const dc = new DatacatClient({
      baseUrl: "http://dc",
      token: "read-secret",
      fetchImpl: mockFetch(
        { "GET /v1/query/logs": { logs: [{ body: "boom", service_name: "billing" }] } },
        captured,
      ),
    });
    const mcp = await connect(dc);
    const res = await mcp.callTool({
      name: "search_logs",
      arguments: { service: "billing", q: "boom" },
    });
    expect(textOf(res).logs[0].body).toBe("boom");
    const req = captured.at(-1)!;
    expect(req.path).toBe("/v1/query/logs");
    expect(req.search).toContain("service=billing");
    expect(req.headers.authorization).toBe("Bearer read-secret");
  });

  it("run_read_sql poste sur /v1/query/sql", async () => {
    const captured: Captured[] = [];
    const dc = new DatacatClient({
      baseUrl: "http://dc",
      fetchImpl: mockFetch(
        { "POST /v1/query/sql": { rows: [{ n: 42 }], row_count: 1, truncated: false } },
        captured,
      ),
    });
    const mcp = await connect(dc);
    const res = await mcp.callTool({
      name: "run_read_sql",
      arguments: { sql: "SELECT count(*) AS n FROM events" },
    });
    expect(textOf(res).rows[0].n).toBe(42);
    const req = captured.at(-1)!;
    expect(req.method).toBe("POST");
    expect(req.path).toBe("/v1/query/sql");
    expect(JSON.parse(req.body!).sql).toBe("SELECT count(*) AS n FROM events");
  });

  it("remonte les erreurs backend comme erreur d'outil", async () => {
    const dc = new DatacatClient({
      baseUrl: "http://dc",
      fetchImpl: (async () => new Response("nope", { status: 403 })) as unknown as typeof fetch,
    });
    const mcp = await connect(dc);
    // deno-lint-ignore no-explicit-any
    const res: any = await mcp.callTool({ name: "ingest_stats", arguments: {} });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toContain("403");
  });
});
