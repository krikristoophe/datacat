/**
 * Client de la couche de lecture Datacat (`/v1/query/*` + `/stats`).
 * `fetchImpl` est injectable pour les tests.
 */

export interface DatacatClientOptions {
  /** URL de base de l'API d'ingestion Datacat (ex. http://localhost:8080). */
  baseUrl: string;
  /** Token de lecture (Bearer) si `query_auth` est activé côté serveur. */
  token?: string;
  fetchImpl?: typeof fetch;
}

export type QueryParams = Record<string, string | number | undefined>;

export class DatacatClient {
  private readonly baseUrl: string;
  private readonly token?: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: DatacatClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.token = opts.token;
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  private async request(url: string, init: RequestInit): Promise<unknown> {
    const headers: Record<string, string> = {
      ...((init.headers as Record<string, string>) ?? {}),
    };
    if (this.token) headers["authorization"] = `Bearer ${this.token}`;
    const res = await this.fetchImpl(url, { ...init, headers });
    const body = await res.text();
    if (!res.ok) {
      throw new Error(`Datacat ${res.status}: ${body.slice(0, 1000)}`);
    }
    return body ? (JSON.parse(body) as unknown) : null;
  }

  private get(path: string, params?: QueryParams): Promise<unknown> {
    const url = new URL(this.baseUrl + path);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, String(v));
      }
    }
    return this.request(url.toString(), { method: "GET" });
  }

  private post(path: string, body: unknown): Promise<unknown> {
    return this.request(this.baseUrl + path, {
      method: "POST",
      body: JSON.stringify(body),
      headers: { "content-type": "application/json" },
    });
  }

  searchLogs(p: QueryParams): Promise<unknown> {
    return this.get("/v1/query/logs", p);
  }
  getTrace(traceId: string): Promise<unknown> {
    return this.get(`/v1/query/traces/${encodeURIComponent(traceId)}`);
  }
  searchEvents(p: QueryParams): Promise<unknown> {
    return this.get("/v1/query/events", p);
  }
  frequentJourneys(p: QueryParams): Promise<unknown> {
    return this.get("/v1/query/journeys", p);
  }
  searchMetrics(p: QueryParams): Promise<unknown> {
    return this.get("/v1/query/metrics", p);
  }
  runReadSql(sql: string, limit?: number): Promise<unknown> {
    return this.post("/v1/query/sql", { sql, limit });
  }
  stats(): Promise<unknown> {
    return this.get("/stats");
  }
}
