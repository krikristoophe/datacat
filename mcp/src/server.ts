/**
 * Serveur MCP Datacat : expose la couche de lecture (logs, traces, events, métriques,
 * parcours, SQL lecture seule, stats) comme outils utilisables par Claude pour debugger,
 * analyser les parcours réels, et vérifier des hypothèses.
 */

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import type { DatacatClient } from "./datacat.js";

type ToolResult = {
  content: { type: "text"; text: string }[];
  isError?: boolean;
};

const ok = (data: unknown): ToolResult => ({
  content: [{ type: "text", text: JSON.stringify(data, null, 2) }],
});

const fail = (e: unknown): ToolResult => ({
  isError: true,
  content: [{ type: "text", text: `Erreur: ${e instanceof Error ? e.message : String(e)}` }],
});

async function run(fn: () => Promise<unknown>): Promise<ToolResult> {
  try {
    return ok(await fn());
  } catch (e) {
    return fail(e);
  }
}

const timeRange = {
  from: z.string().optional().describe("Borne basse RFC3339 (ex. 2026-06-21T10:00:00Z)"),
  to: z.string().optional().describe("Borne haute RFC3339"),
  limit: z.number().int().positive().optional().describe("Nombre max de lignes"),
};

export function buildServer(client: DatacatClient): McpServer {
  const server = new McpServer({ name: "datacat", version: "0.1.0" });

  server.tool(
    "search_logs",
    "Recherche des logs techniques (OTLP) avec filtres : service, session, trace_id, sévérité minimale, sous-chaîne du corps, fenêtre temporelle.",
    {
      service: z.string().optional(),
      session: z.string().optional(),
      trace_id: z.string().optional(),
      severity_min: z.number().int().optional().describe("Sévérité OTLP minimale (1..24)"),
      q: z.string().optional().describe("Sous-chaîne recherchée dans le corps du log"),
      ...timeRange,
    },
    (args) => run(() => client.searchLogs(args)),
  );

  server.tool(
    "get_trace",
    "Récupère tous les spans d'une trace (par trace_id), ordonnés par début — pour comprendre un parcours technique de bout en bout.",
    { trace_id: z.string().describe("trace_id hexadécimal") },
    ({ trace_id }) => run(() => client.getTrace(trace_id)),
  );

  server.tool(
    "search_events",
    "Recherche des events produit (analytics) : filtres actor, session, tenant, nom d'event, fenêtre temporelle.",
    {
      actor: z.string().optional(),
      session: z.string().optional(),
      tenant: z.string().optional(),
      name: z.string().optional().describe("Nom métier de l'event (event_name)"),
      ...timeRange,
    },
    (args) => run(() => client.searchEvents(args)),
  );

  server.tool(
    "frequent_journeys",
    "Séquences de parcours les plus fréquentes (suite ordonnée d'events par session). Utile pour générer/mettre à jour des tests E2E reflétant l'usage réel.",
    {
      actor: z.string().optional(),
      tenant: z.string().optional(),
      limit: z.number().int().positive().optional(),
    },
    (args) => run(() => client.frequentJourneys(args)),
  );

  server.tool(
    "search_metrics",
    "Recherche des points de métriques (OTLP) : filtres nom de métrique, service, fenêtre temporelle.",
    {
      name: z.string().optional().describe("Nom de la métrique (metric_name)"),
      service: z.string().optional(),
      ...timeRange,
    },
    (args) => run(() => client.searchMetrics(args)),
  );

  server.tool(
    "run_read_sql",
    "Exécute une requête SQL EN LECTURE SEULE (SELECT/WITH uniquement) sur les tables Datacat : events, logs, spans, metric_points. Pour de l'analyse ad-hoc (agrégats, jointures de corrélation). Nécessite QUERY_SQL_ENABLED côté serveur.",
    {
      sql: z.string().describe("Requête SELECT/WITH (instruction unique, sans ';')"),
      limit: z.number().int().positive().optional(),
    },
    ({ sql, limit }) => run(() => client.runReadSql(sql, limit)),
  );

  server.tool(
    "ingest_stats",
    "Statistiques d'ingestion : volumes et déduplication par domaine (events, logs, traces, metrics), drops, état du rate limiting et des bannissements.",
    {},
    () => run(() => client.stats()),
  );

  return server;
}

export const TOOL_NAMES = [
  "search_logs",
  "get_trace",
  "search_events",
  "frequent_journeys",
  "search_metrics",
  "run_read_sql",
  "ingest_stats",
] as const;
