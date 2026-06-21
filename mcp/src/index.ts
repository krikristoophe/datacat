#!/usr/bin/env node
/**
 * Point d'entrée du serveur MCP Datacat (transport stdio).
 *
 * Configuration par variables d'environnement :
 *   DATACAT_URL          URL de l'API Datacat (défaut http://localhost:8080)
 *   DATACAT_QUERY_TOKEN  Token de lecture (Bearer) si query_auth est activé
 */

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { DatacatClient } from "./datacat.js";
import { buildServer } from "./server.js";

async function main(): Promise<void> {
  const baseUrl = process.env.DATACAT_URL ?? "http://localhost:8080";
  const token = process.env.DATACAT_QUERY_TOKEN || undefined;

  const client = new DatacatClient({ baseUrl, token });
  const server = buildServer(client);

  const transport = new StdioServerTransport();
  await server.connect(transport);
  // stdout est le canal MCP : toute trace humaine va sur stderr.
  console.error(`datacat-mcp prêt (cible: ${baseUrl})`);
}

main().catch((e) => {
  console.error("datacat-mcp: échec au démarrage:", e);
  process.exit(1);
});
