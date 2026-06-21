# @datacat/mcp-server

Serveur **MCP** (Model Context Protocol) donnant à un agent (Claude) un accès **lecture** aux
données Datacat — logs, traces, events, métriques, parcours — pour **debugger, analyser l'usage
réel, vérifier des hypothèses, et alimenter la génération de tests**.

C'est un client léger de la [couche de lecture](../docs/read-hot.md) (`/v1/query/*`) : aucune
écriture, aucun accès direct à la base.

## Outils exposés

| Outil | Rôle |
|---|---|
| `search_logs` | Recherche de logs (service, session, trace_id, sévérité, sous-chaîne, temps). |
| `get_trace` | Tous les spans d'une trace (par `trace_id`), ordonnés. |
| `search_events` | Recherche d'events produit (actor, session, tenant, name, temps). |
| `frequent_journeys` | Séquences de parcours fréquentes par session (génération de tests E2E). |
| `search_metrics` | Points de métriques (name, service, temps). |
| `run_read_sql` | SQL **lecture seule** ad-hoc (SELECT/WITH) sur events/logs/spans/metric_points. Nécessite `QUERY_SQL_ENABLED` côté serveur. |
| `ingest_stats` | Volumes et déduplication par domaine, drops, état du rate limiting. |

## Configuration

| Variable | Défaut | Rôle |
|---|---|---|
| `DATACAT_URL` | `http://localhost:8080` | URL de l'API Datacat. |
| `DATACAT_QUERY_TOKEN` | — | Token de lecture (Bearer) si `QUERY_AUTH` est activé côté serveur. |

## Installation & build

```bash
cd mcp
npm install
npm run build       # génère dist/
npm test            # tests (transport in-memory + fetch mocké)
```

## Brancher dans Claude Code

```bash
claude mcp add datacat \
  --env DATACAT_URL=http://localhost:8080 \
  --env DATACAT_QUERY_TOKEN=<token-de-lecture> \
  -- node /chemin/vers/datacat/mcp/dist/index.js
```

ou via un `.mcp.json` de projet :

```json
{
  "mcpServers": {
    "datacat": {
      "command": "node",
      "args": ["/chemin/vers/datacat/mcp/dist/index.js"],
      "env": { "DATACAT_URL": "http://localhost:8080", "DATACAT_QUERY_TOKEN": "..." }
    }
  }
}
```

## Brancher dans Claude Desktop

Dans `claude_desktop_config.json` :

```json
{
  "mcpServers": {
    "datacat": {
      "command": "node",
      "args": ["/chemin/vers/datacat/mcp/dist/index.js"],
      "env": { "DATACAT_URL": "http://localhost:8080" }
    }
  }
}
```

Voir [docs/mcp.md](../docs/mcp.md) pour des exemples d'usage (debug, parcours, corrélation).
