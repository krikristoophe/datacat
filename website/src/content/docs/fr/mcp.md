---
title: "Serveur MCP"
description: "Le serveur MCP HTTP intégré donne à un agent (Claude) un accès lecture aux logs, traces, events, métriques et parcours, pour le debug, l'analyse et la génération de tests."
---

Le backend Datacat expose un serveur **MCP** (Model Context Protocol) en **HTTP** sur la route
**`/mcp`** (transport *streamable HTTP*). Il donne à un agent (Claude) un accès **lecture** aux
données — logs, traces, events, métriques, parcours — pour **debugger, analyser l'usage réel,
vérifier des hypothèses et générer des tests**.

> **Intégré, rien à installer.** Le MCP fait partie du binaire d'ingestion (crate Rust `rmcp`),
> ses outils tapent la [couche de lecture](../read-hot/) **en in-process** (aucun saut HTTP).
> Un agent s'y connecte simplement par une URL — pas de process ni de paquet à installer.

## Activation & sécurité

- Activé par défaut (`MCP_ENABLED=true`).
- Protégé par `query_auth` (`auto`|`static`|`jwt`|`none`) + `QUERY_TOKEN` : le token de lecture
  est attendu dans l'en-tête `Authorization: Bearer` (même token que `/v1/query/*`).
- Monté **hors du timeout HTTP global** (le flux SSE est long-vécu).
- Lecture seule de bout en bout.

## Outils exposés

| Outil | Rôle |
|---|---|
| `search_logs` | Recherche de logs (service, session, trace_id, sévérité, sous-chaîne, temps). |
| `get_trace` | Tous les spans d'une trace (par `trace_id`), ordonnés. |
| `search_events` | Recherche d'events produit (actor, session, tenant, name, temps). |
| `frequent_journeys` | Séquences de parcours fréquentes par session (génération de tests E2E). |
| `search_metrics` | Points de métriques (name, service, temps). |
| `ingest_stats` | Volumes et déduplication par domaine, drops. |

## Brancher dans Claude Code

```bash
claude mcp add --transport http datacat https://datacat.example.com/mcp \
  --header "Authorization: Bearer <token-de-lecture>"
```

ou via un `.mcp.json` de projet :

```json
{
  "mcpServers": {
    "datacat": {
      "type": "http",
      "url": "https://datacat.example.com/mcp",
      "headers": { "Authorization": "Bearer <token-de-lecture>" }
    }
  }
}
```

(En local : `http://localhost:8080/mcp`. Si `QUERY_AUTH=none`, l'en-tête est inutile.)

## Exemples d'usage (prompts)

- **Diagnostiquer un incident** : « Cherche les logs ERROR du service `billing` de la dernière
  heure, puis récupère la trace du premier pour voir où ça casse. »
  → `search_logs(service=billing, severity_min=17)` puis `get_trace(trace_id=…)`.
- **Comprendre un utilisateur** : « Quels events a produit l'acteur `user-123` aujourd'hui ? »
  → `search_events(actor=user-123, from=…)`.
- **Générer un test E2E** : « Quels sont les 5 parcours les plus fréquents ? Écris un test
  Playwright pour le plus courant. » → `frequent_journeys(limit=5)` puis génération.
- **Corréler** : « Pour la session `sess-abc`, montre les events et les logs dans l'ordre
  chronologique. »
  → `search_events(session=sess-abc)` et `search_logs(session=sess-abc)`.
- **Surveiller l'ingestion** : « Combien d'events/logs ingérés et dédupliqués ? » → `ingest_stats`.
