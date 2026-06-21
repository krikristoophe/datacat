---
title: "Lecture chaude"
description: "Endpoints de lecture seule sur PostgreSQL pour les données récentes (chaudes) : logs, events, métriques, traces et parcours."
---

Endpoints **en lecture seule** sur PostgreSQL (données chaudes). Découplés de l'ingestion. Pour
l'analyse de masse, voir la lecture froide ([Lecture froide](../read-cold/), DataFusion/Parquet).

Authentification : configurable via `QUERY_AUTH` (`auto`|`static`|`jwt`|`none`) + `QUERY_TOKEN`,
en-tête `Authorization: Bearer`.

## Endpoints

| Endpoint | Paramètres | Réponse |
|---|---|---|
| `GET /v1/query/logs` | `service`, `session`, `trace_id`, `severity_min`, `q` (sous-chaîne du corps), `from`, `to`, `limit` | `{ logs: [...] }` |
| `GET /v1/query/events` | `actor`, `session`, `tenant`, `name`, `from`, `to`, `limit` | `{ events: [...] }` |
| `GET /v1/query/metrics` | `name`, `service`, `from`, `to`, `limit` | `{ metrics: [...] }` |
| `GET /v1/query/traces/{trace_id}` | — | `{ trace_id, span_count, spans: [...] }` (ordonnés par début) |
| `GET /v1/query/journeys` | `actor`, `tenant`, `limit` | `{ journeys: [ { path: [...], occurrences } ] }` |

`from`/`to` sont au format RFC3339. `limit` est plafonné (défaut 100, max 1000 ; journeys 20/200).

## Accès par un agent (MCP)

Le [serveur MCP HTTP](../mcp/) intégré (route `/mcp`) expose ces requêtes comme outils pour Claude.
