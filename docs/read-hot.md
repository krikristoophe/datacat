# Couche de lecture chaude (`/v1/query/*`)

Endpoints **en lecture seule** sur PostgreSQL (données chaudes). Découplés de l'ingestion. Pour
l'analyse de masse, voir la lecture froide ([read-cold.md](read-cold.md), DataFusion/Parquet).

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
| `POST /v1/query/sql` | corps `{ sql, limit? }` | `{ row_count, truncated, rows: [...] }` |

`from`/`to` sont au format RFC3339. `limit` est plafonné (défaut 100, max 1000 ; journeys 20/200).

## SQL ad-hoc en lecture seule (`/v1/query/sql`)

Pour l'analyse exploratoire (agrégats, jointures de corrélation). **Désactivé par défaut**
(`QUERY_SQL_ENABLED=true` pour l'activer). Défenses :

- seules les requêtes `SELECT` / `WITH` sont acceptées ; **instruction unique** (`;` interdit) ;
- exécution dans une transaction **`READ ONLY`** (bloque tout `INSERT`/`UPDATE`/`DELETE`/DDL,
  défense en profondeur) avec **`statement_timeout`** (`QUERY_SQL_TIMEOUT`, défaut 10s) ;
- encapsulation `SELECT to_jsonb(t) FROM (<sql>) AS t LIMIT n` → résultat JSON, lignes plafonnées
  (`QUERY_SQL_MAX_ROWS`, défaut 1000, drapeau `truncated`) ;
- protégé par `QUERY_AUTH`.

Exemple :
```bash
curl -s -X POST "$DATACAT/v1/query/sql" -H 'content-type: application/json' \
  -d '{"sql":"SELECT service_name, count(*) FROM logs WHERE severity_number>=17 GROUP BY 1 ORDER BY 2 DESC"}'
```

Tables interrogeables : `events`, `logs`, `spans`, `metric_points` (corrélables via
`session_id` / `actor_id` / `tenant_id` / `trace_id`).

## Accès par un agent (MCP)

Le [serveur MCP HTTP](mcp.md) intégré (route `/mcp`) expose ces requêtes comme outils pour Claude.
