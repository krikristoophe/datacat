# Serveur MCP — accès lecture pour un agent (Claude)

`mcp/` est un serveur **MCP** (Model Context Protocol) qui donne à Claude un accès **lecture** aux
données Datacat (logs, traces, events, métriques, parcours). Il permet à l'agent de **debugger,
analyser l'usage réel, vérifier des hypothèses, et alimenter la génération de tests** — sans
jamais écrire ni accéder directement à la base (il passe par la [couche de lecture](read-hot.md)).

## Pourquoi

Le système capture déjà les events produit + la télémétrie technique (logs/traces/métriques)
corrélés. Le brancher à un agent ferme la boucle : l'agent peut interroger ce que les
utilisateurs et les services font *réellement* pour diagnostiquer un incident, comprendre un
parcours, ou écrire un test E2E fidèle à l'usage.

## Outils

`search_logs`, `get_trace`, `search_events`, `frequent_journeys`, `search_metrics`,
`run_read_sql` (SQL lecture seule), `ingest_stats`. Détails et schémas : [mcp/README.md](../mcp/README.md).

## Installation & branchement

```bash
cd mcp && npm install && npm run build
claude mcp add datacat \
  --env DATACAT_URL=http://localhost:8080 \
  --env DATACAT_QUERY_TOKEN=<token-de-lecture> \
  -- node "$PWD/dist/index.js"
```

(Pour `run_read_sql`, activer `QUERY_SQL_ENABLED=true` côté serveur Datacat. Pour `query_auth`
activé, fournir `DATACAT_QUERY_TOKEN`.)

## Exemples d'usage (prompts)

- **Diagnostiquer un incident** : « Cherche les logs ERROR du service `billing` de la dernière
  heure, puis récupère la trace du premier pour voir où ça casse. »
  → `search_logs(service=billing, severity_min=17)` puis `get_trace(trace_id=…)`.
- **Comprendre un utilisateur** : « Quels events a produit l'acteur `user-123` aujourd'hui ? »
  → `search_events(actor=user-123, from=…)`.
- **Générer un test E2E** : « Quels sont les 5 parcours les plus fréquents ? Écris un test
  Playwright pour le plus courant. »
  → `frequent_journeys(limit=5)` puis génération du test à partir de la séquence.
- **Corréler** : « Pour la session `sess-abc`, montre les events produit et les logs techniques
  côte à côte, ordonnés dans le temps. »
  → `run_read_sql("SELECT 'event' AS k, timestamp_client AS t, event_name AS d FROM events WHERE session_id='sess-abc' UNION ALL SELECT 'log', log_time, body FROM logs WHERE session_id='sess-abc' ORDER BY t")`.
- **Surveiller l'ingestion** : « Combien d'events/logs ingérés et dédupliqués ? » → `ingest_stats`.

## Sécurité

Lecture seule de bout en bout : le serveur MCP n'appelle que `/v1/query/*` et `/stats`. Le SQL
ad-hoc est borné (SELECT/WITH, transaction READ ONLY, timeout, plafond de lignes) et désactivable.
L'accès est protégé par `QUERY_AUTH` (token de lecture) — le serveur MCP transmet le Bearer.
