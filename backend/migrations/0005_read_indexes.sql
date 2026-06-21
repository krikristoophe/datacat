-- Index de lecture (couche de lecture chaude). Compromis débit d'écriture / latence de lecture
-- assumé dès lors qu'on expose des endpoints de requête (/v1/query/*). Les requêtes lourdes
-- d'analyse de masse restent destinées au stockage froid (Parquet + DataFusion).

-- Events : recherche/parcours par session et par acteur, ordonnés dans le temps.
CREATE INDEX IF NOT EXISTS events_session_idx ON events (session_id, timestamp_client);
CREATE INDEX IF NOT EXISTS events_actor_idx   ON events (actor_id, timestamp_client);

-- Logs : recherche par service / session, et corrélation par trace_id.
CREATE INDEX IF NOT EXISTS logs_service_idx ON logs (service_name, log_time);
CREATE INDEX IF NOT EXISTS logs_session_idx ON logs (session_id, log_time) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS logs_trace_idx   ON logs (trace_id) WHERE trace_id IS NOT NULL;
