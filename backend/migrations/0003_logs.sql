-- Datacat — ingestion des logs techniques (OpenTelemetry / OTLP).
--
-- Même modèle structurant que les events : table partitionnée par jour (sur `log_time`,
-- horodatage stable porté par l'enregistrement), idempotente sur `(log_time, log_id)` où
-- `log_id` est un hash déterministe du contenu du log (cf. backend/src/logs/model.rs).
-- Corrélation aux events via tenant_id / actor_id / session_id, et aux traces via
-- trace_id / span_id (cahier §4.2, §9 : relier events produit et logs techniques).

CREATE TABLE IF NOT EXISTS logs (
    log_id              uuid        NOT NULL,
    log_time            timestamptz NOT NULL,
    observed_time       timestamptz,
    received_at         timestamptz NOT NULL DEFAULT now(),
    severity_number     smallint,
    severity_text       text,
    body                text,
    service_name        text,
    scope_name          text,
    trace_id            text,
    span_id             text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    resource_attributes jsonb       NOT NULL DEFAULT '{}'::jsonb,
    log_attributes      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (log_time, log_id)
) PARTITION BY RANGE (log_time);

COMMENT ON TABLE  logs IS 'Logs techniques OTLP, partitionnés par jour sur log_time. Idempotents sur (log_time, log_id).';
COMMENT ON COLUMN logs.log_id    IS 'Hash déterministe du contenu du log (dédup des renvois OTLP).';
COMMENT ON COLUMN logs.trace_id  IS 'Trace OTel (hex) — corrélation aux traces.';
COMMENT ON COLUMN logs.session_id IS 'Clé de corrélation avec les events produit.';

CREATE UNLOGGED TABLE IF NOT EXISTS logs_staging (
    log_id              uuid        NOT NULL,
    log_time            timestamptz NOT NULL,
    observed_time       timestamptz,
    received_at         timestamptz NOT NULL,
    severity_number     smallint,
    severity_text       text,
    body                text,
    service_name        text,
    scope_name          text,
    trace_id            text,
    span_id             text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    resource_attributes jsonb       NOT NULL,
    log_attributes      jsonb       NOT NULL
);

-- Fonctions de partition/merge parallèles à celles des events (cf. 0002), ciblant `logs`.

CREATE OR REPLACE FUNCTION datacat_ensure_log_partition(p_day date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name text := format('logs_p%s', to_char(p_day, 'YYYYMMDD'));
    start_ts  text := to_char(p_day,     'YYYY-MM-DD') || ' 00:00:00+00';
    end_ts    text := to_char(p_day + 1, 'YYYY-MM-DD') || ' 00:00:00+00';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF logs FOR VALUES FROM (%L) TO (%L)',
            part_name, start_ts, end_ts
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_ensure_log_partitions_for_staging()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE d date;
BEGIN
    FOR d IN SELECT DISTINCT (log_time AT TIME ZONE 'UTC')::date FROM logs_staging LOOP
        PERFORM datacat_ensure_log_partition(d);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_merge_log_staging()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE inserted bigint;
BEGIN
    INSERT INTO logs
        (log_id, log_time, observed_time, received_at, severity_number, severity_text, body,
         service_name, scope_name, trace_id, span_id, tenant_id, actor_id, session_id,
         resource_attributes, log_attributes)
    SELECT DISTINCT ON (log_time, log_id)
           log_id, log_time, observed_time, received_at, severity_number, severity_text, body,
           service_name, scope_name, trace_id, span_id, tenant_id, actor_id, session_id,
           resource_attributes, log_attributes
    FROM logs_staging
    ORDER BY log_time, log_id, received_at
    ON CONFLICT (log_time, log_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    TRUNCATE logs_staging;
    RETURN inserted;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_drop_log_partitions_before(p_day date)
RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    r       record;
    dropped int  := 0;
    cutoff  text := to_char(p_day, 'YYYYMMDD');
BEGIN
    FOR r IN
        SELECT c.relname AS name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = 'logs'
          AND c.relname ~ '^logs_p[0-9]{8}$'
          AND substring(c.relname FROM 'logs_p([0-9]{8})') < cutoff
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', r.name);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END;
$$;
