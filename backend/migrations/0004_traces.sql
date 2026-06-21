-- Datacat — ingestion des traces (OpenTelemetry / OTLP).
--
-- Même modèle structurant que events/logs : table partitionnée par jour (sur `start_time`,
-- horodatage stable porté par le span), idempotente sur `(start_time, trace_id, span_id)`
-- (clé naturelle d'un span, stable entre deux exports → dédup des renvois OTLP).
-- Corrélation aux events/logs via tenant/actor/session, et au reste de la trace via trace_id.

CREATE TABLE IF NOT EXISTS spans (
    trace_id            text        NOT NULL,
    span_id             text        NOT NULL,
    parent_span_id      text,
    start_time          timestamptz NOT NULL,
    end_time            timestamptz,
    duration_ms         double precision,
    name                text        NOT NULL,
    kind                smallint,
    service_name        text,
    scope_name          text,
    status_code         smallint,           -- 0 unset, 1 ok, 2 error (OTLP)
    status_message      text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    received_at         timestamptz NOT NULL DEFAULT now(),
    resource_attributes jsonb       NOT NULL DEFAULT '{}'::jsonb,
    span_attributes     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    events              jsonb       NOT NULL DEFAULT '[]'::jsonb,
    links               jsonb       NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (start_time, trace_id, span_id)
) PARTITION BY RANGE (start_time);

COMMENT ON TABLE spans IS 'Spans OTLP, partitionnés par jour sur start_time. Idempotents sur (start_time, trace_id, span_id).';

-- Index de lecture (couche de lecture) : récupération d'une trace par id, corrélation par session.
-- Compromis débit d'écriture / latence de lecture assumé dès lors qu'on expose des requêtes.
CREATE INDEX IF NOT EXISTS spans_trace_idx   ON spans (trace_id);
CREATE INDEX IF NOT EXISTS spans_session_idx ON spans (session_id, start_time) WHERE session_id IS NOT NULL;

CREATE UNLOGGED TABLE IF NOT EXISTS spans_staging (
    trace_id            text        NOT NULL,
    span_id             text        NOT NULL,
    parent_span_id      text,
    start_time          timestamptz NOT NULL,
    end_time            timestamptz,
    duration_ms         double precision,
    name                text        NOT NULL,
    kind                smallint,
    service_name        text,
    scope_name          text,
    status_code         smallint,
    status_message      text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    received_at         timestamptz NOT NULL,
    resource_attributes jsonb       NOT NULL,
    span_attributes     jsonb       NOT NULL,
    events              jsonb       NOT NULL,
    links               jsonb       NOT NULL
);

CREATE OR REPLACE FUNCTION datacat_ensure_span_partition(p_day date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name text := format('spans_p%s', to_char(p_day, 'YYYYMMDD'));
    start_ts  text := to_char(p_day,     'YYYY-MM-DD') || ' 00:00:00+00';
    end_ts    text := to_char(p_day + 1, 'YYYY-MM-DD') || ' 00:00:00+00';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF spans FOR VALUES FROM (%L) TO (%L)',
            part_name, start_ts, end_ts
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_ensure_span_partitions_for_staging()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE d date;
BEGIN
    FOR d IN SELECT DISTINCT (start_time AT TIME ZONE 'UTC')::date FROM spans_staging LOOP
        PERFORM datacat_ensure_span_partition(d);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_merge_span_staging()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE inserted bigint;
BEGIN
    INSERT INTO spans
        (trace_id, span_id, parent_span_id, start_time, end_time, duration_ms, name, kind,
         service_name, scope_name, status_code, status_message, tenant_id, actor_id, session_id,
         received_at, resource_attributes, span_attributes, events, links)
    SELECT DISTINCT ON (start_time, trace_id, span_id)
           trace_id, span_id, parent_span_id, start_time, end_time, duration_ms, name, kind,
           service_name, scope_name, status_code, status_message, tenant_id, actor_id, session_id,
           received_at, resource_attributes, span_attributes, events, links
    FROM spans_staging
    ORDER BY start_time, trace_id, span_id, received_at
    ON CONFLICT (start_time, trace_id, span_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    TRUNCATE spans_staging;
    RETURN inserted;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_drop_span_partitions_before(p_day date)
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
        WHERE p.relname = 'spans'
          AND c.relname ~ '^spans_p[0-9]{8}$'
          AND substring(c.relname FROM 'spans_p([0-9]{8})') < cutoff
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', r.name);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END;
$$;
