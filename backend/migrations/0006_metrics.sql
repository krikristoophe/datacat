-- Datacat — ingestion des métriques (OpenTelemetry / OTLP).
--
-- Même modèle structurant que events/logs/traces : table partitionnée par jour (sur `time`,
-- horodatage stable porté par le point de mesure), idempotente sur `(time, point_id)` où
-- `point_id` est un hash déterministe du contenu du point (cf. backend/src/metrics/model.rs).
-- Chaque point de donnée (gauge / sum / histogram) est aplati en une ligne. Corrélation aux
-- events/logs/traces via tenant_id / actor_id / session_id.

CREATE TABLE IF NOT EXISTS metric_points (
    point_id            uuid        NOT NULL,
    time                timestamptz NOT NULL,
    metric_name         text        NOT NULL,
    metric_type         text        NOT NULL,   -- gauge | sum | histogram
    unit                text,
    value_double        double precision,
    value_int           bigint,
    count               bigint,                 -- histogram : nombre de mesures
    sum                 double precision,        -- histogram : somme des mesures
    buckets             jsonb,                   -- histogram : { bounds: [...], counts: [...] }
    service_name        text,
    scope_name          text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    received_at         timestamptz NOT NULL DEFAULT now(),
    resource_attributes jsonb       NOT NULL DEFAULT '{}'::jsonb,
    attributes          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (time, point_id)
) PARTITION BY RANGE (time);

COMMENT ON TABLE  metric_points IS 'Points de métriques OTLP (gauge/sum/histogram), partitionnés par jour sur time. Idempotents sur (time, point_id).';
COMMENT ON COLUMN metric_points.point_id    IS 'Hash déterministe du contenu du point (dédup des renvois OTLP — pas d''id natif).';
COMMENT ON COLUMN metric_points.metric_type IS 'gauge | sum | histogram (summary / exponentialHistogram ignorés).';
COMMENT ON COLUMN metric_points.buckets     IS 'Histogram : { "bounds": [...explicitBounds], "counts": [...bucketCounts] }.';

-- Index de lecture (couche de lecture) : agrégation par métrique et par service sur une fenêtre.
-- Compromis débit d'écriture / latence de lecture assumé dès lors qu'on expose des requêtes.
CREATE INDEX IF NOT EXISTS metric_points_name_idx    ON metric_points (metric_name, time);
CREATE INDEX IF NOT EXISTS metric_points_service_idx ON metric_points (service_name, time) WHERE service_name IS NOT NULL;

CREATE UNLOGGED TABLE IF NOT EXISTS metric_points_staging (
    point_id            uuid        NOT NULL,
    time                timestamptz NOT NULL,
    metric_name         text        NOT NULL,
    metric_type         text        NOT NULL,
    unit                text,
    value_double        double precision,
    value_int           bigint,
    count               bigint,
    sum                 double precision,
    buckets             jsonb,
    service_name        text,
    scope_name          text,
    tenant_id           text,
    actor_id            text,
    session_id          text,
    received_at         timestamptz NOT NULL,
    resource_attributes jsonb       NOT NULL,
    attributes          jsonb       NOT NULL
);

CREATE OR REPLACE FUNCTION datacat_ensure_metric_partition(p_day date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name text := format('metric_points_p%s', to_char(p_day, 'YYYYMMDD'));
    start_ts  text := to_char(p_day,     'YYYY-MM-DD') || ' 00:00:00+00';
    end_ts    text := to_char(p_day + 1, 'YYYY-MM-DD') || ' 00:00:00+00';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF metric_points FOR VALUES FROM (%L) TO (%L)',
            part_name, start_ts, end_ts
        );
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_ensure_metric_partitions_for_staging()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE d date;
BEGIN
    FOR d IN SELECT DISTINCT (time AT TIME ZONE 'UTC')::date FROM metric_points_staging LOOP
        PERFORM datacat_ensure_metric_partition(d);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_merge_metric_staging()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE inserted bigint;
BEGIN
    INSERT INTO metric_points
        (point_id, time, metric_name, metric_type, unit, value_double, value_int, count, sum,
         buckets, service_name, scope_name, tenant_id, actor_id, session_id,
         received_at, resource_attributes, attributes)
    SELECT DISTINCT ON (time, point_id)
           point_id, time, metric_name, metric_type, unit, value_double, value_int, count, sum,
           buckets, service_name, scope_name, tenant_id, actor_id, session_id,
           received_at, resource_attributes, attributes
    FROM metric_points_staging
    ORDER BY time, point_id, received_at
    ON CONFLICT (time, point_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    TRUNCATE metric_points_staging;
    RETURN inserted;
END;
$$;

CREATE OR REPLACE FUNCTION datacat_drop_metric_partitions_before(p_day date)
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
        WHERE p.relname = 'metric_points'
          AND c.relname ~ '^metric_points_p[0-9]{8}$'
          AND substring(c.relname FROM 'metric_points_p([0-9]{8})') < cutoff
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', r.name);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END;
$$;
