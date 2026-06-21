-- Fonctions de gestion des partitions et du merge idempotent.
-- Le SQL dynamique vit dans la base (auditable, testable) ; le backend Rust se contente
-- d'orchestrer (création anticipée des partitions, purge périodique, flush des micro-batches).

-- Crée (si absente) la partition journalière contenant le jour UTC `p_day`.
-- Bornes explicites en UTC, indépendantes du fuseau de session.
CREATE OR REPLACE FUNCTION datacat_ensure_partition(p_day date)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    part_name text := format('events_p%s', to_char(p_day, 'YYYYMMDD'));
    start_ts  text := to_char(p_day,     'YYYY-MM-DD') || ' 00:00:00+00';
    end_ts    text := to_char(p_day + 1, 'YYYY-MM-DD') || ' 00:00:00+00';
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = part_name) THEN
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            part_name, start_ts, end_ts
        );
    END IF;
END;
$$;

-- Garantit l'existence d'une partition pour chaque jour présent dans le staging,
-- afin que le merge ne puisse jamais échouer faute de partition cible.
CREATE OR REPLACE FUNCTION datacat_ensure_partitions_for_staging()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE d date;
BEGIN
    FOR d IN
        SELECT DISTINCT (timestamp_client AT TIME ZONE 'UTC')::date FROM events_staging
    LOOP
        PERFORM datacat_ensure_partition(d);
    END LOOP;
END;
$$;

-- Fusionne le staging dans `events` de façon idempotente, puis vide le staging.
-- DISTINCT ON collapse les doublons intra-batch ; ON CONFLICT DO NOTHING collapse les
-- doublons inter-batch / déjà persistés. Retourne le nombre de lignes RÉELLEMENT insérées
-- (après déduplication). Atomique : INSERT + TRUNCATE dans la transaction de la fonction.
CREATE OR REPLACE FUNCTION datacat_merge_staging()
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE inserted bigint;
BEGIN
    INSERT INTO events AS e
        (event_id, event_name, tenant_id, actor_id, session_id, timestamp_client, received_at, properties)
    SELECT DISTINCT ON (timestamp_client, event_id)
           event_id, event_name, tenant_id, actor_id, session_id, timestamp_client, received_at, properties
    FROM events_staging
    ORDER BY timestamp_client, event_id, received_at
    ON CONFLICT (timestamp_client, event_id) DO NOTHING;

    GET DIAGNOSTICS inserted = ROW_COUNT;
    TRUNCATE events_staging;
    RETURN inserted;
END;
$$;

-- Purge de la rétention : DROP des partitions strictement antérieures au jour `p_day`.
-- DROP TABLE d'une partition est instantané (pas de DELETE/VACUUM). Retourne le nombre
-- de partitions supprimées.
CREATE OR REPLACE FUNCTION datacat_drop_partitions_before(p_day date)
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
        WHERE p.relname = 'events'
          AND c.relname ~ '^events_p[0-9]{8}$'
          AND substring(c.relname FROM 'events_p([0-9]{8})') < cutoff
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', r.name);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END;
$$;
