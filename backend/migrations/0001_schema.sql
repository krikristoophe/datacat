-- Datacat — schéma d'ingestion v1
--
-- Idempotence + partitionnement par temps : décision structurante.
--
-- La table `events` est partitionnée par RANGE sur `timestamp_client`. PostgreSQL impose
-- que toute contrainte UNIQUE d'une table partitionnée inclue la clé de partition. Or
-- `timestamp_client` est le SEUL horodatage stable entre deux envois d'un même event :
-- `event_id` ET `timestamp_client` sont figés à la création de l'event côté SDK et réutilisés
-- à l'identique sur chaque retry (cf. docs/CONTRACT.md §2.2). Tout doublon retombe donc dans
-- la même partition, ce qui rend la déduplication `ON CONFLICT (timestamp_client, event_id)`
-- globalement correcte. `received_at` (serveur, différent à chaque réception) ne conviendrait
-- pas comme clé de dédup. Voir docs/architecture.md pour le raisonnement complet.

CREATE TABLE IF NOT EXISTS events (
    event_id         uuid        NOT NULL,
    event_name       text        NOT NULL,
    tenant_id        text,
    actor_id         text        NOT NULL,
    session_id       text        NOT NULL,
    timestamp_client timestamptz NOT NULL,
    received_at      timestamptz NOT NULL DEFAULT now(),
    properties       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    -- Clé d'idempotence. La clé de partition (timestamp_client) DOIT en faire partie.
    PRIMARY KEY (timestamp_client, event_id)
) PARTITION BY RANGE (timestamp_client);

COMMENT ON TABLE  events IS 'Events d''analytics, partitionnés par jour sur timestamp_client. Idempotents sur (timestamp_client, event_id).';
COMMENT ON COLUMN events.event_id         IS 'UUID généré côté client à la création. Clé d''idempotence.';
COMMENT ON COLUMN events.timestamp_client IS 'Horodatage client figé à la création (stable entre retries). Clé de partition.';
COMMENT ON COLUMN events.received_at      IS 'Horodatage serveur de réception. Jamais fourni par le client.';

-- Table de staging pour le chargement en masse par COPY.
-- UNLOGGED : pas de WAL → débit d'écriture maximal. La perte des données récentes de staging
-- en cas de crash est acceptable (tolérance à la perte, cahier §2) ; le merge vers `events`
-- est, lui, journalisé et durable. Aucune contrainte ici : le dédoublonnage a lieu au merge.
CREATE UNLOGGED TABLE IF NOT EXISTS events_staging (
    event_id         uuid        NOT NULL,
    event_name       text        NOT NULL,
    tenant_id        text,
    actor_id         text        NOT NULL,
    session_id       text        NOT NULL,
    timestamp_client timestamptz NOT NULL,
    received_at      timestamptz NOT NULL,
    properties       jsonb       NOT NULL
);

COMMENT ON TABLE events_staging IS 'Cible des COPY de micro-batch (UNLOGGED). Fusionnée puis vidée par datacat_merge_staging().';

-- v1 = ingestion uniquement : aucun index secondaire (priorité au débit d'écriture, cahier §2).
-- Les index du chemin de lecture (actor_id, session_id, tenant_id) seront ajoutés avec la
-- couche de lecture analytique (hors v1), sur le stockage froid de préférence.
