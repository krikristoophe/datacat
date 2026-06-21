---
title: "Lecture froide"
description: "SQL analytique sur le stockage froid avec Apache DataFusion : interroger directement les fichiers Parquet exportés sur S3, sans passer par PostgreSQL."
---

## Vue d'ensemble

Le crate `datacat-reader` fournit un moteur de requête SQL analytique sur le
stockage froid de Datacat.  Il s'appuie sur [Apache DataFusion](https://datafusion.apache.org/)
pour exécuter du SQL arbitraire directement sur les fichiers Parquet exportés
sur S3-compatible (AWS S3, MinIO), sans passer par la base PostgreSQL.

```
         ┌────────────┐     export quotidien     ┌──────────────────────┐
         │ PostgreSQL │ ─────────────────────────▶│  S3 / MinIO          │
         │ (chaud)    │    datacat-exporter       │  Parquet + Hive      │
         └────────────┘                           │  layout              │
                                                  └──────────┬───────────┘
                                                             │
                                              ┌──────────────▼──────────────┐
                                              │   datacat-reader             │
                                              │   DataFusion (SQL)           │
                                              │   object_store (S3/MinIO)    │
                                              └──────────────────────────────┘
```

**Positionnement dans l'architecture Datacat :**

| Couche             | Technologie                  | Latence     | Usage                              |
|--------------------|------------------------------|-------------|------------------------------------|
| Lecture chaude     | PostgreSQL (backend REST)    | < 50 ms     | API temps-réel, dashboards live    |
| Lecture analytique | DataFusion sur Parquet S3    | secondes    | Requêtes historiques, exports, BI  |
| Stockage froid     | Parquet zstd sur S3          | —           | Archive longue durée, format ouvert|

La lecture lente est **acceptée et documentée** : DataFusion scanne les fichiers
Parquet depuis S3 à chaque requête.  Ce n'est pas un remplacement des requêtes
temps-réel du backend — c'est la couche analytique sur le froid.

---

## Layout S3 (Hive-partition)

```
<bucket>/
  events/
    date=2024-06-15/
      part-0000.parquet
    date=2024-06-16/
      part-0000.parquet
    ...
  logs/
    date=2024-06-15/
      part-0000.parquet
    ...
```

Ce layout est **compatible Iceberg/Spark** : la partition `date=YYYY-MM-DD`
est une Hive-partition standard que les outils de l'écosystème Data reconnaissent
nativement.

---

## Schémas Parquet

### Table `events`

| Colonne          | Type Arrow                       | Nullable |
|------------------|----------------------------------|----------|
| event_id         | Utf8                             | non      |
| event_name       | Utf8                             | non      |
| tenant_id        | Utf8                             | oui      |
| actor_id         | Utf8                             | non      |
| session_id       | Utf8                             | non      |
| timestamp_client | Timestamp(Microsecond, UTC)      | non      |
| received_at      | Timestamp(Microsecond, UTC)      | non      |
| properties       | Utf8 (JSON sérialisé)            | non      |

### Table `logs`

| Colonne              | Type Arrow                  | Nullable |
|----------------------|-----------------------------|----------|
| log_id               | Utf8                        | non      |
| log_time             | Timestamp(Microsecond, UTC) | non      |
| observed_time        | Timestamp(Microsecond, UTC) | oui      |
| received_at          | Timestamp(Microsecond, UTC) | non      |
| severity_number      | Int16                       | oui      |
| severity_text        | Utf8                        | oui      |
| body                 | Utf8                        | oui      |
| service_name         | Utf8                        | oui      |
| scope_name           | Utf8                        | oui      |
| trace_id             | Utf8                        | oui      |
| span_id              | Utf8                        | oui      |
| tenant_id            | Utf8                        | oui      |
| actor_id             | Utf8                        | oui      |
| session_id           | Utf8                        | oui      |
| resource_attributes  | Utf8 (JSON sérialisé)       | non      |
| log_attributes       | Utf8 (JSON sérialisé)       | non      |

---

## Configuration

Les variables d'environnement sont identiques à celles de `datacat-exporter` :

| Variable              | Obligatoire | Défaut     | Description                              |
|-----------------------|-------------|------------|------------------------------------------|
| `S3_ENDPOINT`         | non         | AWS S3     | URL endpoint (ex. `http://localhost:9200`) |
| `S3_REGION`           | non         | `eu-west-1`| Région AWS / MinIO                       |
| `S3_BUCKET`           | oui         | —          | Nom du bucket                            |
| `AWS_ACCESS_KEY_ID`   | oui         | —          | Access key                               |
| `AWS_SECRET_ACCESS_KEY`| oui        | —          | Secret key                               |
| `S3_ALLOW_HTTP`       | non         | `false`    | `true` pour MinIO local (sans TLS)       |
| `S3_PREFIX`           | non         | racine     | Préfixe dans le bucket (ex. `prod/`)     |

---

## CLI : `datacat-query-cold`

```bash
# Comptage global
datacat-query-cold --table events \
  --sql "SELECT count(*) FROM events"

# TOP 10 events les plus fréquents
datacat-query-cold --table events \
  --sql "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC LIMIT 10"

# Filtrer sur une date précise
datacat-query-cold --table events --date 2024-06-15 \
  --sql "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC"

# Format JSON
datacat-query-cold --table events --date 2024-06-15 \
  --sql "SELECT actor_id, count(*) AS n FROM events GROUP BY actor_id" \
  --format json
```

### Variables d'environnement pour MinIO local

```bash
export S3_ENDPOINT=http://localhost:9200
export S3_REGION=us-east-1
export S3_BUCKET=datacat
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_ALLOW_HTTP=true
```

---

## Exemples de requêtes analytiques

### 1. Événements les plus fréquents

```sql
SELECT event_name, count(*) AS n
FROM events
GROUP BY event_name
ORDER BY n DESC
LIMIT 20
```

### 2. Activité par actor

```sql
SELECT actor_id, count(*) AS n_events, count(DISTINCT session_id) AS n_sessions
FROM events
GROUP BY actor_id
ORDER BY n_events DESC
```

### 3. Séquences d'events par session (parcours utilisateur)

Utile pour la **génération de tests E2E** : récupère les séquences d'events
dans l'ordre chronologique par session.

```sql
-- Vue d'ensemble : nombre d'events et durée par session
SELECT
    session_id,
    count(*) AS n_events,
    min(timestamp_client) AS first_event_at,
    max(timestamp_client) AS last_event_at,
    max(timestamp_client) - min(timestamp_client) AS session_duration
FROM events
GROUP BY session_id
ORDER BY first_event_at
```

Pour obtenir la séquence complète des events par session (ordre chronologique) :

```sql
-- Séquence d'events pour une session donnée
SELECT event_name, timestamp_client, actor_id, properties
FROM events
WHERE session_id = 'session-42'
ORDER BY timestamp_client
```

> **Note DataFusion** : `array_agg(event_name ORDER BY timestamp_client)` est
> supporté en DataFusion 54 mais les résultats sont des colonnes de type `List`.
> L'approche recommandée pour l'intégration CI est d'utiliser le GROUP BY +
> ORDER BY ci-dessus pour inspecter les séquences, puis de générer les scripts
> de test en post-traitement.

### 4. Répartition temporelle (par heure)

```sql
SELECT
    date_trunc('hour', timestamp_client) AS hour,
    count(*) AS n_events
FROM events
GROUP BY date_trunc('hour', timestamp_client)
ORDER BY hour
```

### 5. Analyse des logs par sévérité

```sql
SELECT
    severity_text,
    count(*) AS n,
    count(DISTINCT trace_id) AS n_traces
FROM logs
WHERE severity_number >= 9  -- WARNING et au-dessus
GROUP BY severity_text
ORDER BY n DESC
```

### 6. Corrélation events/logs par session

```sql
-- Requête cross-table : sessions avec à la fois des events et des logs d'erreur
SELECT e.session_id, count(DISTINCT e.event_name) AS n_event_types
FROM events e
WHERE e.session_id IN (
    SELECT DISTINCT session_id FROM logs WHERE severity_number >= 17
)
GROUP BY e.session_id
ORDER BY n_event_types DESC
```

---

## Architecture technique

### Flux de données

```
S3 (Parquet zstd)
    │
    ├── object_store 0.13 (AWS SigV4, HTTP/HTTPS)
    │       └── ListingTable (scan Hive partitions)
    │
    └── DataFusion 54 (SQL → plan physique → RecordBatch Arrow)
            ├── Projection / Filter / Aggregation
            ├── Parquet reader (predicate pushdown, column pruning)
            └── Arrow RecordBatch → sortie (table ASCII / JSON)
```

### Compatibilité Iceberg

Le layout Hive `table/date=YYYY-MM-DD/part-*.parquet` est le substrat naturel
d'une table Iceberg v2.  La migration vers Iceberg est possible sans re-écriture
des données : il suffit d'ajouter un catalogue Iceberg (ex. REST catalog) qui
pointe sur les mêmes fichiers Parquet.  DataFusion dispose d'un connecteur
Iceberg (`datafusion-iceberg`) qui peut être ajouté ultérieurement.

### Performances

- **Lecture lente acceptée** : chaque requête scanne les fichiers Parquet
  depuis S3.  Pour des plages de dates courtes (`--date YYYY-MM-DD`), la
  latence est de l'ordre de la seconde.  Pour des scans complets d'un mois,
  compter plusieurs secondes à dizaines de secondes selon la volumétrie.
- **Predicate pushdown** : DataFusion pousse les filtres sur les colonnes
  Parquet (min/max statistics, bloom filters) pour réduire les I/O.
- **Parallélisme** : DataFusion exécute les plans en parallèle sur plusieurs
  threads (configurable via `SessionConfig`).

---

## Tests e2e

Le test e2e se lance via `reader/run-tests.sh` :

```bash
cd reader && ./run-tests.sh
```

Ce script :
1. Démarre un conteneur MinIO sur les ports 9200/9201.
2. Lance `cargo test -- --nocapture`.
3. Le test génère des Parquet d'events synthétiques, les uploade sur MinIO,
   puis exécute des requêtes DataFusion et vérifie les résultats.
4. Supprime le conteneur MinIO.

Les tests e2e ne sont **pas lancés en CI** (nécessitent Docker + MinIO).
Le job CI `reader` se limite à `cargo build --release`, `cargo clippy`,
`cargo fmt --check`.

---

## Lien avec les autres composants

| Composant              | Rôle                                                        |
|------------------------|-------------------------------------------------------------|
| `exporter/`            | Exporte PostgreSQL → Parquet S3 (produit les fichiers lus). Un crate standalone, également embarqué & planifié dans le backend. |
| `backend/`             | API REST temps-réel sur PostgreSQL (lecture chaude)         |
| `reader/` (ce crate)   | Requêtes analytiques SQL sur Parquet S3 (lecture froide)    |

Le `reader` est **en lecture seule** : il ne modifie jamais les données S3.
