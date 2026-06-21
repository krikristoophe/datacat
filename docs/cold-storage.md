# Stockage froid — Export PostgreSQL → Parquet sur S3

## Vue d'ensemble

```
PostgreSQL (events / logs)
        │
        │  SELECT … WHERE timestamp_client IN [jour, jour+1)
        │  par lots de 10 000 lignes
        ▼
  Arrow RecordBatch (colonnes typées)
        │
        │  ArrowWriter (Parquet zstd level 3)
        ▼
  Fichier Parquet en mémoire
        │
        │  object_store PUT
        ▼
  S3-compatible (MinIO / Scaleway / Cloudflare R2 / AWS S3 EU)
        │
        ▼  (futur) DataFusion / DuckDB lisent directement
```

Le crate `exporter/` est un binaire standalone (`datacat-export`) sans dépendance vers
le cœur d'ingestion (`backend/`). Les frontières sont nettes : l'ingestion écrit en base,
l'export lit en base et écrit sur S3. Les deux composants n'ont aucune dépendance mutuelle
(cahier §9, docs/architecture.md §7).

---

## Layout S3 — partitionnement Hive / Iceberg

```
s3://<bucket>/
  events/
    date=2024-06-15/
      part-0000.parquet
    date=2024-06-16/
      part-0000.parquet
  logs/
    date=2024-06-15/
      part-0000.parquet
```

- Clé de partition : `date=YYYY-MM-DD` (convention Hive / Apache Iceberg).
- Chaque fichier couvre **une journée UTC** complète.
- Le préfixe facultatif (`--prefix` / `S3_PREFIX`) permet d'organiser plusieurs
  environnements dans le même bucket (ex. `prod/events/date=…`, `staging/events/date=…`).

---

## Schéma Parquet

### Table `events`

| Colonne           | Type Parquet Arrow                    | Nullable | Notes |
|---|---|---|---|
| `event_id`        | `Utf8`                                | non      | UUID texte (36 chars) |
| `event_name`      | `Utf8`                                | non      | |
| `tenant_id`       | `Utf8`                                | oui      | |
| `actor_id`        | `Utf8`                                | non      | |
| `session_id`      | `Utf8`                                | non      | |
| `timestamp_client`| `Timestamp(Microsecond, UTC)`         | non      | Partitionné / trié |
| `received_at`     | `Timestamp(Microsecond, UTC)`         | non      | |
| `properties`      | `Utf8`                                | non      | JSON sérialisé |

**Décisions de mapping :**
- `uuid` → `Utf8` : format ouvert, lisible par DuckDB/DataFusion sans extension.
- `timestamptz` → `Timestamp(Microsecond, UTC)` : précision microseconde, timezone UTC
  explicite dans les metadata Arrow (compatible Iceberg / Spark / DuckDB).
- `jsonb` → `Utf8` (JSON string) : le format Parquet n'a pas de type JSON natif ; stocker
  la représentation texte JSON permet à tout moteur de lecture de la parser.

### Table `logs`

| Colonne               | Type Parquet Arrow            | Nullable |
|---|---|---|
| `log_id`              | `Utf8`                        | non      |
| `log_time`            | `Timestamp(Microsecond, UTC)` | non      |
| `observed_time`       | `Timestamp(Microsecond, UTC)` | oui      |
| `received_at`         | `Timestamp(Microsecond, UTC)` | non      |
| `severity_number`     | `Int16`                       | oui      |
| `severity_text`       | `Utf8`                        | oui      |
| `body`                | `Utf8`                        | oui      |
| `service_name`        | `Utf8`                        | oui      |
| `scope_name`          | `Utf8`                        | oui      |
| `trace_id`            | `Utf8`                        | oui      |
| `span_id`             | `Utf8`                        | oui      |
| `tenant_id`           | `Utf8`                        | oui      |
| `actor_id`            | `Utf8`                        | oui      |
| `session_id`          | `Utf8`                        | oui      |
| `resource_attributes` | `Utf8`                        | non      |
| `log_attributes`      | `Utf8`                        | non      |

---

## Compression

**zstd niveau 3** (ratio élevé, décompression rapide). Alternatives disponibles via la
constante `writer_properties()` dans `src/export.rs` : snappy (décompression plus rapide),
lz4 (compromis). Le choix est centralisé et modifiable sans changer le reste du code.

---

## Idempotence

**Stratégie : PUT sans condition (écrasement).**

Ré-exécuter l'export d'un même jour *remplace* l'objet S3 existant. Le résultat est
déterministe car :
1. La requête PostgreSQL trie sur `(timestamp_client, event_id)` (ORDER BY stable).
2. La sérialisation Arrow/Parquet est déterministe pour un RecordBatch d'entrée identique.
3. Le même lot de données produit donc le même fichier Parquet, bit pour bit.

Conséquence : un re-run après une panne partielle ou une correction de données produit
exactement le fichier final attendu. Aucune logique de déduplication côté S3 n'est
nécessaire.

**Cas de correction :** si des données sont corrigées en base (ex. rejeu d'un batch
manquant), il suffit de relancer l'export du même jour ; le fichier S3 est écrasé.

---

## Planification (cron)

Export quotidien J-1 (une fois que la journée est complète) :

```
# /etc/cron.d/datacat-export ou systemd timer
0 1 * * * datacat-export \
    --table events \
    --date $(date -d yesterday +%Y-%m-%d) \
    --bucket <bucket> 2>&1 | logger -t datacat-export

0 2 * * * datacat-export \
    --table logs \
    --date $(date -d yesterday +%Y-%m-%d) \
    --bucket <bucket> 2>&1 | logger -t datacat-export
```

Alternative : job Kubernetes CronJob ou AWS EventBridge Scheduler → ECS Task.

Variables d'environnement requises :

| Variable                | Exemple                              |
|---|---|
| `DATABASE_URL`          | `postgres://datacat:…@host:5432/db`  |
| `S3_ENDPOINT`           | (vide pour AWS S3, ou URL MinIO)     |
| `S3_REGION`             | `eu-west-3`                          |
| `S3_BUCKET`             | `my-datacat-cold`                    |
| `AWS_ACCESS_KEY_ID`     | `AKIA…`                              |
| `AWS_SECRET_ACCESS_KEY` | `…`                                  |
| `S3_ALLOW_HTTP`         | `true` (MinIO dev sans TLS seulement)|

---

## Streaming par lots

Les données sont lues par **lots de 10 000 lignes** (`BATCH_SIZE` dans `src/export.rs`).
Chaque lot produit un **row group** Parquet séparé dans le même fichier. Cela :
- Borne la consommation mémoire à ~10 000 lignes × (taille d'une ligne).
- Permet à DataFusion/DuckDB de lire par row group (predicate pushdown).

Pour des volumes très importants (> 100 M lignes/jour), plusieurs `part-NNNN.parquet` peuvent
être produits en modifiant le compteur de part dans `hive_path()` — la logique de batching
est déjà en place, seule la coupure du fichier reste à câbler si nécessaire.

---

## Lecture analytique future (DataFusion / DuckDB)

Le layout Hive est directement consommable :

```sql
-- DuckDB
SELECT date_trunc('day', timestamp_client), event_name, count(*)
FROM read_parquet('s3://bucket/events/date=*/*.parquet', hive_partitioning=true)
WHERE date = '2024-06-15'
GROUP BY 1, 2;
```

```rust
// DataFusion
let ctx = SessionContext::new();
ctx.register_listing_table(
    "events",
    "s3://bucket/events/",
    ListingTableConfig::new(…).with_listing_options(
        ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_table_partition_cols(vec![("date".to_string(), DataType::Utf8)]),
    ),
    None,
).await?;
```

La corrélation events ↔ logs se fait via les colonnes partagées `tenant_id`, `actor_id`,
`session_id` (cf. cahier §4.2).
