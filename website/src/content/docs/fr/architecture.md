---
title: "Architecture"
description: "Décisions de conception de la v1 centrée sur l'ingestion : idempotence × partitionnement, chemin d'écriture, et comment l'architecture prépare les extensions futures."
---

Ce document explique les choix structurants de la v1 et comment l'architecture **prépare**
les extensions hors scope (cahier §9) sans les déployer.

## 1. Vue d'ensemble

```
Events (web / mobile / backend)
        │  POST /v1/events   (batch JSON, Bearer <jwt>)
        ▼
┌─────────────────────────────────────────────────────────────┐
│ API d'ingestion (Axum)                                        │
│  • garde-fous : CORS, limite de taille, timeout, ban d'IP     │
│  • vérif token (clé publique)  • rate limiting 2 niveaux      │
│  • validation stricte → enfilage non bloquant (202 immédiat)  │
│         │ mpsc (back-pressure bornée)                         │
│         ▼                                                     │
│  Batcher (tâche unique) : micro-batch en mémoire              │
└─────────┬───────────────────────────────────────────────────┘
          │ COPY (CSV) → events_staging (UNLOGGED)
          ▼
   datacat_merge_staging()  →  INSERT … ON CONFLICT DO NOTHING
          ▼
   events  (table partitionnée par jour sur timestamp_client)
          │
          ▼  (hors v1) export Parquet/Iceberg · lecture DataFusion/DuckDB
```

Frontières nettes : **ingestion** (`ingest`), **stockage** (`db` + migrations), **lecture**
(absente en v1). Le cœur d'ingestion n'a aucune dépendance vers une couche de lecture, ce qui
permet d'ajouter le froid / la lecture / un tampon d'écriture **sans réécriture** (cahier §9).

## 2. Idempotence × partitionnement : la décision centrale

Le système doit être **partitionné par temps** ET garantir qu'un même `event_id` n'est stocké
qu'une fois (idempotence stricte). En PostgreSQL, ces deux exigences se télescopent :

> Une contrainte `UNIQUE` sur une table partitionnée **doit inclure la clé de partition**.

Donc on ne peut pas avoir un simple `UNIQUE(event_id)` global sur une table partitionnée. La
clé d'unicité doit contenir la colonne de partition. Quelle colonne temporelle choisir ?

| Candidat | Stable entre 2 envois du même event ? | Conséquence |
|---|---|---|
| `received_at` (serveur) | **Non** — chaque réception ⇒ nouvel horodatage | Deux retries auraient deux clés différentes ⇒ **doublons**. Inutilisable pour la dédup. |
| `timestamp_client` (client) | **Oui** — figé à la création, réutilisé à l'identique sur retry | Tout doublon retombe dans la **même partition** ⇒ `ON CONFLICT` dédup globalement. ✅ |

**Décision : partitionner par `timestamp_client`**, clé d'idempotence `(timestamp_client, event_id)`.
C'est le seul choix qui réconcilie partitionnement temporel et idempotence native.

Cela impose un **contrat SDK** (cf. [Contrat](../contract/) §2.2) : `event_id` **et** `timestamp_client`
sont figés à la création et jamais régénérés sur retry. Les deux SDKs le respectent.

### Garde-fous associés

- `timestamp_client` étant fourni par le client (donc falsifiable / horloge fausse), il est
  **borné** par validation : rejeté hors de `[received_at - MAX_PAST_SKEW, received_at + MAX_FUTURE_SKEW]`
  (défaut : 31 j / 24 h). Cela évite la **création de partitions arbitraires** (poisoning) :
  au plus ~33 partitions journalières peuvent exister pour la fenêtre autorisée.
- `received_at` reste stocké comme colonne (analyse ultérieure : horloge serveur fiable).

## 3. Chemin d'écriture optimisé

1. **Acquittement immédiat** : le handler valide puis enfile dans un canal `mpsc` borné et
   répond `202`. La latence d'écriture n'est jamais sur le chemin de la requête.
2. **Micro-batch** : une tâche **unique** (un seul writer ⇒ zéro contention sur le staging)
   accumule les events et flush quand (a) la taille `FLUSH_BATCH_SIZE` est atteinte ou
   (b) l'intervalle `FLUSH_INTERVAL` s'écoule.
3. **COPY** : écriture en masse au format CSV vers `events_staging`, table **`UNLOGGED`**
   (pas de WAL ⇒ débit maximal). La perte du staging récent en cas de crash est acceptable
   (tolérance §2) ; au redémarrage, tout résidu est fusionné (`drain_staging`).
4. **Merge idempotent** : `datacat_merge_staging()` fait
   `INSERT … SELECT DISTINCT ON (timestamp_client, event_id) … ON CONFLICT DO NOTHING`
   (collapse intra-batch via `DISTINCT ON`, inter-batch via `ON CONFLICT`), puis `TRUNCATE`
   le staging. La fonction retourne le nombre de lignes **réellement** insérées (post-dédup).

`COPY` (et non des `INSERT` ligne à ligne) est ce qui donne le débit ; le merge est WAL-loggé
et durable.

## 4. Rétention par DROP PARTITION

La purge se fait par `datacat_drop_partitions_before(jour)` qui exécute `DROP TABLE` sur les
partitions plus anciennes que `RETENTION_DAYS`. `DROP TABLE` d'une partition est **instantané**
(libération de fichiers), contrairement à un `DELETE` massif (réécriture + VACUUM). Aucun
impact sur le chemin d'écriture (les écritures visent les partitions récentes).

## 5. Back-pressure & tolérance à la perte

Le canal `mpsc` est borné (`CHANNEL_CAPACITY`). Sous surcharge extrême, `try_enqueue` échoue :
l'event est **abandonné** (compteur `dropped_channel_full_total`) et la réponse reste `202`
avec `received` = nombre réellement enfilé. C'est l'application concrète de la *tolérance à la
perte non biaisée* (§2) : on ne renvoie pas `5xx` qui déclencherait des retries aggravant la
surcharge. **Jamais de doublon** en revanche : l'idempotence est garantie en base.

## 6. Sécurité (résumé ; détails dans [Sécurité](../security/))

Endpoint public, non authentifié au sens fort. Défenses **100 % serveur-side** :
vérification du token par signature **asymétrique** (clé publique seule, l'ingestion ne peut
pas forger de token), rate limiting à deux niveaux + filet global, validation stricte, bornes
de taille, CORS, bannissement d'IP anormales, logs structurés traçables, TLS au déploiement.

## 7. Préparation des extensions hors v1 (cahier §9)

| Extension | Comment la v1 l'accueille sans réécriture |
|---|---|
| **Stockage froid** (Parquet/Iceberg sur S3 EU) | `events` est déjà partitionnée par jour : un job d'export lit partition par partition vers Parquet, sans toucher l'ingestion. |
| **Lecture analytique** (DataFusion/DuckDB) | Couche de lecture séparée branchée sur le froid (et/ou le chaud). Le cœur d'ingestion ne référence aucune lecture. Aucun index de lecture en v1 pour préserver le débit d'écriture ; ils seront créés côté froid. |
| **Logs techniques** | **Ingérés en v1** via `POST /v1/logs` (OTLP/HTTP), même socle générique partitionné/idempotent que les events, corrélés via `tenant_id` / `actor_id` / `session_id` et `trace_id`. Voir [Logs OTLP](../otel-logs/). |
| **Stockage froid S3** | Export Parquet partitionné par date via l'exporteur froid — un crate standalone, également embarqué & planifié dans le backend (hors cœur d'ingestion). Voir [Stockage froid](../cold-storage/). |
| **Scale-out écriture** (Citus / Redpanda) | Le writer est isolé derrière un canal : on peut insérer un tampon distribué devant, ou sharder via Citus, sans changer le contrat d'ingestion. |
| **Scale-out lecture** (Ballista) | Garanti par le format ouvert (Iceberg) côté froid. |

## 8. Configuration & multi-projet

La configuration tient dans un unique **fichier TOML** (`datacat.toml`, gabarit
`datacat.example.toml`) qui décrit tout le déploiement, plus **un fichier TOML par projet**
sous `projects/*.toml`. Toute valeur chaîne peut référencer une variable d'environnement via
`${VAR}` (ou `${VAR:-default}`), résolue au démarrage, donc aucun secret n'est committé. Un
fallback historique par variables d'environnement subsiste pour le développement et la suite de
tests. Voir [Configuration](../configuration/) pour la référence complète.

Datacat est **multi-projet au niveau configuration** : chaque projet porte ses propres règles
d'alerting et canaux de notification, et le backend exécute **un évaluateur d'alerting par
projet**. Le pipeline d'ingestion et les données stockées sont **partagés** — l'isolation des
projets est au niveau configuration, pas au niveau des données.

L'export froid est piloté depuis la même configuration : une section TOML `[export]` planifie
l'exporteur embarqué (la logique d'export froid est un crate standalone, également embarqué &
planifié dans le backend).

## 9. Carte des modules (backend)

Découpage en sous-modules cohérents (domaines au-dessus d'une infrastructure partagée) :

| Module | Rôle |
|---|---|
| `config` | Configuration depuis le fichier TOML (`datacat.toml`) avec expansion des secrets `${ENV}` et fichiers par projet (`projects/*.toml`), valeurs par défaut sûres, validation au démarrage ; fallback historique par variables d'environnement. |
| `error`, `telemetry` | Erreurs typées (→ réponses HTTP) ; logs structurés. |
| `events/model` | Wire format des events + validation stricte ; impl `Ingestable`. |
| `logs/model` | Wire format OTLP des logs + aplatissement/corrélation/dédup ; impl `Ingestable`. |
| `ingest` | **Générique** : trait `Ingestable`, canal, batcher, `COPY`, merge idempotent, métriques (partagé events + logs). |
| `db` (+ `db/partitions`) | Pool, migrations, gestion/purge des partitions (events & logs), drain du staging. |
| `security` (`token`, `ratelimit`, `anomaly`) | Vérif JWT asymétrique (PEM/JWKS, `kid`) ; token buckets + plafond sessions/IP ; résolution d'IP + ban d'anomalies. |
| `api` (+ `api/routes`) | Assemblage du routeur + garde-fous (CORS, taille, timeout, traçage) ; handlers (`/v1/events`, `/v1/logs`, `/healthz`, `/readyz`, `/stats`). |
| `lib` | `AppState` (ingestors events & logs) + déclarations de modules. |
