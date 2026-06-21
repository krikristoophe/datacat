---
title: "Métriques OTLP"
description: "Ingestion des métriques OpenTelemetry dans Datacat."
---

Datacat ingère les **métriques** au format **OpenTelemetry / OTLP-HTTP (JSON)** et **OTLP/gRPC**,
dans le même socle que les events, logs et traces : table partitionnée par jour, idempotente,
écriture par `COPY`. Objectif : compléter l'observabilité (APM) en reliant les métriques aux
events / logs / traces via `tenant_id` / `actor_id` / `session_id`.

## 1. Transports

| Transport | Endpoint | Activation |
|---|---|---|
| **OTLP/HTTP (JSON)** | `POST /v1/metrics` | toujours actif (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`) |
| **OTLP/gRPC** | service `MetricsService/Export` sur `:4317` | `[server.grpc].enabled = true` (port `[server.grpc].bind_addr`) |

Corps : un `ExportMetricsServiceRequest` OTLP. Réponse : `ExportMetricsServiceResponse` (vide, ou
`partialSuccess` avec `rejectedDataPoints` si des points ont été écartés sous back-pressure). Les
deux transports partagent **exactement** la même logique d'admission (auth, rate limit,
corrélation, dédup) et produisent le même `point_id` pour un contenu identique.

## 1.1 Authentification

L'auth est **identique à celle des logs/traces** : token de service (`[auth.logs].mode`,
`[auth.logs].static_token`). Les métriques sont émises de service à service ; voir
[logs OTLP](../otel-logs/) §1.1. Le rate limiting est clé sur le `service.name` (à défaut l'IP).

## 2. Modèle de données

Chaque **point de donnée** (data point) d'une métrique est aplati en une ligne de la table
`metric_points`. La hiérarchie OTLP est :
`resourceMetrics → scopeMetrics → metrics → {gauge|sum|histogram}.dataPoints`.

| Colonne | Source OTLP |
|---|---|
| `point_id` | **hash déterministe** du contenu (dédup des renvois — OTLP n'a pas d'id natif) |
| `time` | `timeUnixNano` du data point (ou réception si absent). **Clé de partition.** |
| `metric_name` | `metric.name` |
| `metric_type` | `gauge` \| `sum` \| `histogram` |
| `unit` | `metric.unit` |
| `value_double` / `value_int` | `NumberDataPoint.asDouble` / `asInt` (gauge, sum) |
| `count` / `sum` | `HistogramDataPoint.count` / `sum` (histogram) |
| `buckets` | `{ "bounds": [...explicitBounds], "counts": [...bucketCounts] }` (histogram, JSONB) |
| `service_name` | attribut de resource `service.name` |
| `scope_name` | `scope.name` |
| `tenant_id` / `actor_id` / `session_id` | attributs (data point puis resource) — **corrélation** |
| `resource_attributes` / `attributes` | attributs complets (JSONB) |

### Types pris en charge

| Type OTLP | Ingéré ? | Aplati en |
|---|---|---|
| **Gauge** | ✅ | une ligne par `NumberDataPoint` (`value_double` ou `value_int`) |
| **Sum** | ✅ | idem gauge (`metric_type = sum`) |
| **Histogram** | ✅ | une ligne par `HistogramDataPoint` (`count`, `sum`, `buckets`) |
| **Summary** | ❌ ignoré | — (type hérité Prometheus, non mergeable proprement) |
| **ExponentialHistogram** | ❌ ignoré | — (hors périmètre ; histogram explicite recommandé) |

Les points de type `summary` et `exponentialHistogram` sont **silencieusement ignorés** (ni
stockés, ni comptés comme rejet). La granularité d'agrégation (temporality delta/cumulative) et
le caractère monotone d'un `sum` ne sont pas interprétés : chaque point est stocké tel quel.

### Idempotence

Comme les autres domaines, l'idempotence repose sur `(time, point_id)`. `point_id` est un hash
SHA-256 (tronqué à 128 bits → UUID) du contenu normalisé du point :
`time + metric_name + metric_type + service_name + valeur(s) + buckets + attributs`.
Renvoyer le **même** point N fois ne crée qu'une ligne (dédup en base, `ON CONFLICT DO NOTHING`).

## 3. Partitionnement, rétention, staging

Identique aux autres domaines :
- table `metric_points` partitionnée par jour sur `time` (RANGE) ;
- staging `UNLOGGED` (`metric_points_staging`) + `COPY` → merge idempotent ;
- fonctions SQL `datacat_ensure_metric_partition(date)`,
  `datacat_ensure_metric_partitions_for_staging()`, `datacat_merge_metric_staging()`,
  `datacat_drop_metric_partitions_before(date)` (cf. `migrations/0006_metrics.sql`) ;
- rétention via `[ingest].retention_days`, partitions futures via `[ingest].partition_future_days` ;
- skew d'horodatage borné par `MAX_PAST_SKEW` / `MAX_FUTURE_SKEW` (points hors fenêtre écartés,
  comptés dans `dropped_skew_total`).

Index de lecture : `(metric_name, time)` et `(service_name, time)`.

## 4. Lecture

`GET /v1/query/metrics?name=&service=&from=&to=&limit=` retourne les points correspondants
(ordre `time` décroissant). Authentifié par `[auth.query]` (token de lecture), comme les autres
endpoints `/v1/query/*`.

## 5. Observabilité

`GET /stats` expose les compteurs d'ingestion du domaine sous la clé `metrics`
(`received_total`, `inserted_total`, `deduplicated_total`, `dropped_skew_total`, …).

## 6. Exemple (OTLP/HTTP JSON)

```json
{
  "resourceMetrics": [{
    "resource": { "attributes": [
      { "key": "service.name", "value": { "stringValue": "api" } }
    ]},
    "scopeMetrics": [{
      "metrics": [
        {
          "name": "process.cpu.utilization", "unit": "1",
          "gauge": { "dataPoints": [
            { "timeUnixNano": "1718900000000000000", "asDouble": 0.42 }
          ]}
        },
        {
          "name": "http.server.duration", "unit": "ms",
          "histogram": { "dataPoints": [
            { "timeUnixNano": "1718900000000000000",
              "count": "3", "sum": 600.0,
              "bucketCounts": ["1", "2", "0"], "explicitBounds": [100.0, 500.0] }
          ]}
        }
      ]
    }]
  }]
}
```
