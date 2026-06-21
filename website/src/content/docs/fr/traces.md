---
title: "Traces OTLP"
description: "Ingestion des traces OpenTelemetry et corrélation avec les logs."
---

Datacat ingère les traces distribuées au format **OpenTelemetry (OTLP)**, en **HTTP** (`POST
/v1/traces`) et en **gRPC** (`TraceService`). Les spans sont stockés dans PostgreSQL aux côtés des
logs, events et métriques, et **corrélés** aux logs via le `trace_id` partagé.

Comme tous les flux Datacat, l'ingestion des traces est **strictement idempotente** : un span est
identifié par `(start_time, trace_id, span_id)` et fusionné avec `ON CONFLICT DO NOTHING` — les
retries ne créent jamais de doublon.

## Endpoints

| Transport | Endpoint | Notes |
|---|---|---|
| HTTP | `POST /v1/traces` | corps OTLP/JSON ou OTLP/protobuf (`ExportTraceServiceRequest`) |
| gRPC | `TraceService/Export` | activé via `[server.grpc].enabled = true` |

Les deux sont authentifiés **de service à service** par `[auth.logs]` (l'auth télémétrie partagée
par logs, traces et métriques) — voir [configuration](../configuration/). Un token de service fixe
(`mode = "static"`) est recommandé pour les backends de confiance.

## Modèle de stockage

Les spans vivent dans la table partitionnée `spans` (une partition par jour sur `start_time`) :

| Colonne | Sens |
|---|---|
| `trace_id`, `span_id`, `parent_span_id` | identifiants OTel (hex) |
| `start_time`, `end_time`, `duration_ms` | horodatage du span (`start_time` = clé de partition) |
| `name`, `kind` | nom d'opération et span kind OTel |
| `service_name`, `scope_name` | ressource / scope d'instrumentation |
| `status_code`, `status_message` | statut OTel (`2` = erreur) |
| `tenant_id`, `actor_id`, `session_id` | clés de corrélation (partagées avec events/logs) |
| `resource_attributes`, `span_attributes` | sacs d'attributs JSONB |
| `events`, `links` | events et links du span (JSONB) |

## Corrélation avec les logs

Un log portant un `trace_id` (et éventuellement un `span_id`) est relié à son span : on peut donc
passer d'une ligne de log à la trace complète, ou lister tous les logs émis pendant une requête :

- `GET /v1/query/traces/{trace_id}` renvoie tous les spans d'une trace, ordonnés par `start_time`.
- `GET /v1/query/logs?trace_id=…` renvoie les logs rattachés à une trace.

Voir [lecture (chaude)](../read-hot/) pour les endpoints et [MCP](../mcp/) pour les mêmes données
exposées à un agent via l'outil `get_trace`.

## Alerting sur les traces

Le moteur d'alerting peut cibler les spans directement (`source = "spans"`) :

- `span_duration` — agrégat de latence (`avg`/`max`/`p50`…`p99`) de `duration_ms`, filtrable par
  `service`, `operation` et `error_only`. Exemple : « p99 de `checkout` > 2 s ».
- `error_ratio` avec `source = "spans"` — fraction de spans dont `status_code = 2` (erreur).

Voir [alerting](../alerting/) pour le schéma complet des règles.
