---
title: "Logs OTLP"
description: "Ingestion des logs techniques OpenTelemetry dans Datacat."
---

Datacat ingère les **logs techniques** au format **OpenTelemetry / OTLP-HTTP (JSON)**, dans le
même socle que les events produit : table partitionnée par jour, idempotente, écriture par
`COPY`. Objectif (cahier §4.2, §9) : **relier** events produit et logs techniques via
`tenant_id` / `actor_id` / `session_id`, et aux traces via `trace_id` / `span_id`.

## 1. Transports

Datacat accepte les logs OTLP sur **deux transports**, tous deux standards (drop-in pour un SDK
OpenTelemetry ou un Collector) :

| Transport | Endpoint | Activation |
|---|---|---|
| **OTLP/HTTP (JSON)** | `POST /v1/logs` | toujours actif (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`) |
| **OTLP/gRPC** | service `LogsService/Export` sur `:4317` | `[server.grpc].enabled = true` (port `[server.grpc].bind_addr`) |

Corps : un `ExportLogsServiceRequest` OTLP. Réponse : `ExportLogsServiceResponse` (vide, ou
`partialSuccess` si des enregistrements ont été écartés sous back-pressure). Les deux transports
partagent **exactement** la même logique d'admission (auth, rate limit, corrélation, dédup) et
produisent le même `log_id` pour un contenu identique.

## 1.1 Authentification (token de service, fixe)

Contrairement aux events (front web/mobile, token JWT court-vécu par session car un client ne
peut pas détenir de secret), les logs sont émis **de service à service** : un backend de confiance
**peut** détenir un secret. L'auth des logs est donc, par défaut, un **token de service fixe**.

Modes (`[auth.logs].mode`) :

| Mode | Comportement |
|---|---|
| `static` (**recommandé**) | en-tête/métadonnée `Authorization: Bearer <static_token>`, comparé à **temps constant**. Le token est fixe (config du service émetteur), rotation par changement de valeur. |
| `jwt` | vérification JWT par clé publique (un token de service **long-vécu** signé asymétriquement) — utile pour partager l'infra de clés des events. |
| `none` | aucune auth (endpoint sur réseau interne / mTLS terminé au proxy). |
| `auto` (défaut) | `static` si `static_token` est défini, sinon `jwt` si la vérif token est activée, sinon `none`. |

Le token statique se configure via `[auth.logs].static_token = "${LOGS_STATIC_TOKEN:-}"`.

Le token (statique ou JWT) sert aussi indirectement de filtre ; le rate limiting des logs est en
revanche clé sur le `service.name` (source de confiance pour des logs service-à-service).

## 2. Modèle de données

Chaque `LogRecord` OTLP est aplati dans la table `logs` :

| Colonne | Source OTLP |
|---|---|
| `log_id` | **hash déterministe** du contenu (dédup des renvois — OTLP n'a pas d'id natif) |
| `log_time` | `timeUnixNano` (ou `observedTimeUnixNano`, ou réception). **Clé de partition.** |
| `observed_time` | `observedTimeUnixNano` |
| `severity_number` / `severity_text` | `severityNumber` / `severityText` |
| `body` | `body` (aplati en texte) |
| `service_name` | attribut de resource `service.name` |
| `scope_name` | `scope.name` |
| `trace_id` / `span_id` | `traceId` / `spanId` (corrélation aux traces) |
| `tenant_id` / `actor_id` / `session_id` | attributs (log puis resource) — **corrélation aux events** |
| `resource_attributes` / `log_attributes` | attributs complets (JSONB) |

### Idempotence

Comme pour les events, l'idempotence repose sur `(log_time, log_id)` :
- `log_time` est porté par l'enregistrement (stable entre deux exports identiques) → la clé de
  partition est stable ;
- `log_id` = `SHA-256(log_time, service, body, trace_id, span_id, severity, attributs)` tronqué
  en UUID → deux exports identiques (retry de l'exporter OTLP) produisent le même id, donc une
  seule ligne (`ON CONFLICT DO NOTHING`).

### Clés de corrélation

Les clés cherchées dans les attributs (log puis resource), pour relier logs et events :
- tenant : `tenant_id`, `tenant.id`, `tenant`
- acteur : `actor_id`, `actor.id`, `user.id`, `enduser.id`, `user_id`
- session : `session_id`, `session.id`, `session`

Exemple de jointure (cœur du futur besoin de debug) :

```sql
SELECT e.event_name, l.body, l.severity_text
FROM events e
JOIN logs l ON e.session_id = l.session_id
WHERE e.session_id = 'sess-abc'
ORDER BY e.timestamp_client;
```

## 3. Émettre des logs vers Datacat

### Depuis un backend instrumenté OpenTelemetry

Configurer l'exporter OTLP vers Datacat, ajouter les attributs de corrélation (`session_id`,
`actor_id`, `tenant_id`) sur les logs (ou la resource), et joindre le **token de service fixe**
via l'en-tête/métadonnée `Authorization`.

HTTP/JSON :
```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.example.com/v1/logs
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<LOGS_STATIC_TOKEN>
```

gRPC (port 4317, si `[server.grpc].enabled = true` côté Datacat) :
```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.example.com:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<LOGS_STATIC_TOKEN>
```

Un exemple complet (backend Rust + app React) est fourni dans `examples/` (voir son README).

## 4. Bornes & sécurité

- `MAX_LOGS_RECORDS` (défaut 2048) : nombre max de `LogRecord` par requête.
- `MAX_LOGS_PAYLOAD_BYTES` (défaut 4 MiB) : taille max du corps OTLP (route dédiée).
- Mêmes garde-fous que les events : token (clé publique), rate limiting, ban d'IP, fenêtre de
  skew (logs hors fenêtre écartés, perte tolérée), validation du JSON.

## 5. Flux liés

Les **traces** OTLP ([traces](../traces/)) et les **métriques** ([métriques](../otel-metrics/)) sont
ingérées par le même mécanisme générique (`Ingestable` + table partitionnée idempotente), corrélées
aux logs via le `trace_id` / `service_name` partagés. La lecture analytique froide sur Parquet est
décrite dans [lecture (froide)](../read-cold/).
