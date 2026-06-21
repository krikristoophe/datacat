# Logs techniques OpenTelemetry (OTLP)

Datacat ingère les **logs techniques** au format **OpenTelemetry / OTLP-HTTP (JSON)**, dans le
même socle que les events produit : table partitionnée par jour, idempotente, écriture par
`COPY`. Objectif (cahier §4.2, §9) : **relier** events produit et logs techniques via
`tenant_id` / `actor_id` / `session_id`, et aux traces via `trace_id` / `span_id`.

## 1. Endpoint

```
POST /v1/logs
Content-Type: application/json
Authorization: Bearer <jwt-d-ingestion>     # même token que /v1/events
```

Corps : un `ExportLogsServiceRequest` OTLP standard. Réponse : `200` + `ExportLogsServiceResponse`
(`{}`, ou `{ "partialSuccess": { "rejectedLogRecords": N } }` si des enregistrements ont été
écartés sous back-pressure).

C'est le **même protocole** que celui d'un OpenTelemetry Collector : n'importe quel SDK OTel ou
Collector peut exporter vers Datacat en pointant l'endpoint OTLP/HTTP sur `…/v1/logs` avec
`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`.

> Les logs proviennent de **backends de confiance** (côté serveur), qui présentent un token
> d'ingestion signé au même titre que les SDKs. Le token sert aussi de clé de rate limiting.

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

Configurer l'exporter OTLP/HTTP JSON vers Datacat, et ajouter les attributs de corrélation
(`session_id`, `actor_id`, `tenant_id`) sur les logs (ou la resource). Le token d'ingestion est
joint via l'en-tête `Authorization` (par ex. `OTEL_EXPORTER_OTLP_HEADERS`).

```
OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://ingest.example.com/v1/logs
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
OTEL_EXPORTER_OTLP_HEADERS=Authorization=Bearer%20<jwt>
```

Un exemple complet (backend Rust + app React) est fourni dans `examples/` (voir son README).

## 4. Bornes & sécurité

- `MAX_LOGS_RECORDS` (défaut 2048) : nombre max de `LogRecord` par requête.
- `MAX_LOGS_PAYLOAD_BYTES` (défaut 4 MiB) : taille max du corps OTLP (route dédiée).
- Mêmes garde-fous que les events : token (clé publique), rate limiting, ban d'IP, fenêtre de
  skew (logs hors fenêtre écartés, perte tolérée), validation du JSON.

## 5. Hors scope v1

Métriques et **traces** OTLP ne sont pas ingérées en v1 (seulement les **logs**). Le même
mécanisme générique (`Ingestable` + table partitionnée idempotente) permet de les ajouter sans
réécriture du cœur. La lecture analytique (requêtes de parcours, corrélation avancée) reste hors
v1.
