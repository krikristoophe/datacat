---
title: "Tutoriel : instrumenter un service avec OTLP"
description: "Envoyer logs, traces et métriques OpenTelemetry à Datacat en OTLP/HTTP, et les corréler avec les events produit."
---

Datacat ingère les trois signaux OpenTelemetry — **logs**, **traces** et **métriques** — pour un
même service, en OTLP/HTTP (JSON) et OTLP/gRPC. Ce tutoriel envoie un exemplaire de chaque à la main
pour montrer le wire format exact, puis renvoie vers les vrais SDK et le Collector OpenTelemetry.

Il suppose un backend démarré comme dans [Tracer votre premier event](../first-event/). Les
endpoints de télémétrie s'authentifient avec le **token de service statique** (`[auth.logs]`), pas
le JWT produit — ici `dev-logs-token` (ou n'importe quelle chaîne avec `--features dev`).

## 1. Envoyer un log

```bash
curl -s -X POST http://localhost:8080/v1/logs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceLogs": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } },
        { "key": "tenant_id",    "value": { "stringValue": "clinic-7" } }
      ]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1750586130000000000",
          "severityText": "ERROR",
          "body": { "stringValue": "payment gateway timeout" },
          "traceId": "5b8efff798038103d269b633813fc60c",
          "attributes": [
            { "key": "session_id", "value": { "stringValue": "sess-1" } }
          ]
        }]
      }]
    }]
  }'
```

La réponse est un `ExportLogsServiceResponse` OTLP (vide en cas de succès complet). Datacat aplatit
chaque enregistrement, extrait `service.name`, et relève `tenant_id` / `actor_id` / `session_id` /
`trace_id` des attributs pour la corrélation.

## 2. Envoyer une trace

Les spans passent par `POST /v1/traces`. Le `traceId` partagé est ce qui reliera ce span au log
ci-dessus.

```bash
curl -s -X POST http://localhost:8080/v1/traces \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceSpans": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } }
      ]},
      "scopeSpans": [{
        "spans": [{
          "traceId": "5b8efff798038103d269b633813fc60c",
          "spanId":  "eee19b7ec3c1b174",
          "name":    "POST /checkout",
          "kind": 2,
          "startTimeUnixNano": "1750586129000000000",
          "endTimeUnixNano":   "1750586130000000000",
          "status": { "code": 2, "message": "gateway timeout" }
        }]
      }]
    }]
  }'
```

## 3. Envoyer une métrique

Les gauges et sums utilisent `NumberDataPoint` ; les histogrammes portent des buckets. Postez sur
`POST /v1/metrics` :

```bash
curl -s -X POST http://localhost:8080/v1/metrics \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceMetrics": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "checkout" } }
      ]},
      "scopeMetrics": [{
        "metrics": [{
          "name": "http.server.duration",
          "unit": "ms",
          "histogram": { "dataPoints": [{
            "timeUnixNano": "1750586130000000000",
            "count": "3", "sum": 1850.0,
            "bucketCounts": ["1","1","1"],
            "explicitBounds": [100.0, 500.0]
          }] }
        }]
      }]
    }]
  }'
```

## 4. Corréler

Comme le log et le span partagent `traceId`, vous pouvez les joindre. Idem entre events produit et
télémétrie via `session_id` / `actor_id` / `tenant_id` :

```sql
SELECT l.body, s.name AS span, s.status_code
FROM   logs l
JOIN   spans s ON s.trace_id = l.trace_id
WHERE  l.trace_id = '5b8efff798038103d269b633813fc60c';
```

## Utiliser un vrai SDK ou Collector OpenTelemetry

On écrit rarement de l'OTLP à la main. Pointez n'importe quel exporteur OpenTelemetry vers les
endpoints de Datacat :

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:8080
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer dev-logs-token
OTEL_EXPORTER_OTLP_PROTOCOL=http/json
```

Pour acheminer les logs de conteneurs et métriques hôte via le Collector OpenTelemetry sur Docker
Compose et Swarm, suivez [Logs & métriques avec Docker](../../docker-telemetry/). gRPC est aussi
disponible — voir [logs OTLP](../../otel-logs/), [métriques](../../otel-metrics/) et
[traces](../../traces/).

## Étapes suivantes

- [Alerter sur Slack](../alert-to-slack/) sur les erreurs que vous venez d'envoyer.
