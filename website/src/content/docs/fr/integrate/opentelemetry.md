---
title: Intégrer OpenTelemetry
description: Pointez votre SDK ou Collector OpenTelemetry existant vers Datacat — logs, traces et métriques en OTLP, sans réinstrumenter.
---

Datacat parle **OTLP** nativement, en HTTP (JSON) et gRPC, sur `/v1/logs`, `/v1/traces` et
`/v1/metrics`. Si vos services sont déjà instrumentés avec OpenTelemetry, vous ne changez pas une
ligne d'instrumentation — vous ajoutez simplement Datacat comme cible d'export. La télémétrie
s'authentifie avec le **token de service** statique (`[auth.logs]`).

## Option A — depuis un SDK OpenTelemetry

Définissez les variables OTLP standards et votre app exporte directement vers Datacat :

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=https://ingest.example.com
OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer ${DATACAT_SERVICE_TOKEN}
OTEL_EXPORTER_OTLP_PROTOCOL=http/json   # ou grpc
```

Le SDK ajoute automatiquement `/v1/logs`, `/v1/traces`, `/v1/metrics` à l'endpoint.

## Option B — depuis le Collector OpenTelemetry

Ajoutez un exporteur `otlphttp` (ou `otlp` pour gRPC) vers Datacat :

```yaml
exporters:
  otlphttp/datacat:
    endpoint: https://ingest.example.com
    headers:
      authorization: "Bearer ${DATACAT_SERVICE_TOKEN}"

service:
  pipelines:
    logs:    { exporters: [otlphttp/datacat] }
    traces:  { exporters: [otlphttp/datacat] }
    metrics: { exporters: [otlphttp/datacat] }
```

C'est la voie recommandée pour acheminer **les logs de conteneurs et les métriques hôte** — voir
[Logs & métriques avec Docker](../../docker-telemetry/) pour Compose et Swarm.

## Corréler avec les events produit

Datacat relève quelques attributs de vos resources/records pour corréler la télémétrie aux events
produit et entre signaux. Renseignez-les quand vous le pouvez :

- `service.name` — regroupe la télémétrie par service.
- `tenant_id`, `actor_id`, `session_id` — relient un log/span/point à un tenant, un utilisateur, une
  session.
- `trace_id` / `span_id` — déjà standards sur les spans ; les logs portant le même `trace_id` sont
  reliés.

## Ce qui est ingéré

Logs, spans et points de métriques **gauge / sum / histogram** sont stockés. Les types `summary` et
`exponentialHistogram` ne sont pas ingérés (documenté). Chaque enregistrement est borné par un
plafond de taille ; les enregistrements trop gros sont écartés (perte tolérée), jamais la requête
entière.

## Étapes suivantes

- [Logs & métriques avec Docker](../../docker-telemetry/) — Collector sur Compose & Swarm.
- Référence : [logs OTLP](../../otel-logs/) · [métriques](../../otel-metrics/) · [traces](../../traces/).
