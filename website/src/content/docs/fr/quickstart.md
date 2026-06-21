---
title: "Démarrage rapide"
description: "Lancer Datacat en local de bout en bout : PostgreSQL, le backend, un event de test et un log OTLP de test."
---

Ce guide vous mène d'un dépôt vide à un service d'ingestion qui tourne et accepte un **event**
produit et un **log** OTLP — en quelques minutes, sur une machine de dev. La seule dépendance est
PostgreSQL. Pour une mise en place de production, voir [installation](../installation/) et
[déploiement](../deployment/).

## 1. Démarrer PostgreSQL

Le dépôt fournit un `docker-compose.yml` avec un unique service `postgres` réglé pour le débit
d'écriture (c'est la seule dépendance requise du service d'ingestion v1).

```bash
docker compose up -d postgres

# Le fichier compose mappe PostgreSQL sur le port hôte 55432.
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat
```

## 2. Créer un fichier de configuration

Copiez le modèle et ajustez-le. Chaque secret est référencé depuis l'environnement via `${VAR}`,
donc rien de sensible n'est écrit en clair.

```bash
cp datacat.example.toml datacat.toml
```

Pour un premier démarrage sans friction, désactivez la vérification du token — mais notez que cela
n'est accepté **que** par un binaire compilé avec la feature Cargo `dev` (voir
[installation](../installation/)). Dans `datacat.toml` :

```toml
[database]
url = "${DATABASE_URL}"

[token]
enabled = false          # dev uniquement — nécessite `--features dev`

[auth.logs]
mode = "static"
static_token = "${LOGS_STATIC_TOKEN:-dev-logs-token}"
```

:::note
Si vous omettez complètement le fichier de configuration, Datacat retombe sur sa configuration
historique par variables d'environnement (`BIND_ADDR`, `DATABASE_URL`, …), suffisante pour le
développement. Voir [configuration](../configuration/) pour l'ordre de résolution (`$DATACAT_CONFIG`,
puis `./datacat.toml`, puis `/etc/datacat/datacat.toml`).
:::

## 3. Lancer le backend

Les migrations sont embarquées dans le binaire et appliquées automatiquement au démarrage : il n'y a
donc aucune étape manuelle de schéma.

```bash
cd backend
cargo run --features dev          # écoute sur :8080 par défaut
```

Vérifiez qu'il répond :

```bash
curl -s http://localhost:8080/healthz        # liveness
curl -s http://localhost:8080/readyz         # readiness (base joignable)
```

## 4. Envoyer un event de test

Les events vont sur `POST /v1/events` sous forme de **batch** (`{ "events": [ ... ] }`), avec le JWT
d'ingestion dans l'en-tête `Authorization`. Avec `[token].enabled = false` (dev), n'importe quel
bearer est accepté, vous pouvez donc envoyer un jeton fictif :

```bash
curl -s -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d '{
    "events": [
      {
        "event_id":         "550e8400-e29b-41d4-a716-446655440000",
        "event_name":       "validate_planning",
        "tenant_id":        "clinic-42",
        "actor_id":         "user-123",
        "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f",
        "timestamp_client": "2026-06-21T10:15:30.123Z",
        "properties":       { "planning_id": 42 }
      }
    ]
  }'
```

L'API accuse réception immédiatement :

```json
202 Accepted
{ "received": 1 }
```

`received` est le nombre d'events acceptés pour écriture asynchrone — **pas** le nombre inséré. La
déduplication a lieu en base : renvoyer le même `event_id` est silencieusement ignoré
(`ON CONFLICT DO NOTHING`). Voir le [contrat](../contract/) pour le wire format complet.

En production, le JWT n'est **pas** fictif : c'est un token éphémère signé par votre backend
authentifié et vérifié par Datacat avec la clé publique seule. Voir [token](../token/).

## 5. Envoyer un log OTLP de test

Les logs techniques utilisent le format standard **OTLP/HTTP (JSON)** sur `POST /v1/logs`,
authentifié avec le **token de service statique** (`[auth.logs]`). Le corps est un
`ExportLogsServiceRequest` OTLP :

```bash
curl -s -X POST http://localhost:8080/v1/logs \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-logs-token' \
  -d '{
    "resourceLogs": [{
      "resource": { "attributes": [
        { "key": "service.name", "value": { "stringValue": "demo-api" } }
      ]},
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "1718900000000000000",
          "severityText": "INFO",
          "body": { "stringValue": "hello from quickstart" }
        }]
      }]
    }]
  }'
```

La réponse est un `ExportLogsServiceResponse` OTLP (vide en cas de succès complet). Le même endpoint
accepte les logs de n'importe quel SDK OpenTelemetry ou Collector — voir [logs OTLP](../otel-logs/)
et, pour expédier les logs et métriques de conteneurs,
[Logs & métriques avec Docker](../docker-telemetry/).

## 6. Inspecter ce qui a été reçu

`GET /stats` expose des compteurs par domaine (reçus, insérés, dédupliqués, rejetés) :

```bash
curl -s http://localhost:8080/stats
```

## Étapes suivantes

- [Installation](../installation/) — builds release, features `dev`/`export`, TLS.
- [SDKs](../sdks/) — envoyer des events depuis une app web (TypeScript) ou mobile (Flutter).
- [Logs & métriques avec Docker](../docker-telemetry/) — expédier la télémétrie de conteneurs avec un Collector OTel.
- [Token](../token/) — émettre de vrais tokens d'ingestion éphémères depuis votre backend.
