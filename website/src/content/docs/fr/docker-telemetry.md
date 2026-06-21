---
title: "Logs & métriques avec Docker"
description: "Expédier les logs et métriques de conteneurs vers Datacat avec un Collector OpenTelemetry — Compose et Swarm."
---

Ce guide montre comment expédier les **logs et métriques** de vos conteneurs vers Datacat à l'aide
d'un **Collector OpenTelemetry** en sidecar (Docker Compose) ou en service par nœud (Docker Swarm).
Le Collector lit les logs et les stats des conteneurs, puis les transmet aux endpoints OTLP de
Datacat avec un token de service statique.

## Comment Datacat reçoit la télémétrie

Datacat expose les endpoints OTLP standard. Pointez l'exporter de votre Collector dessus :

| Signal | Endpoint OTLP/HTTP | OTLP/gRPC |
|---|---|---|
| Logs | `POST /v1/logs` | `LogsService/Export` |
| Métriques | `POST /v1/metrics` | `MetricsService/Export` |
| Traces | `POST /v1/traces` | `TracesService/Export` |

- **HTTP** est toujours actif, sur le port serveur (par défaut `8080`) — par ex.
  `http://datacat:8080`. L'exporter `otlphttp` ajoute lui-même `/v1/logs` et `/v1/metrics`.
- **gRPC** est optionnel : mettez `[server.grpc].enabled = true` ; il écoute sur
  `[server.grpc].bind_addr` (par défaut `0.0.0.0:4317`).

La télémétrie est authentifiée service-à-service avec un **token statique**. Côté Datacat :

```toml
[auth.logs]
mode = "static"
static_token = "${LOGS_STATIC_TOKEN}"
```

Le Collector envoie ce token en `Authorization: Bearer <LOGS_STATIC_TOKEN>`. Logs, métriques et
traces partagent la même auth `[auth.logs]`. Voir [logs OTLP](../otel-logs/) et
[métriques OTLP](../otel-metrics/) pour le modèle de données, l'idempotence et les clés de
corrélation (`tenant_id` / `actor_id` / `session_id`).

## Docker Compose

Lancez le Collector comme un service à côté de votre app. Il scrape les stats Docker par conteneur
(métriques) et tail les fichiers de logs des conteneurs (logs), puis exporte les deux vers Datacat
en OTLP/HTTP.

### Extrait `docker-compose.yml`

```yaml
services:
  # ... vos services applicatifs ...

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otelcol/config.yaml"]
    environment:
      # Le token de service statique attendu par Datacat (à garder hors de l'image).
      LOGS_STATIC_TOKEN: ${LOGS_STATIC_TOKEN}
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol/config.yaml:ro
      # Lire les logs des conteneurs et parler à l'API Docker pour les stats.
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      - datacat
```

Ici `datacat` est le nom de service de votre backend d'ingestion sur le même réseau Compose
(joignable en `http://datacat:8080`).

### `config.yaml` du Collector

```yaml
receivers:
  # Accepter aussi l'OTLP de vos propres apps instrumentées.
  otlp:
    protocols:
      http:
      grpc:

  # Tail les fichiers de logs écrits par le driver de logging json-file de Docker.
  filelog:
    include: [ /var/lib/docker/containers/*/*-json.log ]
    operators:
      - type: json_parser            # chaque ligne est un objet JSON : {log, stream, time}
      - type: move
        from: attributes.log
        to: body

  # Usage des ressources par conteneur (CPU, mémoire, réseau, IO bloc) en métriques.
  docker_stats:
    endpoint: unix:///var/run/docker.sock
    collection_interval: 30s

processors:
  batch:
  resourcedetection:
    detectors: [ env, system ]

exporters:
  otlphttp:
    endpoint: http://datacat:8080         # l'exporter ajoute /v1/logs et /v1/metrics
    headers:
      Authorization: "Bearer ${env:LOGS_STATIC_TOKEN}"

service:
  pipelines:
    logs:
      receivers: [ otlp, filelog ]
      processors: [ resourcedetection, batch ]
      exporters: [ otlphttp ]
    metrics:
      receivers: [ otlp, docker_stats ]
      processors: [ resourcedetection, batch ]
      exporters: [ otlphttp ]
```

Pour utiliser **gRPC** à la place, activez `[server.grpc]` côté Datacat et changez l'exporter :

```yaml
exporters:
  otlp:
    endpoint: datacat:4317
    tls:
      insecure: true                      # sur réseau privé ; sinon terminez TLS à un proxy
    headers:
      Authorization: "Bearer ${env:LOGS_STATIC_TOKEN}"
```

Démarrez-le :

```bash
export LOGS_STATIC_TOKEN='…'              # même valeur que le [auth.logs].static_token de Datacat
docker compose up -d otel-collector
```

## Docker Swarm

Dans un cluster Swarm, lancez le Collector en **mode global** pour qu'exactement une instance
atterrisse sur chaque nœud, tailant les logs des conteneurs de ce nœud. Passez le token via un
**secret** Docker plutôt qu'une chaîne d'environnement gravée dans l'image.

### Créer le secret

```bash
printf '%s' "$LOGS_STATIC_TOKEN" | docker secret create datacat_logs_token -
```

### Fichier de stack (`docker stack deploy`)

```yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otelcol/config.yaml"]
    deploy:
      mode: global                         # un collector par nœud
    secrets:
      - datacat_logs_token
    configs:
      - source: otelcol_config
        target: /etc/otelcol/config.yaml
    volumes:
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro

configs:
  otelcol_config:
    file: ./otel-collector-config.yaml

secrets:
  datacat_logs_token:
    external: true
```

Déployez avec :

```bash
docker stack deploy -c stack.yaml telemetry
```

### Lire le token depuis le secret

Docker monte un secret en fichier sous `/run/secrets/<nom>`. Référencez-le dans l'exporter avec
l'expansion `${file:…}` du Collector pour que le token n'apparaisse jamais dans le fichier de stack :

```yaml
exporters:
  otlphttp:
    endpoint: http://datacat:8080
    headers:
      Authorization: "Bearer ${file:/run/secrets/datacat_logs_token}"
```

Côté Datacat, la même valeur est fournie à `[auth.logs].static_token` — elle-même référencée depuis
l'environnement (`LOGS_STATIC_TOKEN`) ou un secret — donc jamais écrite en clair dans `datacat.toml`.
La rotation du token revient à mettre à jour le secret des deux côtés.

## Notes

- Le receiver `filelog` et le receiver `docker_stats` vivent dans la distribution **contrib**
  (`otel/opentelemetry-collector-contrib`), pas dans l'image core.
- Datacat déduplique les enregistrements OTLP renvoyés par un hash de contenu déterministe, donc les
  réessais du Collector sont sûrs (pas de doublons). Voir [logs OTLP](../otel-logs/) §idempotence.
- Ajoutez `tenant_id` / `actor_id` / `session_id` en attributs de ressource ou d'enregistrement pour
  corréler les logs de conteneurs avec les events produit au moment de la lecture.
