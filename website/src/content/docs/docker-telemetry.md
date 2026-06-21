---
title: "Logs & metrics with Docker"
description: "Ship container logs and metrics to Datacat with an OpenTelemetry Collector — Compose and Swarm."
---

This guide shows how to ship your containers' **logs and metrics** to Datacat using an
**OpenTelemetry Collector** as a sidecar (Docker Compose) or a per-node service (Docker Swarm). The
Collector reads container logs and stats, then forwards them to Datacat's OTLP endpoints with a
static service token.

## How Datacat receives telemetry

Datacat exposes the standard OTLP endpoints. Point your Collector's exporter at them:

| Signal | OTLP/HTTP endpoint | OTLP/gRPC |
|---|---|---|
| Logs | `POST /v1/logs` | `LogsService/Export` |
| Metrics | `POST /v1/metrics` | `MetricsService/Export` |
| Traces | `POST /v1/traces` | `TracesService/Export` |

- **HTTP** is always on, on the server port (default `8080`) — e.g. `http://datacat:8080`. The
  `otlphttp` exporter appends `/v1/logs` and `/v1/metrics` itself.
- **gRPC** is opt-in: set `[server.grpc].enabled = true`; it listens on `[server.grpc].bind_addr`
  (default `0.0.0.0:4317`).

Telemetry is authenticated service-to-service with a **static token**. On the Datacat side:

```toml
[auth.logs]
mode = "static"
static_token = "${LOGS_STATIC_TOKEN}"
```

The Collector sends that token as `Authorization: Bearer <LOGS_STATIC_TOKEN>`. Logs, metrics and
traces share the same `[auth.logs]` auth. See [OTLP logs](../otel-logs/) and
[OTLP metrics](../otel-metrics/) for the data model, idempotency and correlation keys
(`tenant_id` / `actor_id` / `session_id`).

## Docker Compose

Run the Collector as a service alongside your app. It scrapes per-container Docker stats (metrics)
and tails the container log files (logs), then exports both to Datacat over OTLP/HTTP.

### `docker-compose.yml` snippet

```yaml
services:
  # ... your app services ...

  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otelcol/config.yaml"]
    environment:
      # The static service token Datacat expects (keep it out of the image).
      LOGS_STATIC_TOKEN: ${LOGS_STATIC_TOKEN}
    volumes:
      - ./otel-collector-config.yaml:/etc/otelcol/config.yaml:ro
      # Read container logs and talk to the Docker API for stats.
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro
    depends_on:
      - datacat
```

Here `datacat` is the service name of your running ingestion backend on the same Compose network
(reachable as `http://datacat:8080`).

### Collector `config.yaml`

```yaml
receivers:
  # Accept OTLP from your own instrumented apps too.
  otlp:
    protocols:
      http:
      grpc:

  # Tail the container log files written by the Docker json-file logging driver.
  filelog:
    include: [ /var/lib/docker/containers/*/*-json.log ]
    operators:
      - type: json_parser            # each line is a JSON object: {log, stream, time}
      - type: move
        from: attributes.log
        to: body

  # Per-container resource usage (CPU, memory, network, block IO) as metrics.
  docker_stats:
    endpoint: unix:///var/run/docker.sock
    collection_interval: 30s

processors:
  batch:
  resourcedetection:
    detectors: [ env, system ]

exporters:
  otlphttp:
    endpoint: http://datacat:8080         # exporter appends /v1/logs and /v1/metrics
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

To use **gRPC** instead, enable `[server.grpc]` on Datacat and swap the exporter:

```yaml
exporters:
  otlp:
    endpoint: datacat:4317
    tls:
      insecure: true                      # behind a private network; terminate TLS at a proxy otherwise
    headers:
      Authorization: "Bearer ${env:LOGS_STATIC_TOKEN}"
```

Start it:

```bash
export LOGS_STATIC_TOKEN='…'              # same value as Datacat's [auth.logs].static_token
docker compose up -d otel-collector
```

## Docker Swarm

In a Swarm cluster, run the Collector as a **global-mode** service so exactly one instance lands on
each node, tailing that node's container logs. Pass the token as a Docker **secret** rather than an
environment string baked into the image.

### Create the secret

```bash
printf '%s' "$LOGS_STATIC_TOKEN" | docker secret create datacat_logs_token -
```

### Stack file (`docker stack deploy`)

```yaml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    command: ["--config=/etc/otelcol/config.yaml"]
    deploy:
      mode: global                         # one collector per node
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

Deploy with:

```bash
docker stack deploy -c stack.yaml telemetry
```

### Reading the token from the secret

Docker mounts a secret as a file at `/run/secrets/<name>`. Reference it in the exporter with the
Collector's `${file:…}` expansion so the token never appears in the stack file:

```yaml
exporters:
  otlphttp:
    endpoint: http://datacat:8080
    headers:
      Authorization: "Bearer ${file:/run/secrets/datacat_logs_token}"
```

On the Datacat side, the same value is provided to `[auth.logs].static_token` — itself referenced
from the environment (`LOGS_STATIC_TOKEN`) or a secret — so it is never written in clear text in
`datacat.toml`. Rotating the token is a matter of updating the secret on both sides.

## Notes

- The `filelog` receiver and `docker_stats` receiver live in the **contrib** distribution
  (`otel/opentelemetry-collector-contrib`), not the core image.
- Datacat deduplicates resent OTLP records by a deterministic content hash, so Collector retries are
  safe (no duplicates). See [OTLP logs](../otel-logs/) §idempotency.
- Add `tenant_id` / `actor_id` / `session_id` as resource or record attributes to correlate
  container logs with product events at read time.
