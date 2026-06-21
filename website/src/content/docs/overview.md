---
title: "Overview"
description: "Index of the Datacat documentation: contract, architecture, integration, telemetry, reads."
---

## Contract & design
| Document | Contents |
|---|---|
| [Contract](../contract/) | **Source of truth**: event wire format + token contract (backend & SDKs). |
| [Architecture](../architecture/) | Design decisions (idempotence × partitioning, write path, scalability). |
| [Security](../security/) | Threat model, controls, HDS audit posture. |

## Integration & deployment
| Document | Contents |
|---|---|
| [Token](../token/) | Token issuance specification for consumer backends. |
| [Deployment](../deployment/) | Simple, reproducible deployment (Docker, env, migrations, retention, health). |

## Telemetry (OpenTelemetry)
| Document | Contents |
|---|---|
| [OTLP logs](../otel-logs/) | OTLP **log** ingestion (HTTP + gRPC) + service token + correlation. |
| [OTLP metrics](../otel-metrics/) | OTLP **metric** ingestion (gauge/sum/histogram). |
| [Alerting](../alerting/) | **Alerting** engine (rules, cooldown) + Slack & e-mail notifications. |

## Reading & operations
| Document | Contents |
|---|---|
| [Hot reads](../read-hot/) | **Hot** read layer (`/v1/query/*`). |
| [Cold reads](../read-cold/) | **Cold** reads (DataFusion over Parquet on S3). |
| [Cold storage](../cold-storage/) | Cold export PostgreSQL → Parquet on S3 (Iceberg-friendly). |
| [MCP](../mcp/) | **MCP** server: read access for an agent (Claude) — debug, exploration, correlation. |

> OTLP **traces** (HTTP + gRPC) and logs↔traces correlation are described in
> [Architecture](../architecture/) §7 and covered by [Hot reads](../read-hot/) (`/v1/query/traces`).
