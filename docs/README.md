# Datacat Documentation

## Contract & design
| Document | Contents |
|---|---|
| [CONTRACT.md](CONTRACT.md) | **Source of truth**: event wire format + token contract (backend & SDKs). |
| [architecture.md](architecture.md) | Design decisions (idempotence × partitioning, write path, scalability). |
| [security.md](security.md) | Threat model, controls, HDS audit posture. |
| [CONFORMITE.md](CONFORMITE.md) | Matrix: acceptance criteria §12 → implementation + test evidence. |

## Integration & deployment
| Document | Contents |
|---|---|
| [integration.md](integration.md) | Fast integration into an existing app (token endpoint, web & Flutter SDKs). |
| [token-contract.md](token-contract.md) | Token issuance specification for consumer backends. |
| [deployment.md](deployment.md) | Simple, reproducible deployment (Docker, env, migrations, retention, health). |

## Telemetry (OpenTelemetry)
| Document | Contents |
|---|---|
| [otel-logs.md](otel-logs.md) | OTLP **log** ingestion (HTTP + gRPC) + service token + correlation. |
| [otel-metrics.md](otel-metrics.md) | OTLP **metric** ingestion (gauge/sum/histogram). |
| [alerting.md](alerting.md) | **Alerting** engine (rules, cooldown) + Slack & e-mail notifications. |

## Reading & operations
| Document | Contents |
|---|---|
| [read-hot.md](read-hot.md) | **Hot** read layer (`/v1/query/*`, read-only SQL). |
| [read-cold.md](read-cold.md) | **Cold** reads (DataFusion over Parquet on S3). |
| [cold-storage.md](cold-storage.md) | Cold export PostgreSQL → Parquet on S3 (Iceberg-friendly). |
| [mcp.md](mcp.md) | **MCP** server: read access for an agent (Claude) — debug, exploration, correlation. |

> OTLP **traces** (HTTP + gRPC) and logs↔traces correlation are described in
> [architecture.md](architecture.md) §7 and covered by `read-hot.md` (`/v1/query/traces`).
