# Documentation Datacat

## Contrat & conception
| Document | Contenu |
|---|---|
| [CONTRACT.md](CONTRACT.md) | **Source de vérité** : wire format des events + contrat du token (backend & SDKs). |
| [architecture.md](architecture.md) | Décisions de conception (idempotence × partitionnement, chemin d'écriture, évolutivité). |
| [security.md](security.md) | Modèle de menace, contrôles, posture d'audit HDS. |
| [CONFORMITE.md](CONFORMITE.md) | Matrice : critères d'acceptation §12 → implémentation + preuve de test. |

## Intégration & déploiement
| Document | Contenu |
|---|---|
| [integration.md](integration.md) | Intégration rapide dans une app existante (endpoint de token, SDK web & Flutter). |
| [token-contract.md](token-contract.md) | Spécification d'émission du token pour les backends consommateurs. |
| [deployment.md](deployment.md) | Déploiement simple et reproductible (Docker, env, migrations, rétention, santé). |

## Télémétrie (OpenTelemetry)
| Document | Contenu |
|---|---|
| [otel-logs.md](otel-logs.md) | Ingestion des **logs** OTLP (HTTP + gRPC) + token de service + corrélation. |
| [otel-metrics.md](otel-metrics.md) | Ingestion des **métriques** OTLP (gauge/sum/histogram). |
| [alerting.md](alerting.md) | Moteur d'**alerting** (règles, cooldown) + notifications Slack & email. |

## Lecture & exploitation
| Document | Contenu |
|---|---|
| [read-hot.md](read-hot.md) | Couche de lecture **chaude** (`/v1/query/*`, SQL lecture seule). |
| [read-cold.md](read-cold.md) | Lecture **froide** (DataFusion sur Parquet S3). |
| [cold-storage.md](cold-storage.md) | Export froid PostgreSQL → Parquet sur S3 (Iceberg-friendly). |
| [mcp.md](mcp.md) | Serveur **MCP** : accès lecture pour un agent (Claude) — debug, parcours, corrélation. |

> Les **traces** OTLP (HTTP + gRPC) et la corrélation logs↔traces sont décrites dans
> [architecture.md](architecture.md) §7 et couvertes par `read-hot.md` (`/v1/query/traces`).
