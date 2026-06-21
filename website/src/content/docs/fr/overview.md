---
title: "Vue d’ensemble"
description: "Index de la documentation Datacat : contrat, architecture, intégration, télémétrie, lecture."
---

## Contrat & conception
| Document | Contenu |
|---|---|
| [Contrat](../contract/) | **Source de vérité** : wire format des events + contrat du token (backend & SDKs). |
| [Architecture](../architecture/) | Décisions de conception (idempotence × partitionnement, chemin d'écriture, évolutivité). |
| [Sécurité](../security/) | Modèle de menace, contrôles, posture d'audit HDS. |

## Intégration & déploiement
| Document | Contenu |
|---|---|
| [Token](../token/) | Spécification d'émission du token pour les backends consommateurs. |
| [Déploiement](../deployment/) | Déploiement simple et reproductible (Docker, env, migrations, rétention, santé). |

## Télémétrie (OpenTelemetry)
| Document | Contenu |
|---|---|
| [Logs OTLP](../otel-logs/) | Ingestion des **logs** OTLP (HTTP + gRPC) + token de service + corrélation. |
| [Métriques OTLP](../otel-metrics/) | Ingestion des **métriques** OTLP (gauge/sum/histogram). |
| [Alerting](../alerting/) | Moteur d'**alerting** (règles, cooldown) + notifications Slack & email. |

## Lecture & exploitation
| Document | Contenu |
|---|---|
| [Lecture chaude](../read-hot/) | Couche de lecture **chaude** (`/v1/query/*`, SQL lecture seule). |
| [Lecture froide](../read-cold/) | Lecture **froide** (DataFusion sur Parquet S3). |
| [Stockage froid](../cold-storage/) | Export froid PostgreSQL → Parquet sur S3 (Iceberg-friendly). |
| [MCP](../mcp/) | Serveur **MCP** : accès lecture pour un agent (Claude) — debug, parcours, corrélation. |

> Les **traces** OTLP (HTTP + gRPC) et la corrélation logs↔traces sont décrites dans
> [Architecture](../architecture/) §7 et couvertes par [Lecture chaude](../read-hot/) (`/v1/query/traces`).
