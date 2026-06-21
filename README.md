# Datacat

Système d'analytics d'events **maison, léger et auto-hébergé** pour applications B2B.
La **v1 se concentre exclusivement sur l'ingestion** : capturer des events de façon robuste,
idempotente, scalable et auditable, avec PostgreSQL comme unique base.

> Deux usages cibles à terme : analyse des parcours réels et génération automatique de tests
> E2E à partir de l'usage observé. La v1 construit le socle d'ingestion ; la lecture
> analytique est *préparée par l'architecture* mais hors périmètre.

## Composants

| Dossier | Description |
|---|---|
| [`backend/`](backend/) | API d'ingestion **Axum** (Rust) : events + télémétrie OTLP (logs/traces/métriques, HTTP+gRPC), lecture, alerting, **serveur MCP HTTP** (`/mcp`) + migrations **sqlx** + tests |
| [`sdks/typescript/`](sdks/typescript/) | SDK web analytics (TypeScript) |
| [`sdks/flutter/`](sdks/flutter/) | SDK mobile analytics (Dart, compatible Flutter) |
| [`exporter/`](exporter/) | Export froid PostgreSQL → **Parquet** sur S3 (crate Rust standalone) |
| [`reader/`](reader/) | Lecture analytique froide **DataFusion** sur Parquet S3 (crate Rust standalone) |
| [`examples/`](examples/) | Mini-projet d'intégration : backend Rust de démo + app React |
| [`docs/`](docs/) | Contrat, architecture, déploiement, intégration, télémétrie OTLP, lecture, alerting, MCP, sécurité |

## Architecture (v1)

```
Events (web / mobile / backend)
        │  POST /v1/events  (batch, Bearer <jwt>)
        ▼
   API d'ingestion (Axum)
        │  validation stricte · rate limiting 2 niveaux · vérif token (clé publique)
        │  micro-batch en mémoire
        ▼
   PostgreSQL  ── COPY → staging UNLOGGED → MERGE idempotent → table partitionnée par temps
        │
        ▼  (hors v1) export froid Parquet/Iceberg · lecture DataFusion/DuckDB
```

Décisions clés :

- **Idempotence** : clé `(timestamp_client, event_id)`, `INSERT … ON CONFLICT DO NOTHING`.
  Un même event reçu *N* fois n'est stocké qu'une fois. Voir [docs/architecture.md](docs/architecture.md).
- **Débit d'écriture** : `COPY` depuis un micro-batch en mémoire vers une table de **staging
  `UNLOGGED`**, puis merge idempotent vers la table partitionnée. Purge par `DROP PARTITION`.
- **Sécurité** (endpoint public, non authentifié au sens fort) : validation stricte,
  **rate limiting à deux niveaux** (par `session_id` + plafond de sessions par IP) + filet
  global, **vérification du token** par signature asymétrique (clé publique seule), CORS,
  détection d'anomalies, TLS. Conçu pour passer un **audit HDS**.
- **Logs techniques OpenTelemetry** : ingestion **OTLP/HTTP** (`POST /v1/logs`) **et OTLP/gRPC**
  (`:4317`), même socle partitionné/idempotent que les events, **token de service fixe**
  (service-à-service), corrélés aux events via `session_id` / `actor_id` / `tenant_id` et aux
  traces via `trace_id`. Voir [docs/otel-logs.md](docs/otel-logs.md).
- **Mini-projet d'intégration** : `examples/` contient un backend Rust de démo (émet le token
  d'ingestion + ses logs OTLP) et une app React (SDK analytics) pour valider l'intégration
  complète events + logs de bout en bout.

## Démarrage rapide

```bash
# 1. PostgreSQL
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

# 2. Backend (migrations appliquées au démarrage)
cd backend
cargo run            # écoute sur :8080 par défaut

# 3. Envoyer un batch d'events (token de dev généré par les outils de test)
#    Voir docs/integration.md et docs/token-contract.md.
```

Intégration côté application : [`docs/integration.md`](docs/integration.md).
Déploiement de production : [`docs/deployment.md`](docs/deployment.md).
Contrat de token (à implémenter par chaque backend consommateur) : [`docs/token-contract.md`](docs/token-contract.md).

## Périmètre

v1 = **ingestion uniquement**. Hors v1 (préparé, non déployé) : lecture analytique, stockage
froid, UI, funnels, logs techniques, scale-out (Citus/Redpanda). Détails dans le
[cahier des charges](cahier_des_charges_analytics.md) §11.

## Licence

Propriétaire — usage interne.
