# Datacat — guide de contribution (pour agents & humains)

Système d'analytics d'events **maison, auto-hébergé**. La v1 se concentre **exclusivement
sur l'ingestion** : capturer des events de façon robuste, scalable et auditable. Voir
`cahier_des_charges_analytics.md` pour le cahier des charges complet.

## Principes non négociables (résumé)

- **PostgreSQL unique** comme base centrale. Pas de Kafka/ClickHouse/Zookeeper.
- **Idempotence stricte** : un même `event_id` ne compte qu'une fois.
- **Priorité au débit d'écriture** (COPY, micro-batch, table de staging UNLOGGED).
- **Léger maintenant, scalable plus tard** : frontières nettes ingestion / stockage / lecture.
- **Auditable HDS** : validation stricte des entrées, endpoint public défendu côté serveur,
  token vérifié par signature asymétrique (clé publique seule), traçabilité, TLS.
- **Tolérance à la perte** d'une petite fraction d'events — **jamais aux doublons**.

## Structure du dépôt

```
backend/          API d'ingestion Axum (events + logs OTLP) + migrations sqlx + tests
  src/            sous-modules : api/ db/ events/ logs/ ingest/ security/ config telemetry error
sdks/typescript/  SDK web (TypeScript)
sdks/flutter/     SDK mobile (Dart, compatible Flutter)
exporter/         export froid PostgreSQL → Parquet sur S3 (crate standalone)
examples/         mini-projet d'intégration : backend Rust de démo + app React (events + logs)
docs/             CONTRACT.md (source de vérité), architecture, déploiement, intégration, token, otel-logs, sécurité
docker-compose.yml  PostgreSQL pour dev/test
```

## Source de vérité

`docs/CONTRACT.md` définit le wire format des events et le contrat du token. Backend et
**les deux** SDKs doivent y être strictement conformes. Toute évolution du contrat s'y fait
d'abord.

## Développement

```bash
# Base de données (dev/test)
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

# Backend
cd backend
cargo build
cargo test                 # nécessite DATABASE_URL (tests d'intégration PG)
cargo clippy --all-targets -- -D warnings
cargo fmt --check

# SDK TypeScript
cd sdks/typescript && npm install && npm test && npm run build

# SDK Flutter/Dart
cd sdks/flutter && dart pub get && dart test
```

## Conventions de code

- **Navigation** : préférer les outils LSP (goToDefinition, findReferences, diagnostics)
  à grep pour tout ce qui touche au code.
- Rust : pas de `unwrap()`/`expect()` dans les chemins de requête ; erreurs typées
  (`thiserror`) ; `#![forbid(unsafe_code)]`. Pas de boilerplate inutile.
- Dépendances **maîtrisées** : surface d'attaque minimale, versions à jour, pas de superflu.
- Tout code livré est **testé**.

## Hors scope v1 (ne pas implémenter)

Lecture analytique (DataFusion/DuckDB), stockage froid (Parquet/Iceberg), UI/dashboard,
funnels, registre d'events, intégration des logs techniques, émission du token (backend
consommateur), RGPD applicatif. L'architecture **prépare** ces extensions sans les déployer.
