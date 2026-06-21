# Matrice de conformité — critères d'acceptation (cahier §12)

Chaque critère est relié à son implémentation et à sa preuve de test.

| # | Critère (§12) | Implémentation | Preuve |
|---|---|---|---|
| 1 | Pic d'écriture sans perte au-delà de la tolérance et **sans doublon** ; idempotence vérifiée (même `event_id` n×→ 1) | COPY + staging UNLOGGED + merge `ON CONFLICT DO NOTHING`, clé `(timestamp_client, event_id)` (`ingest.rs`, `migrations/`) | Tests `write_spike_no_duplicates` (2000 uniques × 2 envois concurrents → 2000 lignes), `same_event_id_counts_once` ; smoke binaire (3 envois → `inserted=1, deduplicated=2`) |
| 2 | Écriture par `COPY`, table **partitionnée**, purge par `DROP PARTITION` sans impact écriture | `copy_in_raw` CSV, `PARTITION BY RANGE (timestamp_client)`, `datacat_drop_partitions_before` | Tests `copy_persists_distinct_events`, `purge_drops_old_partition` (partition DROP, events purgés) |
| 3 | Deux SDKs même contrat : batching, retry idempotent, `tenant`+`actor`+`session`, token jamais en dur | `sdk-typescript/`, `sdk-flutter/` conformes à `CONTRACT.md` | 26 tests vitest (TS) + 28 tests dart (Flutter), dont idempotence event_id/timestamp figés, renouvellement token |
| 4 | Rate limiting aux **deux niveaux** : session abusive limitée sans impacter les autres sessions de l'IP ; IP ne peut créer un nombre déraisonnable de sessions | `ratelimit.rs` (token bucket par session + fenêtre glissante sessions/IP + filet global) | Tests unitaires (5) + intégration `session_rate_limit_isolates_sessions`, `ip_session_cap_blocks_fake_session_flood` |
| 5 | Token vérifié par signature **asymétrique** (clé publique seule) ; ingestion ne peut pas forger ; contrat d'émission documenté | `token.rs` (EdDSA/RS256, PEM/JWKS, `none`/symétrique rejetés), `docs/token-contract.md` | Tests `token_is_required_and_verified`, `rs256_token_accepted`, `token_in_body_works_for_beacon` (401 si absent/invalide/expiré) |
| 6 | Migrations présentes, schéma reconstruit de façon **reproductible** | `backend/migrations/0001_schema.sql`, `0002_functions.sql`, appliquées au démarrage (`sqlx::migrate!`) | Smoke binaire + conteneur Docker (migrations appliquées, `/readyz` ok), chaque test d'intégration repart d'une base fraîche |
| 7 | Déploiement **documenté et simple** à reproduire | `docs/deployment.md`, `backend/Dockerfile`, `docker-compose.yml`, `.env.example` | Image Docker construite et **démarrée** (boot → `/readyz` ready → POST 202) |
| 8 | Code **testé**, standards respectés, **pas de boilerplate** | `#![forbid(unsafe_code)]`, erreurs typées, modules cohésifs | `cargo test` (32), `cargo clippy --all-targets -- -D warnings` (0), `cargo fmt --check` ; CI multi-jobs |
| 9 | Pas de faille évidente (revue HDS) : validation, protection endpoint public, traçabilité, TLS | Validation stricte (`model.rs`), rate limit + ban (`security.rs`), CORS, logs JSON + request-id, rustls | `docs/security.md` (modèle de menace + contrôles) ; tests `rejects_invalid_and_oversized`, `out_of_skew_event_dropped_not_rejected` |
| 10 | Frontières ingestion / stockage / lecture **découplées** | Modules `ingest` / `db` séparés, aucune dépendance vers une couche de lecture ; aucun index de lecture en v1 | `docs/architecture.md` §7 (carte d'extension sans réécriture) |

## Commandes de vérification

```bash
# Base de données
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

# Backend : standards + tests (unitaires + intégration PostgreSQL)
cd backend
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test                      # 32 tests

# SDK TypeScript
cd ../sdk-typescript && npm install && npm run typecheck && npm test && npm run build

# SDK Flutter/Dart
cd ../sdk-flutter && dart pub get && dart analyze && dart test   # 28 tests

# Déploiement
docker build -f backend/Dockerfile -t datacat-ingest .
```
