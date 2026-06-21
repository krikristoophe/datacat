# Déploiement

Objectif : un déploiement **simple et reproductible**. La seule dépendance de la v1 est
**PostgreSQL**. Les migrations sont **embarquées dans le binaire** et appliquées automatiquement
au démarrage.

## 1. Prérequis

- PostgreSQL **14+** (testé sur 17), accessible via `DATABASE_URL`.
- Un reverse-proxy terminant **TLS** (nginx, Caddy, Traefik, ALB…) devant le service.
- La **clé publique** de vérification du token (PEM ou JWKS) — voir `token-contract.md`.

## 2. Configuration

Toute la configuration passe par l'environnement. Voir [`.env.example`](../.env.example) pour la
liste complète et commentée. Seul `DATABASE_URL` est **obligatoire**.

Variables essentielles en production :

| Variable | Rôle |
|---|---|
| `DATABASE_URL` | connexion PostgreSQL (obligatoire) |
| `BIND_ADDR` | adresse d'écoute (défaut `0.0.0.0:8080`) |
| `CORS_ALLOWED_ORIGINS` | liste blanche des origines web (ne pas laisser `*` en prod) |
| `TOKEN_PUBLIC_KEY_FILE` **ou** `TOKEN_JWKS_URL` | clé publique de vérification du token |
| `TOKEN_ALG` (mode PEM) | `EdDSA` ou `RS256` |
| `RETENTION_DAYS` | fenêtre de rétention (purge par DROP PARTITION) |
| `TRUST_FORWARDED_FOR` | `true` **uniquement** derrière un proxy de confiance unique |

## 3. Build & exécution

### Avec Docker (recommandé)

```bash
# Build de l'image (depuis la racine du dépôt)
docker build -f backend/Dockerfile -t datacat-ingest:latest .

# Exécution
docker run --rm -p 8080:8080 \
  -e DATABASE_URL='postgres://datacat:datacat@db:5432/datacat' \
  -e CORS_ALLOWED_ORIGINS='https://app.example.com' \
  -e TOKEN_ALG=EdDSA \
  -e TOKEN_PUBLIC_KEY_PEM="$(cat ingest_pub.pem)" \
  datacat-ingest:latest
```

L'image runtime est minimale (`debian:bookworm-slim` + binaire + CA certs), tourne en
utilisateur non-root. TLS est assuré par rustls (pas d'OpenSSL).

### Sans Docker

```bash
export DATABASE_URL='postgres://…'
export TOKEN_ALG=EdDSA TOKEN_PUBLIC_KEY_FILE=/etc/datacat/ingest_pub.pem
cargo run --release --bin datacat-ingest
```

## 4. Migrations

Aucune étape manuelle : au démarrage, le service applique les migrations versionnées du dossier
`backend/migrations/` (embarquées via `sqlx::migrate!`). Le schéma se reconstruit donc de façon
**reproductible** depuis un dépôt propre. Pour appliquer/inspecter manuellement :

```bash
cd backend
export DATABASE_URL='postgres://…'
sqlx migrate run        # nécessite sqlx-cli
sqlx migrate info
```

## 5. Santé & observabilité

| Endpoint | Usage |
|---|---|
| `GET /healthz` | **liveness** (le process répond) |
| `GET /readyz` | **readiness** (process prêt + base joignable) — pour les sondes k8s/LB |
| `GET /stats` | compteurs : reçus, insérés, dédupliqués, écartés (skew/saturation), bans, etc. |

Logs **structurés JSON** (`LOG_FORMAT=json`), niveau via `RUST_LOG`, avec `x-request-id`
propagé (traçabilité §7.4).

## 6. Rétention

Au démarrage puis toutes les heures, le service :
- crée les partitions journalières à venir (et celles couvrant la fenêtre de skew passée) ;
- purge par `DROP PARTITION` les partitions plus anciennes que `RETENTION_DAYS` (instantané).

## 7. Dimensionnement & PostgreSQL

- Régler `DB_MAX_CONNECTIONS` selon `max_connections` de PostgreSQL.
- `synchronous_commit=off` (cf. `docker-compose.yml`) augmente le débit d'écriture en acceptant
  la perte des toutes dernières transactions en cas de crash — cohérent avec la tolérance §2.
- La table de staging est `UNLOGGED` (pas de WAL).

## 8. Arrêt propre

Sur `SIGTERM`/`Ctrl-C`, le service arrête d'accepter, **flush** le micro-batch en cours, puis
ferme le pool. Prévoir un `terminationGracePeriod` suffisant (quelques secondes).

## 9. Évolutivité (hors v1)

L'ajout ultérieur du stockage froid, de la lecture analytique ou d'un tampon d'écriture
distribué (Citus/Redpanda) n'impacte pas le cœur d'ingestion (frontières découplées — cf.
`architecture.md` §7). Aucune de ces briques n'est déployée en v1.
