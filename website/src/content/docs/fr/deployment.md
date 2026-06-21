---
title: "Déploiement"
description: "Déploiement et exploitation de Datacat en production."
---

Objectif : un déploiement **simple et reproductible**. La seule dépendance de la v1 est
**PostgreSQL**. Les migrations sont **embarquées dans le binaire** et appliquées automatiquement
au démarrage.

## 1. Prérequis

- PostgreSQL **14+** (testé sur 17), accessible via l'URL de base de données.
- Un reverse-proxy terminant **TLS** (nginx, Caddy, Traefik, ALB…) devant le service.
- La **clé publique** de vérification du token (PEM ou JWKS) — voir [token](../token/).

## 2. Configuration

Tout le déploiement est décrit par un unique **fichier TOML**, `datacat.toml`. Il est résolu au
démarrage dans cet ordre :

1. `$DATACAT_CONFIG` (chemin explicite),
2. `./datacat.toml` (répertoire courant),
3. `/etc/datacat/datacat.toml`.

Copiez `datacat.example.toml` et adaptez-le. Seul `[database].url` est **obligatoire** ; toutes les
autres sections sont optionnelles et retombent sur des valeurs par défaut sûres. Voir
[configuration](../configuration/) pour la référence complète.

Les secrets ne sont **jamais** écrits en clair : toute valeur chaîne peut référencer une variable
d'environnement via `${VAR}` (ou `${VAR:-défaut}`), résolue au démarrage. Une `${VAR}` requise sans
défaut fait refuser le démarrage du service (fail-closed) — exigence HDS.

> **Repli développement.** Si **aucun** `datacat.toml` n'est trouvé, Datacat retombe sur la
> configuration historique par variables d'environnement (`BIND_ADDR`, `DATABASE_URL`, … ; voir
> `.env.example`). Ce mode est destiné au développement et à la suite de tests ; les déploiements
> de production doivent utiliser le fichier TOML.

Les sections principales :

| Section | Rôle |
|---|---|
| `[server]` | `bind_addr` (défaut `0.0.0.0:8080`), `request_timeout`, `trust_forwarded_for` ; `[server.grpc]` (OTLP/gRPC), `[server.cors]` (liste blanche d'origines — ne pas laisser `["*"]` en production) |
| `[database]` | `url` (**obligatoire**), `max_connections` |
| `[ingest]` | micro-batch (`flush_interval`, `flush_batch_size`, `channel_capacity`), `retention_days`, `partition_future_days` ; `[ingest.limits]`, `[ingest.rate_limit]`, `[ingest.anomaly]` |
| `[token]` | vérification asymétrique du token (clé publique seule) : `enabled`, `algorithms`, source de clé (`jwks_url` \| `public_key_pem` \| `public_key_file`), `alg`, `issuer`, `audience` |
| `[auth.logs]` / `[auth.query]` | auth service-à-service des flux télémétrie et des endpoints de lecture : `mode` (`auto`\|`static`\|`jwt`\|`none`) + `static_token` |
| `[mcp]` | serveur MCP HTTP embarqué (`enabled`) |
| `[export]` | export froid planifié embarqué (voir §10) |
| `[notifications]` | canaux Slack / e-mail globaux par défaut (repli pour les projets) |
| `[projects]` | où charger les fichiers de projet (`dir` et/ou `files`) |

Clés essentielles en production :

```toml
[server]
bind_addr = "0.0.0.0:8080"
trust_forwarded_for = false          # true UNIQUEMENT derrière un proxy de confiance unique

[server.cors]
allowed_origins = ["https://app.example.com"]   # jamais ["*"] en production

[database]
url = "${DATABASE_URL}"
max_connections = 10

[ingest]
retention_days = 90                  # fenêtre de rétention (purge par DROP PARTITION)

[token]
enabled = true
algorithms = ["EdDSA", "RS256"]
alg = "EdDSA"
public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"
# ou : public_key_file = "/etc/datacat/ingest_pub.pem"
# ou : jwks_url = "https://issuer.example.com/.well-known/jwks.json"
```

Exactement une source de clé publique est utilisée, par ordre de priorité : `jwks_url`, puis
`public_key_pem`, puis `public_key_file`. Avec `[token].enabled = true` et aucune source, le
démarrage échoue.

## 3. Build & exécution

### Avec Docker (recommandé)

```bash
# Build de l'image (depuis la racine du dépôt)
docker build -f backend/Dockerfile -t datacat-ingest:latest .

# Exécution, en montant la config et en passant les secrets par l'environnement
docker run --rm -p 8080:8080 \
  -v /etc/datacat/datacat.toml:/etc/datacat/datacat.toml:ro \
  -e DATABASE_URL='postgres://datacat:datacat@db:5432/datacat' \
  -e TOKEN_PUBLIC_KEY_PEM="$(cat ingest_pub.pem)" \
  datacat-ingest:latest
```

L'image runtime est minimale (`debian:bookworm-slim` + binaire + CA certs) et tourne en
utilisateur non-root. TLS est assuré par rustls (pas d'OpenSSL).

### Sans Docker

```bash
export DATACAT_CONFIG=/etc/datacat/datacat.toml
export DATABASE_URL='postgres://…'
export TOKEN_PUBLIC_KEY_PEM="$(cat /etc/datacat/ingest_pub.pem)"
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

Logs **structurés JSON** (`LOG_FORMAT=json`), niveau via `RUST_LOG`, avec `x-request-id` propagé
(traçabilité §7.4).

## 6. Rétention

Au démarrage puis toutes les heures, le service :
- crée les partitions journalières à venir (et celles couvrant la fenêtre de skew passée) ;
- purge par `DROP PARTITION` les partitions plus anciennes que `[ingest].retention_days`
  (instantané).

## 7. Dimensionnement & PostgreSQL

- Régler `[database].max_connections` selon `max_connections` de PostgreSQL.
- `synchronous_commit=off` (cf. `docker-compose.yml`) augmente le débit d'écriture en acceptant
  la perte des toutes dernières transactions en cas de crash — cohérent avec la tolérance §2.
- La table de staging est `UNLOGGED` (pas de WAL).

## 8. Arrêt propre

Sur `SIGTERM`/`Ctrl-C`, le service arrête d'accepter, **flush** le micro-batch en cours, puis
ferme le pool. Prévoir un `terminationGracePeriod` suffisant (quelques secondes).

## 9. Évolutivité (hors v1)

L'ajout ultérieur du stockage froid, de la lecture analytique ou d'un tampon d'écriture distribué
(Citus/Redpanda) n'impacte pas le cœur d'ingestion (frontières découplées — cf. architecture §7).
Aucune de ces briques n'est déployée en v1.

## 10. Export froid planifié embarqué

Lorsque la feature Cargo `export` est compilée (activée par défaut) et que `[export].enabled = true`,
le backend exécute une tâche de fond qui exporte la **veille UTC** vers Parquet sur un stockage
S3-compatible à chaque tick. Les secrets (identifiants S3, endpoint) sont passés via `${ENV}` :

```toml
[export]
enabled = true
schedule = "24h"
bucket = "datacat-cold"
region = "eu-west-1"
endpoint = "${S3_ENDPOINT:-}"           # vide = AWS S3 ; renseigner pour MinIO/compatible
access_key_id = "${AWS_ACCESS_KEY_ID:-}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY:-}"
allow_http = false
tables = ["events", "logs"]
```

L'export est idempotent (relancer un jour écrase son objet). La même logique est aussi disponible
en CLI standalone — voir [stockage froid](../cold-storage/).
