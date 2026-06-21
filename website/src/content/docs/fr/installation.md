---
title: "Installation"
description: "Compiler le backend Datacat, résoudre son fichier de configuration, et le faire tourner derrière TLS en production."
---

Cette page couvre la compilation et l'installation du backend d'ingestion pour un vrai déploiement.
Pour un lancement local rapide, voir [démarrage rapide](../quickstart/) ; pour les détails
d'exploitation (sondes de santé, rétention, arrêt gracieux), voir [déploiement](../deployment/).

## 1. Compiler le backend

Le backend est une unique crate Rust qui produit le binaire `datacat-ingest`. Compilez-le en mode
release depuis la racine du dépôt :

```bash
cd backend
cargo build --release          # target/release/datacat-ingest
```

Les migrations sont **embarquées** dans le binaire (`sqlx::migrate!`) et appliquées automatiquement
au démarrage : le schéma se reconstruit donc de façon reproductible depuis un checkout propre — il
n'y a pas d'étape de migration séparée à lancer avant le premier démarrage.

La crate compile aussi un binaire `mint-dev-token` réservé au dev, utilisé pour forger des tokens
d'ingestion pour les tests ; il n'est **jamais** déployé en production (l'émission de token est hors
scope — voir [token](../token/)).

## 2. Résolution du fichier de configuration

Tout le déploiement est décrit par un unique `datacat.toml`. Au démarrage, il est recherché dans cet
ordre :

1. `$DATACAT_CONFIG` — un chemin explicite (recommandé en production) ;
2. `./datacat.toml` — le répertoire courant ;
3. `/etc/datacat/datacat.toml`.

Si **aucun** fichier n'est trouvé, Datacat retombe sur la configuration historique par variables
d'environnement (`BIND_ADDR`, `DATABASE_URL`, …), pratique pour le développement et la suite de
tests mais non destinée à la production. Fixez le chemin explicitement :

```bash
export DATACAT_CONFIG=/etc/datacat/datacat.toml
export DATABASE_URL='postgres://…'
export TOKEN_PUBLIC_KEY_PEM="$(cat /etc/datacat/ingest_pub.pem)"
./target/release/datacat-ingest
```

Seul `[database].url` est requis ; toute autre section retombe sur des valeurs par défaut sûres. Les
secrets sont référencés depuis l'environnement via `${VAR}` (ou `${VAR:-défaut}`) et résolus au
démarrage — une `${VAR}` requise sans défaut fait refuser le démarrage du service (fail-closed).
Voir [configuration](../configuration/) pour la référence complète.

## 3. TLS et reverse proxy

Datacat est un endpoint public et exposé, et **doit** se trouver derrière TLS. Terminez TLS au
niveau d'un reverse proxy (nginx, Caddy, Traefik, un ALB…) placé devant le service.

- Faites écouter le backend sur une adresse interne (par défaut `0.0.0.0:8080`) et laissez le proxy
  le servir.
- Mettez `[server].trust_forwarded_for = true` **uniquement** derrière un proxy de confiance unique,
  afin que la vraie IP cliente soit lue depuis `X-Forwarded-For` (le rate limiting et les bans d'IP
  en dépendent).
- Restreignez CORS à vos vraies origines dans `[server.cors].allowed_origins` — ne laissez jamais
  `["*"]` (voir §4).

L'image runtime (`backend/Dockerfile`) est minimale (`debian:bookworm-slim` + binaire + certificats
CA), tourne en non-root et utilise rustls (pas d'OpenSSL). Voir [déploiement](../deployment/) pour
les commandes de build/run Docker et les sondes `/healthz`, `/readyz`, `/stats`.

## 4. La feature Cargo `dev` (garde-fous production)

Deux relâchements sont dangereux en production et sont donc **refusés** sauf si le binaire est
compilé avec la feature Cargo `dev` (désactivée par défaut) :

| Relâchement | Sans `dev` | Avec `--features dev` |
|---|---|---|
| `[server.cors].allowed_origins = ["*"]` (CORS joker) | démarrage **échoue** | autorisé |
| `[token].enabled = false` (pas de vérification de token) | démarrage **échoue** | autorisé |

```bash
# Build production : garde-fous actifs (features par défaut).
cargo build --release

# Build local/dev : CORS joker et token désactivé sont permis.
cargo run --features dev
```

Cela rend une configuration de production non sûre **impossible à démarrer par accident** : un
binaire release ne bootera pas avec une politique CORS ouverte ou la vérification de token
désactivée.

## 5. La feature optionnelle `export`

Le pipeline d'export froid (PostgreSQL → Parquet sur S3) est conditionné par la feature Cargo
`export`, **activée par défaut** (`default = ["export"]`). Une fois compilée et avec
`[export].enabled = true`, le backend lance une tâche planifiée en arrière-plan qui exporte le jour
UTC précédent à chaque tick.

```bash
# Le build par défaut inclut export.
cargo build --release

# Build allégé sans les dépendances Arrow/Parquet/S3 :
cargo build --release --no-default-features
```

La même logique est aussi disponible en CLI autonome (la crate `exporter/`). Voir
[configuration](../configuration/) §5 et [stockage froid](../cold-storage/) pour la disposition sur
disque.

## Étapes suivantes

- [Déploiement](../deployment/) — Docker, migrations, sondes de santé, rétention, arrêt gracieux.
- [Configuration](../configuration/) — la référence complète de `datacat.toml`.
- [Sécurité](../security/) — la posture d'audit HDS et les contrôles.
