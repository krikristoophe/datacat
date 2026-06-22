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
backend/          API Axum (events + logs/traces/metrics OTLP) + alerting + MCP + migrations + tests
  src/            api/ db/ events/ logs/ traces/ metrics/ otlp/ ingest/ query/ alerting/ security/
                  config settings (config TOML multi-projet) telemetry error
sdks/typescript/  SDK web (TypeScript)
sdks/flutter/     SDK mobile (Dart, compatible Flutter)
exporter/         export froid PostgreSQL → Parquet/S3 (crate ; aussi embarqué dans le backend via feature `export`)
reader/           lecture froide DataFusion sur Parquet/S3 (crate standalone)
examples/         mini-projet d'intégration : backend Rust de démo + app React (events + logs)
docs/             docs Markdown (source de vérité technique) — voir docs/CONTRACT.md
website/          site de doc Astro Starlight bilingue FR/EN (déployé sur GitHub Pages)
datacat.example.toml + projects/example.toml   modèles de configuration unifiée
docker-compose.yml  PostgreSQL pour dev/test
```

## Configuration

Toute la config passe par un **fichier TOML** (`datacat.toml`, modèle `datacat.example.toml`) :
config globale du déploiement + **un fichier par projet** sous `projects/*.toml` (règles
d'alerting + canaux de notification + filtre service/tenant). Les **secrets** ne sont jamais en
clair : on référence l'environnement via `${VAR}` (ou `${VAR:-défaut}`). Sans `datacat.toml`, repli
sur les variables d'environnement. Détails : `docs/configuration.md`.

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
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check

# SDK TypeScript
cd sdks/typescript && npm install && npm test && npm run build

# SDK Flutter/Dart
cd sdks/flutter && dart pub get && dart test

# Site de doc
cd website && npm install && npm run build
```

## Conventions de code

- **Langue : tout en ANGLAIS.** Code, commentaires, noms d'identifiants, messages de commit,
  logs, et docs Markdown (`docs/*.md`) sont rédigés en anglais. Les pages **destinées aux
  utilisateurs** (site `website/`) sont **en plus traduites en français** (Starlight bilingue
  EN/FR). Seul ce `CLAUDE.md` reste en français.
- **Navigation** : préférer les outils LSP (goToDefinition, findReferences, diagnostics)
  à grep pour tout ce qui touche au code.
- Rust : pas de `unwrap()`/`expect()` dans les chemins de requête ; erreurs typées
  (`thiserror`) ; `#![forbid(unsafe_code)]`. Pas de boilerplate inutile.
- Dépendances **maîtrisées** : surface d'attaque minimale, versions à jour, pas de superflu.
- Tout code livré est **testé**.

## Processus — à la fin de CHAQUE feature (obligatoire)

1. **Documentation à jour** : toute feature met à jour TOUTE la doc concernée — le **`README.md`**
   racine, `docs/*.md` (en **anglais**), le site `website/` (EN + **FR**), `CLAUDE.md`, et les
   modèles `datacat.example.toml` / `.env.example` si la config change. La doc ne doit jamais
   diverger du code.
2. **Revue de code** : lancer un **`/code-review`** (revue cloud multi-agents de la branche).
   Pour l'instant on **pousse directement sur `master`** ; passage en **PR** prévu quand le
   projet sera en production.
3. **Revue de sécurité** : par défaut, la revue de sécurité porte sur **TOUT le code** (niveau
   HDS), pas seulement le diff — sauf demande explicite de la limiter. Documenter dans
   `docs/security-review.md`. **Aucun finding « planifié » / « à faire plus tard » :** tout
   problème trouvé doit être **corrigé dans la foulée** (niveau HDS). `docs/security-review.md` ne
   contient donc que des contrôles vérifiés et des notes « pas un risque / par conception » —
   **jamais** de section « accepted risks » ni « next steps » : un point est soit un non-risque
   justifié, soit corrigé.
4. **Changelog** : tenir à jour un **`CHANGELOG.md`** racine (format *Keep a Changelog*) à chaque
   feature ou correctif notable (y compris les corrections de sécurité). C'est là que vit
   l'historique des findings corrigés, pas dans `docs/security-review.md`.
5. **Vérifs vertes** avant de pousser : `cargo fmt --check`, `cargo clippy --all-targets
   --all-features -- -D warnings`, `cargo test`, et les tests des SDKs si touchés.

## Portée

V1 = ingestion robuste + télémétrie OTLP (logs/traces/metrics) + couche de lecture (chaude PG /
froide Parquet) + alerting + export froid + MCP. **Hors scope** pour l'instant : UI/dashboard,
funnels, registre d'events, isolation complète des données entre projets (le multi-projet est
au niveau config : alerting/notifs/filtre, ingestion partagée).
