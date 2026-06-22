---
title: "Configuration"
description: "Configurer Datacat avec un fichier TOML unique et des fichiers par projet."
---

Datacat se configure via un unique **fichier TOML** (`datacat.toml`). Il décrit tout le déploiement
(serveur, base de données, ingestion, sécurité, couche de lecture, MCP, export froid) et pointe vers
**un fichier TOML par projet** sous `projects/*.toml`, chacun portant les règles d'alerting et les
canaux de notification de ce projet.

Les secrets ne sont jamais écrits en clair : toute valeur chaîne peut référencer une variable
d'environnement via `${VAR}` (ou `${VAR:-défaut}`), résolue au démarrage. Une `${VAR}` requise sans
défaut fait refuser le démarrage du service (fail-closed) — exigence HDS.

Un modèle prêt à copier se trouve dans `datacat.example.toml`, avec un projet d'exemple dans
`projects/example.toml`.

## 1. Résolution du fichier

Au démarrage, le fichier est recherché dans cet ordre :

1. `$DATACAT_CONFIG` (chemin explicite),
2. `./datacat.toml` (répertoire courant),
3. `/etc/datacat/datacat.toml`.

Si **aucun** fichier n'est trouvé, Datacat retombe sur la configuration historique par **variables
d'environnement** (`BIND_ADDR`, `DATABASE_URL`, …), pratique pour le développement et la suite de
tests. Dans ce mode, un unique projet nommé `default` est dérivé des variables `ALERT_*`.

## 2. Expansion des secrets

Toute chaîne du TOML — config de premier niveau **et** fichiers de projet — est scannée pour
`${...}` :

| Forme | Comportement |
|---|---|
| `${VAR}` | remplacée par la valeur de `VAR` ; **erreur** si non définie (fail-closed) |
| `${VAR:-défaut}` | remplacée par `VAR`, ou `défaut` si non définie |
| `prefix-${VAR}-suffix` | l'expansion partielle est supportée |

```toml
[database]
url = "${DATABASE_URL}"

[notifications.slack]
bot_token = "${SLACK_BOT_TOKEN}"         # bot token Slack (API Web, chat.postMessage)
```

Cela maintient tout secret hors du contrôle de version. Les vrais fichiers `datacat.toml` et
`projects/*.toml` sont git-ignorés ; seuls les modèles `*.example.toml` sont versionnés.

## 3. Configuration globale (`datacat.toml`)

Toutes les sections sont optionnelles sauf `[database].url` ; les valeurs omises utilisent des
défauts sûrs.

| Section | Rôle |
|---|---|
| `[server]` | `bind_addr`, `request_timeout`, `trust_forwarded_for` ; `[server.grpc]` (OTLP/gRPC), `[server.cors]` |
| `[database]` | `url` (obligatoire), `max_connections` |
| `[ingest]` | micro-batch (`flush_interval`, `flush_batch_size`, `channel_capacity`), `retention_days`, `partition_future_days` ; sous-sections `[ingest.limits]`, `[ingest.rate_limit]`, `[ingest.anomaly]` |
| `[token]` | vérification asymétrique du token (clé publique seule) — `enabled`, `algorithms`, source de clé (`jwks_url` \| `public_key_pem` \| `public_key_file`), `issuer`, `audience` |
| `[auth.logs]` / `[auth.query]` | auth service-à-service de l'ingestion télémétrie et des endpoints de lecture — `mode` (`auto`\|`static`\|`jwt`\|`none`) + `static_token` |
| `[mcp]` | serveur MCP HTTP embarqué (`enabled`) |
| `[export]` | export froid planifié (voir §5) |
| `[notifications]` | canaux Slack / e-mail globaux par défaut (repli pour les projets sans canaux propres) |
| `[projects]` | où charger les fichiers de projet (`dir` et/ou `files`) |

### Source de la clé du token

Exactement une source de clé publique est utilisée, par ordre de priorité : `jwks_url`, puis
`public_key_pem`, puis `public_key_file`. Avec `enabled = true` et aucune source, le démarrage
échoue.

```toml
[token]
enabled = true
algorithms = ["EdDSA", "RS256"]
public_key_pem = "${TOKEN_PUBLIC_KEY_PEM}"
alg = "EdDSA"
# ou : jwks_url = "https://issuer.example.com/.well-known/jwks.json"
```

### Limites d'ingestion (`[ingest.limits]`)

Tous les champs sont optionnels et utilisent des valeurs par défaut sûres.

| Champ | Défaut | Rôle |
|---|---|---|
| `max_batch_events` | `500` | events par requête `/v1/events` |
| `max_payload_bytes` | `1048576` | taille max du corps d'une requête events |
| `max_properties_bytes` | `16384` | taille max du JSON `properties` d'un event |
| `max_string_len` | `200` | longueur max d'un champ chaîne d'event |
| `max_json_depth` | `16` | profondeur max d'imbrication des payloads d'events |
| `max_past_skew` | `"31d"` | horodatage le plus ancien accepté |
| `max_future_skew` | `"24h"` | avance maximale acceptée |
| `max_otlp_record_bytes` | `65536` | plafond de taille **par enregistrement** OTLP (logs/spans/points) |

`max_otlp_record_bytes` est un garde-fou en défense en profondeur : le corps de la requête est déjà
borné, mais un seul enregistrement surdimensionné (un body de log énorme, un span avec des milliers
d'events, un bloc d'attributs) est écarté individuellement plutôt que de laisser un enregistrement
dominer un batch. Les enregistrements écartés sont comptés dans `dropped_oversized_total` (exposé sur
`/stats`) et journalisés en `warn` — perte tolérée, jamais de doublon, jamais d'échec global de la
requête.

## 4. Projets (`projects/*.toml`)

Un **projet** regroupe des règles d'alerting et des canaux de notification, optionnellement
limités par un filtre `service` / `tenant` par défaut. Datacat exécute **un évaluateur d'alerting
par projet**. Le pipeline d'ingestion et les données stockées sont partagés (l'isolation des projets
se fait au niveau de la configuration, pas des données).

`[projects]` sélectionne les fichiers à charger :

```toml
[projects]
dir = "projects"                       # charge tous les *.toml de ce répertoire
# files = ["projects/billing.toml"]    # et/ou des fichiers explicites
```

Un fichier de projet :

```toml
[project]
id = "billing"
name = "Billing"
service = "billing"     # filtre `service` par défaut des règles de ce projet
# tenant = "acme"       # filtre `tenant` par défaut (logs / spans / events)

[alerting]
eval_interval = "60s"

# Canaux de ce projet (sinon repli sur le [notifications] global).
[notifications.slack]
bot_token = "${BILLING_SLACK_BOT_TOKEN}"
channel = "#billing-alerts"

[[alerting.rules]]
name = "Taux d'erreur élevé"
kind = "error_ratio"
severity_min = 17
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05
cooldown_secs = 300
severity = "critical"

[[alerting.rules.actions]]
type = "slack"
```

- Le `service` / `tenant` du projet sont appliqués comme **valeurs par défaut** à chaque règle (et
  sous-condition de composite) qui n'en précise pas — ainsi les règles ci-dessus ciblent
  implicitement `service=billing`.
- Résolution des notifications : un projet utilise ses propres `[notifications.*]` s'ils sont
  présents, sinon le `[notifications]` global de `datacat.toml`.
- Le schéma des règles (kinds, comparateurs, actions) est documenté dans [alerting](../alerting/).

## 5. Export froid planifié

Lorsque la feature Cargo `export` est compilée (activée par défaut) et que `[export].enabled = true`,
le backend exécute une tâche de fond qui exporte la **veille UTC** vers Parquet sur un stockage
S3-compatible à chaque tick.

```toml
[export]
enabled = true
schedule = "24h"
bucket = "datacat-cold"
prefix = ""
region = "eu-west-1"
endpoint = "${S3_ENDPOINT:-}"           # vide = AWS S3 ; renseigner pour MinIO/compatible
access_key_id = "${AWS_ACCESS_KEY_ID:-}"
secret_access_key = "${AWS_SECRET_ACCESS_KEY:-}"
allow_http = false
tables = ["events", "logs"]
```

L'export est idempotent : relancer un jour écrase son objet. Voir [stockage froid](../cold-storage/)
pour le layout sur disque et la CLI d'export standalone.

## 6. Repli par variables d'environnement (historique)

Sans `datacat.toml`, tous les réglages proviennent des variables d'environnement (voir
`.env.example` pour la liste complète). Ce mode est destiné au développement et aux tests ; les
déploiements de production doivent utiliser le fichier TOML avec des références de secret `${ENV}`.
