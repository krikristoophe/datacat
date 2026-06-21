---
title: "Alerting"
description: "Configuration des alertes sur les données ingérées."
---

Datacat embarque un moteur d'alerting léger : des **règles déclaratives** sont évaluées
périodiquement sur les données ingérées (`logs`, `metric_points`, …), et chaque franchissement de
seuil déclenche une ou plusieurs **actions** modulables (Slack, e-mail, webhook HTTP générique). Le
moteur est **entièrement optionnel** : sans règle, ou sans aucune action ni canal configuré, il
reste désactivé (no-op au démarrage).

L'alerting se configure **par projet**. Chaque projet est un fichier TOML sous `projects/*.toml` qui
porte ses règles d'alerting (`[[alerting.rules]]`) et, optionnellement, ses propres canaux de
notification (`[notifications.slack]` / `[notifications.email]`). Datacat exécute **un évaluateur par
projet**. Voir [configuration](../configuration/) pour le chargement des projets.

## 1. Activation

Pour un projet donné, l'évaluateur démarre si le projet déclare au moins une règle **et** qu'il
existe au moins une cible de notification, c'est-à-dire :
- un **canal** configuré pour le projet (ou, par repli, le `[notifications]` global de
  `datacat.toml`), **ou**
- au moins une règle portant ses propres `[[alerting.rules.actions]]` (un webhook suffit, sans
  aucune config globale).

Sinon, un message d'info/warning est journalisé et l'évaluateur n'est pas lancé pour ce projet.

### Portée du projet (filtre `service` / `tenant` par défaut)

Un projet peut déclarer un `service` et/ou un `tenant` par défaut. Ils sont appliqués comme
**valeurs par défaut** à chaque règle (et chaque sous-condition de composite) qui n'en précise pas —
ainsi un projet limité à `service = "billing"` cible implicitement `service=billing` sur toutes ses
règles. Une règle peut toujours surcharger le filtre explicitement.

```toml
[project]
id = "billing"
name = "Billing"
service = "billing"     # filtre `service` par défaut des règles de ce projet
# tenant = "acme"       # filtre `tenant` par défaut (logs / spans / events)

[alerting]
eval_interval = "60s"
```

## 2. Schéma des règles (`[[alerting.rules]]`)

Chaque règle est une table `[[alerting.rules]]` dans le fichier de projet :

| Champ | Requis | Description |
|---|---|---|
| `name` | ✅ | nom lisible ; identifie l'état (machine ok↔firing) et apparaît dans l'alerte |
| `kind` | ✅ | type de condition (voir tableau ci-dessous) |
| `source` | — | `logs` (défaut) \| `events` \| `spans` \| `metrics` — pour `telemetry_count` / `error_ratio` / `relative_change` |
| `service` | — | filtre `service.name` (logs/spans/metrics) ; par défaut le `service` du projet |
| `window_secs` | ✅ | fenêtre glissante (secondes) sur laquelle la valeur est calculée |
| `comparator` | ✅ | `gt` \| `gte` \| `lt` \| `lte` (compare la valeur au seuil) |
| `threshold` | ✅ | seuil numérique (compte, fraction 0..1 pour les ratios, ms pour `span_duration`, multiplicateur pour `relative_change`) |
| `cooldown_secs` | — | durée minimale entre deux notifications (défaut 0) |
| `severity` | — | sévérité de l'alerte émise (libre : `info`/`warning`/`critical`, défaut `warning`) |
| `severity_min` | — | sévérité OTLP minimale des logs (ex. `17` = ERROR) |
| `metric_name` | (metric_threshold) | nom de la métrique évaluée |
| `agg` | (metric_threshold / span_duration) | `avg`\|`max`\|`min`\|`sum`\|`count`\|`last`\|`p50`\|`p90`\|`p95`\|`p99` |
| `event_name` | — | filtre `event_name` (source `events`) |
| `operation` | — | filtre le nom de l'opération du span (spans) |
| `error_only` | — | restreint aux erreurs (spans : status=error ; logs : sévérité ≥ `severity_min`/17) |
| `min_count` | — | échantillon/baseline minimal (`error_ratio` / `relative_change` / `anomaly`) sous lequel on ne déclenche pas |
| `baseline_secs` | (log_new_signature / anomaly) | fenêtre de référence : lookback « connu » (défaut 24 h) / durée des buckets (défaut 30×`window_secs`) |
| `group_by` | (log_group_count / log_new_signature) | clé de regroupement (défaut `body`) — voir ci-dessous |
| `op` | (composite) | `all` (ET, défaut) \| `any` (OU) |
| `conditions` | (composite) | liste de sous-conditions (règles scalaires, sans `name`) |
| `actions` | — | actions à déclencher (slack/email/webhook). Vide ⇒ canaux du projet/globaux par défaut |

### Kinds (cas d'usage standard)

| `kind` | Calcule | Cas d'usage typique |
|---|---|---|
| `log_count` | compte de logs (service, `severity_min`) sur la fenêtre | « > 10 logs ERROR billing en 5 min » |
| `log_group_count` | compte de logs **groupé par signature** (`group_by`) — un état par groupe | « 5 erreurs **identiques** → webhook » |
| `metric_threshold` | agrégat d'une métrique (`avg`/`max`/`p95`/`p99`/…) | « **p95** `http.server.duration` > 800 ms » |
| `telemetry_count` | compte de lignes sur une `source` | **heartbeat/no-data** (`lte` 0), chute de trafic (`lt`), pic de volume (`gt`) |
| `error_ratio` | fraction d'erreurs sur `logs` ou `spans` (garde-fou `min_count`) | « **taux d'erreur** > 5 % sur ≥ 50 requêtes » |
| `span_duration` | agrégat de la latence des spans (`duration_ms`, ms) | « **p99** de l'opération `checkout` > 2 s » |
| `relative_change` | ratio volume(fenêtre courante)/volume(fenêtre précédente) | « erreurs **× 3** vs la période précédente » |
| `composite` | combine des sous-conditions par `op` (`all`=ET, `any`=OU) | « taux d'erreur élevé **ET** latence p95 dégradée » |
| `log_new_signature` | signature de log absente de la fenêtre `baseline` | « **nouvelle erreur** jamais vue en 24 h » |
| `anomaly` | z-score du volume vs baseline glissante (μ ± σ) | « volume **anormal** (+3σ) vs l'historique » |

Détails utiles :

- **`log_group_count`** — `group_by` est en **liste blanche** (anti-injection) : `body`,
  `service_name`, `severity_text`, `trace_id`, ou `attr:<clé>` (attribut de log, ex.
  `attr:error.code`). Une alerte par signature, portant sa `group_key`.
- **`telemetry_count`** — couvre trois besoins via le comparateur : `lte 0` = *dead man's switch*
  (aucune donnée reçue = service muet/down), `lt N` = chute de trafic, `gt N` = pic. La `source`
  choisit la table (logs/events/spans/metrics).
- **`error_ratio`** — valeur ∈ [0, 1]. Numérateur = lignes en erreur (logs : sévérité ≥
  `severity_min`/17 ; spans : status=error), dénominateur = total. Si le total < `max(min_count, 1)`,
  la valeur est 0 (on n'alerte pas sur 1 erreur / 1 requête).
- **`span_duration`** — `agg` par défaut `p95`. Filtrable par `service`, `operation`, `error_only`.
- **`relative_change`** — la fenêtre précédente est `[now-2w, now-w]`. La base est plafonnée par
  `max(min_count, 1)` pour éviter les faux pics quand il n'y a pas d'historique.
- **percentiles** (`p50`/`p90`/`p95`/`p99`) — disponibles pour `metric_threshold` **et**
  `span_duration` (via `percentile_cont`).
- **`composite`** — chaque sous-condition est une règle scalaire (`error_ratio`, `span_duration`,
  `telemetry_count`, `metric_threshold`, `relative_change`, `anomaly`, `log_count`) avec sa propre
  fenêtre/seuil. `op=all` (ET) déclenche quand **toutes** sont franchies, `op=any` (OU) dès qu'**une**
  l'est. Les kinds groupés (`log_group_count`, `log_new_signature`) et les composites imbriqués sont
  interdits comme sous-condition. La valeur de l'alerte = nombre de sous-conditions franchies.
- **`log_new_signature`** — détecte la **première apparition** d'une signature (`group_by`) :
  présente sur la fenêtre courante mais **absente** de `[now-baseline_secs, now-window]`. Un état par
  signature (comme `log_group_count`) ; `threshold`/`comparator` fixent un minimum d'occurrences
  récentes (typiquement `gte 1`). Quand la signature vieillit dans la baseline, l'alerte se résout.
- **`anomaly`** — découpe `[now-baseline_secs, now-window]` en buckets de `window_secs` (zéros
  inclus), calcule moyenne μ et écart-type σ du volume, puis le **z-score** du volume courant
  `(courant-μ)/σ`. `comparator gt 3` = pic à +3σ ; `lt -3` = chute. Renvoie 0 (pas d'alerte) si
  l'historique a < 3 buckets, si μ < `min_count`, ou si σ ≈ 0 (variance nulle = indécidable).

### Exemple (fichier de projet)

```toml
[project]
id = "api"
name = "API"
service = "api"           # filtre service par défaut des règles ci-dessous

[alerting]
eval_interval = "60s"

# Canal du projet (sinon repli sur le [notifications] global).
[notifications.slack]
webhook_url = "${API_SLACK_WEBHOOK_URL}"

# 5 erreurs identiques (groupées par message) -> webhook + Slack.
[[alerting.rules]]
name = "erreurs identiques répétées"
kind = "log_group_count"
severity_min = 17
group_by = "body"
window_secs = 300
comparator = "gte"
threshold = 5
cooldown_secs = 300
severity = "critical"

[[alerting.rules.actions]]
type = "webhook"
url = "https://hooks.internal/alert"
headers = { Authorization = "Bearer ${INTERNAL_HOOK_TOKEN}" }

[[alerting.rules.actions]]
type = "slack"

# Taux d'erreur > 5 % sur ≥ 50 requêtes (spans).
[[alerting.rules]]
name = "taux d'erreur api"
kind = "error_ratio"
source = "spans"
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05
cooldown_secs = 300
severity = "critical"

# Latence p95 de l'opération checkout.
[[alerting.rules]]
name = "latence p95 checkout"
kind = "span_duration"
agg = "p95"
operation = "checkout"
window_secs = 300
comparator = "gt"
threshold = 2000
cooldown_secs = 600
severity = "warning"

# Heartbeat d'ingestion (dead man's switch).
[[alerting.rules]]
name = "heartbeat ingestion"
kind = "telemetry_count"
source = "metrics"
window_secs = 300
comparator = "lte"
threshold = 0
cooldown_secs = 600
severity = "critical"

# Pic d'erreurs vs la période précédente.
[[alerting.rules]]
name = "pic d'erreurs"
kind = "relative_change"
source = "logs"
severity_min = 17
min_count = 20
window_secs = 300
comparator = "gt"
threshold = 3
cooldown_secs = 600
severity = "warning"

# Latence p99 (métrique).
[[alerting.rules]]
name = "latence p99 (métrique)"
kind = "metric_threshold"
metric_name = "http.server.duration"
agg = "p99"
window_secs = 300
comparator = "gt"
threshold = 900
cooldown_secs = 600

# Incident api (taux d'erreur ET latence).
[[alerting.rules]]
name = "incident api (taux d'erreur ET latence)"
kind = "composite"
op = "all"
severity = "critical"
cooldown_secs = 600

[[alerting.rules.conditions]]
kind = "error_ratio"
source = "spans"
min_count = 50
window_secs = 300
comparator = "gt"
threshold = 0.05

[[alerting.rules.conditions]]
kind = "span_duration"
agg = "p95"
window_secs = 300
comparator = "gt"
threshold = 2000

# Nouvelle erreur (jamais vue en 24 h).
[[alerting.rules]]
name = "nouvelle erreur (jamais vue en 24h)"
kind = "log_new_signature"
severity_min = 17
group_by = "body"
baseline_secs = 86400
window_secs = 600
comparator = "gte"
threshold = 1
cooldown_secs = 0

# Volume de logs anormal.
[[alerting.rules]]
name = "volume de logs anormal"
kind = "anomaly"
source = "logs"
severity_min = 17
baseline_secs = 18000
min_count = 5
window_secs = 300
comparator = "gt"
threshold = 3
cooldown_secs = 600
```

Les règles ci-dessus héritent de `service = "api"` du projet ; aucune n'a besoin de le répéter.

## 3. Évaluation, machine à états et cooldown

Une tâche de fond évalue toutes les règles d'un projet toutes les `[alerting].eval_interval` (défaut
`60s`). Pour chaque règle, le moteur maintient un **état** (`ok` ↔ `firing`) :

- **ok → firing** (le seuil vient d'être franchi) : une alerte `[FIRING]` est notifiée ;
- **firing → ok** (la condition n'est plus remplie) : une alerte `[RESOLVED]` est notifiée ;
- pas de transition : rien n'est envoyé (pas de spam à chaque évaluation).

Le **cooldown** (`cooldown_secs`) borne la fréquence : après une notification, aucune nouvelle
notification n'est émise pour cette clé d'état avant l'expiration du cooldown — y compris une
résolution. Ainsi, deux évaluations rapprochées d'une même règle en `firing` ne notifient
**qu'une** fois.

Pour `log_group_count`, l'état (et le cooldown) est maintenu **par groupe** (clé interne
`<règle>::<group_key>`) : chaque signature a sa propre machine ok↔firing. Un groupe précédemment
en alerte mais disparu de la fenêtre est résolu (compte retombé à 0).

La fonction `evaluate_once(&pool, &rules, &mut state, &dispatcher, now)` est exposée et **testable**
(l'horloge `now` est injectée), ce qui rend la logique de seuil et de cooldown déterministe. Le
`Dispatcher` résout, pour chaque règle, les notifiers à déclencher (ses `actions`, sinon les canaux
par défaut du projet).

## 4. Actions (modulables, par règle)

Chaque transition déclenche un ensemble de **notifiers**. Le contenu textuel est un message
mono-ligne `[FIRING] <name> (<severity>) [<group_key>] — <condition> (valeur=…, seuil=…)` ; les
webhooks reçoivent en plus une charge utile JSON structurée :

```json
{ "rule": "...", "severity": "...", "state": "FIRING|RESOLVED", "value": 5.0,
  "threshold": 5.0, "description": "...", "group_key": "...", "summary": "..." }
```

Le champ `actions` d'une règle déclare **explicitement** ses cibles. Si `actions` est vide, la
règle retombe sur les **canaux par défaut** (Slack et/ou e-mail configurés pour le projet, sinon le
`[notifications]` global). Trois types d'actions :

| Type | Champs | Comportement |
|---|---|---|
| `slack` | `webhook_url` (optionnel) | POST `{ "text": … }` sur le webhook ; à défaut le webhook Slack du projet/global |
| `email` | `to` (optionnel) | e-mail SMTP ; `to` surcharge les destinataires par défaut. Réutilise la config SMTP du projet/globale |
| `webhook` | `url` (requis), `headers` (optionnel) | POST de la charge utile JSON ci-dessus, avec en-têtes arbitraires (ex. `Authorization`) |

Une même règle peut combiner plusieurs actions (ex. webhook interne **et** Slack). Une action mal
configurée (ex. `email` sans config SMTP, `slack` sans URL) est journalisée puis ignorée, sans
faire échouer les autres.

### Canaux de notification (`[notifications.*]`)

Les canaux sont résolus par projet : un projet utilise ses propres `[notifications.*]` s'ils sont
présents, sinon le `[notifications]` global de `datacat.toml`. Ils sont utilisés par les règles sans
`actions`, et servent de repli/config de base aux actions. Les secrets sont passés par référence
`${ENV}`.

```toml
# Dans un fichier de projet (projects/api.toml) — ou globalement dans datacat.toml.
[notifications.slack]
webhook_url = "${SLACK_WEBHOOK_URL}"          # POST { "text": … }

[notifications.email]
smtp_host = "smtp.example.com"
smtp_port = 587                                # STARTTLS
username = "${SMTP_USERNAME:-}"
password = "${SMTP_PASSWORD:-}"
from = "Datacat <alerts@example.com>"
to = ["ops@example.com"]
```

Le transport SMTP utilise **STARTTLS via rustls** (pas d'OpenSSL). Le canal e-mail n'est activé que
si `smtp_host`, `from` et au moins un destinataire `to` sont fournis.

## 5. Configuration (résumé)

| Réglage | Où | Rôle |
|---|---|---|
| `[[alerting.rules]]` | `projects/*.toml` | les règles du projet (active son évaluateur) |
| `[alerting].eval_interval` | `projects/*.toml` | période d'évaluation (défaut `60s`) |
| `[project].service` / `[project].tenant` | `projects/*.toml` | filtre de règle par défaut du projet |
| `[notifications.slack]` | projet, sinon `datacat.toml` | webhook Slack |
| `[notifications.email]` | projet, sinon `datacat.toml` | relais SMTP + expéditeur/destinataires |
| `[projects].dir` / `[projects].files` | `datacat.toml` | quels fichiers de projet charger |

Tous les réglages sont optionnels ; le moteur se désactive proprement pour un projet dont la
configuration est incomplète. Voir [configuration](../configuration/) pour le modèle de
configuration complet et l'expansion des secrets.
