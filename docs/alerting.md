# Moteur d'alerting (actions modulables : Slack, e-mail, webhook)

Datacat embarque un moteur d'alerting léger : des **règles déclaratives** (fichier JSON) sont
évaluées périodiquement sur les données ingérées (`logs`, `metric_points`), et chaque
franchissement de seuil déclenche une ou plusieurs **actions** modulables (Slack, e-mail,
webhook HTTP générique). Le moteur est **entièrement optionnel** : sans fichier de règles, ou
sans aucune action ni canal configuré, il reste désactivé (no-op au démarrage).

## 1. Activation

Le moteur démarre si `ALERT_RULES_FILE` pointe vers un fichier de règles non vide **et** qu'il
existe au moins une cible de notification, c'est-à-dire :
- un canal **global** configuré (Slack via `SLACK_WEBHOOK_URL`, **ou** e-mail complet), **ou**
- au moins une règle portant ses propres `actions` (un webhook suffit, sans aucune config globale).

Sinon, un message d'info/warning est journalisé et l'évaluateur n'est pas lancé.

## 2. Schéma des règles (`ALERT_RULES_FILE`)

Fichier JSON `{ "rules": [ … ] }`. Chaque règle :

| Champ | Requis | Description |
|---|---|---|
| `name` | ✅ | nom lisible ; identifie l'état (machine ok↔firing) et apparaît dans l'alerte |
| `kind` | ✅ | type de condition (voir tableau ci-dessous) |
| `source` | — | `logs` (défaut) \| `events` \| `spans` \| `metrics` — pour `telemetry_count` / `error_ratio` / `relative_change` |
| `service` | — | filtre `service.name` (logs/spans/metrics) |
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
| `min_count` | — | échantillon minimal (`error_ratio` / `relative_change`) sous lequel on ne déclenche pas |
| `group_by` | (log_group_count) | clé de regroupement (défaut `body`) — voir ci-dessous |
| `actions` | — | actions à déclencher (slack/email/webhook). Vide ⇒ canaux globaux par défaut |

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

### Exemple

```json
{
  "rules": [
    {
      "name": "erreurs identiques répétées", "kind": "log_group_count",
      "severity_min": 17, "group_by": "body", "window_secs": 300,
      "comparator": "gte", "threshold": 5, "cooldown_secs": 300, "severity": "critical",
      "actions": [
        { "type": "webhook", "url": "https://hooks.internal/alert",
          "headers": { "Authorization": "Bearer s3cr3t" } },
        { "type": "slack" }
      ]
    },
    {
      "name": "taux d'erreur api", "kind": "error_ratio", "source": "spans", "service": "api",
      "min_count": 50, "window_secs": 300, "comparator": "gt", "threshold": 0.05,
      "cooldown_secs": 300, "severity": "critical"
    },
    {
      "name": "latence p95 checkout", "kind": "span_duration", "agg": "p95",
      "service": "api", "operation": "checkout", "window_secs": 300,
      "comparator": "gt", "threshold": 2000, "cooldown_secs": 600, "severity": "warning"
    },
    {
      "name": "heartbeat ingestion", "kind": "telemetry_count", "source": "metrics",
      "service": "api", "window_secs": 300, "comparator": "lte", "threshold": 0,
      "cooldown_secs": 600, "severity": "critical"
    },
    {
      "name": "pic d'erreurs", "kind": "relative_change", "source": "logs",
      "severity_min": 17, "min_count": 20, "window_secs": 300,
      "comparator": "gt", "threshold": 3, "cooldown_secs": 600, "severity": "warning"
    },
    {
      "name": "latence p99 (métrique)", "kind": "metric_threshold",
      "metric_name": "http.server.duration", "service": "api", "agg": "p99",
      "window_secs": 300, "comparator": "gt", "threshold": 900, "cooldown_secs": 600
    }
  ]
}
```

## 3. Évaluation, machine à états et cooldown

Une tâche de fond évalue toutes les règles toutes les `ALERT_EVAL_INTERVAL` (défaut `60s`). Pour
chaque règle, le moteur maintient un **état** (`ok` ↔ `firing`) :

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
`Dispatcher` résout, pour chaque règle, les notifiers à déclencher (ses `actions`, sinon les
canaux globaux par défaut).

## 4. Actions (modulables, par règle)

Chaque transition déclenche un ensemble de **notifiers**. Le contenu textuel est un message
mono-ligne `[FIRING] <name> (<severity>) [<group_key>] — <condition> (valeur=…, seuil=…)` ; les
webhooks reçoivent en plus une charge utile JSON structurée :

```json
{ "rule": "...", "severity": "...", "state": "FIRING|RESOLVED", "value": 5.0,
  "threshold": 5.0, "description": "...", "group_key": "...", "summary": "..." }
```

Le champ `actions` d'une règle déclare **explicitement** ses cibles. Si `actions` est vide, la
règle retombe sur les **canaux globaux par défaut** (Slack et/ou e-mail configurés par
l'environnement). Trois types d'actions :

| Type | Champs | Comportement |
|---|---|---|
| `slack` | `webhook_url` (optionnel) | POST `{ "text": … }` sur le webhook ; à défaut `SLACK_WEBHOOK_URL` global |
| `email` | `to` (optionnel) | e-mail SMTP ; `to` surcharge les destinataires globaux. Réutilise la config SMTP globale |
| `webhook` | `url` (requis), `headers` (optionnel) | POST de la charge utile JSON ci-dessus, avec en-têtes arbitraires (ex. `Authorization`) |

Une même règle peut combiner plusieurs actions (ex. webhook interne **et** Slack). Une action mal
configurée (ex. `email` sans SMTP global, `slack` sans URL) est journalisée puis ignorée, sans
faire échouer les autres.

### Canaux globaux (par défaut)

Utilisés par les règles sans `actions`, et servant de repli/config de base aux actions :

| Variable | Rôle |
|---|---|
| `SLACK_WEBHOOK_URL` | webhook entrant Slack (POST `{ "text": … }`) |
| `SMTP_HOST` | hôte du relais SMTP |
| `SMTP_PORT` | port (défaut `587`, STARTTLS) |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | identifiants (optionnels) |
| `ALERT_EMAIL_FROM` | expéditeur (ex. `Datacat <alerts@example.com>`) |
| `ALERT_EMAIL_TO` | destinataires, séparés par des virgules |

Le transport SMTP utilise **STARTTLS via rustls** (pas d'OpenSSL). Le canal e-mail global n'est
activé que si `SMTP_HOST`, `ALERT_EMAIL_FROM` et au moins un `ALERT_EMAIL_TO` sont fournis.

## 5. Configuration (résumé)

| Variable | Défaut | Rôle |
|---|---|---|
| `ALERT_RULES_FILE` | — | chemin du fichier JSON des règles (active le moteur) |
| `ALERT_EVAL_INTERVAL` | `60s` | période d'évaluation |
| `SLACK_WEBHOOK_URL` | — | webhook Slack |
| `SMTP_HOST` / `SMTP_PORT` | — / `587` | relais SMTP |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | — | auth SMTP |
| `ALERT_EMAIL_FROM` / `ALERT_EMAIL_TO` | — | expéditeur / destinataires |

Toutes les variables sont optionnelles ; le moteur se désactive proprement si la configuration est
incomplète.
