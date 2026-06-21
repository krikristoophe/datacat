# Moteur d'alerting (Slack + e-mail)

Datacat embarque un moteur d'alerting léger : des **règles déclaratives** (fichier JSON) sont
évaluées périodiquement sur les données ingérées (`logs`, `metric_points`), et chaque
franchissement de seuil déclenche une **notification** Slack et/ou e-mail. Le moteur est
**entièrement optionnel** : sans fichier de règles, ou sans aucun canal de notification
configuré, il reste désactivé (no-op au démarrage).

## 1. Activation

Le moteur démarre uniquement si **les deux** conditions sont réunies :
1. `ALERT_RULES_FILE` pointe vers un fichier de règles non vide ;
2. au moins un canal de notification est configuré (Slack **ou** e-mail complet).

Sinon, un message d'info/warning est journalisé et l'évaluateur n'est pas lancé.

## 2. Schéma des règles (`ALERT_RULES_FILE`)

Fichier JSON `{ "rules": [ … ] }`. Chaque règle :

| Champ | Requis | Description |
|---|---|---|
| `name` | ✅ | nom lisible ; identifie l'état (machine ok↔firing) et apparaît dans l'alerte |
| `kind` | ✅ | `log_count` ou `metric_threshold` |
| `service` | — | filtre `service.name` (toutes sources si absent) |
| `window_secs` | ✅ | fenêtre glissante (secondes) sur laquelle la valeur est calculée |
| `comparator` | ✅ | `gt` \| `gte` \| `lt` \| `lte` (compare la valeur au seuil) |
| `threshold` | ✅ | seuil numérique |
| `cooldown_secs` | — | durée minimale entre deux notifications de cette règle (défaut 0) |
| `severity` | — | sévérité de l'alerte émise (libre : `info`/`warning`/`critical`, défaut `warning`) |
| `severity_min` | (log_count) | sévérité OTLP minimale des logs comptés (ex. `17` = ERROR) |
| `metric_name` | (metric_threshold) | nom de la métrique évaluée |
| `agg` | (metric_threshold) | `avg` \| `max` \| `last` sur la fenêtre |

### Kinds

- **`log_count`** : compte les logs sur la fenêtre, filtrés par `service` et `severity_min`, puis
  compare ce compte au seuil. Ex. « plus de 10 logs ERROR du service billing en 5 min ».
- **`metric_threshold`** : agrège (`avg` / `max` / `last`) les points d'un `metric_name` (valeur
  `value_double`, à défaut `value_int`) sur la fenêtre, filtrés par `service`, puis compare au
  seuil. Ex. « latence moyenne `http.server.duration` > 500 ms sur 5 min ».

### Exemple

```json
{
  "rules": [
    {
      "name": "erreurs billing", "kind": "log_count", "service": "billing",
      "severity_min": 17, "window_secs": 300, "comparator": "gt", "threshold": 10,
      "cooldown_secs": 600, "severity": "critical"
    },
    {
      "name": "latence api", "kind": "metric_threshold", "metric_name": "http.server.duration",
      "service": "api", "agg": "avg", "window_secs": 300, "comparator": "gt",
      "threshold": 500, "cooldown_secs": 600, "severity": "warning"
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
notification n'est émise pour cette règle avant l'expiration du cooldown — y compris une
résolution. Ainsi, deux évaluations rapprochées d'une même règle en `firing` ne notifient
**qu'une** fois.

La fonction `evaluate_once(&pool, &rules, &mut state, &notifiers, now)` est exposée et **testable**
(l'horloge `now` est injectée), ce qui rend la logique de seuil et de cooldown déterministe.

## 4. Notifications

Le moteur diffuse chaque transition sur **tous** les canaux configurés (`Vec<Arc<dyn Notifier>>`).
Le contenu est un message mono-ligne :
`[FIRING] <name> (<severity>) — <condition> (valeur=…, seuil=…)`.

### Slack

`SLACK_WEBHOOK_URL` : URL d'un webhook entrant Slack. Le moteur POST un JSON `{ "text": … }`.

### E-mail (SMTP)

| Variable | Rôle |
|---|---|
| `SMTP_HOST` | hôte du relais SMTP |
| `SMTP_PORT` | port (défaut `587`, STARTTLS) |
| `SMTP_USERNAME` / `SMTP_PASSWORD` | identifiants (optionnels) |
| `ALERT_EMAIL_FROM` | expéditeur (ex. `Datacat <alerts@example.com>`) |
| `ALERT_EMAIL_TO` | destinataires, séparés par des virgules |

Le transport SMTP utilise **STARTTLS via rustls** (pas d'OpenSSL). Le canal e-mail n'est activé que
si `SMTP_HOST`, `ALERT_EMAIL_FROM` et au moins un `ALERT_EMAIL_TO` sont fournis.

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
