---
title: "Tutoriel : alerter sur Slack"
description: "Définir une règle d'alerting de projet qui surveille vos logs et poste sur Slack via l'API Bot quand un taux d'erreur dépasse un seuil."
---

Datacat évalue des **règles d'alerting par projet** à intervalle fixe et route les alertes
déclenchées vers des canaux — Slack, e-mail ou webhooks. Ce tutoriel branche une règle de taux
d'erreur sur le service `checkout` vers un canal Slack.

Il s'appuie sur la télémétrie de [Instrumenter un service](../instrument-a-service/).

## 1. Créer un bot token Slack

Les notifications Slack utilisent l'**API Web** (`chat.postMessage`), pas les anciens incoming
webhooks. Dans votre workspace, créez une app, donnez-lui le scope `chat:write`, installez-la et
copiez le **bot token** (`xoxb-…`). Invitez le bot dans le canal cible (ex. `#alerts`).

## 2. Référencer le token depuis l'environnement

Les secrets ne vivent jamais en clair dans le TOML — référencez-les avec `${VAR}` :

```bash
export CHECKOUT_SLACK_BOT_TOKEN=xoxb-votre-vrai-token
```

## 3. Définir le projet et la règle

Créez `projects/checkout.toml`. Le bloc `[project]` applique par défaut `service = "checkout"` à
chaque règle ; `[notifications.slack]` est le canal ; la règle se déclenche quand plus de 5 % d'au
moins 50 logs sur une fenêtre de 5 minutes sont des erreurs (`severity_min = 17` = ERROR OTLP).

```toml
[project]
id = "checkout"
name = "Checkout"
service = "checkout"          # filtre service par défaut des règles de ce projet

[alerting]
eval_interval = "60s"

[notifications.slack]
bot_token = "${CHECKOUT_SLACK_BOT_TOKEN}"
channel = "#alerts"

[[alerting.rules]]
name = "High error rate"
kind = "error_ratio"
source = "logs"
severity_min = 17            # ERROR OTLP et au-dessus
min_count = 50               # ignore le bruit à faible trafic
window_secs = 300
comparator = "gt"
threshold = 0.05             # 5 %
cooldown_secs = 300          # pas de re-déclenchement pendant 5 min
severity = "critical"
```

Pointez `datacat.toml` vers le répertoire des projets (il charge chaque `projects/*.toml`) :

```toml
[projects]
dir = "projects"
```

## 4. Redémarrer et observer le déclenchement

Redémarrez le backend pour qu'il prenne le projet. Datacat lance un évaluateur d'alerting pour
`checkout`. Générez des erreurs — rejouez le log d'erreur du tutoriel précédent en boucle jusqu'à
dépasser 50 enregistrements avec >5 % d'erreurs dans la fenêtre :

```bash
for i in $(seq 1 60); do
  curl -s -X POST http://localhost:8080/v1/logs \
    -H 'Authorization: Bearer dev-logs-token' -H 'Content-Type: application/json' \
    -d '{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}}]},"scopeLogs":[{"logRecords":[{"severityText":"ERROR","severityNumber":17,"body":{"stringValue":"payment gateway timeout"}}]}]}]}' \
    > /dev/null
done
```

En une fenêtre d'évaluation, un message arrive dans `#alerts`. Quand le taux repasse sous le seuil,
Datacat poste un message de **résolution**.

## Aller plus loin

- Autres types de règles : percentiles de latence (`metric_threshold` + `agg = "p95"`), heartbeats
  (`telemetry_count`), pics (`relative_change`), erreurs inédites (`log_new_signature`), anomalies
  statistiques (`anomaly`), et `composite` (ET/OU) — voir [Alerting](../../alerting/).
- Routez la même règle vers un e-mail ou un webhook en ajoutant des blocs
  `[[alerting.rules.actions]]`.
- Gardez les nœuds principal et distants synchronisés avec le [Companion](../../companion/).
