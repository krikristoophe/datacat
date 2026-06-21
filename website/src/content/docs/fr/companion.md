---
title: "Liveness companion"
description: "Un dead man's switch multi-nœuds : l'instance principale et les companions distants se surveillent mutuellement."
---

Un **companion** est un agent léger déployé sur un nœud *distinct* (un autre hôte, région ou cloud).
Lui et l'instance **principale** de Datacat surveillent mutuellement leur liveness, de sorte qu'une
partition réseau ou un nœud à terre est transformé en alerte. N'importe quel nombre de companions est
supporté, chacun identifié par un `id` stable.

Le mécanisme est **bidirectionnel** à dessein — une principale à terre ne peut pas alerter sur
elle-même :

- **Companion → principale :** le companion POST un heartbeat sur `POST /v1/heartbeat` à chaque
  `interval`. L'instance principale enregistre l'instant de dernière vue par `id` ; un moniteur en
  arrière-plan lève une alerte quand un companion reste silencieux plus longtemps que son `timeout`,
  et la résout quand le companion revient.
- **Principale → companion :** si le companion ne parvient pas à joindre l'instance principale pendant
  `failure_threshold` tentatives consécutives, il lève **sa propre** alerte (via son propre canal
  Slack/webhook), et une alerte de rétablissement quand la connectivité revient.

## 1. Endpoint de heartbeat

| Méthode | Endpoint | Auth | Corps |
|---|---|---|---|
| `POST` | `/v1/heartbeat` | service-à-service (`[auth.logs]`) | `{ "id": "<companion_id>" }` |

Renvoie `204 No Content`. L'id du companion est enregistré dans un registre en mémoire (état souple :
après un redémarrage de la principale, les companions se re-signalent dans la fenêtre d'un `timeout`).
Le registre courant est visible dans la réponse authentifiée de `/stats` sous `companions`.

## 2. Configuration côté principale (`datacat.toml`)

```toml
[companions]
check_interval = "30s"          # fréquence à laquelle le moniteur vérifie la liveness

[[companions.expected]]
id = "edge-eu"
timeout = "90s"                 # un silence plus long que cela ⇒ alerte
severity = "critical"

[[companions.expected]]
id = "edge-us"
timeout = "90s"
```

Les alertes de companion à terre sont envoyées via les canaux **globaux** `[notifications]` (Slack /
e-mail). Si aucun companion attendu n'est configuré, ou si aucun canal global n'existe, le moniteur
reste désactivé.

## 3. L'agent companion (`datacat-companion`)

La crate autonome `companion/` tourne sur le nœud distant. Sa configuration (`companion.toml`, modèle
`companion.example.toml`) définit l'URL de la principale, son `id`, le token de heartbeat (depuis
l'environnement), l'`interval` d'envoi, le `failure_threshold`, et son propre canal d'auto-alerte
(bot Slack ou webhook générique) utilisé quand il ne peut plus joindre la principale. Les secrets sont
référencés depuis l'environnement via `${VAR}`, comme la configuration principale.

```toml
main_url = "https://datacat.example.com"
id = "edge-eu"
token = "${DATACAT_HEARTBEAT_TOKEN}"
interval = "30s"
failure_threshold = 3

[alert.slack]                   # ou [alert.webhook] url = "..."
bot_token = "${SLACK_BOT_TOKEN}"
channel = "#alerts"
```

## 4. Pourquoi bidirectionnel

Si une région hébergeant un companion s'éteint, le moniteur de la **principale** se déclenche (plus de
heartbeats). Si l'instance **principale** (ou le réseau vers elle) s'éteint, chaque companion se
déclenche localement. L'un ou l'autre des modes de défaillance fait remonter une alerte depuis le côté
qui est toujours debout — il n'y a pas de point unique dont la mort fait taire l'alarme.
