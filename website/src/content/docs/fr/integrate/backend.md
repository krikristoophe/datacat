---
title: Intégrer un service backend
description: Envoyer events produit et logs/traces/métriques OpenTelemetry à Datacat depuis n'importe quel langage serveur, en HTTP.
---

Depuis un backend, vous envoyez en général deux choses à Datacat :

1. **Events produit** — sur `POST /v1/events`, authentifiés par un **JWT** court que votre service
   signe pour l'utilisateur courant.
2. **Télémétrie** (logs, traces, métriques) — sur les endpoints OTLP, authentifiés par un **token de
   service statique**.

Aucun SDK requis : c'est du HTTP/JSON simple, donc n'importe quel langage fonctionne.

## Envoyer des events produit

Les events partent en **lot**. Chacun porte son `event_id` — réutilisez le même sur un retry et il
n'est compté qu'une fois (`ON CONFLICT DO NOTHING`).

```ts
// Node — n'importe quel client HTTP fonctionne pareil.
await fetch("https://ingest.example.com/v1/events", {
  method: "POST",
  headers: {
    "Content-Type": "application/json",
    Authorization: `Bearer ${await mintAnalyticsToken(user)}`,
  },
  body: JSON.stringify({
    events: [
      {
        event_id: crypto.randomUUID(),
        event_name: "invoice_paid",
        tenant_id: "clinic-7",
        actor_id: user.id,
        session_id: sessionId,
        timestamp_client: new Date().toISOString(),
        properties: { amount_eur: 42 },
      },
    ],
  }),
});
// → 202 Accepted { "received": 1 }
```

Le JWT est signé par votre backend et vérifié par Datacat **avec la clé publique seule**. Voir
[token](../../token/) pour les claims et algorithmes attendus.

## Envoyer la télémétrie (logs, traces, métriques)

La télémétrie utilise les endpoints OTLP standards (`/v1/logs`, `/v1/traces`, `/v1/metrics`) et le
token de service statique de `[auth.logs]`. Le plus simple est de pointer votre SDK OpenTelemetry
existant vers Datacat — voir [Intégrer OpenTelemetry](../opentelemetry/). À la main :

```bash
curl -X POST https://ingest.example.com/v1/logs \
  -H "Authorization: Bearer $DATACAT_SERVICE_TOKEN" \
  -H "Content-Type: application/json" \
  -d @log.otlp.json
```

Remontez `tenant_id`, `actor_id` et `session_id` dans les attributs (resource ou record) pour que
votre télémétrie se corrèle aux events produit.

## Quel token

| Surface | Endpoint | Auth |
|---|---|---|
| Events produit | `/v1/events` | **JWT** court par utilisateur (asymétrique) |
| Logs / traces / métriques | `/v1/logs`, `/v1/traces`, `/v1/metrics` | **token de service** statique |

## Étapes suivantes

- [Intégrer OpenTelemetry](../opentelemetry/) — réutilisez votre instrumentation existante.
- [Contrat d'event](../../contract/) — le wire format exact et les limites.
- [Tutoriel : instrumenter un service](../../tutorials/instrument-a-service/).
