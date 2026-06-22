---
title: "Tutoriel : tracer votre premier event"
description: "De bout en bout — lancer Datacat en local, envoyer un event produit depuis le SDK web, et vérifier qu'il est bien stocké dans PostgreSQL."
---

À la fin de ce tutoriel vous aurez une instance Datacat qui tourne, un event envoyé depuis le SDK
web TypeScript, et une requête SQL prouvant qu'il a été stocké — en une dizaine de minutes, sur un
poste de dev.

Il vous faut Docker (pour PostgreSQL) et une toolchain Rust récente. Node est optionnel (étape SDK
uniquement).

## 1. Démarrer PostgreSQL et le backend

```bash
docker compose up -d postgres
export DATABASE_URL=postgres://datacat:datacat@localhost:55432/datacat

cd backend
cargo run --features dev          # écoute sur :8080
```

La feature `dev` permet de tourner avec la vérification du token désactivée — à ne jamais utiliser
en production. Voir le [Démarrage rapide](../../quickstart/) pour la configuration par fichier.

Vérifiez que c'est en ligne :

```bash
curl -s http://localhost:8080/readyz     # "ok" dès que la base est joignable
```

## 2. Envoyer un event avec curl

Les events sont envoyés en **lot** sur `POST /v1/events`. Chaque event porte son propre `event_id` —
le même id n'est compté qu'une fois, donc les retries sont sans danger.

```bash
curl -s -X POST http://localhost:8080/v1/events \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev' \
  -d '{
    "events": [{
      "event_id":         "550e8400-e29b-41d4-a716-446655440000",
      "event_name":       "appointment_booked",
      "tenant_id":        "clinic-7",
      "actor_id":         "user-123",
      "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f",
      "timestamp_client": "2026-06-22T10:15:30.123Z",
      "properties":       { "duration_ms": 412 }
    }]
  }'
# → 202 Accepted  { "received": 1 }
```

`received` est le nombre d'events **acceptés pour écriture asynchrone**, pas le nombre inséré.
Renvoyez exactement le même corps : vous obtenez encore `received: 1`, mais la base ne garde qu'une
seule ligne (`ON CONFLICT DO NOTHING`). C'est l'idempotence à l'œuvre.

## 3. Envoyer le même event depuis le SDK web

Dans une vraie app, vous n'écrivez pas les `event_id` à la main — le SDK s'en charge. Installez-le
et branchez un endpoint de token (en production le token est un JWT court signé par votre backend
authentifié ; ici, en dev, n'importe quelle chaîne convient puisque la vérification est coupée).

```bash
npm install @datacat/sdk-web
```

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const datacat = createDatacatClient({
  endpoint: "http://localhost:8080/v1/events",
  getToken: () => Promise.resolve("dev"),   // prod : récupérez un vrai JWT depuis votre backend
});

// actor_id est requis — identifiez une fois, puis tracez autant que voulu.
datacat.identify({ actorId: "user-123", tenantId: "clinic-7" });
datacat.track("appointment_booked", { duration_ms: 412 });

// Le SDK regroupe et envoie automatiquement ; forcez avant la sortie dans un script :
await datacat.flush();
```

Le SDK fige `event_id` et `timestamp_client` à la création, réessaie avec backoff, et bascule sur un
beacon au déchargement de page — les events survivent aux réseaux instables et aux fermetures
d'onglet.

## 4. Vérifier la persistance

```bash
docker compose exec postgres \
  psql -U datacat -d datacat -c \
  "SELECT event_name, actor_id, properties FROM events ORDER BY received_at DESC LIMIT 5;"
```

Vous devriez voir votre ligne `appointment_booked`. Une seule — même si vous l'avez envoyée depuis
curl et le SDK avec des ids différents, et même si vous avez relancé le curl.

## Étapes suivantes

- [Instrumenter un service avec OTLP](../instrument-a-service/) — logs, traces et métriques.
- [Alerter sur Slack](../alert-to-slack/) — être notifié quand quelque chose casse.
- [Contrat d'event](../../contract/) — le wire format complet et les règles de token.
