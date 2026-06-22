---
title: Intégrer une app web
description: Ajouter l'analytics produit Datacat à un front web avec le SDK TypeScript — gestion du token, identify/track et bonnes pratiques.
---

Utilisez le client **`@datacat/sdk-web`** pour envoyer des events produit depuis une app navigateur
(React, Vue, Svelte ou JS pur). Le SDK regroupe les events, réessaie sur réseau instable et envoie
au déchargement de page — vous n'appelez que `identify()` et `track()`.

## 1. Installer

```bash
npm install @datacat/sdk-web
```

## 2. Créer le client

Le SDK ne détient jamais de secret durable. Il appelle votre callback **`getToken`** pour récupérer
un JWT court depuis *votre* backend, qui le signe pour l'utilisateur connecté avec une clé que
Datacat vérifie par clé publique seule (voir [token](../../token/)).

```ts
import { createDatacatClient } from "@datacat/sdk-web";

export const datacat = createDatacatClient({
  // URL complète de l'endpoint d'ingestion, /v1/events inclus.
  endpoint: "https://ingest.example.com/v1/events",
  // Récupère un token frais depuis votre backend ; renouvelé ~30s avant expiry et sur 401.
  getToken: () => fetch("/api/analytics-token").then((r) => r.text()),
  // Optionnel : retirer les champs sensibles avant que quoi que ce soit ne quitte le navigateur.
  redact: (props) => ({ ...props, email: undefined }),
});
```

## 3. Identifier, puis tracer

`actor_id` est requis : appelez `identify()` dès que vous connaissez l'utilisateur (en général juste
après le login). Les events tracés avant `identify` — sans acteur — sont écartés et signalés via le
callback d'erreur.

```ts
// Après authentification :
datacat.identify({ actorId: user.id, tenantId: user.clinicId });

// N'importe où dans l'UI :
datacat.track("appointment_booked", { duration_ms: 412, channel: "web" });
```

Le SDK fige `event_id` et `timestamp_client`, regroupe les events et les envoie sur minuterie et au
déchargement de page (via `navigator.sendBeacon`). Les renvois d'un même `event_id` sont dédupliqués
côté serveur : les retries ne gonflent jamais vos chiffres.

## 4. Notes par framework

- **React / SPA** : créez le client une fois (portée module ou provider de contexte), appelez
  `identify()` dans votre effet d'auth, et `track()` depuis les handlers. Ne recréez pas le client à
  chaque rendu.
- **SSR / Next.js** : n'instanciez que côté navigateur (`typeof window !== "undefined"`), ou
  injectez un `StorageAdapter` no-op côté serveur.
- **Flush manuel** : `await datacat.flush()` avant une navigation dure que vous maîtrisez (ex.
  redirection pleine page après un paiement).

## Bonnes pratiques

- **Jamais de secret ni de PII dans `properties`** (mots de passe, tokens, noms, e-mails). Utilisez
  le hook `redact` pour l'imposer de façon centralisée.
- Gardez un `event_name` stable et à faible cardinalité (`appointment_booked`, pas
  `appointment_booked_42`) ; mettez les parties variables dans `properties`.
- L'endpoint de token est le vôtre : authentifiez l'utilisateur, puis renvoyez un JWT court. Voir
  [token](../../token/) pour les claims attendus.

## Étapes suivantes

- [Intégrer un backend](../backend/) pour les events côté serveur et la télémétrie.
- [Tutoriel : tracer votre premier event](../../tutorials/first-event/).
- [Référence SDKs](../../sdks/) pour toutes les options et le client Flutter.
