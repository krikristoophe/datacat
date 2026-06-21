---
title: "SDKs"
description: "Envoyer des events produit depuis le web (TypeScript) et le mobile (Flutter/Dart)."
---

Datacat fournit deux SDKs clients qui émettent des **events** produit conformes au même wire format :
un SDK web en TypeScript et un SDK mobile en Dart (compatible Flutter). Les deux figent `event_id` et
`timestamp_client` au moment de l'appel, batchent automatiquement, réessaient de façon idempotente,
et renouvellent le token d'ingestion tout seuls.

## Le token, pas un secret

Aucun des SDKs ne contient de secret. À l'exécution, chacun appelle un callback `getToken` que vous
fournissez, lequel récupère un **JWT éphémère** depuis votre backend déjà authentifié ; le SDK
l'attache en `Authorization: Bearer`. Datacat vérifie la signature avec la **clé publique seule**,
de sorte que l'endpoint d'ingestion exposé ne peut jamais forger un token. Le JWT porte `actor_id` et
`session_id` (requis) plus un `tenant_id` optionnel.

Les SDKs mettent le token en cache, le rafraîchissent ~30 s avant expiration (en décodant `exp`), et
le rafraîchissent à nouveau sur un `401`. Lisez la spécification d'émission du [token](../token/) et
le [contrat](../contract/) pour les détails de claims et de wire format qui font foi.

## TypeScript (web)

Paquet : `@datacat/sdk-web` (sources sous `sdks/typescript/`). Aucune dépendance runtime ; cible les
navigateurs modernes et Node 24+.

```bash
npm install @datacat/sdk-web
```

Init minimal et un appel `track()` :

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const analytics = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  // getToken DOIT récupérer un JWT depuis VOTRE backend — ne jamais embarquer de token dans le code.
  getToken: () =>
    fetch("/api/analytics-token", { credentials: "include" })
      .then((r) => r.json())
      .then((d) => d.token),
  actorId: "user-123",     // ou plus tard via identify()
  tenantId: "clinic-42",   // optionnel (multi-tenant B2B)
});

// Une fois l'utilisateur authentifié, (re)définissez l'identité :
analytics.identify({ actorId: "user-123", tenantId: "clinic-42" });

// Émettre un event métier (mis en file et envoyé par batches) :
analytics.track("validate_planning", { planning_id: 42, count: 3 });

// Forcer un envoi si besoin ; flush + teardown à la fermeture de l'app :
await analytics.flush();
await analytics.shutdown();
```

Le flush de fin de session est géré pour vous via `visibilitychange` / `pagehide` / `beforeunload`,
en préférant `fetch(..., { keepalive: true })` avec repli sur `navigator.sendBeacon`. Un hook
`redact` permet de retirer les champs sensibles des `properties` avant qu'ils ne quittent le client.
Voir `sdks/typescript/README.md` pour la table complète des options et un exemple de provider React.

## Flutter / Dart (mobile)

Paquet : `datacat_sdk` (sources sous `sdks/flutter/`). Cœur pur-Dart avec intégration optionnelle du
cycle de vie Flutter — `dart pub get` et `dart test` fonctionnent sans le SDK Flutter.

```yaml
# pubspec.yaml
dependencies:
  datacat_sdk: ^0.1.0
```

Init minimal et un appel `track()` :

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    // Ne JAMAIS coder le token en dur. Récupérez-le depuis votre backend à l'exécution.
    getToken: () => myBackend.getAnalyticsToken(),
    actorId: currentUser?.id,        // optionnel — ou appelez identify() après login
    tenantId: currentUser?.tenantId, // optionnel
  ),
);

// Après authentification (les apps B2B se connectent souvent après la création du SDK) :
analytics.identify(actorId: user.id, tenantId: user.tenantId);

// Tracker un event avec des propriétés :
analytics.track('button_tapped', { 'button_id': 'validate_planning', 'planning_id': 42 });
```

Les events trackés **sans** acteur (ni `actorId` en config ni appel à `identify()`) sont rejetés et
le callback `onError` est invoqué — `actor_id` est requis par le contrat. Dans une app Flutter,
flushez quand l'app passe en arrière-plan pour que l'OS ne tue pas le processus en plein batch :

```dart
@override
void didChangeAppLifecycleState(AppLifecycleState state) {
  if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
    analytics.flush();
  }
}
```

Le stockage de session en mémoire par défaut ne survit pas aux redémarrages ; fournissez un
`DatacatStorage` adossé à `shared_preferences` pour persister le `session_id`. Voir
`sdks/flutter/README.md` pour l'adaptateur de stockage et la référence de configuration.

## Contrat commun

Les deux SDKs produisent des events conformes au même [contrat](../contract/) : un batch
`{ "events": [ ... ] }` vers `POST /v1/events`, `event_id` / `timestamp_client` figés, réessai
idempotent (même `event_id` au renvoi), et la gestion du [token](../token/) décrite ci-dessus. Les
`properties` sont libres mais ne doivent **jamais** contenir de données sensibles (mots de passe,
PII, secrets) — les deux SDKs exposent un hook de redaction pour cela.
