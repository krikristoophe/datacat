---
title: Intégrer une app Flutter
description: Ajouter l'analytics produit Datacat à une app mobile Flutter / Dart avec le package datacat_sdk.
---

Utilisez le package **`datacat_sdk`** (Dart pur, compatible Flutter) pour envoyer des events produit
depuis une app mobile. Comme le SDK web, il regroupe, réessaie et ne détient jamais de secret
durable — il récupère un token court depuis votre backend.

## 1. Ajouter la dépendance

```yaml
# pubspec.yaml
dependencies:
  datacat_sdk: ^0.1.0
```

## 2. Créer le client

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    // Ne codez JAMAIS le token en dur — récupérez-le depuis votre backend au runtime.
    getToken: () => myBackend.getAnalyticsToken(),
    actorId: currentUser?.id,        // optionnel ici — ou appelez identify() après login
    tenantId: currentUser?.tenantId,
  ),
);
```

## 3. Identifier et tracer

```dart
// Les apps B2B se connectent souvent après la création du client :
analytics.identify(actorId: user.id, tenantId: user.tenantId);

analytics.track('button_tapped', {
  'button_id': 'validate_planning',
  'planning_id': 42,
});
```

`actor_id` est requis par le [contrat](../../contract/) : les events sans acteur sont écartés et le
callback `onError` est invoqué.

## 4. Flush selon le cycle de vie

Envoyez quand l'app passe en arrière-plan pour que l'OS ne tue pas le process en plein lot :

```dart
@override
void didChangeAppLifecycleState(AppLifecycleState state) {
  if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
    analytics.flush();
  }
}
```

## 5. Persister la session

Le `InMemoryStorage` par défaut ne survit pas aux redémarrages. Fournissez un `DatacatStorage`
adossé à `shared_preferences` pour persister le `session_id` — voir `sdks/flutter/README.md`.

## Bonnes pratiques

- Pas de secret ni de PII dans `properties` ; les deux SDKs exposent un hook de redaction.
- `event_name` stables et à faible cardinalité ; les données variables vont dans `properties`.

## Étapes suivantes

- [Intégrer un backend](../backend/) pour signer le token et envoyer des events côté serveur.
- [Référence SDKs](../../sdks/) · [Contrat d'event](../../contract/).
