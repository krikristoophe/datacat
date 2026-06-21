# Guide d'intégration rapide

Objectif : brancher Datacat sur une application existante **sans friction**. Deux étapes :
(1) exposer un endpoint de token côté backend consommateur, (2) initialiser le SDK côté client.

## 1. Côté backend consommateur : endpoint de token

Le SDK ne contient **aucun secret**. Il récupère, à l'exécution, un token court-vécu signé par
votre backend (déjà authentifié). Exposez un endpoint **authentifié** qui renvoie ce token.

Voir [`token-contract.md`](token-contract.md) pour la spécification complète et des exemples
d'émission (Node `jose`, Python `PyJWT`). Exemple minimal (Express) :

```ts
app.get("/api/analytics-token", requireAuth, async (req, res) => {
  const token = await issueIngestToken(
    { id: req.user.id, tenantId: req.user.tenantId },
    req.sessionId,            // l'identifiant de session que vous propagez
  );
  res.json({ token });
});
```

Côté Datacat (ingestion), configurez la **clé publique** correspondante (`TOKEN_PUBLIC_KEY_FILE`
ou `TOKEN_JWKS_URL`) — cf. [`deployment.md`](deployment.md).

## 2. SDK Web (TypeScript / React)

Installation : le package `@datacat/sdk-web` (dossier [`sdks/typescript/`](../sdks/typescript/)).

```ts
import { createDatacatClient } from "@datacat/sdk-web";

const analytics = createDatacatClient({
  endpoint: "https://ingest.example.com/v1/events",
  // Récupère le token via VOTRE endpoint authentifié ; renouvelé automatiquement.
  getToken: () =>
    fetch("/api/analytics-token", { credentials: "include" })
      .then((r) => r.json())
      .then((d) => d.token),
  // actor_id peut être défini ici, ou plus tard via identify() après authentification.
  actorId: currentUser?.id,
  tenantId: currentUser?.tenantId,
  // redaction optionnelle (jamais de données sensibles dans properties)
  redact: (props) => ({ ...props, password: undefined }),
});

// Après authentification de l'utilisateur :
analytics.identify({ actorId: user.id, tenantId: user.tenantId });

// Émettre un event métier :
analytics.track("validate_planning", { planningId: 42, count: 3 });

// Le SDK envoie par batch automatiquement ; flush de fin de page géré (sendBeacon/keepalive).
// Pour forcer : await analytics.flush();
```

Le SDK gère : génération d'`event_id` (figé), `timestamp_client` (figé), batching, retry
idempotent (même `event_id` sur renvoi), persistance du `session_id` (sessionStorage),
renouvellement du token, et flush de fin de session via `navigator.sendBeacon`/`fetch keepalive`.

## 3. SDK Mobile (Flutter / Dart)

Package `datacat_sdk` (dossier [`sdks/flutter/`](../sdks/flutter/)).

```dart
import 'package:datacat_sdk/datacat_sdk.dart';

final analytics = DatacatClient(
  config: DatacatConfig(
    endpoint: 'https://ingest.example.com/v1/events',
    getToken: () async {
      final res = await http.get(Uri.parse('https://app.example.com/api/analytics-token'));
      return (jsonDecode(res.body) as Map)['token'] as String;
    },
    actorId: currentUser?.id, // ou via identify() après login
    tenantId: currentUser?.tenantId,
  ),
);

// Après authentification :
analytics.identify(actorId: user.id, tenantId: user.tenantId);

analytics.track('validate_planning', {'planningId': 42, 'count': 3});
```

Intégration du cycle de vie (flush quand l'app passe en arrière-plan — équivalent `sendBeacon`) :

```dart
class _AppState extends State<App> with WidgetsBindingObserver {
  @override
  void initState() { super.initState(); WidgetsBinding.instance.addObserver(this); }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state == AppLifecycleState.paused || state == AppLifecycleState.detached) {
      analytics.flush();
    }
  }

  @override
  void dispose() { WidgetsBinding.instance.removeObserver(this); analytics.dispose(); super.dispose(); }
}
```

Pour persister le `session_id` entre les lancements, fournir une implémentation de
`DatacatStorage` basée sur `shared_preferences` (voir le README du SDK).

## 4. Contrat commun (les deux SDKs)

Les deux SDKs produisent des events **conformes au même wire format** ([`CONTRACT.md`](CONTRACT.md)),
avec la même logique d'`event_id`/`timestamp_client` figés, de batching, de retry idempotent, et
de gestion du token. `tenant_id` (si dispo) + `actor_id` + `session_id` sont joints à chaque event.

## 5. Données sensibles

Les `properties` sont **libres** mais ne doivent **jamais** contenir de données sensibles
(mots de passe, PII non nécessaire, tokens). Les deux SDKs exposent un hook de redaction et le
documentent. Cette responsabilité est côté émetteur (cf. `security.md`).
