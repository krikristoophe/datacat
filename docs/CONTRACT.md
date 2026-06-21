# Datacat — Contrat partagé (event + token)

Ce document est la **source de vérité unique** du contrat d'ingestion. Le backend
(`backend/`) et les deux SDKs (`sdk-typescript/`, `sdk-flutter/`) DOIVENT s'y conformer
à l'identique. Toute évolution se fait ici d'abord.

---

## 1. Endpoint d'ingestion

```
POST /v1/events
Content-Type: application/json
Authorization: Bearer <jwt-d-ingestion>      # cf. §4
Origin: https://app.example.com               # validé par CORS (web)
```

Réponse de succès :

```
202 Accepted
{ "received": 12 }          # nombre d'events acceptés pour écriture asynchrone
```

> L'API **acquitte immédiatement** (202) puis écrit en base derrière (micro-batch).
> `received` n'est PAS le nombre d'events réellement insérés : la déduplication
> (idempotence) a lieu en base, de façon asynchrone. Un `event_id` déjà connu est
> ignoré silencieusement (`ON CONFLICT DO NOTHING`).

### 1.1 Transport du token (en-tête vs `sendBeacon`)

Le token est transmis par l'en-tête `Authorization: Bearer <jwt>` **par défaut**.

`navigator.sendBeacon` (utilisé en fin de page/session côté web) **ne permet pas** d'ajouter
d'en-tête `Authorization`. Pour ce cas — et **uniquement** ce cas de repli — le SDK peut
placer le token dans une propriété de premier niveau du corps JSON :

```jsonc
{ "token": "<jwt>", "events": [ ... ] }   // repli beacon : token dans le corps
```

Côté ingestion, la résolution du token suit cet ordre : (1) en-tête `Authorization: Bearer` ;
(2) à défaut, champ `token` du corps. Le token n'est **jamais** transmis en *query string*
(éviter sa journalisation dans les access logs). Le chemin nominal reste l'en-tête ; le SDK
privilégie `fetch(..., { keepalive: true })` avec en-tête à l'unload et ne recourt à
`sendBeacon` (token dans le corps) qu'en dernier ressort.

Codes d'erreur :

| Code | Signification | Corps |
|---|---|---|
| `400 Bad Request` | payload malformé / validation structurelle échouée | `{ "error": "...", "details": [...] }` |
| `401 Unauthorized` | token absent, invalide, expiré, ou claims manquants | `{ "error": "..." }` |
| `413 Payload Too Large` | payload ou batch au-delà des bornes | `{ "error": "..." }` |
| `429 Too Many Requests` | rate limit atteint (un des trois niveaux) | `{ "error": "...", "scope": "session|ip_sessions|global" }` + en-tête `Retry-After` |
| `503 Service Unavailable` | arrêt en cours / non prêt | `{ "error": "..." }` |

---

## 2. Format d'un event (wire format)

Le corps est un objet `{ "events": [ <event>, ... ] }` (batch, jamais un event seul).

```jsonc
{
  "events": [
    {
      "event_id":         "550e8400-e29b-41d4-a716-446655440000", // UUID, généré CLIENT, clé d'idempotence
      "event_name":       "validate_planning",                    // libre, 1..=200 chars
      "tenant_id":        "clinic-42",                            // optionnel (string|null|absent), <=200
      "actor_id":         "user-123",                             // requis, 1..=200
      "session_id":       "8f14e45f-ceea-467d-9c2e-1b2e3c4d5e6f", // requis, 1..=200
      "timestamp_client": "2026-06-21T10:15:30.123Z",            // RFC3339/ISO-8601 UTC, FIGÉ à la création
      "properties":       { "planning_id": 42, "count": 3 }       // optionnel, objet JSON, défaut {}
    }
  ]
}
```

### 2.1 Champs

| Champ | Type wire | Obligatoire | Contrainte de validation serveur |
|---|---|---|---|
| `event_id` | string (UUID) | oui | UUID valide. **Clé d'idempotence.** |
| `event_name` | string | oui | 1..=200 caractères (après trim non vide) |
| `tenant_id` | string\|null | non | si présent : 1..=200 caractères |
| `actor_id` | string | oui | 1..=200 caractères |
| `session_id` | string | oui | 1..=200 caractères |
| `timestamp_client` | string RFC3339 | oui | parsable ; dans `[received_at - MAX_PAST_SKEW, received_at + MAX_FUTURE_SKEW]` |
| `properties` | object | non | objet JSON ; taille sérialisée <= `MAX_PROPERTIES_BYTES` ; profondeur <= `MAX_JSON_DEPTH` |

`received_at` (timestamp serveur) **n'est jamais envoyé par le client** : il est renseigné par l'API.

### 2.2 Règle d'or de l'idempotence (impératif SDK)

> **`event_id` ET `timestamp_client` sont figés à la *création* de l'event et réutilisés
> *à l'identique* à chaque renvoi (retry).** Ne JAMAIS les régénérer sur un retry.

Raison technique (cf. `docs/architecture.md`) : la table est partitionnée par
`timestamp_client` et la clé d'idempotence est `(timestamp_client, event_id)`. C'est le
seul horodatage stable entre deux envois d'un même event ; il garantit qu'un doublon
retombe toujours dans la même partition et est donc dédupliqué globalement.

### 2.3 Bornes (valeurs par défaut, configurables côté serveur)

| Constante | Défaut | Rôle |
|---|---|---|
| `MAX_BATCH_EVENTS` | 500 | nombre max d'events par requête |
| `MAX_PAYLOAD_BYTES` | 1 048 576 (1 MiB) | taille max du corps HTTP |
| `MAX_PROPERTIES_BYTES` | 16 384 (16 KiB) | taille sérialisée max de `properties` |
| `MAX_STRING_LEN` | 200 | longueur max des champs texte (name/ids) |
| `MAX_JSON_DEPTH` | 16 | profondeur max de `properties` |
| `MAX_PAST_SKEW` | 31 jours | rejet si `timestamp_client` trop ancien |
| `MAX_FUTURE_SKEW` | 24 heures | rejet si `timestamp_client` trop futur |

Politique de validation :
- **Erreurs structurelles** (JSON invalide, champ requis manquant, mauvais type, batch
  vide ou trop grand, payload trop gros) → **rejet de toute la requête** (`400`/`413`).
- **Filtres sémantiques par event** (`timestamp_client` hors fenêtre de skew) → l'event
  fautif est **écarté** (perte tolérée, compteur incrémenté), les autres events du batch
  sont acceptés. `received` reflète le nombre d'events retenus.

---

## 3. Identité & corrélation

- `actor_id` : identité persistante d'un acteur (fournie par l'application).
- `session_id` : **identifiant structurant**. Généré et persisté par le SDK pour la durée
  d'une session. Sert (a) au rate limiting fin par session et (b) de **clé de corrélation
  future** entre events produit et logs techniques.
- `tenant_id` : multi-tenant B2B, optionnel.

Tous trois sont du **texte**, joints à chaque event.

---

## 4. Contrat du token d'ingestion (JWT, signature asymétrique)

> L'**émission** du token est **hors scope** de ce projet : elle relève de chaque backend
> consommateur (Swappy, etc.). Ce document spécifie le contrat pour que tout backend
> l'implémente à l'identique. Côté ingestion (dans le scope) : **vérification uniquement**,
> avec la **clé publique seule**. Côté SDK (dans le scope) : récupération, jointure et
> renouvellement du token, **jamais embarqué en dur**.

### 4.1 Algorithme

- **Asymétrique uniquement.** Recommandé : **EdDSA (Ed25519)**. Alternative : **RS256**.
- Le backend consommateur signe avec la **clé privée**. L'ingestion vérifie avec la **clé
  publique uniquement** → l'endpoint public ne détient aucun secret capable de *forger* un
  token, seulement de le *vérifier*.

### 4.2 En-tête JWT

```jsonc
{ "alg": "EdDSA", "typ": "JWT", "kid": "2026-06-key-1" }
```

- `kid` (recommandé) : identifie la clé pour permettre la **rotation** (plusieurs clés
  publiques actives côté ingestion, sélection par `kid`).

### 4.3 Claims (payload)

| Claim | Type | Obligatoire | Description |
|---|---|---|---|
| `iss` | string | recommandé | émetteur (backend consommateur). Vérifié si `TOKEN_ISSUER` est configuré. |
| `aud` | string | recommandé | audience, valeur attendue `datacat-ingest`. Vérifié si `TOKEN_AUDIENCE` est configuré. |
| `sub` | string | recommandé | = `actor_id` (sujet standard) |
| `actor_id` | string | **oui** | acteur authentifié |
| `session_id` | string | **oui** | session authentifiée — **clé du rate limiting fin** |
| `tenant_id` | string | non | tenant (si applicable) |
| `iat` | number (epoch s) | **oui** | émission |
| `exp` | number (epoch s) | **oui** | expiration (**court-vécu**, cf. §4.5) |
| `jti` | string | non | identifiant de token (anti-rejeu applicatif éventuel) |

### 4.4 Règles de vérification (côté ingestion)

Dans l'ordre, échec → `401` :

1. En-tête `Authorization: Bearer <jwt>` présent et bien formé.
2. `alg` ∈ algorithmes autorisés (`EdDSA`/`RS256`) — **jamais `none`**, jamais d'algo
   symétrique. Sélection de la clé via `kid` si fourni.
3. Signature valide contre la clé **publique**.
4. `exp` non dépassé (avec une tolérance d'horloge `TOKEN_LEEWAY`, défaut 60 s).
5. `iat` présent et non aberrant (pas dans le futur au-delà du leeway).
6. Claims requis présents et non vides : `actor_id`, `session_id`.
7. Si configuré : `iss == TOKEN_ISSUER`, `aud == TOKEN_AUDIENCE`.

Le token authentifie la **qualité du trafic** (sessions issues du système principal), pas
le **contenu** des events (toujours falsifiable). `session_id`/`actor_id` du token sont la
source de confiance pour le rate limiting ; les mêmes champs dans le corps des events sont
stockés tels quels mais **non présumés fiables**.

### 4.5 Durée de vie & renouvellement (côté SDK)

- **Court-vécu** : `exp - iat` recommandé **5 à 15 minutes**.
- Le SDK récupère le token via un callback fourni par l'application (`getToken`), le met en
  cache, et le **renouvelle** : (a) avant expiration (marge ~30 s), et (b) sur un `401`.
- Le token n'est **jamais** stocké en dur dans le SDK ni dans le code applicatif livré.

### 4.6 Mise à disposition de la clé publique (côté ingestion)

Deux modes, configurables :

- **PEM en configuration** (`TOKEN_PUBLIC_KEY_PEM` / chemin) : clé(s) publique(s) fournie(s)
  au déploiement. Rotation = ajout d'une nouvelle clé puis retrait de l'ancienne.
- **JWKS** (`TOKEN_JWKS_URL`) : l'ingestion récupère et met en cache le jeu de clés publiques
  du backend consommateur, rafraîchi périodiquement ; sélection par `kid`. Permet une
  rotation sans redéploiement de l'ingestion.

La spécification d'émission complète (exemples de génération côté consommateur) est dans
`docs/token-contract.md`.

---

## 5. Comportement SDK (commun TS & Flutter)

1. `track(name, properties)` crée un event : `event_id = uuid v4`, `timestamp_client = now()`
   (figés), + `actor_id`/`session_id`/`tenant_id` courants.
2. Les events sont **mis en file** et envoyés par **batch** (déclencheurs : taille de batch
   atteinte, intervalle de flush, ou flush explicite/fin de session).
3. **Retry idempotent** : en cas d'échec réseau/5xx, les events restent en file et sont
   **renvoyés avec le même `event_id`/`timestamp_client`**. Backoff exponentiel borné.
4. Web : `flush` de fin de page/session via `navigator.sendBeacon` (fallback `fetch keepalive`).
5. Token joint à chaque requête (`Authorization: Bearer`), récupéré/renouvelé via `getToken`.
6. Les `properties` ne doivent **jamais** contenir de données sensibles (documenté ; le SDK
   expose un hook de redaction optionnel).

---

## 6. Versionnement du contrat

Le préfixe d'URL `/v1/` porte la version majeure. Tout changement cassant du wire format ou
des claims impose `/v2/`. Les ajouts rétro-compatibles (nouveau champ optionnel) restent en `/v1/`.
