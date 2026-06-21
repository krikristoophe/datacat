# Contrat du token d'ingestion (spécification d'émission)

> **Périmètre.** L'**émission** du token est **hors scope** de Datacat (cahier §7.3.1) : elle
> relève de chaque **backend consommateur** (Swappy ou tout autre projet utilisant l'analytics).
> Ce document est le **livrable** : une spécification suffisamment précise pour qu'un backend
> consommateur implémente l'émission **sans ambiguïté**. Côté Datacat, seule la **vérification**
> est implémentée (clé publique uniquement), conformément à ce contrat.

Voir aussi `CONTRACT.md` §4 (résumé intégré au contrat partagé).

## 1. Principe

Au moment où l'utilisateur est **déjà authentifié** sur l'application, le backend consommateur
signe un **JWT court-vécu** avec sa **clé privée**. Le SDK récupère ce token (via le callback
`getToken`) et le joint aux requêtes d'ingestion (`Authorization: Bearer`). Datacat vérifie la
signature avec la **clé publique uniquement**.

Garantie : le service d'ingestion (endpoint public, plus exposé) ne détient **aucun secret
capable de forger** un token. En cas de compromission de l'ingestion, un attaquant ne peut pas
créer de tokens valides. Le token est un **filtre de qualité du trafic**, pas une preuve
d'intégrité du contenu (le contenu des events reste falsifiable — cf. `security.md`).

## 2. Algorithme de signature

- **Asymétrique obligatoire.** Recommandé : **EdDSA (Ed25519)**. Alternative : **RS256**.
- **Interdits** : `none`, et tout algorithme symétrique (`HS256`, …). Datacat les rejette.
- L'en-tête JWT **devrait** porter un `kid` pour permettre la rotation :

```json
{ "alg": "EdDSA", "typ": "JWT", "kid": "2026-06-key-1" }
```

## 3. Claims (payload)

| Claim | Type | Obligatoire | Description |
|---|---|---|---|
| `iss` | string | recommandé | émetteur (le backend consommateur). Vérifié par Datacat si `TOKEN_ISSUER` est configuré. |
| `aud` | string | recommandé | audience attendue, ex. `datacat-ingest`. Vérifiée si `TOKEN_AUDIENCE` est configuré. |
| `sub` | string | recommandé | = `actor_id`. |
| `actor_id` | string | **oui** | identité de l'acteur authentifié. |
| `session_id` | string | **oui** | session authentifiée. **Clé du rate limiting fin** côté Datacat. |
| `tenant_id` | string | non | tenant (multi-tenant B2B). |
| `iat` | number (epoch s) | **oui** | instant d'émission. |
| `exp` | number (epoch s) | **oui** | expiration. **Court-vécu** (cf. §5). |
| `jti` | string | non | identifiant de token (anti-rejeu applicatif éventuel). |

`actor_id` et `session_id` doivent être **non vides**. `session_id` doit être l'identifiant de
session que le SDK propage (cohérence de la corrélation et du rate limiting).

## 4. Exemple d'émission (backend consommateur)

### 4.1 Node.js (EdDSA, `jose`)

```ts
import { SignJWT, importPKCS8 } from "jose";

// Clé PRIVÉE, gardée UNIQUEMENT côté backend consommateur (jamais exposée).
const privateKey = await importPKCS8(process.env.DATACAT_SIGNING_KEY_PEM!, "EdDSA");

export async function issueIngestToken(user: { id: string; tenantId?: string }, sessionId: string) {
  return await new SignJWT({
    actor_id: user.id,
    session_id: sessionId,
    ...(user.tenantId ? { tenant_id: user.tenantId } : {}),
  })
    .setProtectedHeader({ alg: "EdDSA", kid: "2026-06-key-1" })
    .setSubject(user.id)
    .setIssuer("swappy-backend")
    .setAudience("datacat-ingest")
    .setIssuedAt()
    .setExpirationTime("10m")          // court-vécu
    .sign(privateKey);
}
```

L'application expose un endpoint **authentifié** (ex. `GET /analytics/ingest-token`) qui renvoie
ce token à l'utilisateur connecté ; le SDK l'appelle via `getToken`.

### 4.2 Python (RS256, `PyJWT`)

```python
import jwt, time

def issue_ingest_token(actor_id: str, session_id: str, tenant_id: str | None) -> str:
    now = int(time.time())
    claims = {
        "iss": "swappy-backend", "aud": "datacat-ingest", "sub": actor_id,
        "actor_id": actor_id, "session_id": session_id,
        "iat": now, "exp": now + 600,
    }
    if tenant_id:
        claims["tenant_id"] = tenant_id
    return jwt.encode(claims, PRIVATE_KEY_PEM, algorithm="RS256",
                      headers={"kid": "2026-06-key-1"})
```

### 4.3 Outil de dev fourni par Datacat

Pour tester l'ingestion sans backend consommateur, Datacat fournit un binaire **de dev** :

```bash
cargo run --bin mint-dev-token -- \
  --key keys/ed25519_private.pem --alg EdDSA \
  --actor user-123 --session sess-abc --tenant clinic-42 \
  --ttl 600 --iss swappy-backend --aud datacat-ingest --kid 2026-06-key-1
```

⚠️ Cet outil n'est **jamais** déployé en production ; il illustre le contrat et alimente les tests.

## 5. Durée de vie & renouvellement (côté SDK)

- **Court-vécu** : `exp - iat` recommandé **5 à 15 minutes**.
- Le SDK met le token en cache et le **renouvelle** : avant expiration (marge ~30 s, en décodant
  `exp`) et sur réponse `401`. Implémenté dans les deux SDKs.

## 6. Mise à disposition de la clé publique (côté Datacat)

Deux modes, au choix du déploiement :

### 6.1 Clé PEM en configuration

```bash
TOKEN_ALG=EdDSA
TOKEN_PUBLIC_KEY_FILE=/etc/datacat/ingest_pub.pem   # ou TOKEN_PUBLIC_KEY_PEM="-----BEGIN PUBLIC KEY----- …"
TOKEN_KID=2026-06-key-1                              # optionnel
```

Rotation : déployer la nouvelle clé, basculer l'émission, puis retirer l'ancienne (fenêtre de
recouvrement). Pour une rotation sans redéploiement, préférer JWKS.

### 6.2 JWKS (rotation sans redéploiement)

```bash
TOKEN_JWKS_URL=https://swappy-backend.example.com/.well-known/jwks.json
TOKEN_JWKS_REFRESH=1h
```

Le backend consommateur publie un endpoint JWKS standard exposant ses clés **publiques** :

```json
{
  "keys": [
    { "kty": "OKP", "crv": "Ed25519", "kid": "2026-06-key-1", "x": "…", "alg": "EdDSA", "use": "sig" }
  ]
}
```

Datacat récupère et met en cache le JWKS, rafraîchi périodiquement, et sélectionne la clé par
`kid`. Rotation = publier la nouvelle clé dans le JWKS (garder l'ancienne le temps que les
tokens en circulation expirent), puis la retirer.

## 7. Règles de vérification appliquées par Datacat

Échec → `401`. Dans l'ordre :

1. En-tête `Authorization: Bearer <jwt>` présent et bien formé (ou champ `token` du corps en
   repli `sendBeacon`, cf. `CONTRACT.md` §1.1).
2. `alg` ∈ {`EdDSA`, `RS256`} (configurable via `TOKEN_ALGORITHMS`) — **jamais `none`/symétrique**.
   Sélection de la clé via `kid` (correspondance exacte si fourni).
3. Signature valide contre la **clé publique**.
4. `exp` non dépassé (tolérance `TOKEN_LEEWAY`, défaut 60 s).
5. `iat` présent.
6. Claims requis présents et non vides : `actor_id`, `session_id`.
7. Si configuré : `iss == TOKEN_ISSUER`, `aud == TOKEN_AUDIENCE`.

## 8. Limites assumées (à présenter honnêtement en audit)

Ce mécanisme garantit que le trafic provient de **sessions authentifiées** par le système
principal et permet la **révocation** (rotation de clé). Il **ne** garantit **pas**
l'infalsifiabilité du contenu : un utilisateur légitime peut émettre de faux events « valides ».
C'est le niveau de garantie adéquat pour des analytics tolérantes au bruit. Voir `security.md`.
