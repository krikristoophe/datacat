# Sécurité & auditabilité (posture HDS)

Le système doit pouvoir passer un **audit de sécurité poussé de type HDS** sans réserve. Ce
document décrit le modèle de menace, les contrôles, et ce qui est — honnêtement — garanti ou non.

## 1. Modèle de menace (cahier §7.1)

Hypothèse fondatrice : **toute requête entrante peut être falsifiée**. L'endpoint d'ingestion
est public et non authentifié au sens fort. `actor_id`, `session_id`, le contenu des events, et
même le token peuvent être extraits d'un client (web ou mobile) et rejoués. **Aucune défense
côté client n'est considérée comme une garantie.** La sécurité réelle est **entièrement
serveur-side**.

## 2. Ce qui est garanti / ce qui ne l'est pas

| Garanti | Non garanti (assumé) |
|---|---|
| Le trafic provient de **sessions authentifiées** par le système principal (token signé). | L'**infalsifiabilité du contenu** : un utilisateur légitime peut émettre de faux events « valides ». |
| L'ingestion ne peut **pas forger** de token (clé publique seule). | — |
| **Idempotence stricte** (aucun doublon). | — |
| Révocation possible (rotation de clé). | — |

C'est le niveau de garantie **adéquat** pour des analytics tolérantes au bruit, et il est
défendable en audit **à condition d'être présenté honnêtement** : le token est un *filtre de
qualité du trafic*, pas une authentification du contenu.

## 3. Contrôles implémentés (mapping cahier §7)

### 3.1 Token d'ingestion — signature asymétrique (§7.3)
- Vérification **EdDSA / RS256**, **clé publique uniquement**. `none` et les algorithmes
  symétriques sont **rejetés**. Sélection de clé par `kid`, rotation via PEM multiple ou JWKS.
- Pas de secret en dur dans les SDKs : token récupéré à l'exécution, renouvelé, jamais embarqué.
- Vérifs : signature, `exp` (+ leeway), `iat`, claims requis (`actor_id`, `session_id`),
  `iss`/`aud` si configurés. Détails : [`token-contract.md`](token-contract.md) §7.

### 3.2 Rate limiting à deux niveaux + filet global (§7.2)
- **Par `session_id`** (token bucket) : empêche une session d'impacter ses collègues —
  indispensable en B2B (établissements derrière une IP NAT unique).
- **Plafond de sessions distinctes par IP** (fenêtre glissante) : referme le contournement
  « générer des milliers de fausses sessions » sans pénaliser un établissement légitime.
- **Filet global** (token bucket) : protège l'infrastructure d'un flood massif multi-sources.
- Structures bornées en mémoire (caps + purge périodique) → pas de DoS sur le limiteur lui-même.

### 3.3 Validation stricte des entrées (§7.4)
- Bornes : taille du payload (`MAX_PAYLOAD_BYTES`, → `413`), taille de batch (`MAX_BATCH_EVENTS`),
  longueurs de champs, taille **et profondeur** de `properties` (anti-JSON-bomb), `event_id`
  UUID valide, `timestamp_client` parsable et **borné** (anti-partition-poisoning).
- Erreur structurelle → rejet de toute la requête (`400`). Filtre sémantique (skew) → event
  écarté (perte tolérée), jamais de doublon.

### 3.4 Détection d'anomalies (§7.4)
- Comptage des requêtes « mauvaises » (400/401/429) par IP sur une fenêtre ; au-delà d'un seuil,
  **bannissement temporaire** de l'IP (réponse immédiate `429`).

### 3.5 CORS (§7.4)
- Liste blanche d'origines (`CORS_ALLOWED_ORIGINS`). `*` réservé au dev (documenté).

### 3.6 Résolution d'IP
- Par défaut, IP du pair TCP (non falsifiable réseau). `X-Forwarded-For` n'est pris en compte
  que si `TRUST_FORWARDED_FOR=true`, à n'activer **que derrière un proxy de confiance unique**
  (sinon l'en-tête est forgeable). On retient alors l'entrée ajoutée par le proxy.

### 3.7 Traçabilité (§7.4)
- Logs **structurés JSON**, `x-request-id` généré/propagé, erreurs internes journalisées mais
  **jamais renvoyées** au client (pas de fuite d'information).

### 3.8 Chiffrement en transit (§7.4)
- TLS terminé au reverse-proxy ; le binaire utilise **rustls** (pas d'OpenSSL) pour ses appels
  sortants (JWKS). Hébergement maîtrisé attendu (HDS).

### 3.9 Données sensibles (§7.4)
- `properties` libres mais documentées comme **ne devant pas** contenir de données sensibles ;
  les SDKs exposent un hook de **redaction**. Responsabilité côté émetteur, outillée techniquement.

### 3.10 Dépendances maîtrisées (§7.4)
- Surface minimale, crates maintenues, versions à jour. `#![forbid(unsafe_code)]` sur le backend.
  Audit possible via `cargo audit` / `cargo deny` (cf. CI).

## 4. Surface exposée

| Endpoint | Auth | Données |
|---|---|---|
| `POST /v1/events` | token (filtre qualité) | ingestion |
| `GET /healthz`, `/readyz` | aucune | statut, aucune donnée métier |
| `GET /stats` | aucune | compteurs agrégés (aucune donnée d'event). À placer derrière le réseau interne / l'ingress si souhaité. |

> Recommandation déploiement : restreindre `/stats` (et `/readyz`) au réseau interne via le
> reverse-proxy/ingress.

## 5. Points d'attention pour l'audit

- Le token **ne protège pas** l'intégrité du contenu (assumé, cf. §2). Toute exigence d'intégrité
  forte du contenu sortirait du cadre analytics et nécessiterait un autre mécanisme.
- La tolérance à la perte (sous surcharge) est **intentionnelle** et bornée ; **jamais** au prix
  d'un doublon.
- Pas de gestion RGPD applicative en v1 (hors scope, cahier §11) — à traiter au niveau
  organisationnel/hébergement.
