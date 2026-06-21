# Cahier des charges — Système d'analytics d'events maison

## 1. Contexte et objectif

Construire un système d'analytics d'events **maison, léger et auto-hébergé**, destiné à capturer les comportements réels des utilisateurs d'une application B2B, puis à exploiter ces données pour deux usages :

1. **Analyse comportementale** : comprendre les parcours réellement empruntés par les utilisateurs (quelles actions, dans quel ordre, à quelle fréquence).
2. **Génération automatique de tests E2E** : alimenter un pipeline qui, à partir des parcours observés en production, génère ou met à jour des scénarios de test reflétant l'usage réel.

Le système n'est **pas** un clone complet de PostHog. La **v1 se concentre exclusivement sur l'ingestion** : capturer des events de façon robuste, scalable et auditable. L'exploitation analytique (requêtes, restitution) est préparée par l'architecture mais hors périmètre de la v1.

**Objectif transverse** : intégration **rapide et simple** dans les projets existants. Le système doit pouvoir être branché sur une application sans friction.

## 2. Principes directeurs (non négociables)

- **Réutiliser PostgreSQL** comme base centrale (déjà présent dans la stack, déjà maîtrisé). **PostgreSQL unique** pour l'instant ; l'amélioration ou la migration vers une solution distribuée sera envisagée plus tard, si le besoin se confirme.
- **Idempotence.** Un même event reçu plusieurs fois (retry réseau, double envoi, rejeu de file) ne doit être stocké et compté qu'une seule fois. C'est une garantie structurante à acter dès la conception, pas une option.
- **Priorité au débit d'écriture.** Le système doit encaisser des charges d'écriture (pics d'ingestion). La latence de lecture est secondaire : une requête analytique qui prend plusieurs dizaines de secondes serait acceptable (lecture hors v1).
- **Léger maintenant, scalable plus tard.** Aucune brique distribuée (log distribué, sharding) n'est déployée au départ. L'architecture doit toutefois permettre de les insérer ultérieurement **sans réécriture du cœur**.
- **Pas d'usine à gaz.** Pas de ClickHouse, pas de Kafka, pas de Zookeeper au démarrage.
- **Stack Rust** pour le backend (cohérence avec l'écosystème existant : Axum, écosystème async Rust).
- **Tolérance à la perte d'events.** Perdre une petite fraction d'events de manière non biaisée est acceptable (les analytics sont indicatives, pas comptables). À NE PAS confondre avec une tolérance aux doublons : voir idempotence.
- **Qualité de production.** Tout le code est testé, respecte les normes et standards en vigueur, et n'est pas du boilerplate. Le système complet doit pouvoir subir un **audit de sécurité poussé (type HDS)** sans réserve.

## 3. Architecture cible

```
Events (front web / mobile / backend)
        │
        ▼
   API d'ingestion (Axum)
        │  micro-batch en mémoire
        ▼
   PostgreSQL  ──────────────► table d'events partitionnée par temps,
   (écriture via COPY,          idempotente
    idempotent, partitionné)
        │
        ▼  (hors v1) export périodique vers Parquet/Iceberg sur Object Storage EU
        ▼  (hors v1) lecture analytique à la demande (DataFusion / DuckDB)
```

### Rôles des composants (v1)

- **API d'ingestion (Axum)** : reçoit les events en HTTP, les valide, les accumule en micro-batches, les écrit dans PostgreSQL via `COPY`.
- **PostgreSQL** : stockage des events, optimisé pour l'écriture, partitionné par temps, idempotent.
- **SDKs clients** : émission des events depuis les applications (TypeScript et Flutter).

Les couches de stockage froid et de lecture analytique sont décrites en section 9 (évolutivité) mais ne sont pas construites en v1.

## 4. Modèle de données

### 4.1 Structure d'un event

Chaque event est un enregistrement horodaté comportant au minimum :

| Champ | Type | Description |
|---|---|---|
| `event_id` | UUID | Identifiant unique généré **côté client** à la création de l'event. Clé d'idempotence. |
| `event_name` | texte | Nom de l'action métier (ex. `validate_planning`). **Libre** (pas de registre imposé). |
| `tenant_id` | texte | Identifiant du tenant (multi-tenant B2B). **Optionnel.** |
| `actor_id` | texte | Identifiant de l'acteur (identité persistante). Envoyé avec chaque event. |
| `session_id` | texte | Identifiant de session. **Champ structurant** : sert au rate limiting fin et de **clé de corrélation** future entre events produit et logs techniques. Envoyé avec chaque event. |
| `timestamp_client` | timestamp | Horodatage côté client (moment de l'action). |
| `received_at` | timestamp | Horodatage serveur (moment de réception). Renseigné par l'API. |
| `properties` | JSONB | Propriétés arbitraires de l'event (contexte métier). |

### 4.2 Identité et corrélation

- **Identité persistante.** Chaque event transporte `tenant_id` (optionnel), `actor_id` et `session_id`, tous en **texte**. L'`actor_id` permet de relier l'activité d'un même acteur dans le temps.
- **`session_id` comme clé de corrélation.** Le `session_id` est un identifiant structurant, pas un simple détail technique. Il sert à la fois au rate limiting fin (voir section 7) et de clé de jointure future entre les events produit et les logs techniques. Les SDKs doivent le générer et le propager proprement, et il doit être documenté comme identifiant de corrélation.
- **Objectif futur (hors scope v1)** : intégrer ultérieurement les logs techniques à ce même système et pouvoir tout relier (events produit + logs) via `tenant_id` / `actor_id` / `session_id`, afin de faciliter le debug. Le modèle de données doit être conçu pour ne pas fermer cette porte.

### 4.3 Conventions

- **Events libres** : pas de registre de définitions imposé à l'ingestion. Les `event_name` et `properties` sont libres. La cohérence éventuelle (regroupement, normalisation, gestion des renommages) se traitera **à la lecture**, pas à l'ingestion.
- **Double horodatage** : conserver `timestamp_client` ET `received_at`. Le client peut avoir une horloge fausse ; le serveur peut recevoir en retard (batch, retry). Choisir l'un ou l'autre selon l'analyse ultérieure.

## 5. Exigences fonctionnelles (v1)

### 5.1 Ingestion

- **Endpoint HTTP** acceptant un POST avec un **tableau d'events** (batch), pas un event unique par requête.
- **Authentification non requise** pour l'endpoint d'ingestion. Le système doit néanmoins rester auditable sécurité (voir section 7) : l'absence d'auth d'ingestion ne doit pas créer de faille (validation stricte des entrées, protection contre les abus, etc.).
- L'API répond immédiatement (acquittement rapide) ; l'écriture en base se fait derrière (micro-batch).

### 5.2 SDKs clients

Deux SDKs à fournir, cohérents entre eux (même contrat d'event, même logique d'`event_id` et de batching) :

- **TypeScript** : pour les applications web (React, etc.).
- **Flutter** : pour les applications mobiles.

Chaque SDK doit :

- générer un `event_id` (UUID) à la **création** de l'event ;
- **réutiliser le même `event_id`** en cas de renvoi après échec (retry) — ne JAMAIS régénérer ;
- accumuler les events et les envoyer par batch ;
- côté web : utiliser `navigator.sendBeacon` (ou équivalent) pour l'envoi en fin de session/page ;
- joindre `tenant_id` (si disponible), `actor_id` et `session_id` à chaque event ;
- récupérer le **token d'ingestion** depuis le backend consommateur à l'exécution (voir section 7) et le joindre aux requêtes ; le renouveler à expiration. Le token n'est **jamais** embarqué en dur dans le SDK.

### 5.3 Idempotence (déduplication)

- `event_id` porte une contrainte **UNIQUE** en base.
- L'insertion utilise `INSERT ... ON CONFLICT (event_id) DO NOTHING` (ou l'équivalent au sein du `COPY` via table de staging).
- Objectif : un même event reçu plusieurs fois n'est compté qu'une fois.

### 5.4 Écriture optimisée

- Écriture par `COPY` (pas d'`INSERT` ligne par ligne) depuis un buffer de micro-batch.
- Table d'events **partitionnée par temps** (ex. par jour ou par semaine).
- Purge de la rétention par `DROP PARTITION` (instantané), pas par `DELETE`.
- Évaluer les tables `UNLOGGED` pour la table de staging si la tolérance à la perte récente le permet (gain d'écriture en supprimant le coût du WAL).

### 5.5 Migrations

- Toutes les migrations de schéma (ex. via **sqlx**) doivent être **présentes et versionnées dans le repo**. Le schéma se reconstruit de façon reproductible depuis les migrations.

## 6. Multi-tenant

- Chaque event **peut** porter un `tenant_id` (optionnel).
- **Séparation logique** actée : un seul stockage, filtrage par `tenant_id` dans les requêtes. Cela permettra plus tard de produire aussi bien des statistiques générales que par tenant, sans cloisonnement physique.

## 7. Sécurité et auditabilité

Le système complet doit pouvoir passer un **audit de sécurité poussé de type HDS** sans réserve. L'endpoint d'ingestion est public et non authentifié au sens fort ; la sécurité repose donc **entièrement sur des défenses côté serveur**, sous l'hypothèse que toute requête entrante peut être falsifiée (le client — web ou mobile — n'est jamais fiable).

### 7.1 Modèle de menace

- Tout ce qui provient du client est falsifiable : `actor_id`, `session_id`, contenu des events, et même un éventuel token peuvent être extraits et rejoués. Aucune défense côté client n'est considérée comme une garantie d'intégrité.
- La sécurité réelle est serveur-side. Le token (section 7.3) est un **filtre de qualité du trafic**, pas un mécanisme d'authentification du contenu, et doit être présenté comme tel en audit.

### 7.2 Rate limiting à deux niveaux

Le rate limiting combine deux dimensions complémentaires, pour protéger à la fois les clients légitimes entre eux et le système contre le flood :

- **Limite par `session_id`** : limite fine du débit d'events par session individuelle. C'est elle qui empêche un utilisateur d'impacter ses collègues. **Indispensable en contexte B2B** : les établissements clients (EHPAD, cliniques, hôpitaux) sortent souvent derrière une **IP publique unique** (NAT) ; une limite par IP seule traiterait tous les utilisateurs d'un établissement comme une seule source et les bloquerait mutuellement.
- **Limite du nombre de sessions distinctes par IP** (`rate_session` par IP) : sur une fenêtre glissante, une même IP ne peut pas créer un nombre déraisonnable de sessions distinctes (ex. pas 10 000 sessions en 30 min). Cela referme le contournement évident de la limite par session (un attaquant qui génère des milliers de fausses sessions falsifiées), tout en laissant respirer un établissement légitime derrière son NAT.
- **Filet global** : un rate limit global protège l'infrastructure contre un flood massif multi-sources.

Les seuils précis sont à calibrer ; le principe des deux niveaux (par session + plafond de sessions par IP) est non négociable.

### 7.3 Token d'ingestion (filtre de qualité, non embarqué)

- **Pas de secret en dur dans les SDKs.** Un token embarqué dans un front est extractible et impossible à faire tourner sans redéployer toutes les apps (avec les délais des stores pour le mobile) — donc un secret mort.
- **Token délivré à l'exécution** par le **backend consommateur** (Swappy ou tout autre projet utilisant l'analytics) : au moment où l'utilisateur est déjà authentifié sur l'application, le backend consommateur signe un token court-vécu que le SDK récupère et joint aux requêtes d'ingestion. Le SDK le renouvelle à expiration.
- **Vérification par signature asymétrique** : le backend consommateur signe avec une **clé privée** ; le service d'ingestion **vérifie avec la clé publique uniquement**. Ainsi le service d'ingestion (endpoint public, plus exposé) ne détient aucun secret capable de *forger* des tokens — il ne peut que les *vérifier*. En cas de compromission de l'ingestion, un attaquant ne peut pas créer de tokens valides. Cohérent avec les flux de signature asymétrique déjà en place (RS256 / EdDSA).
- **Statut assumé** : ce mécanisme garantit que le trafic provient de sessions authentifiées par le système principal, sans prétendre à l'infalsifiabilité du contenu. Il élimine le spam automatisé anonyme et permet la révocation (rotation de la clé), mais un utilisateur légitime peut toujours émettre de faux events « valides ». C'est le niveau de garantie adéquat pour des analytics tolérantes au bruit, et il est défendable en audit à condition d'être présenté honnêtement.

#### 7.3.1 Répartition des responsabilités (émission hors scope)

- **Émission du token = HORS SCOPE de ce projet.** La génération du token (signature après authentification de l'utilisateur) relève de chaque **backend consommateur** (Swappy et tout autre projet qui utilisera l'analytics). Ce projet ne modifie aucun backend existant.
- **Livrable du projet : le contrat documenté.** Le projet doit fournir une **spécification claire et complète du contrat de token**, suffisante pour qu'un backend consommateur l'implémente à l'identique sans ambiguïté. Cette spécification précise au minimum :
  - l'algorithme de signature attendu (asymétrique : RS256 ou EdDSA) ;
  - le format et les claims attendus du token (ex. `actor_id`, `tenant_id`, `session_id`, `iat`, `exp`, et tout claim nécessaire à la vérification) ;
  - la durée de vie recommandée (court-vécu) et la logique de renouvellement côté SDK ;
  - le mécanisme de mise à disposition de la clé publique au service d'ingestion (ex. JWKS, ou clé fournie en configuration) et la stratégie de rotation ;
  - les règles de vérification appliquées côté ingestion (signature, expiration, claims requis).
- **Côté ingestion (dans le scope)** : implémenter la **vérification** du token selon ce contrat (clé publique uniquement). Côté SDK (dans le scope) : récupérer, joindre et renouveler le token, sans jamais l'embarquer en dur.

### 7.4 Autres exigences

- **Validation stricte** de toutes les entrées de l'endpoint d'ingestion (taille, structure, types, bornes, nombre d'events par batch).
- **Limites de taille** (payload, taille de batch) pour borner l'impact d'un abus.
- **CORS** pour restreindre les origines web légitimes.
- **Détection d'anomalies** : repérer et couper une IP / `session_id` au comportement manifestement anormal.
- **Pas de données sensibles non maîtrisées** : les `properties` étant libres, documenter que les SDKs ne doivent pas y placer de données sensibles, et prévoir les moyens techniques de maîtrise.
- **Traçabilité** des accès et opérations côté serveur (logs applicatifs propres).
- **Chiffrement en transit** (TLS) et cohérence avec les exigences d'hébergement maîtrisé.
- **Dépendances maîtrisées** : pas de dépendances superflues, versions à jour, surface d'attaque minimale.

## 8. Organisation du dépôt

- **Monorepo unique et organisé**, contenant :
  - le **backend** (API d'ingestion Axum + migrations) ;
  - le **SDK TypeScript** ;
  - le **SDK Flutter** ;
  - la **documentation** (déploiement, intégration, contribution).
- Structure claire et cohérente, frontières nettes entre composants.

## 9. Évolutivité (préparée, non déployée en v1)

L'architecture doit permettre, **sans réécriture du cœur**, d'ajouter ultérieurement :

- **Stockage froid** : export périodique PostgreSQL → **Parquet (format Iceberg)** sur Object Storage S3-compatible en région EU, partitionné par date. Format ouvert pour garantir l'interopérabilité.
- **Lecture analytique** : moteur de requête embarqué (**DataFusion** en Rust, ou **DuckDB**) scannant le froid (et/ou le chaud) en SQL, à la demande, lecture lente acceptée. Requêtes prioritaires à terme : séquences/parcours fréquents par acteur/session (cœur du besoin de génération de tests).
- **Logs techniques** : intégration des logs techniques au même système, reliés aux events via `tenant_id` / `actor_id` / `session_id`, pour faciliter le debug.
- **Scale-out d'écriture** : via **Citus** (sharding PostgreSQL, pour rester en terrain connu) ou un **tampon distribué** (Redpanda, log compatible Kafka écrit en Rust, sans Zookeeper/JVM) placé devant l'ingestion, si le débit dépasse ce qu'un PostgreSQL unique encaisse.
- **Scale-out de lecture** : grâce au format ouvert (Iceberg), brancher un moteur distribué (ex. Ballista, pendant distribué de DataFusion) sur les mêmes données, sans migration.

Ces briques ne sont **pas** construites en v1. La v1 doit seulement être conçue pour les accueillir (frontières nettes : ingestion / stockage / lecture découplés).

## 10. Périmètre de la v1 (à construire)

1. **Schéma PostgreSQL** : table d'events partitionnée par temps, contrainte d'idempotence (`event_id` UNIQUE), stratégie de purge par `DROP PARTITION`. Migrations versionnées (sqlx) dans le repo.
2. **API d'ingestion Axum** : endpoint batch, validation stricte, micro-batch en mémoire, écriture `COPY` avec idempotence, rate limiting à deux niveaux (par `session_id` + plafond de sessions par IP + filet global), vérification du token d'ingestion par signature asymétrique (clé publique côté ingestion), garde-fous de sécurité.
3. **SDK TypeScript** : génération d'`event_id`, batching, `sendBeacon`, retry réutilisant l'`event_id`, envoi de `tenant_id` + `actor_id` + `session_id`, récupération et renouvellement du token d'ingestion (jamais en dur).
4. **SDK Flutter** : même contrat et même logique que le SDK TypeScript.
5. **Documentation** : déploiement simple et documenté, guide d'intégration rapide dans un projet existant, et **spécification du contrat de token** (section 7.3.1) permettant à tout backend consommateur d'implémenter l'émission à l'identique.
6. **Tests** : couverture des composants (ingestion, idempotence, SDKs), conformité aux standards en vigueur.

## 11. Hors périmètre (explicitement exclu de la v1)

- Couche de lecture / requêtes analytiques (DataFusion, DuckDB).
- Stockage froid (Parquet, Iceberg, Object Storage).
- Toute UI / dashboard / visualisation.
- Moteur de requête visuel ou query builder.
- Funnels et rétention.
- Registre d'events / versioning à l'ingestion.
- Intégration des logs techniques.
- ClickHouse, Kafka, Zookeeper, Redpanda, Citus (préparés conceptuellement, non déployés).
- Authentification forte de l'utilisateur sur l'endpoint d'ingestion (le token d'ingestion est un filtre de qualité, pas une authentification du contenu — voir section 7.3).
- **Émission** du token (signature côté backend consommateur) et toute modification de Swappy ou d'un autre backend : hors scope. Seul le **contrat** est documenté (section 7.3.1), à charge de chaque projet de l'implémenter.
- Gestion RGPD applicative (effacement, durées, etc.) — hors scope de ce système.

## 12. Critères d'acceptation

- L'ingestion encaisse un pic d'écriture représentatif sans perte au-delà de la tolérance fixée et **sans doublon** (idempotence vérifiée par test : un même `event_id` envoyé plusieurs fois n'apparaît qu'une fois).
- L'écriture utilise `COPY` et la table est partitionnée ; la purge s'effectue par `DROP PARTITION` sans impact sur l'écriture.
- Les deux SDKs (TypeScript et Flutter) produisent des events conformes au même contrat, gèrent le batching et le retry idempotent, et envoient `tenant_id` + `actor_id` + `session_id`, sans jamais embarquer le token en dur.
- Le rate limiting fonctionne aux deux niveaux : une session abusive est limitée sans impacter les autres sessions de la même IP, et une IP ne peut pas créer un nombre déraisonnable de sessions distinctes sur la fenêtre (vérifié par test, ex. pas 10 000 sessions en 30 min).
- Le token d'ingestion est vérifié côté ingestion par signature asymétrique (clé publique uniquement) ; l'ingestion ne détient aucun secret permettant de forger un token. Le contrat d'émission est documenté de façon suffisante pour qu'un backend consommateur l'implémente sans ambiguïté (l'émission elle-même est hors scope).
- Les migrations sont présentes dans le repo et reconstruisent le schéma de façon reproductible.
- Le déploiement est documenté et simple à reproduire.
- Le code est testé et respecte les standards en vigueur ; aucun boilerplate.
- Le système ne présente pas de faille évidente lors d'une revue de sécurité de type HDS (validation des entrées, protection de l'endpoint public, traçabilité, TLS).
- Les frontières ingestion / stockage / lecture sont découplées (l'ajout ultérieur du froid, de la lecture ou d'un tampon d'écriture n'impacte pas le cœur d'ingestion).
