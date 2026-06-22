---
title: Qu'est-ce que Datacat ?
description: Datacat en bref — ce que vous pouvez en faire, et où aller ensuite selon ce que vous voulez livrer.
---

Datacat est une **plateforme d'ingestion auto-hébergée** pour l'analytics produit et
l'observabilité. Vous l'exécutez sur votre propre infrastructure (PostgreSQL est la seule
dépendance), vous y pointez vos apps et services, et vous gardez la maîtrise totale des données de
vos utilisateurs — utile quand ces données sont sensibles ou régulées (santé, HDS, RGPD).

## Ce que vous pouvez en faire

- **Capter des events produit** — ce que font les utilisateurs dans votre app — depuis le web et le
  mobile, avec une idempotence stricte pour que les retries ne comptent jamais deux fois.
- **Collecter l'observabilité** — logs, traces et métriques via OpenTelemetry (OTLP) — depuis vos
  services, corrélés à ces events produit par tenant, utilisateur et session.
- **Lire vos données** — interroger les données récentes depuis PostgreSQL (chaud) ou les données
  long terme exportées en Parquet sur S3 (froid), ou laisser un agent IA les explorer via le serveur
  MCP.
- **Être alerté** — règles par projet sur les taux d'erreur, la latence, les anomalies et plus,
  routées vers Slack, e-mail ou webhooks.

Le tout sur une base que vous connaissez déjà, sans Kafka, ClickHouse ni Zookeeper à exploiter.

## Par où commencer

- **Juste pour essayer ?** Lancez-le en local en quelques minutes avec le
  [Démarrage rapide](../quickstart/), puis [tracez votre premier event](../tutorials/first-event/).
- **Pour l'ajouter à votre produit ?** Choisissez votre surface dans **Intégrer** :
  [app web](../integrate/web-app/), [backend](../integrate/backend/),
  [Flutter](../integrate/flutter/), ou une stack
  [OpenTelemetry](../integrate/opentelemetry/) existante.
- **Pour la production ?** Voir [Installation](../installation/),
  [Configuration](../configuration/) et [Déploiement](../deployment/).

Vous cherchez le wire format exact, les règles de token ou les détails internes ? Ils sont sous
**Référence** ([contrat](../contract/), [architecture](../architecture/), [sécurité](../security/)).
