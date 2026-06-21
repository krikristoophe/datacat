//! Couche de **lecture chaude** : endpoints de requête en lecture seule sur PostgreSQL.
//!
//! Découplée de l'ingestion (frontières nettes, cahier §9). Les requêtes lourdes d'analyse de
//! masse visent le **stockage froid** (Parquet + DataFusion, cf. crate `reader/`). Ces endpoints
//! servent l'exploration interactive : recherche de logs, récupération d'une trace, recherche
//! d'events, et **séquences de parcours** (cœur du besoin de génération de tests).

pub mod routes;
