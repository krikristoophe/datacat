//! Datacat — bibliothèque de l'API d'ingestion.
//!
//! Frontières nettes (cahier §9) : domaines `events` (et `logs`, à venir) au-dessus d'une
//! infrastructure partagée — `ingest` (écriture par micro-batch + COPY), `db` (stockage),
//! `security` (token, rate limiting, anomalies), `api` (couche HTTP). Le cœur d'ingestion n'a
//! aucune dépendance vers une couche de lecture.

#![forbid(unsafe_code)]

pub mod alerting;
pub mod api;
pub mod config;
pub mod db;
pub mod error;
pub mod events;
pub mod grpc;
pub mod ingest;
pub mod logs;
pub mod metrics;
pub mod otlp;
pub mod query;
pub mod security;
pub mod settings;
pub mod telemetry;
pub mod traces;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use sqlx::PgPool;

use crate::config::{Config, ValidationLimits};
use crate::events::model::StoredEvent;
use crate::ingest::Ingestor;
use crate::logs::StoredLog;
use crate::metrics::StoredMetricPoint;
use crate::security::{AnomalyGuard, RateLimiter, TokenVerifier};
use crate::traces::StoredSpan;

pub use api::build_router;

/// État partagé par tous les handlers (cloné par requête ; tout est derrière `Arc`).
#[derive(Clone)]
pub struct AppState {
    /// Ingestion des events produit (`/v1/events`).
    pub events: Ingestor<StoredEvent>,
    /// Ingestion des logs techniques OTLP (`/v1/logs`).
    pub logs: Ingestor<StoredLog>,
    /// Ingestion des traces OTLP (`/v1/traces`).
    pub spans: Ingestor<StoredSpan>,
    /// Ingestion des métriques OTLP (`/v1/metrics`).
    pub metric_points: Ingestor<StoredMetricPoint>,
    pub limiter: Arc<RateLimiter>,
    pub verifier: Arc<TokenVerifier>,
    pub anomaly: Arc<AnomalyGuard>,
    pub limits: Arc<ValidationLimits>,
    pub config: Arc<Config>,
    pub pool: PgPool,
    pub ready: Arc<AtomicBool>,
}
