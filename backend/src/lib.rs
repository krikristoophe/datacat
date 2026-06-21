//! Datacat — bibliothèque de l'API d'ingestion.
//!
//! Frontières nettes (cahier §9) : `ingest` (écriture) / `db` (stockage) / et plus tard la
//! lecture, découplés. Le cœur d'ingestion n'a aucune dépendance vers une couche de lecture.

#![forbid(unsafe_code)]

pub mod config;
pub mod db;
pub mod error;
pub mod ingest;
pub mod model;
pub mod ratelimit;
pub mod routes;
pub mod security;
pub mod telemetry;
pub mod token;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use sqlx::PgPool;
use tower::ServiceBuilder;
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::config::{Config, CorsOrigins, ValidationLimits};
use crate::ingest::{IngestMetrics, Ingestor};
use crate::ratelimit::RateLimiter;
use crate::security::AnomalyGuard;
use crate::token::TokenVerifier;

/// État partagé par tous les handlers (cloné par requête ; tout est derrière `Arc`).
#[derive(Clone)]
pub struct AppState {
    pub ingestor: Ingestor,
    pub limiter: Arc<RateLimiter>,
    pub verifier: Arc<TokenVerifier>,
    pub anomaly: Arc<AnomalyGuard>,
    pub limits: Arc<ValidationLimits>,
    pub config: Arc<Config>,
    pub metrics: Arc<IngestMetrics>,
    pub pool: PgPool,
    pub ready: Arc<AtomicBool>,
}

/// Construit le routeur avec ses garde-fous (CORS, limite de taille, timeout, traçage).
pub fn build_router(state: AppState) -> Router {
    let cors = build_cors(&state.config.cors);
    let body_limit = state.config.limits.max_payload_bytes;
    let timeout = state.config.request_timeout;

    Router::new()
        .route("/v1/events", post(routes::ingest))
        .route("/healthz", get(routes::healthz))
        .route("/readyz", get(routes::readyz))
        .route("/stats", get(routes::stats))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    timeout,
                ))
                // Borne la taille du corps (anti-abus). Au-delà → 413.
                .layer(axum::extract::DefaultBodyLimit::max(body_limit))
                .layer(cors),
        )
        .with_state(state)
}

fn build_cors(origins: &CorsOrigins) -> CorsLayer {
    let base = CorsLayer::new()
        .allow_methods([Method::POST, Method::GET, Method::OPTIONS])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    match origins {
        CorsOrigins::Any => base.allow_origin(Any),
        CorsOrigins::List(list) => {
            let parsed: Vec<HeaderValue> = list.iter().filter_map(|o| o.parse().ok()).collect();
            base.allow_origin(parsed)
        }
    }
}
