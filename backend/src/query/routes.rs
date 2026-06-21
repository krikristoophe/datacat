//! Handlers HTTP de lecture (`/v1/query/*`). Lecture seule, authentifiés par `query_auth`.
//! Toute la logique de requête vit dans [`crate::query::engine`] (partagée avec le serveur MCP).

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap};
use axum::response::IntoResponse;
use axum::Json;

use crate::error::{AppError, AppResult};
use crate::query::engine::{self, EventsParams, JourneysParams, LogsParams, MetricsParams};
use crate::security::check_service_token;
use crate::AppState;

/// Extrait le token `Authorization: Bearer …`.
pub(crate) fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
                .map(|s| s.trim().to_string())
                .filter(|t| !t.is_empty())
        })
}

/// Authentifie une requête de lecture (`query_auth`).
pub(crate) fn authorize_query(state: &AppState, headers: &HeaderMap) -> AppResult<()> {
    check_service_token(
        &state.config.query_auth,
        &state.verifier,
        bearer(headers).as_deref(),
    )
    .map_err(AppError::Unauthorized)
}

pub async fn query_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<LogsParams>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    Ok(Json(engine::search_logs(&state.pool, &p).await?))
}

pub async fn query_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<EventsParams>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    Ok(Json(engine::search_events(&state.pool, &p).await?))
}

pub async fn query_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<MetricsParams>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    Ok(Json(engine::search_metrics(&state.pool, &p).await?))
}

pub async fn query_journeys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(p): Query<JourneysParams>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    Ok(Json(engine::frequent_journeys(&state.pool, &p).await?))
}

pub async fn query_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    Ok(Json(engine::get_trace(&state.pool, &trace_id).await?))
}
