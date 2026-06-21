//! Couche HTTP : assemblage du routeur, garde-fous transverses (CORS, taille, timeout,
//! traçage) et handlers.

pub mod routes;

use axum::extract::{Request, State};
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::{from_fn_with_state, Next};
use axum::response::Response;
use axum::routing::{get, post};
use axum::Router;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::config::CorsOrigins;
use crate::error::{AppError, AppResult};
use crate::AppState;

/// Construit le routeur avec ses garde-fous (CORS, limite de taille, timeout, traçage).
pub fn build_router(state: AppState) -> Router {
    let cors = build_cors(&state.config.cors);
    let body_limit = state.config.limits.max_payload_bytes;
    let logs_body_limit = state.config.max_logs_payload_bytes;
    let timeout = state.config.request_timeout;

    // Les flux OTLP (logs, traces, métriques) ont leur propre (plus grande) limite de corps :
    // routes isolées puis fusionnées. La limite la plus interne l'emporte pour ces routes.
    let otlp_routes = Router::new()
        .route("/v1/logs", post(routes::ingest_logs))
        .route("/v1/traces", post(routes::ingest_traces))
        .route("/v1/metrics", post(routes::ingest_metrics))
        .layer(axum::extract::DefaultBodyLimit::max(logs_body_limit));

    let app = Router::new()
        .route("/v1/events", post(routes::ingest_events))
        .route("/healthz", get(routes::healthz))
        .route("/readyz", get(routes::readyz))
        .route("/stats", get(routes::stats))
        // Couche de lecture (lecture seule, authentifiée par query_auth).
        .route("/v1/query/logs", get(crate::query::routes::query_logs))
        .route(
            "/v1/query/metrics",
            get(crate::query::routes::query_metrics),
        )
        .route("/v1/query/events", get(crate::query::routes::query_events))
        .route(
            "/v1/query/traces/{trace_id}",
            get(crate::query::routes::query_trace),
        )
        .route(
            "/v1/query/journeys",
            get(crate::query::routes::query_journeys),
        )
        .route("/v1/query/sql", post(crate::query::routes::query_sql))
        .merge(otlp_routes)
        .layer(
            ServiceBuilder::new()
                // Outermost : convertit tout panic d'un handler en 500 au lieu de crasher le
                // process (disponibilité HDS). Requiert l'unwind (profil release sans panic=abort).
                .layer(CatchPanicLayer::new())
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    timeout,
                ))
                // Borne la taille du corps (anti-abus). Au-delà → 413.
                .layer(axum::extract::DefaultBodyLimit::max(body_limit)),
        );

    // Serveur MCP HTTP (streamable) monté sur /mcp, hors du timeout global (le SSE est
    // long-vécu), protégé par `query_auth`. Tools en in-process sur la couche de lecture.
    let app = if state.config.mcp_enabled {
        let mcp = Router::new()
            .nest_service("/mcp", crate::query::mcp::service(state.clone()))
            .layer(from_fn_with_state(state.clone(), mcp_auth))
            .layer(
                ServiceBuilder::new()
                    .layer(CatchPanicLayer::new())
                    .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                    .layer(PropagateRequestIdLayer::x_request_id())
                    .layer(TraceLayer::new_for_http()),
            );
        app.merge(mcp)
    } else {
        app
    };

    app.layer(cors).with_state(state)
}

/// Middleware d'authentification du serveur MCP (`query_auth`).
async fn mcp_auth(State(state): State<AppState>, req: Request, next: Next) -> AppResult<Response> {
    let token = crate::query::routes::bearer(req.headers());
    crate::security::check_service_token(
        &state.config.query_auth,
        &state.verifier,
        token.as_deref(),
    )
    .map_err(AppError::Unauthorized)?;
    Ok(next.run(req).await)
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // oneshot

    async fn boom() -> StatusCode {
        panic!("panic volontaire dans un handler de test")
    }

    /// Un panic dans un handler est rattrapé par CatchPanicLayer → 500 (le process ne crashe pas).
    #[tokio::test]
    async fn handler_panic_becomes_500() {
        let app = Router::new()
            .route("/boom", get(boom))
            .layer(CatchPanicLayer::new());

        let resp = app
            .oneshot(Request::builder().uri("/boom").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
