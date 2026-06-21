//! Handlers HTTP : ingestion (batch) + santé/observabilité.

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use axum::extract::{ConnectInfo, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::Json;
use chrono::Utc;
use serde_json::json;

use crate::error::{AppError, AppResult};
use crate::events::model::{check_event, EventCheck, IngestBody, StructuralError};
use crate::logs::{accept_logs, authorize_logs, otlp_to_logs, ExportLogsServiceRequest};
use crate::metrics::{
    accept_metric_points, authorize_metrics, otlp_to_metrics, ExportMetricsServiceRequest,
};
use crate::security::{self, Decision};
use crate::traces::{accept_spans, authorize_traces, otlp_to_spans, ExportTraceServiceRequest};
use crate::AppState;

/// `POST /v1/events` — ingestion d'un batch d'events.
///
/// Acquittement immédiat (202) ; l'écriture en base est asynchrone (micro-batch). La
/// déduplication a lieu en base : `received` est le nombre d'events acceptés pour écriture,
/// pas le nombre d'insertions réelles.
pub async fn ingest_events(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> AppResult<impl IntoResponse> {
    let now = Instant::now();
    let ip = security::client_ip(&headers, peer.ip(), state.config.trust_forwarded_for);

    // 0. IP bannie pour comportement anormal ?
    if state.anomaly.is_banned(ip, now) {
        return Err(AppError::RateLimited {
            scope: "anomaly_ban",
            retry_after_secs: 60,
        });
    }

    // 1. Parsing strict du corps JSON.
    let parsed: IngestBody = serde_json::from_slice(&body).map_err(|e| {
        state.anomaly.record_bad(ip, now);
        tracing::debug!(error = %e, "corps JSON events invalide");
        AppError::bad_request("corps JSON invalide")
    })?;

    // 2. Bornes du batch.
    if parsed.events.is_empty() {
        return Err(AppError::bad_request("batch vide"));
    }
    if parsed.events.len() > state.limits.max_batch_events {
        state.anomaly.record_bad(ip, now);
        return Err(AppError::PayloadTooLarge(format!(
            "batch de {} events > maximum {}",
            parsed.events.len(),
            state.limits.max_batch_events
        )));
    }

    // 3. Résolution + vérification du token (clé publique uniquement). La session de confiance
    //    pour le rate limiting provient du token (le corps des events n'est pas fiable).
    let session_key = if state.verifier.enabled() {
        let token = extract_token(&headers, parsed.token.as_deref()).ok_or_else(|| {
            state.anomaly.record_bad(ip, now);
            AppError::Unauthorized("token d'ingestion requis".into())
        })?;
        match state.verifier.verify(token) {
            Ok(v) => v.session_id,
            Err(msg) => {
                state.anomaly.record_bad(ip, now);
                return Err(AppError::Unauthorized(msg));
            }
        }
    } else {
        // Dev local (token désactivé) : repli sur la session du premier event, sinon l'IP.
        parsed
            .events
            .first()
            .map(|e| e.session_id.clone())
            .unwrap_or_else(|| ip.to_string())
    };

    // 4. Rate limiting à deux niveaux + filet global.
    let n = parsed.events.len() as u32;
    if let Decision::Deny {
        scope,
        retry_after_secs,
    } = state.limiter.check(now, ip, &session_key, n)
    {
        state.anomaly.record_bad(ip, now);
        return Err(AppError::RateLimited {
            scope,
            retry_after_secs,
        });
    }

    // 5. Validation stricte + conversion. Erreur structurelle → rejet du batch ;
    //    hors fenêtre de skew → event écarté (perte tolérée).
    let received_at = Utc::now();
    let mut stored = Vec::with_capacity(parsed.events.len());
    let mut dropped_skew = 0u64;
    for (i, ev) in parsed.events.into_iter().enumerate() {
        match check_event(ev, received_at, &state.limits, i) {
            Ok(EventCheck::Ok(se)) => stored.push(se),
            Ok(EventCheck::OutOfSkew) => dropped_skew += 1,
            Err(StructuralError(msg)) => {
                state.anomaly.record_bad(ip, now);
                return Err(AppError::BadRequest {
                    message: "validation échouée".into(),
                    details: vec![msg],
                });
            }
        }
    }
    if dropped_skew > 0 {
        state
            .events
            .metrics
            .dropped_skew_total
            .fetch_add(dropped_skew, Ordering::Relaxed);
    }

    // 6. Enfilage non bloquant (acquittement immédiat).
    let received = state.events.try_enqueue(stored);

    Ok((StatusCode::ACCEPTED, Json(json!({ "received": received }))))
}

/// `POST /v1/logs` — ingestion de logs techniques au format **OTLP/HTTP JSON**
/// (`ExportLogsServiceRequest`). Compatible avec tout SDK OpenTelemetry / Collector
/// (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`).
///
/// Acquittement OTLP : `200` + `ExportLogsServiceResponse` (`partialSuccess` si des
/// enregistrements ont été écartés).
pub async fn ingest_logs(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> AppResult<impl IntoResponse> {
    let now = Instant::now();
    let ip = security::client_ip(&headers, peer.ip(), state.config.trust_forwarded_for);

    if state.anomaly.is_banned(ip, now) {
        return Err(AppError::RateLimited {
            scope: "anomaly_ban",
            retry_after_secs: 60,
        });
    }

    // Authentification de service (token fixe par défaut ; cf. LogsAuth).
    let token = extract_token(&headers, None);
    authorize_logs(&state, ip, now, token)?;

    let req: ExportLogsServiceRequest = serde_json::from_slice(&body).map_err(|e| {
        state.anomaly.record_bad(ip, now);
        // Détail journalisé côté serveur uniquement : l'erreur serde peut contenir un fragment du
        // corps (potentiellement PII) — on ne le renvoie jamais au client (HDS).
        tracing::debug!(error = %e, "corps OTLP JSON invalide");
        AppError::bad_request("corps OTLP JSON invalide")
    })?;

    let parsed = otlp_to_logs(req, Utc::now(), &state.limits);
    let (total, enqueued) = accept_logs(&state, ip, now, parsed)?;
    let rejected = total - enqueued;

    // Réponse OTLP : partialSuccess si des enregistrements n'ont pas été retenus.
    let response = if rejected > 0 {
        json!({ "partialSuccess": { "rejectedLogRecords": rejected, "errorMessage": "back-pressure" } })
    } else {
        json!({})
    };
    Ok((StatusCode::OK, Json(response)))
}

/// `POST /v1/traces` — ingestion de traces au format **OTLP/HTTP JSON**
/// (`ExportTraceServiceRequest`). Même auth de service que les logs.
pub async fn ingest_traces(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> AppResult<impl IntoResponse> {
    let now = Instant::now();
    let ip = security::client_ip(&headers, peer.ip(), state.config.trust_forwarded_for);

    if state.anomaly.is_banned(ip, now) {
        return Err(AppError::RateLimited {
            scope: "anomaly_ban",
            retry_after_secs: 60,
        });
    }

    let token = extract_token(&headers, None);
    authorize_traces(&state, ip, now, token)?;

    let req: ExportTraceServiceRequest = serde_json::from_slice(&body).map_err(|e| {
        state.anomaly.record_bad(ip, now);
        // Détail journalisé côté serveur uniquement : l'erreur serde peut contenir un fragment du
        // corps (potentiellement PII) — on ne le renvoie jamais au client (HDS).
        tracing::debug!(error = %e, "corps OTLP JSON invalide");
        AppError::bad_request("corps OTLP JSON invalide")
    })?;

    let parsed = otlp_to_spans(req, Utc::now(), &state.limits);
    let (total, enqueued) = accept_spans(&state, ip, now, parsed)?;
    let rejected = total - enqueued;

    let response = if rejected > 0 {
        json!({ "partialSuccess": { "rejectedSpans": rejected, "errorMessage": "back-pressure" } })
    } else {
        json!({})
    };
    Ok((StatusCode::OK, Json(response)))
}

/// `POST /v1/metrics` — ingestion de métriques au format **OTLP/HTTP JSON**
/// (`ExportMetricsServiceRequest`). Même auth de service que les logs/traces.
pub async fn ingest_metrics(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> AppResult<impl IntoResponse> {
    let now = Instant::now();
    let ip = security::client_ip(&headers, peer.ip(), state.config.trust_forwarded_for);

    if state.anomaly.is_banned(ip, now) {
        return Err(AppError::RateLimited {
            scope: "anomaly_ban",
            retry_after_secs: 60,
        });
    }

    let token = extract_token(&headers, None);
    authorize_metrics(&state, ip, now, token)?;

    let req: ExportMetricsServiceRequest = serde_json::from_slice(&body).map_err(|e| {
        state.anomaly.record_bad(ip, now);
        // Détail journalisé côté serveur uniquement : l'erreur serde peut contenir un fragment du
        // corps (potentiellement PII) — on ne le renvoie jamais au client (HDS).
        tracing::debug!(error = %e, "corps OTLP JSON invalide");
        AppError::bad_request("corps OTLP JSON invalide")
    })?;

    let parsed = otlp_to_metrics(req, Utc::now(), &state.limits);
    let (total, enqueued) = accept_metric_points(&state, ip, now, parsed)?;
    let rejected = total - enqueued;

    let response = if rejected > 0 {
        json!({ "partialSuccess": { "rejectedDataPoints": rejected, "errorMessage": "back-pressure" } })
    } else {
        json!({})
    };
    Ok((StatusCode::OK, Json(response)))
}

/// Extrait le token : en-tête `Authorization: Bearer` en priorité, sinon champ `token` du corps
/// (repli `sendBeacon`, cf. CONTRACT §1.1). Jamais en query string.
fn extract_token<'a>(headers: &'a HeaderMap, body_token: Option<&'a str>) -> Option<&'a str> {
    if let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(rest) = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
        {
            let token = rest.trim();
            if !token.is_empty() {
                return Some(token);
            }
        }
    }
    body_token.map(str::trim).filter(|t| !t.is_empty())
}

/// `GET /healthz` — liveness (le process répond).
pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

/// `GET /readyz` — readiness (process prêt + base joignable).
pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if !state.ready.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "starting" })),
        );
    }
    match sqlx::query("SELECT 1").execute(&state.pool).await {
        Ok(_) => (StatusCode::OK, Json(json!({ "status": "ready" }))),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "db_unavailable" })),
        ),
    }
}

/// `GET /stats` — compteurs d'observabilité (events + logs).
pub async fn stats(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> AppResult<impl IntoResponse> {
    // `/stats` expose des métriques opérationnelles (sessions/IP suivies, IP bannies, volumes
    // d'ingestion) : authentifié comme la lecture (`query_auth`), pas public. cf. revue de sécurité.
    security::check_service_token(
        &state.config.query_auth,
        &state.verifier,
        crate::query::routes::bearer(&headers).as_deref(),
    )
    .map_err(AppError::Unauthorized)?;
    Ok(Json(json!({
        "events": state.events.metrics.snapshot(),
        "logs": state.logs.metrics.snapshot(),
        "traces": state.spans.metrics.snapshot(),
        "metrics": state.metric_points.metrics.snapshot(),
        "rate_limit": {
            "tracked_sessions": state.limiter.tracked_sessions(),
            "tracked_ips": state.limiter.tracked_ips(),
        },
        "anomaly": { "banned_ips": state.anomaly.banned_count() },
        "companions": state.companions.snapshot().iter().map(|(id, t)| {
            json!({ "id": id, "last_seen": t.to_rfc3339() })
        }).collect::<Vec<_>>(),
    })))
}

/// Heartbeat d'un nœud companion distant (`POST /v1/heartbeat`). Auth service-à-service (logs_auth).
#[derive(serde::Deserialize)]
pub struct Heartbeat {
    pub id: String,
}

pub async fn heartbeat(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    axum::Json(hb): axum::Json<Heartbeat>,
) -> AppResult<impl IntoResponse> {
    security::check_service_token(
        &state.config.logs_auth,
        &state.verifier,
        crate::query::routes::bearer(&headers).as_deref(),
    )
    .map_err(AppError::Unauthorized)?;
    let id = hb.id.trim();
    if id.is_empty() {
        return Err(AppError::bad_request("companion id requis"));
    }
    state.companions.record(id, Utc::now());
    Ok(axum::http::StatusCode::NO_CONTENT)
}
