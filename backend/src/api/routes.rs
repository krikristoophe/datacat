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
use crate::security::{self, Decision};
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
        AppError::bad_request(format!("JSON invalide: {e}"))
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
            .metrics
            .dropped_skew_total
            .fetch_add(dropped_skew, Ordering::Relaxed);
    }

    // 6. Enfilage non bloquant (acquittement immédiat).
    let received = state.ingestor.try_enqueue(stored);

    Ok((StatusCode::ACCEPTED, Json(json!({ "received": received }))))
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

/// `GET /stats` — compteurs d'observabilité.
pub async fn stats(State(state): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "ingest": state.metrics.snapshot(),
        "rate_limit": {
            "tracked_sessions": state.limiter.tracked_sessions(),
            "tracked_ips": state.limiter.tracked_ips(),
        },
        "anomaly": { "banned_ips": state.anomaly.banned_count() },
    }))
}
