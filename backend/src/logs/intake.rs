//! Logique d'admission des logs, partagée par les transports HTTP et gRPC :
//! authentification (token de service), rate limiting par service, bornes, enfilage.

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::error::AppError;
use crate::logs::LogsParse;
use crate::security::{check_service_token, Decision};
use crate::AppState;

/// Authentifie une requête de logs selon `logs_auth` (token de service statique / JWT / aucune).
/// `token` = credential présenté (en-tête HTTP `Authorization: Bearer` ou métadonnée gRPC).
pub fn authorize_logs(
    state: &AppState,
    ip: IpAddr,
    now: Instant,
    token: Option<&str>,
) -> Result<(), AppError> {
    check_service_token(&state.config.logs_auth, &state.verifier, token).map_err(|msg| {
        state.anomaly.record_bad(ip, now);
        AppError::Unauthorized(msg)
    })
}

/// Applique les bornes, le rate limiting (par `service.name`) et enfile les logs.
/// Retourne `(total_retenus, enfilés)` ; `total - enfilés` = écartés (back-pressure).
pub fn accept_logs(
    state: &AppState,
    ip: IpAddr,
    now: Instant,
    mut parse: LogsParse,
) -> Result<(u64, u64), AppError> {
    if parse.stored.len() > state.config.max_logs_records {
        state.anomaly.record_bad(ip, now);
        return Err(AppError::PayloadTooLarge(format!(
            "{} LogRecords > maximum {}",
            parse.stored.len(),
            state.config.max_logs_records
        )));
    }

    // Coût du rate limiting = nombre de records SOUMIS (borné par max_logs_records ci-dessus),
    // capturé AVANT d'écarter les surdimensionnés : sinon une requête pleine de records géants
    // tous écartés ne coûterait qu'un jeton et contournerait le débit réel (revue de sécurité).
    let submitted = parse.stored.len();

    // Garde-fou de taille par enregistrement (S-7) : un seul log surdimensionné est écarté
    // (perte tolérée §2) même si la requête entière reste sous `max_payload_bytes`.
    crate::ingest::drop_oversized(
        &mut parse.stored,
        state.config.limits.max_otlp_record_bytes,
        |l| l.approx_content_bytes(),
        &state.logs.metrics,
        "logs",
    );

    // Rate limiting : la clé fine est le `service.name` (source de confiance pour des logs
    // service-à-service), à défaut l'IP. Le plafond par IP limite alors le nombre de services
    // distincts par IP, et le filet global protège l'infrastructure.
    let service_key = parse
        .stored
        .first()
        .and_then(|l| l.service_name.clone())
        .unwrap_or_else(|| ip.to_string());
    let cost = submitted.max(1) as u32;
    if let Decision::Deny {
        scope,
        retry_after_secs,
    } = state.limiter.check(now, ip, &service_key, cost)
    {
        state.anomaly.record_bad(ip, now);
        return Err(AppError::RateLimited {
            scope,
            retry_after_secs,
        });
    }

    if parse.dropped_skew > 0 {
        state
            .logs
            .metrics
            .dropped_skew_total
            .fetch_add(parse.dropped_skew, Ordering::Relaxed);
    }

    let accepted = std::mem::take(&mut parse.stored);
    let total = accepted.len() as u64;
    let enqueued = state.logs.try_enqueue(accepted) as u64;
    Ok((total, enqueued))
}
