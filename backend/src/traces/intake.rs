//! Admission des spans : bornes, rate limiting par service, enfilage.
//! L'authentification réutilise celle des logs (token de service, `crate::logs::authorize_logs`).

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::error::AppError;
use crate::security::Decision;
use crate::traces::SpansParse;
use crate::AppState;

pub use crate::logs::authorize_logs as authorize_traces;

/// Applique les bornes, le rate limiting (par `service.name`) et enfile les spans.
pub fn accept_spans(
    state: &AppState,
    ip: IpAddr,
    now: Instant,
    mut parse: SpansParse,
) -> Result<(u64, u64), AppError> {
    if parse.stored.len() > state.config.max_logs_records {
        state.anomaly.record_bad(ip, now);
        return Err(AppError::PayloadTooLarge(format!(
            "{} spans > maximum {}",
            parse.stored.len(),
            state.config.max_logs_records
        )));
    }

    let service_key = parse
        .stored
        .first()
        .and_then(|s| s.service_name.clone())
        .unwrap_or_else(|| ip.to_string());
    // Coût = nombre de spans (borné ci-dessus), pas 1 (cf. revue de sécurité).
    let cost = parse.stored.len().max(1) as u32;
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
            .spans
            .metrics
            .dropped_skew_total
            .fetch_add(parse.dropped_skew, Ordering::Relaxed);
    }

    let accepted = std::mem::take(&mut parse.stored);
    let total = accepted.len() as u64;
    let enqueued = state.spans.try_enqueue(accepted) as u64;
    Ok((total, enqueued))
}
