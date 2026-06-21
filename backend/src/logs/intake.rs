//! Logique d'admission des logs, partagée par les transports HTTP et gRPC :
//! authentification (token de service), rate limiting par service, bornes, enfilage.

use std::net::IpAddr;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::config::LogsAuth;
use crate::error::AppError;
use crate::logs::LogsParse;
use crate::security::Decision;
use crate::AppState;

/// Authentifie une requête de logs selon `LogsAuth` (statique / JWT / aucune).
/// `token` = credential présenté (en-tête HTTP `Authorization: Bearer` ou métadonnée gRPC).
pub fn authorize_logs(
    state: &AppState,
    ip: IpAddr,
    now: Instant,
    token: Option<&str>,
) -> Result<(), AppError> {
    match &state.config.logs_auth {
        LogsAuth::None => Ok(()),
        LogsAuth::Static(secret) => {
            let provided = token.ok_or_else(|| {
                state.anomaly.record_bad(ip, now);
                AppError::Unauthorized("token de service requis".into())
            })?;
            if constant_time_eq(provided.as_bytes(), secret.as_bytes()) {
                Ok(())
            } else {
                state.anomaly.record_bad(ip, now);
                Err(AppError::Unauthorized("token de service invalide".into()))
            }
        }
        LogsAuth::Jwt => {
            let provided = token.ok_or_else(|| {
                state.anomaly.record_bad(ip, now);
                AppError::Unauthorized("token de service requis".into())
            })?;
            state.verifier.verify(provided).map(|_| ()).map_err(|msg| {
                state.anomaly.record_bad(ip, now);
                AppError::Unauthorized(msg)
            })
        }
    }
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

    // Rate limiting : la clé fine est le `service.name` (source de confiance pour des logs
    // service-à-service), à défaut l'IP. Le plafond par IP limite alors le nombre de services
    // distincts par IP, et le filet global protège l'infrastructure.
    let service_key = parse
        .stored
        .first()
        .and_then(|l| l.service_name.clone())
        .unwrap_or_else(|| ip.to_string());
    if let Decision::Deny {
        scope,
        retry_after_secs,
    } = state.limiter.check(now, ip, &service_key, 1)
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

/// Comparaison à temps constant (anti-timing) ; la différence de longueur est révélée (standard).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_basic() {
        assert!(constant_time_eq(b"secret-token", b"secret-token"));
        assert!(!constant_time_eq(b"secret-token", b"secret-toketn"));
        assert!(!constant_time_eq(b"short", b"longer-value"));
        assert!(constant_time_eq(b"", b""));
    }
}
