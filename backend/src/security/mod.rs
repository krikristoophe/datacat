//! Garde-fous de sécurité (cahier §7) : vérification du token, rate limiting, anomalies/IP.

pub mod anomaly;
pub mod ratelimit;
pub mod token;

pub use anomaly::{client_ip, AnomalyGuard};
pub use ratelimit::{Decision, RateLimiter};
pub use token::{TokenVerifier, VerifiedToken};

use crate::config::LogsAuth;

/// Vérifie un credential de service selon le mode d'auth (token statique / JWT / aucun).
/// Partagé par l'ingestion télémétrie (logs, traces) et la couche de lecture.
pub fn check_service_token(
    auth: &LogsAuth,
    verifier: &TokenVerifier,
    token: Option<&str>,
) -> Result<(), String> {
    match auth {
        LogsAuth::None => Ok(()),
        LogsAuth::Static(secret) => {
            let provided = token.ok_or_else(|| "token de service requis".to_string())?;
            if constant_time_eq(provided.as_bytes(), secret.as_bytes()) {
                Ok(())
            } else {
                Err("token de service invalide".to_string())
            }
        }
        LogsAuth::Jwt => {
            let provided = token.ok_or_else(|| "token de service requis".to_string())?;
            verifier.verify(provided).map(|_| ())
        }
    }
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
