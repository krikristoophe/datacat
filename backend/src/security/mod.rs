//! Garde-fous de sécurité (cahier §7) : vérification du token, rate limiting, anomalies/IP.

pub mod anomaly;
pub mod ratelimit;
pub mod token;

pub use anomaly::{client_ip, AnomalyGuard};
pub use ratelimit::{Decision, RateLimiter};
pub use token::{TokenVerifier, VerifiedToken};
