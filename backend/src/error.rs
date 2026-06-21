//! Type d'erreur applicatif et conversion en réponse HTTP.
//!
//! Les erreurs internes ne fuitent jamais de détails au client (audit HDS) : elles sont
//! journalisées côté serveur et renvoyées en `500` générique.

use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    /// Validation structurelle échouée : rejet de toute la requête.
    #[error("requête invalide: {message}")]
    BadRequest {
        message: String,
        details: Vec<String>,
    },

    /// Payload ou batch au-delà des bornes.
    #[error("payload trop volumineux: {0}")]
    PayloadTooLarge(String),

    /// Token absent, invalide, expiré ou claims manquants.
    #[error("non autorisé: {0}")]
    Unauthorized(String),

    /// Un des niveaux de rate limiting a été atteint.
    #[error("trop de requêtes ({scope})")]
    RateLimited {
        scope: &'static str,
        retry_after_secs: u64,
    },

    /// Service en cours d'arrêt ou non prêt.
    #[error("service indisponible: {0}")]
    Unavailable(String),

    /// Erreur interne : jamais détaillée au client.
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl AppError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest {
            message: message.into(),
            details: Vec::new(),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::BadRequest { message, details } => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": message, "details": details })),
            )
                .into_response(),

            AppError::PayloadTooLarge(message) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(json!({ "error": message })),
            )
                .into_response(),

            AppError::Unauthorized(message) => (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, "Bearer")],
                Json(json!({ "error": message })),
            )
                .into_response(),

            AppError::RateLimited {
                scope,
                retry_after_secs,
            } => (
                StatusCode::TOO_MANY_REQUESTS,
                [(header::RETRY_AFTER, retry_after_secs.to_string())],
                Json(json!({ "error": "rate limit atteint", "scope": scope })),
            )
                .into_response(),

            AppError::Unavailable(message) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": message })),
            )
                .into_response(),

            AppError::Internal(err) => {
                // On journalise la cause réelle, on ne la renvoie jamais au client.
                tracing::error!(error = ?err, "erreur interne");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "erreur interne" })),
                )
                    .into_response()
            }
        }
    }
}

pub type AppResult<T> = Result<T, AppError>;
