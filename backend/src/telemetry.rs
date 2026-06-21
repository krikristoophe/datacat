//! Initialisation de la traçabilité (logs structurés).
//!
//! Logs JSON en production (parsables, traçabilité HDS), texte lisible en dev.
//! Niveau pilotable par `RUST_LOG` (défaut `info`).

use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialise le subscriber global. Idempotent-safe via `try_init` (utile en tests).
pub fn init() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,datacat_ingest=info,sqlx=warn"));

    let json = std::env::var("LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(true);

    let registry = tracing_subscriber::registry().with(filter);

    if json {
        let _ = registry
            .with(fmt::layer().json().with_current_span(true))
            .try_init();
    } else {
        let _ = registry.with(fmt::layer().with_target(true)).try_init();
    }
}
