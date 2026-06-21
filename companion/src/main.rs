//! `datacat-companion` binary entry point.
//!
//! Initializes tracing, loads the TOML config, builds the agent, and runs the heartbeat loop until
//! SIGINT or SIGTERM.

#![forbid(unsafe_code)]

use anyhow::Result;
use datacat_companion::{Agent, Config};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datacat_companion=info".parse().unwrap()),
        )
        .init();

    let config = Config::load()?;
    let agent = Agent::new(config)?;
    agent.run(shutdown_signal()).await;
    Ok(())
}

/// Resolve once either SIGINT (Ctrl-C) or SIGTERM is received.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "failed to listen for SIGINT");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received SIGINT"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}
