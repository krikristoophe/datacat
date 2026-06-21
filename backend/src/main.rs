//! Point d'entrée du service d'ingestion Datacat.

#![forbid(unsafe_code)]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use sqlx::PgPool;
use tokio::time::{interval, Duration};

use datacat_ingest::config::Config;
use datacat_ingest::events::model::StoredEvent;
use datacat_ingest::ingest::{self, IngestMetrics};
use datacat_ingest::logs::StoredLog;
use datacat_ingest::security::AnomalyGuard;
use datacat_ingest::security::RateLimiter;
use datacat_ingest::security::TokenVerifier;
use datacat_ingest::{build_router, db, telemetry, AppState};

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::init();
    let config = Arc::new(Config::from_env()?);

    // --- Base de données : connexion, migrations, partitions ---
    let pool = db::connect(&config.database_url, config.db_max_connections).await?;
    db::run_migrations(&pool).await?;

    let past_days = (config.limits.max_past_skew.as_secs() / 86_400) as i64 + 1;
    let future_days = std::cmp::max(
        config.partition_future_days,
        (config.limits.max_future_skew.as_secs() / 86_400) as i64 + 1,
    );
    db::ensure_partition_window(&pool, past_days, future_days).await?;
    db::ensure_log_partition_window(&pool, past_days, future_days).await?;

    for (domain, drained) in [
        ("events", db::drain_staging(&pool).await),
        ("logs", db::drain_log_staging(&pool).await),
    ] {
        match drained {
            Ok(n) if n > 0 => tracing::info!(domain, merged = n, "staging résiduel fusionné"),
            Ok(_) => {}
            Err(e) => tracing::warn!(domain, error = %e, "drain du staging au démarrage ignoré"),
        }
    }
    if let Err(e) = db::purge_old_partitions(&pool, config.retention_days).await {
        tracing::warn!(error = %e, "purge initiale des partitions (events) ignorée");
    }
    if let Err(e) = db::purge_old_log_partitions(&pool, config.retention_days).await {
        tracing::warn!(error = %e, "purge initiale des partitions (logs) ignorée");
    }

    // --- Composants d'ingestion (un batcher par domaine) ---
    let (events, events_batcher) = ingest::spawn::<StoredEvent>(
        pool.clone(),
        config.flush_interval,
        config.flush_batch_size,
        config.channel_capacity,
        Arc::new(IngestMetrics::default()),
    );
    let (logs, logs_batcher) = ingest::spawn::<StoredLog>(
        pool.clone(),
        config.flush_interval,
        config.flush_batch_size,
        config.channel_capacity,
        Arc::new(IngestMetrics::default()),
    );

    let verifier = TokenVerifier::new(&config.token).await?;
    verifier.spawn_refresh();
    if !verifier.enabled() {
        tracing::warn!(
            "VÉRIFICATION DU TOKEN DÉSACTIVÉE — dev local uniquement, jamais en production"
        );
    }

    let limiter = Arc::new(RateLimiter::new(config.rate_limit.clone(), Instant::now()));
    let anomaly = Arc::new(AnomalyGuard::new(config.anomaly.clone()));

    spawn_maintenance(
        pool.clone(),
        Arc::clone(&config),
        Arc::clone(&limiter),
        Arc::clone(&anomaly),
        past_days,
        future_days,
    );

    let state = AppState {
        events,
        logs,
        limiter,
        verifier,
        anomaly,
        limits: Arc::new(config.limits.clone()),
        config: Arc::clone(&config),
        pool: pool.clone(),
        ready: Arc::new(AtomicBool::new(true)),
    };

    // --- Serveur HTTP ---
    let app = build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>();
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "datacat-ingest démarré");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    // --- Arrêt propre : flush final des batchers, fermeture du pool ---
    tracing::info!("arrêt en cours — flush final des batchers");
    events_batcher.shutdown().await;
    logs_batcher.shutdown().await;
    pool.close().await;
    tracing::info!("arrêt terminé");
    Ok(())
}

/// Tâches de fond : maintenance des partitions (horaire) et purge mémoire des limiteurs (minute).
fn spawn_maintenance(
    pool: PgPool,
    config: Arc<Config>,
    limiter: Arc<RateLimiter>,
    anomaly: Arc<AnomalyGuard>,
    past_days: i64,
    future_days: i64,
) {
    // Partitions : création anticipée + purge de rétention.
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(3_600));
        loop {
            tick.tick().await;
            if let Err(e) = db::ensure_partition_window(&pool, past_days, future_days).await {
                tracing::warn!(error = %e, "maintenance: création de partitions (events) échouée");
            }
            if let Err(e) = db::ensure_log_partition_window(&pool, past_days, future_days).await {
                tracing::warn!(error = %e, "maintenance: création de partitions (logs) échouée");
            }
            match db::purge_old_partitions(&pool, config.retention_days).await {
                Ok(n) if n > 0 => {
                    tracing::info!(domain = "events", dropped = n, "partitions purgées")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "maintenance: purge (events) échouée"),
            }
            match db::purge_old_log_partitions(&pool, config.retention_days).await {
                Ok(n) if n > 0 => {
                    tracing::info!(domain = "logs", dropped = n, "partitions purgées")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "maintenance: purge (logs) échouée"),
            }
        }
    });

    // Purge mémoire des structures de rate limiting / anomalies.
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let now = Instant::now();
            limiter.prune(now);
            anomaly.prune(now);
        }
    });
}

/// Attend Ctrl-C ou SIGTERM pour déclencher l'arrêt propre.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("installation du handler Ctrl-C");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installation du handler SIGTERM")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
