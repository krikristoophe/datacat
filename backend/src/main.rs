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
use datacat_ingest::metrics::StoredMetricPoint;
use datacat_ingest::security::AnomalyGuard;
use datacat_ingest::security::RateLimiter;
use datacat_ingest::security::TokenVerifier;
use datacat_ingest::traces::StoredSpan;
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
    db::ensure_span_partition_window(&pool, past_days, future_days).await?;
    db::ensure_metric_partition_window(&pool, past_days, future_days).await?;

    for (domain, drained) in [
        ("events", db::drain_staging(&pool).await),
        ("logs", db::drain_log_staging(&pool).await),
        ("traces", db::drain_span_staging(&pool).await),
        ("metrics", db::drain_metric_staging(&pool).await),
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
    if let Err(e) = db::purge_old_span_partitions(&pool, config.retention_days).await {
        tracing::warn!(error = %e, "purge initiale des partitions (traces) ignorée");
    }
    if let Err(e) = db::purge_old_metric_partitions(&pool, config.retention_days).await {
        tracing::warn!(error = %e, "purge initiale des partitions (metrics) ignorée");
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
    let (spans, spans_batcher) = ingest::spawn::<StoredSpan>(
        pool.clone(),
        config.flush_interval,
        config.flush_batch_size,
        config.channel_capacity,
        Arc::new(IngestMetrics::default()),
    );
    let (metric_points, metrics_batcher) = ingest::spawn::<StoredMetricPoint>(
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
        spans,
        metric_points,
        limiter,
        verifier,
        anomaly,
        limits: Arc::new(config.limits.clone()),
        config: Arc::clone(&config),
        pool: pool.clone(),
        ready: Arc::new(AtomicBool::new(true)),
    };

    // --- Signal d'arrêt diffusé à tous les serveurs (HTTP + gRPC + alerting) ---
    let (sd_tx, sd_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = sd_tx.send(true);
    });

    // --- Moteur d'alerting (optionnel) : actif si des règles ET au moins un notifier ---
    spawn_alerting(pool.clone(), Arc::clone(&config), sd_rx.clone());

    // --- Serveur OTLP/gRPC (logs), optionnel ---
    let grpc_handle = if config.grpc_enabled {
        let listener = tokio::net::TcpListener::bind(config.grpc_bind_addr).await?;
        tracing::info!(addr = %config.grpc_bind_addr, "OTLP/gRPC (logs + traces + metrics) à l'écoute");
        let st = state.clone();
        let mut rx = sd_rx.clone();
        Some(tokio::spawn(async move {
            let shutdown = async move {
                let _ = rx.changed().await;
            };
            if let Err(e) = datacat_ingest::grpc::serve(st, listener, shutdown).await {
                tracing::error!(error = %e, "serveur gRPC arrêté sur erreur");
            }
        }))
    } else {
        None
    };

    // --- Serveur HTTP ---
    let app = build_router(state).into_make_service_with_connect_info::<std::net::SocketAddr>();
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    tracing::info!(addr = %config.bind_addr, "datacat-ingest démarré");

    let mut http_rx = sd_rx.clone();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = http_rx.changed().await;
        })
        .await?;

    // --- Arrêt propre : gRPC, flush final des batchers, fermeture du pool ---
    tracing::info!("arrêt en cours — flush final des batchers");
    if let Some(h) = grpc_handle {
        let _ = h.await;
    }
    events_batcher.shutdown().await;
    logs_batcher.shutdown().await;
    spans_batcher.shutdown().await;
    metrics_batcher.shutdown().await;
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
            if let Err(e) = db::ensure_span_partition_window(&pool, past_days, future_days).await {
                tracing::warn!(error = %e, "maintenance: création de partitions (traces) échouée");
            }
            if let Err(e) = db::ensure_metric_partition_window(&pool, past_days, future_days).await
            {
                tracing::warn!(error = %e, "maintenance: création de partitions (metrics) échouée");
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
            match db::purge_old_span_partitions(&pool, config.retention_days).await {
                Ok(n) if n > 0 => {
                    tracing::info!(domain = "traces", dropped = n, "partitions purgées")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "maintenance: purge (traces) échouée"),
            }
            match db::purge_old_metric_partitions(&pool, config.retention_days).await {
                Ok(n) if n > 0 => {
                    tracing::info!(domain = "metrics", dropped = n, "partitions purgées")
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "maintenance: purge (metrics) échouée"),
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

/// Démarre le moteur d'alerting si configuré (fichier de règles + ≥1 notifier). No-op sinon.
fn spawn_alerting(pool: PgPool, config: Arc<Config>, shutdown: tokio::sync::watch::Receiver<bool>) {
    use datacat_ingest::alerting::{
        run_eval_loop, DispatchSettings, Dispatcher, EmailConfig, EmailNotifier, Notifier,
        SlackNotifier,
    };

    let ac = &config.alerting;
    let Some(rules_file) = ac.rules_file.clone() else {
        tracing::info!("alerting désactivé (ALERT_RULES_FILE non défini)");
        return;
    };
    let rules = match datacat_ingest::alerting::load_rules(&rules_file) {
        Ok(r) if !r.is_empty() => r,
        Ok(_) => {
            tracing::warn!(file = %rules_file, "fichier de règles vide — alerting désactivé");
            return;
        }
        Err(e) => {
            tracing::error!(file = %rules_file, error = %e, "chargement des règles échoué — alerting désactivé");
            return;
        }
    };

    // Une règle peut porter ses propres `actions` (webhook/slack/email) : dans ce cas l'alerting
    // est utile même sans notifier global configuré.
    let has_actions = rules.iter().any(|r| !r.actions.is_empty());
    if !ac.has_notifier() && !has_actions {
        tracing::warn!(
            "ALERT_RULES_FILE défini mais aucun notifier global ni action de règle — alerting désactivé"
        );
        return;
    }

    // Client HTTP partagé par tous les canaux (Slack + webhooks génériques).
    let http = reqwest::Client::new();

    // Configuration SMTP de base (repli des actions `email`, et notifier e-mail global).
    let email_base = match (ac.smtp_host.clone(), ac.email_from.clone()) {
        (Some(host), Some(from)) => Some(EmailConfig {
            smtp_host: host,
            smtp_port: ac.smtp_port,
            username: ac.smtp_username.clone(),
            password: ac.smtp_password.clone(),
            from,
            to: ac.email_to.clone(),
        }),
        _ => None,
    };

    // Notifiers globaux par défaut (règles sans `actions`).
    let mut default: Vec<Arc<dyn Notifier>> = Vec::new();
    if let Some(url) = ac.slack_webhook_url.clone() {
        default.push(Arc::new(SlackNotifier::with_client(http.clone(), url)));
    }
    if let Some(base) = &email_base {
        if !base.to.is_empty() {
            match EmailNotifier::new(base) {
                Ok(n) => default.push(Arc::new(n)),
                Err(e) => {
                    tracing::error!(error = %e, "configuration e-mail invalide — canal ignoré")
                }
            }
        }
    }

    let settings = DispatchSettings {
        http,
        slack_webhook_url: ac.slack_webhook_url.clone(),
        email: email_base,
    };
    let dispatcher = Dispatcher::build(&rules, &settings, default);

    let interval = ac.eval_interval;
    tracing::info!(rules = rules.len(), "alerting activé");
    tokio::spawn(run_eval_loop(pool, rules, dispatcher, interval, shutdown));
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
