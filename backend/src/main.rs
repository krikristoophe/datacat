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
use datacat_ingest::settings::{Project, Settings};
use datacat_ingest::traces::StoredSpan;
use datacat_ingest::{build_router, db, telemetry, AppState};

#[tokio::main]
async fn main() -> Result<()> {
    telemetry::init();
    // Configuration unifiée : datacat.toml (multi-projet) ou variables d'environnement (repli).
    let Settings {
        config,
        projects,
        export,
    } = Settings::load()?;
    // Garde-fous production : refuse les relâchements dev-only (CORS `*`, token désactivé) sauf
    // build `--features dev`.
    config.enforce_runtime_guards()?;
    let config = Arc::new(config);

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

    // --- Moteur d'alerting : un évaluateur par projet configuré ---
    spawn_alerting(pool.clone(), projects, sd_rx.clone());

    // --- Export froid planifié (optionnel, feature `export`) ---
    spawn_export(pool.clone(), export, sd_rx.clone());

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

/// Démarre un évaluateur d'alerting par projet configuré (chacun avec ses règles + ses canaux).
fn spawn_alerting(
    pool: PgPool,
    projects: Vec<Project>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if projects.is_empty() {
        tracing::info!("alerting désactivé (aucun projet configuré)");
        return;
    }
    for project in projects {
        spawn_project_alerting(pool.clone(), project, shutdown.clone());
    }
}

fn spawn_project_alerting(
    pool: PgPool,
    project: Project,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    use datacat_ingest::alerting::{
        run_eval_loop, DispatchSettings, Dispatcher, EmailNotifier, Notifier, SlackNotifier,
    };

    if project.rules.is_empty() {
        tracing::info!(project = %project.id, "projet sans règle — alerting ignoré");
        return;
    }
    // Une règle peut porter ses propres `actions` : l'alerting reste utile sans canal global.
    let has_actions = project.rules.iter().any(|r| !r.actions.is_empty());
    let has_channel = project.slack_webhook_url.is_some()
        || project.email.as_ref().is_some_and(|e| !e.to.is_empty());
    if !has_channel && !has_actions {
        tracing::warn!(project = %project.id, "règles présentes mais aucun canal ni action — alerting du projet désactivé");
        return;
    }

    // Client HTTP partagé par tous les canaux (Slack + webhooks génériques) du projet.
    let http = reqwest::Client::new();

    let mut default: Vec<Arc<dyn Notifier>> = Vec::new();
    if let Some(url) = project.slack_webhook_url.clone() {
        default.push(Arc::new(SlackNotifier::with_client(http.clone(), url)));
    }
    if let Some(email) = &project.email {
        if !email.to.is_empty() {
            match EmailNotifier::new(email) {
                Ok(n) => default.push(Arc::new(n)),
                Err(e) => {
                    tracing::error!(project = %project.id, error = %e, "config e-mail invalide — canal ignoré")
                }
            }
        }
    }

    let settings = DispatchSettings {
        http,
        slack_webhook_url: project.slack_webhook_url.clone(),
        email: project.email.clone(),
    };
    let dispatcher = Dispatcher::build(&project.rules, &settings, default);

    tracing::info!(project = %project.id, rules = project.rules.len(), "alerting du projet activé");
    tokio::spawn(run_eval_loop(
        pool,
        project.rules,
        dispatcher,
        project.eval_interval,
        shutdown,
    ));
}

/// Démarre l'export froid planifié si la feature `export` est compilée et l'export configuré.
#[cfg(feature = "export")]
fn spawn_export(
    pool: PgPool,
    export: Option<datacat_ingest::settings::ExportSettings>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let Some(export) = export else {
        return;
    };
    tracing::info!(
        bucket = %export.bucket,
        schedule = ?export.schedule,
        "export froid planifié activé"
    );
    tokio::spawn(run_export_loop(pool, export, shutdown));
}

#[cfg(not(feature = "export"))]
fn spawn_export(
    _pool: PgPool,
    export: Option<datacat_ingest::settings::ExportSettings>,
    _shutdown: tokio::sync::watch::Receiver<bool>,
) {
    if export.is_some() {
        tracing::warn!("[export] configuré mais binaire compilé sans la feature `export` — ignoré");
    }
}

/// Boucle d'export : à chaque tick, exporte la veille (UTC) vers Parquet/S3 pour chaque table.
#[cfg(feature = "export")]
async fn run_export_loop(
    pool: PgPool,
    export: datacat_ingest::settings::ExportSettings,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    use datacat_ingest::settings::ExportTable;

    let store = {
        let cfg = datacat_exporter::config::Config {
            database_url: String::new(), // inutilisé pour construire l'object store
            s3_endpoint: export.endpoint.clone(),
            s3_region: export.region.clone(),
            aws_access_key_id: export.access_key_id.clone(),
            aws_secret_access_key: export.secret_access_key.clone(),
            s3_allow_http: export.allow_http,
        };
        match datacat_exporter::config::build_object_store(&cfg, &export.bucket) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "object store invalide — export désactivé");
                return;
            }
        }
    };

    let mut ticker = interval(export.schedule);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("export froid arrêté");
                break;
            }
            _ = ticker.tick() => {
                // Réexporte une petite fenêtre de jours UTC complets (J-1 et J-2) à chaque tick.
                // L'export est idempotent (écrase l'objet du jour) : ce recouvrement garantit
                // qu'aucun jour n'est sauté même si un tick est manqué (process arrêté autour de
                // minuit, intervalle non aligné sur l'horloge). cf. revue de code.
                let now = chrono::Utc::now();
                let prefix = export.prefix.as_deref();
                for back in 1..=2i64 {
                    let date = (now - chrono::Duration::days(back)).date_naive();
                    for table in &export.tables {
                        let result = match table {
                            ExportTable::Events => {
                                datacat_exporter::export::export_events(&pool, &store, date, &export.bucket, prefix).await
                            }
                            ExportTable::Logs => {
                                datacat_exporter::export::export_logs(&pool, &store, date, &export.bucket, prefix).await
                            }
                        };
                        match result {
                            Ok(rows) => tracing::info!(?table, %date, rows, "export froid terminé"),
                            Err(e) => tracing::error!(?table, %date, error = %e, "export froid échoué"),
                        }
                    }
                }
            }
        }
    }
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
