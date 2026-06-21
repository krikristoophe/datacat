//! Utilitaires partagés par les tests d'intégration : démarrage de l'app sur un port
//! éphémère, signature de tokens de test, attente de persistance.

// Module partagé par plusieurs binaires de test ; chacun n'en utilise qu'un sous-ensemble.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use sqlx::PgPool;

use datacat_ingest::config::{
    AlertingConfig, AnomalyConfig, Config, CorsOrigins, KeySource, LogsAuth, RateLimitConfig,
    TokenConfig, ValidationLimits,
};
use datacat_ingest::events::model::StoredEvent;
use datacat_ingest::ingest::{self, BatcherHandle, IngestMetrics};
use datacat_ingest::logs::StoredLog;
use datacat_ingest::metrics::StoredMetricPoint;
use datacat_ingest::security::{AnomalyGuard, RateLimiter, TokenVerifier};
use datacat_ingest::traces::StoredSpan;
use datacat_ingest::{build_router, db, AppState};

pub const ED_PUBLIC: &str = include_str!("../fixtures/ed25519_public.pem");
pub const ED_PRIVATE: &str = include_str!("../fixtures/ed25519_private.pem");
pub const RSA_PUBLIC: &str = include_str!("../fixtures/rsa_public.pem");
pub const RSA_PRIVATE: &str = include_str!("../fixtures/rsa_private.pem");

#[derive(Serialize)]
struct TestClaims {
    actor_id: String,
    session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant_id: Option<String>,
    sub: String,
    iat: i64,
    exp: i64,
}

/// Signe un token EdDSA de test (clé privée fixture). `ttl_secs` négatif ⇒ token déjà expiré.
pub fn mint_ed(actor: &str, session: &str, ttl_secs: i64) -> String {
    mint(
        ED_PRIVATE.as_bytes(),
        Algorithm::EdDSA,
        actor,
        session,
        None,
        ttl_secs,
    )
}

pub fn mint(
    private_pem: &[u8],
    alg: Algorithm,
    actor: &str,
    session: &str,
    tenant: Option<&str>,
    ttl_secs: i64,
) -> String {
    let now = Utc::now().timestamp();
    let claims = TestClaims {
        actor_id: actor.to_string(),
        session_id: session.to_string(),
        tenant_id: tenant.map(str::to_string),
        sub: actor.to_string(),
        iat: now,
        exp: now + ttl_secs,
    };
    let key = match alg {
        Algorithm::EdDSA => EncodingKey::from_ed_pem(private_pem).unwrap(),
        Algorithm::RS256 => EncodingKey::from_rsa_pem(private_pem).unwrap(),
        _ => unreachable!(),
    };
    encode(&Header::new(alg), &claims, &key).unwrap()
}

/// Construit une configuration de test. `tweak` permet d'ajuster (rate limits, etc.).
pub fn test_config(token: TokenConfig, tweak: impl FnOnce(&mut Config)) -> Config {
    let mut cfg = Config {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        grpc_enabled: false,
        grpc_bind_addr: "127.0.0.1:0".parse().unwrap(),
        database_url: String::new(),
        db_max_connections: 5,
        flush_interval: Duration::from_millis(40),
        flush_batch_size: 10_000,
        channel_capacity: 200_000,
        retention_days: 90,
        partition_future_days: 2,
        max_logs_records: 2_048,
        max_logs_payload_bytes: 4_194_304,
        request_timeout: Duration::from_secs(15),
        trust_forwarded_for: false,
        limits: ValidationLimits {
            max_batch_events: 500,
            max_payload_bytes: 1_048_576,
            max_properties_bytes: 16_384,
            max_string_len: 200,
            max_json_depth: 16,
            max_past_skew: Duration::from_secs(31 * 86_400),
            max_future_skew: Duration::from_secs(86_400),
        },
        rate_limit: RateLimitConfig {
            global_per_sec: 1_000_000.0,
            global_burst: 1_000_000.0,
            session_per_sec: 10_000.0,
            session_burst: 100_000.0,
            ip_session_cap: 100_000,
            ip_session_window: Duration::from_secs(1_800),
            max_tracked_sessions: 1_000_000,
            max_tracked_ips: 1_000_000,
        },
        anomaly: AnomalyConfig {
            bad_requests_threshold: 1_000_000,
            window: Duration::from_secs(60),
            ban_duration: Duration::from_secs(300),
            max_tracked_ips: 1_000_000,
        },
        token,
        // Par défaut, les logs sont authentifiés par JWT (les tests logs envoient un JWT).
        // Les tests du token statique surchargent via `tweak`.
        logs_auth: LogsAuth::Jwt,
        // Lecture ouverte par défaut en test ; un test dédié surcharge en Static.
        query_auth: LogsAuth::None,
        mcp_enabled: true,
        cors: CorsOrigins::Any,
        alerting: AlertingConfig {
            rules_file: None,
            eval_interval: Duration::from_secs(60),
            slack_bot_token: None,
            slack_channel: None,
            smtp_host: None,
            smtp_port: 587,
            smtp_username: None,
            smtp_password: None,
            email_from: None,
            email_to: vec![],
        },
    };
    tweak(&mut cfg);
    cfg
}

pub fn token_enabled_ed() -> TokenConfig {
    TokenConfig {
        enabled: true,
        key_source: Some(KeySource::Pem {
            pem: ED_PUBLIC.to_string(),
            alg: Algorithm::EdDSA,
            kid: None,
        }),
        algorithms: vec![Algorithm::EdDSA, Algorithm::RS256],
        issuer: None,
        audience: None,
        leeway_secs: 5,
        jwks_refresh: Duration::from_secs(3_600),
    }
}

pub fn token_disabled() -> TokenConfig {
    TokenConfig {
        enabled: false,
        key_source: None,
        algorithms: vec![Algorithm::EdDSA],
        issuer: None,
        audience: None,
        leeway_secs: 5,
        jwks_refresh: Duration::from_secs(3_600),
    }
}

/// App de test démarrée : URL de base + métriques + poignées d'arrêt des batchers.
pub struct TestApp {
    pub base_url: String,
    /// Métriques du domaine events (compat. tests existants).
    pub metrics: Arc<IngestMetrics>,
    /// Métriques du domaine logs.
    pub logs_metrics: Arc<IngestMetrics>,
    /// Métriques (compteurs d'ingestion) du domaine métriques.
    pub metrics_metrics: Arc<IngestMetrics>,
    /// Adresse du serveur OTLP/gRPC de test (logs + traces + métriques).
    pub grpc_addr: SocketAddr,
    pub pool: PgPool,
    batchers: Vec<BatcherHandle>,
}

impl TestApp {
    pub async fn count_events(&self) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM events")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_event_id(&self, id: uuid::Uuid) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM events WHERE event_id = $1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_logs(&self) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM logs")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_spans(&self) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM spans")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    pub async fn count_metrics(&self) -> i64 {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM metric_points")
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// Attend que le nombre total de points de métriques atteigne `expected`.
    pub async fn wait_metrics(&self, expected: i64, timeout: Duration) -> i64 {
        let deadline = Instant::now() + timeout;
        loop {
            let c = self.count_metrics().await;
            if c >= expected || Instant::now() >= deadline {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Attend que le nombre total de spans atteigne `expected`.
    pub async fn wait_spans(&self, expected: i64, timeout: Duration) -> i64 {
        let deadline = Instant::now() + timeout;
        loop {
            let c = self.count_spans().await;
            if c >= expected || Instant::now() >= deadline {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Attend que le nombre total d'events atteigne `expected` (flush asynchrone).
    pub async fn wait_total(&self, expected: i64, timeout: Duration) -> i64 {
        let deadline = Instant::now() + timeout;
        loop {
            let c = self.count_events().await;
            if c >= expected || Instant::now() >= deadline {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Attend que le nombre total de logs atteigne `expected`.
    pub async fn wait_logs(&self, expected: i64, timeout: Duration) -> i64 {
        let deadline = Instant::now() + timeout;
        loop {
            let c = self.count_logs().await;
            if c >= expected || Instant::now() >= deadline {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub async fn shutdown(mut self) {
        for b in self.batchers.drain(..) {
            b.shutdown().await;
        }
    }
}

/// Démarre l'app complète sur un port éphémère en utilisant le pool de test fourni.
pub async fn start_app(pool: PgPool, cfg: Config) -> TestApp {
    db::ensure_partition_window(&pool, 40, 3).await.unwrap();
    db::ensure_log_partition_window(&pool, 40, 3).await.unwrap();
    db::ensure_span_partition_window(&pool, 40, 3)
        .await
        .unwrap();
    db::ensure_metric_partition_window(&pool, 40, 3)
        .await
        .unwrap();

    let metrics = Arc::new(IngestMetrics::default());
    let logs_metrics = Arc::new(IngestMetrics::default());
    let spans_metrics = Arc::new(IngestMetrics::default());
    let metrics_metrics = Arc::new(IngestMetrics::default());
    let (events, events_batcher) = ingest::spawn::<StoredEvent>(
        pool.clone(),
        cfg.flush_interval,
        cfg.flush_batch_size,
        cfg.channel_capacity,
        Arc::clone(&metrics),
    );
    let (logs, logs_batcher) = ingest::spawn::<StoredLog>(
        pool.clone(),
        cfg.flush_interval,
        cfg.flush_batch_size,
        cfg.channel_capacity,
        Arc::clone(&logs_metrics),
    );
    let (spans, spans_batcher) = ingest::spawn::<StoredSpan>(
        pool.clone(),
        cfg.flush_interval,
        cfg.flush_batch_size,
        cfg.channel_capacity,
        Arc::clone(&spans_metrics),
    );
    let (metric_points, metrics_batcher) = ingest::spawn::<StoredMetricPoint>(
        pool.clone(),
        cfg.flush_interval,
        cfg.flush_batch_size,
        cfg.channel_capacity,
        Arc::clone(&metrics_metrics),
    );
    let verifier = TokenVerifier::new(&cfg.token).await.unwrap();
    let limiter = Arc::new(RateLimiter::new(cfg.rate_limit.clone(), Instant::now()));
    let anomaly = Arc::new(AnomalyGuard::new(cfg.anomaly.clone()));
    let cfg = Arc::new(cfg);

    let state = AppState {
        events,
        logs,
        spans,
        metric_points,
        limiter,
        verifier,
        anomaly,
        limits: Arc::new(cfg.limits.clone()),
        config: Arc::clone(&cfg),
        pool: pool.clone(),
        ready: Arc::new(AtomicBool::new(true)),
        companions: Arc::new(datacat_ingest::companion::CompanionRegistry::default()),
    };

    // Serveur OTLP/gRPC (logs + traces) sur un port éphémère, partageant l'AppState.
    let grpc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc_listener.local_addr().unwrap();
    let grpc_state = state.clone();
    tokio::spawn(async move {
        let _ =
            datacat_ingest::grpc::serve(grpc_state, grpc_listener, std::future::pending::<()>())
                .await;
    });

    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestApp {
        base_url: format!("http://{addr}"),
        metrics,
        logs_metrics,
        metrics_metrics,
        grpc_addr,
        pool,
        batchers: vec![events_batcher, logs_batcher, spans_batcher, metrics_batcher],
    }
}

/// Construit un event JSON conforme au wire format.
pub fn event_json(
    id: uuid::Uuid,
    name: &str,
    session: &str,
    ts: chrono::DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        "event_id": id,
        "event_name": name,
        "actor_id": "actor-1",
        "session_id": session,
        "timestamp_client": ts.to_rfc3339(),
        "properties": { "k": "v" }
    })
}
