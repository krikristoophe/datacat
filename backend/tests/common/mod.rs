//! Utilitaires partagés par les tests d'intégration : démarrage de l'app sur un port
//! éphémère, signature de tokens de test, attente de persistance.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::Serialize;
use sqlx::PgPool;

use datacat_ingest::config::{
    AnomalyConfig, Config, CorsOrigins, KeySource, RateLimitConfig, TokenConfig, ValidationLimits,
};
use datacat_ingest::ingest::{self, BatcherHandle, IngestMetrics};
use datacat_ingest::ratelimit::RateLimiter;
use datacat_ingest::security::AnomalyGuard;
use datacat_ingest::token::TokenVerifier;
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
        database_url: String::new(),
        db_max_connections: 5,
        flush_interval: Duration::from_millis(40),
        flush_batch_size: 10_000,
        channel_capacity: 200_000,
        retention_days: 90,
        partition_future_days: 2,
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
        cors: CorsOrigins::Any,
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

/// App de test démarrée : URL de base + métriques + poignée d'arrêt du batcher.
pub struct TestApp {
    pub base_url: String,
    pub metrics: Arc<IngestMetrics>,
    pub pool: PgPool,
    pub batcher: Option<BatcherHandle>,
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

    pub async fn shutdown(mut self) {
        if let Some(b) = self.batcher.take() {
            b.shutdown().await;
        }
    }
}

/// Démarre l'app complète sur un port éphémère en utilisant le pool de test fourni.
pub async fn start_app(pool: PgPool, cfg: Config) -> TestApp {
    db::ensure_partition_window(&pool, 40, 3).await.unwrap();

    let metrics = Arc::new(IngestMetrics::default());
    let (ingestor, batcher) = ingest::spawn(pool.clone(), &cfg, Arc::clone(&metrics));
    let verifier = TokenVerifier::new(&cfg.token).await.unwrap();
    let limiter = Arc::new(RateLimiter::new(cfg.rate_limit.clone(), Instant::now()));
    let anomaly = Arc::new(AnomalyGuard::new(cfg.anomaly.clone()));
    let cfg = Arc::new(cfg);

    let state = AppState {
        ingestor,
        limiter,
        verifier,
        anomaly,
        limits: Arc::new(cfg.limits.clone()),
        config: Arc::clone(&cfg),
        metrics: Arc::clone(&metrics),
        pool: pool.clone(),
        ready: Arc::new(AtomicBool::new(true)),
    };

    let app = build_router(state).into_make_service_with_connect_info::<SocketAddr>();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    TestApp {
        base_url: format!("http://{addr}"),
        metrics,
        pool,
        batcher: Some(batcher),
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
