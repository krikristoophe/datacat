//! Configuration du service, chargée depuis l'environnement.
//!
//! Surface volontairement explicite (auditable) : chaque variable a une valeur par défaut
//! sûre et est validée au démarrage. Aucune dépendance de configuration lourde.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use jsonwebtoken::Algorithm;

/// Limites de validation des entrées (cf. docs/CONTRACT.md §2.3).
#[derive(Debug, Clone)]
pub struct ValidationLimits {
    pub max_batch_events: usize,
    pub max_payload_bytes: usize,
    pub max_properties_bytes: usize,
    pub max_string_len: usize,
    pub max_json_depth: usize,
    pub max_past_skew: Duration,
    pub max_future_skew: Duration,
}

/// Paramètres du rate limiting à deux niveaux + filet global (cf. cahier §7.2).
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Filet global : events/seconde tous clients confondus.
    pub global_per_sec: f64,
    pub global_burst: f64,
    /// Limite fine par session : events/seconde.
    pub session_per_sec: f64,
    pub session_burst: f64,
    /// Plafond de sessions distinctes par IP sur la fenêtre glissante.
    pub ip_session_cap: u64,
    pub ip_session_window: Duration,
    /// Bornes mémoire (anti-DoS sur les structures du limiteur).
    pub max_tracked_sessions: usize,
    pub max_tracked_ips: usize,
}

/// Garde-fou de détection d'anomalies (bannissement temporaire d'IP abusives).
#[derive(Debug, Clone)]
pub struct AnomalyConfig {
    pub bad_requests_threshold: u32,
    pub window: Duration,
    pub ban_duration: Duration,
    pub max_tracked_ips: usize,
}

/// Source des clés publiques de vérification du token.
#[derive(Debug, Clone)]
pub enum KeySource {
    /// Clé(s) publique(s) fournie(s) en configuration (PEM).
    Pem {
        pem: String,
        alg: Algorithm,
        kid: Option<String>,
    },
    /// Jeu de clés publiques récupéré et mis en cache depuis une URL JWKS.
    Jwks { url: String },
}

#[derive(Debug, Clone)]
pub struct TokenConfig {
    /// Vérification activée (défaut: true). Désactivable pour le dev local uniquement.
    pub enabled: bool,
    pub key_source: Option<KeySource>,
    pub algorithms: Vec<Algorithm>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub leeway_secs: u64,
    pub jwks_refresh: Duration,
}

#[derive(Debug, Clone)]
pub enum CorsOrigins {
    /// Liste blanche d'origines autorisées.
    List(Vec<String>),
    /// `*` — à n'utiliser qu'en dev (documenté).
    Any,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub db_max_connections: u32,

    pub flush_interval: Duration,
    pub flush_batch_size: usize,
    pub channel_capacity: usize,

    pub retention_days: i64,
    pub partition_future_days: i64,

    /// Nombre max de `LogRecord` OTLP par requête.
    pub max_logs_records: usize,
    /// Taille max du corps de la requête de logs (OTLP peut être plus volumineux).
    pub max_logs_payload_bytes: usize,

    pub request_timeout: Duration,
    pub trust_forwarded_for: bool,

    pub limits: ValidationLimits,
    pub rate_limit: RateLimitConfig,
    pub anomaly: AnomalyConfig,
    pub token: TokenConfig,
    pub cors: CorsOrigins,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let limits = ValidationLimits {
            max_batch_events: env_parse("MAX_BATCH_EVENTS", 500)?,
            max_payload_bytes: env_parse("MAX_PAYLOAD_BYTES", 1_048_576)?,
            max_properties_bytes: env_parse("MAX_PROPERTIES_BYTES", 16_384)?,
            max_string_len: env_parse("MAX_STRING_LEN", 200)?,
            max_json_depth: env_parse("MAX_JSON_DEPTH", 16)?,
            max_past_skew: env_duration("MAX_PAST_SKEW", Duration::from_secs(31 * 86_400))?,
            max_future_skew: env_duration("MAX_FUTURE_SKEW", Duration::from_secs(86_400))?,
        };

        let rate_limit = RateLimitConfig {
            global_per_sec: env_parse("RL_GLOBAL_PER_SEC", 50_000.0)?,
            global_burst: env_parse("RL_GLOBAL_BURST", 100_000.0)?,
            // Burst >= MAX_BATCH_EVENTS pour qu'un batch complet légitime passe d'un coup.
            session_per_sec: env_parse("RL_SESSION_PER_SEC", 100.0)?,
            session_burst: env_parse("RL_SESSION_BURST", 1_000.0)?,
            ip_session_cap: env_parse("RL_IP_SESSION_CAP", 200)?,
            ip_session_window: env_duration("RL_IP_SESSION_WINDOW", Duration::from_secs(1_800))?,
            max_tracked_sessions: env_parse("RL_MAX_TRACKED_SESSIONS", 500_000)?,
            max_tracked_ips: env_parse("RL_MAX_TRACKED_IPS", 200_000)?,
        };

        let anomaly = AnomalyConfig {
            bad_requests_threshold: env_parse("ANOMALY_BAD_THRESHOLD", 100)?,
            window: env_duration("ANOMALY_WINDOW", Duration::from_secs(60))?,
            ban_duration: env_duration("ANOMALY_BAN_DURATION", Duration::from_secs(300))?,
            max_tracked_ips: env_parse("ANOMALY_MAX_TRACKED_IPS", 200_000)?,
        };

        let token = TokenConfig::from_env()?;
        let cors = cors_from_env()?;

        let bind_addr = env_str("BIND_ADDR", "0.0.0.0:8080")
            .parse()
            .context("BIND_ADDR invalide")?;

        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL est requis")?;

        Ok(Config {
            bind_addr,
            database_url,
            db_max_connections: env_parse("DB_MAX_CONNECTIONS", 10)?,
            flush_interval: env_duration("FLUSH_INTERVAL", Duration::from_millis(200))?,
            flush_batch_size: env_parse("FLUSH_BATCH_SIZE", 5_000)?,
            channel_capacity: env_parse("CHANNEL_CAPACITY", 100_000)?,
            retention_days: env_parse("RETENTION_DAYS", 90)?,
            partition_future_days: env_parse("PARTITION_FUTURE_DAYS", 2)?,
            max_logs_records: env_parse("MAX_LOGS_RECORDS", 2_048)?,
            max_logs_payload_bytes: env_parse("MAX_LOGS_PAYLOAD_BYTES", 4_194_304)?,
            request_timeout: env_duration("REQUEST_TIMEOUT", Duration::from_secs(15))?,
            trust_forwarded_for: env_bool("TRUST_FORWARDED_FOR", false)?,
            limits,
            rate_limit,
            anomaly,
            token,
            cors,
        })
    }
}

impl TokenConfig {
    fn from_env() -> Result<Self> {
        let enabled = env_bool("TOKEN_ENABLED", true)?;

        let algorithms = env_str("TOKEN_ALGORITHMS", "EdDSA,RS256")
            .split(',')
            .map(|s| parse_alg(s.trim()))
            .collect::<Result<Vec<_>>>()?;
        if algorithms.is_empty() {
            bail!("TOKEN_ALGORITHMS ne doit pas être vide");
        }

        let key_source = if let Ok(url) = std::env::var("TOKEN_JWKS_URL") {
            Some(KeySource::Jwks { url })
        } else if let Some(pem) = read_pem_from_env()? {
            let alg = parse_alg(&env_str("TOKEN_ALG", "EdDSA"))?;
            let kid = std::env::var("TOKEN_KID").ok();
            Some(KeySource::Pem { pem, alg, kid })
        } else {
            None
        };

        if enabled && key_source.is_none() {
            bail!(
                "vérification du token activée mais aucune clé fournie : définir \
                 TOKEN_JWKS_URL, ou TOKEN_PUBLIC_KEY_PEM / TOKEN_PUBLIC_KEY_FILE \
                 (ou TOKEN_ENABLED=false en dev local uniquement)"
            );
        }

        Ok(TokenConfig {
            enabled,
            key_source,
            algorithms,
            issuer: std::env::var("TOKEN_ISSUER").ok(),
            audience: std::env::var("TOKEN_AUDIENCE").ok(),
            leeway_secs: env_parse("TOKEN_LEEWAY", 60)?,
            jwks_refresh: env_duration("TOKEN_JWKS_REFRESH", Duration::from_secs(3_600))?,
        })
    }
}

fn read_pem_from_env() -> Result<Option<String>> {
    if let Ok(pem) = std::env::var("TOKEN_PUBLIC_KEY_PEM") {
        return Ok(Some(pem));
    }
    if let Ok(path) = std::env::var("TOKEN_PUBLIC_KEY_FILE") {
        let pem = std::fs::read_to_string(&path)
            .with_context(|| format!("lecture de TOKEN_PUBLIC_KEY_FILE={path}"))?;
        return Ok(Some(pem));
    }
    Ok(None)
}

fn parse_alg(s: &str) -> Result<Algorithm> {
    match s {
        "EdDSA" | "eddsa" | "Ed25519" => Ok(Algorithm::EdDSA),
        "RS256" | "rs256" => Ok(Algorithm::RS256),
        other => bail!("algorithme non supporté (asymétrique requis): {other}"),
    }
}

fn cors_from_env() -> Result<CorsOrigins> {
    let raw = env_str("CORS_ALLOWED_ORIGINS", "");
    if raw.trim() == "*" {
        return Ok(CorsOrigins::Any);
    }
    let list: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(CorsOrigins::List(list))
}

// --- petits helpers de lecture d'environnement ---

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v
            .trim()
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("{key} invalide: {e}")),
        Err(_) => Ok(default),
    }
}

fn env_bool(key: &str, default: bool) -> Result<bool> {
    match std::env::var(key) {
        Ok(v) => match v.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => bail!("{key} booléen invalide: {other}"),
        },
        Err(_) => Ok(default),
    }
}

fn env_duration(key: &str, default: Duration) -> Result<Duration> {
    match std::env::var(key) {
        Ok(v) => parse_duration(v.trim()).with_context(|| format!("{key} invalide")),
        Err(_) => Ok(default),
    }
}

/// Parse une durée du type `200ms`, `15s`, `30m`, `24h`, `31d`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("durée vide");
    }
    let (num, unit) = s.split_at(
        s.find(|c: char| c.is_ascii_alphabetic())
            .context("durée sans unité (ms/s/m/h/d)")?,
    );
    let value: u64 = num.trim().parse().context("valeur de durée invalide")?;
    let dur = match unit {
        "ms" => Duration::from_millis(value),
        "s" => Duration::from_secs(value),
        "m" => Duration::from_secs(value * 60),
        "h" => Duration::from_secs(value * 3_600),
        "d" => Duration::from_secs(value * 86_400),
        other => bail!("unité de durée inconnue: {other}"),
    };
    Ok(dur)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1_800));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert_eq!(
            parse_duration("31d").unwrap(),
            Duration::from_secs(2_678_400)
        );
        assert!(parse_duration("12").is_err());
        assert!(parse_duration("12x").is_err());
    }

    #[test]
    fn parse_alg_rejects_symmetric() {
        assert!(parse_alg("HS256").is_err());
        assert!(matches!(parse_alg("EdDSA").unwrap(), Algorithm::EdDSA));
        assert!(matches!(parse_alg("RS256").unwrap(), Algorithm::RS256));
    }
}
