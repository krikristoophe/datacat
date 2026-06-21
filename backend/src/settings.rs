//! Configuration unifiée par **fichier TOML** (`datacat.toml`), avec :
//! - une config globale du déploiement (server / database / ingest / token / auth / query / mcp /
//!   export) ;
//! - un **fichier TOML par projet** (`projects/*.toml`) portant ses règles d'alerting, ses canaux
//!   de notification (slack/email) et un filtre par défaut (service / tenant) ;
//! - l'**expansion des secrets** depuis l'environnement via `${VAR}` (ou `${VAR:-défaut}`), pour ne
//!   jamais écrire de secret en clair dans la config (exigence HDS).
//!
//! En l'absence de fichier TOML, on retombe sur l'ancienne configuration par variables
//! d'environnement (`Config::from_env`) — utile en développement et pour les tests.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::alerting::{EmailConfig, Rule, SlackBot};
use crate::config::{
    parse_alg, parse_duration, AlertingConfig, AnomalyConfig, Config, CorsOrigins, KeySource,
    LogsAuth, RateLimitConfig, TokenConfig, ValidationLimits,
};

/// Configuration résolue de tout le déploiement.
pub struct Settings {
    /// Config globale (runtime) consommée par l'API et l'ingestion.
    pub config: Config,
    /// Projets : un évaluateur d'alerting est démarré par projet.
    pub projects: Vec<Project>,
    /// Export froid planifié (si configuré et activé).
    pub export: Option<ExportSettings>,
}

/// Un projet supervisé : ses règles d'alerting + ses canaux de notification.
pub struct Project {
    pub id: String,
    pub name: String,
    pub eval_interval: Duration,
    pub rules: Vec<Rule>,
    /// Bot Slack du projet (repli sur le global si absent).
    pub slack: Option<SlackBot>,
    /// Config e-mail du projet (repli sur le global si absent).
    pub email: Option<EmailConfig>,
}

/// Table exportée à froid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportTable {
    Events,
    Logs,
}

/// Paramètres de l'export froid planifié (PostgreSQL → Parquet/S3).
pub struct ExportSettings {
    /// Intervalle entre deux exécutions de l'export.
    pub schedule: Duration,
    pub bucket: String,
    pub prefix: Option<String>,
    pub region: String,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub allow_http: bool,
    pub tables: Vec<ExportTable>,
}

impl Settings {
    /// Charge la configuration. Ordre de résolution du fichier :
    /// `DATACAT_CONFIG`, puis `./datacat.toml`, puis `/etc/datacat/datacat.toml`. Sans fichier,
    /// retombe sur les variables d'environnement (legacy / dev).
    pub fn load() -> Result<Self> {
        match Self::config_path() {
            Some(path) => Self::from_toml_file(&path),
            None => Self::from_env_fallback(),
        }
    }

    fn config_path() -> Option<PathBuf> {
        if let Ok(p) = std::env::var("DATACAT_CONFIG") {
            if !p.is_empty() {
                return Some(PathBuf::from(p));
            }
        }
        for candidate in ["datacat.toml", "/etc/datacat/datacat.toml"] {
            let p = PathBuf::from(candidate);
            if p.is_file() {
                return Some(p);
            }
        }
        None
    }

    /// Charge depuis un fichier TOML (avec expansion `${ENV}` puis désérialisation typée).
    pub fn from_toml_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("lecture de la configuration {}", path.display()))?;
        let file = parse_file(&raw)
            .with_context(|| format!("configuration invalide {}", path.display()))?;

        let global_notify = file.notifications.clone();
        let config = build_config(&file)?;

        let projects = build_projects(&file.projects, path, &global_notify)?;
        let export = match &file.export {
            Some(e) if e.enabled => Some(build_export(e)?),
            _ => None,
        };

        Ok(Settings {
            config,
            projects,
            export,
        })
    }

    /// Repli historique : config par variables d'environnement (un seul projet « default »).
    fn from_env_fallback() -> Result<Self> {
        tracing::info!("aucun datacat.toml trouvé — configuration par variables d'environnement");
        let config = Config::from_env()?;
        let projects = project_from_env(&config.alerting);
        let export = ExportSettings::from_env()?;
        Ok(Settings {
            config,
            projects,
            export,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Expansion des secrets ${VAR} / ${VAR:-défaut}
// ─────────────────────────────────────────────────────────────────────────────

/// Parse le TOML, **expand les `${ENV}`** dans toutes les chaînes, puis désérialise.
fn parse_file(raw: &str) -> Result<FileConfig> {
    let mut value: toml::Value = toml::from_str(raw).context("TOML invalide")?;
    expand_env(&mut value)?;
    value
        .try_into()
        .context("structure de configuration invalide")
}

/// Expand récursivement les `${VAR}` dans toutes les valeurs chaîne d'un `toml::Value`.
fn expand_env(value: &mut toml::Value) -> Result<()> {
    match value {
        toml::Value::String(s) => *s = expand_str(s)?,
        toml::Value::Array(a) => {
            for v in a {
                expand_env(v)?;
            }
        }
        toml::Value::Table(t) => {
            for (_, v) in t.iter_mut() {
                expand_env(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Remplace `${VAR}` (ou `${VAR:-défaut}`) par la valeur d'environnement. Échoue si une variable
/// requise est absente (fail-closed : on ne démarre pas avec un secret vide).
fn expand_str(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find("${") {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        let end = after
            .find('}')
            .context("`${` non fermé dans la configuration TOML")?;
        let inner = &after[..end];
        let (name, default) = match inner.split_once(":-") {
            Some((n, d)) => (n.trim(), Some(d)),
            None => (inner.trim(), None),
        };
        if name.is_empty() {
            bail!("`${{}}` sans nom de variable d'environnement");
        }
        let val = match std::env::var(name) {
            Ok(v) => v,
            Err(_) => default.map(str::to_string).with_context(|| {
                format!("variable d'environnement requise non définie : {name}")
            })?,
        };
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Modèle TOML (désérialisation). Tous les champs ont un défaut sûr.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
    server: ServerSection,
    database: DatabaseSection,
    ingest: IngestSection,
    token: TokenSection,
    auth: AuthSection,
    mcp: McpSection,
    export: Option<ExportSection>,
    notifications: NotificationsSection,
    projects: ProjectsSection,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServerSection {
    bind_addr: String,
    request_timeout: String,
    trust_forwarded_for: bool,
    grpc: GrpcSection,
    cors: CorsSection,
}
impl Default for ServerSection {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8080".into(),
            request_timeout: "15s".into(),
            trust_forwarded_for: false,
            grpc: GrpcSection::default(),
            cors: CorsSection::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct GrpcSection {
    enabled: bool,
    bind_addr: String,
}
impl Default for GrpcSection {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_addr: "0.0.0.0:4317".into(),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CorsSection {
    /// Liste blanche d'origines. `["*"]` = toute origine (dev uniquement).
    allowed_origins: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DatabaseSection {
    url: String,
    max_connections: u32,
}
impl Default for DatabaseSection {
    fn default() -> Self {
        Self {
            url: String::new(),
            max_connections: 10,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct IngestSection {
    flush_interval: String,
    flush_batch_size: usize,
    channel_capacity: usize,
    retention_days: i64,
    partition_future_days: i64,
    max_logs_records: usize,
    max_logs_payload_bytes: usize,
    limits: LimitsSection,
    rate_limit: RateLimitSection,
    anomaly: AnomalySection,
}
impl Default for IngestSection {
    fn default() -> Self {
        Self {
            flush_interval: "200ms".into(),
            flush_batch_size: 5_000,
            channel_capacity: 100_000,
            retention_days: 90,
            partition_future_days: 2,
            max_logs_records: 2_048,
            max_logs_payload_bytes: 4_194_304,
            limits: LimitsSection::default(),
            rate_limit: RateLimitSection::default(),
            anomaly: AnomalySection::default(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LimitsSection {
    max_batch_events: usize,
    max_payload_bytes: usize,
    max_properties_bytes: usize,
    max_string_len: usize,
    max_json_depth: usize,
    max_past_skew: String,
    max_future_skew: String,
}
impl Default for LimitsSection {
    fn default() -> Self {
        Self {
            max_batch_events: 500,
            max_payload_bytes: 1_048_576,
            max_properties_bytes: 16_384,
            max_string_len: 200,
            max_json_depth: 16,
            max_past_skew: "31d".into(),
            max_future_skew: "24h".into(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RateLimitSection {
    global_per_sec: f64,
    global_burst: f64,
    session_per_sec: f64,
    session_burst: f64,
    ip_session_cap: u64,
    ip_session_window: String,
    max_tracked_sessions: usize,
    max_tracked_ips: usize,
}
impl Default for RateLimitSection {
    fn default() -> Self {
        Self {
            global_per_sec: 50_000.0,
            global_burst: 100_000.0,
            session_per_sec: 100.0,
            session_burst: 1_000.0,
            ip_session_cap: 200,
            ip_session_window: "30m".into(),
            max_tracked_sessions: 500_000,
            max_tracked_ips: 200_000,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AnomalySection {
    bad_requests_threshold: u32,
    window: String,
    ban_duration: String,
    max_tracked_ips: usize,
}
impl Default for AnomalySection {
    fn default() -> Self {
        Self {
            bad_requests_threshold: 100,
            window: "60s".into(),
            ban_duration: "5m".into(),
            max_tracked_ips: 200_000,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TokenSection {
    enabled: bool,
    algorithms: Vec<String>,
    issuer: Option<String>,
    audience: Option<String>,
    leeway: String,
    jwks_refresh: String,
    jwks_url: Option<String>,
    public_key_pem: Option<String>,
    public_key_file: Option<String>,
    alg: String,
    kid: Option<String>,
}
impl Default for TokenSection {
    fn default() -> Self {
        Self {
            enabled: true,
            algorithms: vec!["EdDSA".into(), "RS256".into()],
            issuer: None,
            audience: None,
            leeway: "60s".into(),
            jwks_refresh: "1h".into(),
            jwks_url: None,
            public_key_pem: None,
            public_key_file: None,
            alg: "EdDSA".into(),
            kid: None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuthSection {
    logs: AuthEntry,
    query: AuthEntry,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuthEntry {
    /// `auto` | `static` | `jwt` | `none`.
    mode: String,
    static_token: Option<String>,
}
impl Default for AuthEntry {
    fn default() -> Self {
        Self {
            mode: "auto".into(),
            static_token: None,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct McpSection {
    enabled: bool,
}
impl Default for McpSection {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExportSection {
    enabled: bool,
    schedule: String,
    bucket: String,
    prefix: String,
    region: String,
    endpoint: Option<String>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
    allow_http: bool,
    tables: Vec<String>,
}
impl Default for ExportSection {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule: "24h".into(),
            bucket: String::new(),
            prefix: String::new(),
            region: "eu-west-1".into(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            allow_http: false,
            tables: vec!["events".into(), "logs".into()],
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NotificationsSection {
    slack: Option<SlackSection>,
    email: Option<EmailSection>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlackSection {
    /// Slack bot token (`xoxb-…`).
    bot_token: String,
    /// Default channel (e.g. `#alerts`); a `slack` action may override it per rule.
    channel: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EmailSection {
    smtp_host: String,
    #[serde(default = "default_smtp_port")]
    smtp_port: u16,
    username: Option<String>,
    password: Option<String>,
    from: String,
    #[serde(default)]
    to: Vec<String>,
}

fn default_smtp_port() -> u16 {
    587
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectsSection {
    /// Répertoire dont tous les `*.toml` sont chargés comme projets.
    dir: Option<String>,
    /// Fichiers de projet explicites (en plus / à la place de `dir`).
    files: Vec<String>,
}

// ── Modèle d'un fichier de projet ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectFile {
    project: ProjectMeta,
    #[serde(default)]
    alerting: ProjectAlerting,
    #[serde(default)]
    notifications: NotificationsSection,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectMeta {
    id: String,
    #[serde(default)]
    name: Option<String>,
    /// Filtre `service` appliqué par défaut aux règles du projet.
    #[serde(default)]
    service: Option<String>,
    /// Filtre `tenant` appliqué par défaut aux règles du projet.
    #[serde(default)]
    tenant: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProjectAlerting {
    eval_interval: String,
    rules: Vec<Rule>,
}
impl Default for ProjectAlerting {
    fn default() -> Self {
        Self {
            eval_interval: "60s".into(),
            rules: Vec::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Construction de la config runtime
// ─────────────────────────────────────────────────────────────────────────────

fn build_config(file: &FileConfig) -> Result<Config> {
    let s = &file.server;
    let i = &file.ingest;

    let database_url = if file.database.url.is_empty() {
        bail!("[database].url est requis (ex. url = \"${{DATABASE_URL}}\")");
    } else {
        file.database.url.clone()
    };

    let limits = ValidationLimits {
        max_batch_events: i.limits.max_batch_events,
        max_payload_bytes: i.limits.max_payload_bytes,
        max_properties_bytes: i.limits.max_properties_bytes,
        max_string_len: i.limits.max_string_len,
        max_json_depth: i.limits.max_json_depth,
        max_past_skew: dur(&i.limits.max_past_skew, "ingest.limits.max_past_skew")?,
        max_future_skew: dur(&i.limits.max_future_skew, "ingest.limits.max_future_skew")?,
    };

    let rate_limit = RateLimitConfig {
        global_per_sec: i.rate_limit.global_per_sec,
        global_burst: i.rate_limit.global_burst,
        session_per_sec: i.rate_limit.session_per_sec,
        session_burst: i.rate_limit.session_burst,
        ip_session_cap: i.rate_limit.ip_session_cap,
        ip_session_window: dur(
            &i.rate_limit.ip_session_window,
            "ingest.rate_limit.ip_session_window",
        )?,
        max_tracked_sessions: i.rate_limit.max_tracked_sessions,
        max_tracked_ips: i.rate_limit.max_tracked_ips,
    };

    let anomaly = AnomalyConfig {
        bad_requests_threshold: i.anomaly.bad_requests_threshold,
        window: dur(&i.anomaly.window, "ingest.anomaly.window")?,
        ban_duration: dur(&i.anomaly.ban_duration, "ingest.anomaly.ban_duration")?,
        max_tracked_ips: i.anomaly.max_tracked_ips,
    };

    let token = build_token(&file.token)?;

    let logs_auth = LogsAuth::resolve(
        &file.auth.logs.mode,
        file.auth.logs.static_token.clone(),
        token.enabled,
    )
    .context("[auth.logs]")?;
    let query_auth = LogsAuth::resolve(
        &file.auth.query.mode,
        file.auth.query.static_token.clone(),
        token.enabled,
    )
    .context("[auth.query]")?;

    let cors = build_cors(&s.cors);
    let alerting = build_global_alerting(&file.notifications);

    Ok(Config {
        bind_addr: s.bind_addr.parse().context("[server].bind_addr invalide")?,
        grpc_enabled: s.grpc.enabled,
        grpc_bind_addr: s
            .grpc
            .bind_addr
            .parse()
            .context("[server.grpc].bind_addr invalide")?,
        database_url,
        db_max_connections: file.database.max_connections,
        flush_interval: dur(&i.flush_interval, "ingest.flush_interval")?,
        flush_batch_size: i.flush_batch_size,
        channel_capacity: i.channel_capacity,
        retention_days: i.retention_days,
        partition_future_days: i.partition_future_days,
        max_logs_records: i.max_logs_records,
        max_logs_payload_bytes: i.max_logs_payload_bytes,
        request_timeout: dur(&s.request_timeout, "server.request_timeout")?,
        trust_forwarded_for: s.trust_forwarded_for,
        limits,
        rate_limit,
        anomaly,
        token,
        logs_auth,
        query_auth,
        mcp_enabled: file.mcp.enabled,
        cors,
        alerting,
    })
}

fn build_token(t: &TokenSection) -> Result<TokenConfig> {
    let algorithms = t
        .algorithms
        .iter()
        .map(|a| parse_alg(a))
        .collect::<Result<Vec<_>>>()?;
    if algorithms.is_empty() {
        bail!("[token].algorithms ne doit pas être vide");
    }

    // Alg d'une clé PEM : doit appartenir à la liste d'algorithmes acceptés, sinon TOUS les tokens
    // seraient rejetés au runtime (clé jamais sélectionnée). On le détecte à la configuration.
    let pem_alg = || -> Result<jsonwebtoken::Algorithm> {
        let alg = parse_alg(&t.alg)?;
        if !algorithms.contains(&alg) {
            bail!(
                "[token].alg = {alg:?} absent de [token].algorithms {algorithms:?} : \
                 la clé ne serait jamais utilisée"
            );
        }
        Ok(alg)
    };

    let key_source = if let Some(url) = &t.jwks_url {
        Some(KeySource::Jwks { url: url.clone() })
    } else if let Some(pem) = &t.public_key_pem {
        Some(KeySource::Pem {
            pem: pem.clone(),
            alg: pem_alg()?,
            kid: t.kid.clone(),
        })
    } else if let Some(file) = &t.public_key_file {
        let alg = pem_alg()?;
        let pem = std::fs::read_to_string(file)
            .with_context(|| format!("lecture de [token].public_key_file = {file}"))?;
        Some(KeySource::Pem {
            pem,
            alg,
            kid: t.kid.clone(),
        })
    } else {
        None
    };

    if t.enabled && key_source.is_none() {
        bail!(
            "vérification du token activée mais aucune clé fournie : définir [token].jwks_url, \
             ou [token].public_key_pem / [token].public_key_file (ou enabled = false en dev)"
        );
    }

    Ok(TokenConfig {
        enabled: t.enabled,
        key_source,
        algorithms,
        issuer: t.issuer.clone(),
        audience: t.audience.clone(),
        leeway_secs: dur(&t.leeway, "token.leeway")?.as_secs(),
        jwks_refresh: dur(&t.jwks_refresh, "token.jwks_refresh")?,
    })
}

fn build_cors(c: &CorsSection) -> CorsOrigins {
    if c.allowed_origins.iter().any(|o| o == "*") {
        CorsOrigins::Any
    } else {
        CorsOrigins::List(
            c.allowed_origins
                .iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        )
    }
}

/// Canaux de notification globaux (repli pour les projets sans canaux propres).
fn build_global_alerting(n: &NotificationsSection) -> AlertingConfig {
    let slack = n.slack.as_ref();
    let email = n.email.as_ref();
    AlertingConfig {
        rules_file: None,
        eval_interval: Duration::from_secs(60),
        slack_bot_token: slack.map(|s| s.bot_token.clone()),
        slack_channel: slack.map(|s| s.channel.clone()),
        smtp_host: email.map(|e| e.smtp_host.clone()),
        smtp_port: email.map(|e| e.smtp_port).unwrap_or(587),
        smtp_username: email.and_then(|e| e.username.clone()),
        smtp_password: email.and_then(|e| e.password.clone()),
        email_from: email.map(|e| e.from.clone()),
        email_to: email.map(|e| e.to.clone()).unwrap_or_default(),
    }
}

fn build_export(e: &ExportSection) -> Result<ExportSettings> {
    if e.bucket.trim().is_empty() {
        bail!("[export].bucket est requis quand l'export est activé");
    }
    let tables = e
        .tables
        .iter()
        .map(|t| match t.to_ascii_lowercase().as_str() {
            "events" => Ok(ExportTable::Events),
            "logs" => Ok(ExportTable::Logs),
            other => bail!("[export].tables : table inconnue '{other}' (events|logs)"),
        })
        .collect::<Result<Vec<_>>>()?;
    if tables.is_empty() {
        bail!("[export].tables ne doit pas être vide");
    }
    Ok(ExportSettings {
        schedule: dur(&e.schedule, "export.schedule")?,
        bucket: e.bucket.clone(),
        prefix: Some(e.prefix.clone()).filter(|p| !p.is_empty()),
        region: e.region.clone(),
        // Une valeur vide (ex. `${S3_ENDPOINT:-}` non renseigné) doit valoir « non défini »,
        // sinon `with_endpoint("")` écrase la résolution d'endpoint AWS par défaut. Idem creds.
        endpoint: non_empty(e.endpoint.clone()),
        access_key_id: non_empty(e.access_key_id.clone()),
        secret_access_key: non_empty(e.secret_access_key.clone()),
        allow_http: e.allow_http,
        tables,
    })
}

/// `Some(s)` si non vide après trim, sinon `None`. Neutralise les `${VAR:-}` non renseignés.
fn non_empty(v: Option<String>) -> Option<String> {
    v.filter(|s| !s.trim().is_empty())
}

// ─────────────────────────────────────────────────────────────────────────────
// Projets
// ─────────────────────────────────────────────────────────────────────────────

fn build_projects(
    section: &ProjectsSection,
    main_path: &Path,
    global: &NotificationsSection,
) -> Result<Vec<Project>> {
    let base = main_path.parent().unwrap_or_else(|| Path::new("."));
    let mut paths: Vec<PathBuf> = Vec::new();

    if let Some(dir) = &section.dir {
        let dir = base.join(dir);
        let entries = std::fs::read_dir(&dir)
            .with_context(|| format!("lecture du répertoire de projets {}", dir.display()))?;
        for entry in entries {
            let p = entry?.path();
            if p.extension().is_some_and(|e| e == "toml") {
                paths.push(p);
            }
        }
        paths.sort();
    }
    for f in &section.files {
        paths.push(base.join(f));
    }

    // Le chargement d'un projet est **résilient** : un projet invalide (règles erronées, secret
    // manquant…) est journalisé et ignoré, mais ne doit JAMAIS empêcher l'ingestion de démarrer
    // (l'alerting est une fonctionnalité annexe ; la disponibilité d'ingestion prime).
    let mut projects: Vec<Project> = Vec::with_capacity(paths.len());
    let mut seen_ids = std::collections::HashSet::new();
    for p in &paths {
        match load_project(p, global) {
            Ok(project) => {
                if !seen_ids.insert(project.id.clone()) {
                    tracing::warn!(
                        project = %project.id,
                        file = %p.display(),
                        "id de projet en double — projet ignoré (un seul évaluateur par id)"
                    );
                    continue;
                }
                projects.push(project);
            }
            Err(e) => {
                tracing::error!(file = %p.display(), error = %e, "projet invalide — ignoré");
            }
        }
    }
    Ok(projects)
}

fn load_project(path: &Path, global: &NotificationsSection) -> Result<Project> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("lecture du projet {}", path.display()))?;
    let mut value: toml::Value = toml::from_str(&raw).context("TOML de projet invalide")?;
    expand_env(&mut value)?;
    let pf: ProjectFile = value.try_into().context("structure de projet invalide")?;

    // Filtre par défaut (service/tenant) appliqué aux règles qui n'en définissent pas.
    let mut rules = pf.alerting.rules;
    for r in &mut rules {
        apply_project_filter(
            r,
            pf.project.service.as_deref(),
            pf.project.tenant.as_deref(),
        );
    }
    for r in &rules {
        r.validate()
            .with_context(|| format!("règle invalide dans le projet {}", pf.project.id))?;
    }

    // Canaux : projet sinon global.
    let notify = if pf.notifications.slack.is_some() || pf.notifications.email.is_some() {
        &pf.notifications
    } else {
        global
    };
    let slack = notify.slack.as_ref().map(|s| SlackBot {
        token: s.bot_token.clone(),
        default_channel: s.channel.clone(),
    });
    let email = notify.email.as_ref().map(email_config);

    Ok(Project {
        name: pf.project.name.unwrap_or_else(|| pf.project.id.clone()),
        id: pf.project.id,
        eval_interval: dur(&pf.alerting.eval_interval, "alerting.eval_interval")?,
        rules,
        slack,
        email,
    })
}

/// Renseigne `service`/`tenant` sur une règle (et ses sous-conditions) quand ils sont absents.
fn apply_project_filter(rule: &mut Rule, service: Option<&str>, tenant: Option<&str>) {
    if rule.service.is_none() {
        rule.service = service.map(str::to_string);
    }
    if rule.tenant.is_none() {
        rule.tenant = tenant.map(str::to_string);
    }
    for c in &mut rule.conditions {
        apply_project_filter(c, service, tenant);
    }
}

fn email_config(e: &EmailSection) -> EmailConfig {
    EmailConfig {
        smtp_host: e.smtp_host.clone(),
        smtp_port: e.smtp_port,
        username: e.username.clone(),
        password: e.password.clone(),
        from: e.from.clone(),
        to: e.to.clone(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Repli par variables d'environnement (legacy / dev / tests)
// ─────────────────────────────────────────────────────────────────────────────

/// Construit (au plus) un projet « default » depuis l'`AlertingConfig` issue de l'environnement.
/// **Résilient** : un fichier de règles illisible/invalide est journalisé et l'alerting est
/// simplement désactivé — l'ingestion démarre quand même (disponibilité prioritaire).
fn project_from_env(ac: &AlertingConfig) -> Vec<Project> {
    let Some(rules_file) = &ac.rules_file else {
        return Vec::new();
    };
    let rules = match crate::alerting::load_rules(rules_file) {
        Ok(r) if !r.is_empty() => r,
        Ok(_) => return Vec::new(),
        Err(e) => {
            tracing::error!(file = %rules_file, error = %e, "chargement des règles échoué — alerting désactivé");
            return Vec::new();
        }
    };
    let email = match (&ac.smtp_host, &ac.email_from) {
        (Some(host), Some(from)) => Some(EmailConfig {
            smtp_host: host.clone(),
            smtp_port: ac.smtp_port,
            username: ac.smtp_username.clone(),
            password: ac.smtp_password.clone(),
            from: from.clone(),
            to: ac.email_to.clone(),
        }),
        _ => None,
    };
    let slack = match (&ac.slack_bot_token, &ac.slack_channel) {
        (Some(token), Some(channel)) => Some(SlackBot {
            token: token.clone(),
            default_channel: channel.clone(),
        }),
        _ => None,
    };
    vec![Project {
        id: "default".into(),
        name: "default".into(),
        eval_interval: ac.eval_interval,
        rules,
        slack,
        email,
    }]
}

impl ExportSettings {
    /// Repli : export configuré par variables d'environnement (legacy). `None` si `EXPORT_ENABLED`
    /// n'est pas vrai.
    fn from_env() -> Result<Option<Self>> {
        let enabled = std::env::var("EXPORT_ENABLED")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        if !enabled {
            return Ok(None);
        }
        let bucket =
            std::env::var("S3_BUCKET").context("EXPORT_ENABLED mais S3_BUCKET manquant")?;
        let tables = std::env::var("EXPORT_TABLES")
            .unwrap_or_else(|_| "events,logs".into())
            .split(',')
            .filter_map(|t| match t.trim().to_ascii_lowercase().as_str() {
                "events" => Some(ExportTable::Events),
                "logs" => Some(ExportTable::Logs),
                _ => None,
            })
            .collect::<Vec<_>>();
        if tables.is_empty() {
            bail!(
                "EXPORT_ENABLED mais EXPORT_TABLES ne contient aucune table valide (events|logs)"
            );
        }
        Ok(Some(ExportSettings {
            schedule: parse_duration(
                &std::env::var("EXPORT_SCHEDULE").unwrap_or_else(|_| "24h".into()),
            )?,
            bucket,
            prefix: std::env::var("S3_PREFIX").ok().filter(|p| !p.is_empty()),
            region: std::env::var("S3_REGION").unwrap_or_else(|_| "eu-west-1".into()),
            endpoint: non_empty(std::env::var("S3_ENDPOINT").ok()),
            access_key_id: non_empty(std::env::var("AWS_ACCESS_KEY_ID").ok()),
            secret_access_key: non_empty(std::env::var("AWS_SECRET_ACCESS_KEY").ok()),
            allow_http: std::env::var("S3_ALLOW_HTTP")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false),
            tables,
        }))
    }
}

/// Parse une durée du TOML avec contexte de champ.
fn dur(s: &str, field: &str) -> Result<Duration> {
    parse_duration(s).with_context(|| format!("durée invalide pour {field}: '{s}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_env_with_default() {
        std::env::set_var("DC_TEST_PRESENT", "secret-value");
        std::env::remove_var("DC_TEST_ABSENT");
        assert_eq!(expand_str("${DC_TEST_PRESENT}").unwrap(), "secret-value");
        assert_eq!(
            expand_str("pre-${DC_TEST_PRESENT}-post").unwrap(),
            "pre-secret-value-post"
        );
        assert_eq!(
            expand_str("${DC_TEST_ABSENT:-fallback}").unwrap(),
            "fallback"
        );
        assert_eq!(expand_str("no placeholder").unwrap(), "no placeholder");
    }

    #[test]
    fn missing_required_env_fails() {
        std::env::remove_var("DC_TEST_MISSING_REQUIRED");
        assert!(expand_str("${DC_TEST_MISSING_REQUIRED}").is_err());
    }

    #[test]
    fn minimal_toml_builds_config() {
        std::env::set_var("DC_TEST_DBURL", "postgres://u:p@localhost/db");
        let raw = r#"
            [database]
            url = "${DC_TEST_DBURL}"
            [token]
            enabled = false
        "#;
        let file = parse_file(raw).unwrap();
        let cfg = build_config(&file).unwrap();
        assert_eq!(cfg.database_url, "postgres://u:p@localhost/db");
        assert!(!cfg.token.enabled);
        assert_eq!(cfg.db_max_connections, 10);
        assert_eq!(cfg.flush_batch_size, 5_000);
    }

    #[cfg(not(feature = "dev"))]
    #[test]
    fn production_guards_reject_wildcard_cors() {
        std::env::set_var("DC_GUARD_DBURL", "postgres://x");
        let raw = r#"
            [database]
            url = "${DC_GUARD_DBURL}"
            [token]
            enabled = false
            [server.cors]
            allowed_origins = ["*"]
        "#;
        let cfg = build_config(&parse_file(raw).unwrap()).unwrap();
        // Hors feature `dev` : CORS `*` (et token désactivé) sont refusés.
        assert!(cfg.enforce_runtime_guards().is_err());
    }

    #[test]
    fn token_enabled_without_key_fails() {
        let raw = r#"
            [database]
            url = "postgres://x"
            [token]
            enabled = true
        "#;
        let file = parse_file(raw).unwrap();
        assert!(build_config(&file).is_err());
    }

    #[test]
    fn unknown_field_is_rejected() {
        let raw = r#"
            [database]
            url = "postgres://x"
            nonexistent_field = 1
        "#;
        assert!(parse_file(raw).is_err());
    }

    #[test]
    fn example_toml_files_load() {
        // Garde l'exemple livré (datacat.example.toml + projects/example.toml) cohérent.
        std::env::set_var("DATABASE_URL", "postgres://u:p@localhost/db");
        std::env::set_var("TOKEN_PUBLIC_KEY_PEM", "dummy-pem-for-parsing");
        let path = std::path::Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../datacat.example.toml"
        ));
        let settings = Settings::from_toml_file(path).expect("datacat.example.toml doit charger");
        assert!(settings.config.token.enabled);
        assert!(settings.export.is_none(), "export désactivé par défaut");
        assert_eq!(settings.projects.len(), 1);
        let p = &settings.projects[0];
        assert_eq!(p.id, "example");
        assert!(p.rules.len() >= 3);
        // Le filtre projet (service=api) est appliqué aux règles qui n'en précisent pas.
        assert!(
            p.rules.iter().all(|r| r.service.as_deref() == Some("api")),
            "filtre projet service=api appliqué"
        );
    }

    #[test]
    fn export_disabled_when_not_enabled() {
        let e = ExportSection {
            enabled: false,
            bucket: "b".into(),
            ..Default::default()
        };
        // build_export n'est appelé que si enabled ; ici on vérifie juste la validation bucket.
        let ok = ExportSection {
            enabled: true,
            bucket: "datacat".into(),
            ..Default::default()
        };
        assert!(build_export(&ok).is_ok());
        assert!(build_export(&ExportSection {
            enabled: true,
            bucket: "".into(),
            ..Default::default()
        })
        .is_err());
        let _ = e;
    }
}
