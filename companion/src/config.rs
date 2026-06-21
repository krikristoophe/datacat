//! Companion configuration, loaded from a TOML file.
//!
//! The file path is resolved from `$DATACAT_COMPANION_CONFIG`, then `./companion.toml`. Every
//! string value supports `${VAR}` / `${VAR:-default}` secret expansion so that secrets (the
//! heartbeat token, Slack bot token, …) never have to be written in clear text in the config
//! (mirrors the backend's `settings.rs` expander). Required variables fail closed: the agent
//! refuses to start with an unresolved secret.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

/// Default heartbeat interval when the config omits `interval`.
const DEFAULT_INTERVAL: &str = "30s";
/// Default number of consecutive failures before the agent raises its own alert.
const DEFAULT_FAILURE_THRESHOLD: u32 = 3;
/// Per-request timeout for a single heartbeat POST.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// Fully resolved runtime configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Base URL of the Datacat main instance (e.g. `https://datacat.example.com`).
    pub main_url: String,
    /// Stable identifier of this companion node (matches `[[companions.expected]].id` on main).
    pub id: String,
    /// Service-to-service heartbeat token (sent as `Authorization: Bearer <token>`).
    pub token: String,
    /// Interval between two heartbeats.
    pub interval: Duration,
    /// Per-request timeout of a single heartbeat POST.
    pub request_timeout: Duration,
    /// Consecutive failures required before raising the self-alert.
    pub failure_threshold: u32,
    /// Self-alert channel used when main becomes unreachable.
    pub alert: AlertChannel,
}

/// The companion's own alert channel (used to report that it cannot reach main).
#[derive(Debug, Clone)]
pub enum AlertChannel {
    /// Slack Web API (`chat.postMessage`) with a bot token.
    Slack { bot_token: String, channel: String },
    /// Generic JSON webhook.
    Webhook {
        url: String,
        headers: BTreeMap<String, String>,
    },
}

impl Config {
    /// Resolve the config file path: `$DATACAT_COMPANION_CONFIG`, then `./companion.toml`.
    pub fn resolve_path() -> PathBuf {
        if let Ok(p) = std::env::var("DATACAT_COMPANION_CONFIG") {
            if !p.is_empty() {
                return PathBuf::from(p);
            }
        }
        PathBuf::from("companion.toml")
    }

    /// Load and validate the configuration from the default path.
    pub fn load() -> Result<Self> {
        Self::from_file(&Self::resolve_path())
    }

    /// Load and validate the configuration from a specific TOML file.
    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading companion config {}", path.display()))?;
        Self::from_toml_str(&raw)
            .with_context(|| format!("invalid companion config {}", path.display()))
    }

    /// Parse + expand `${ENV}` + validate from a raw TOML string. Exposed for tests.
    pub fn from_toml_str(raw: &str) -> Result<Self> {
        let mut value: toml::Value = toml::from_str(raw).context("invalid TOML")?;
        expand_env(&mut value)?;
        let file: FileConfig = value
            .try_into()
            .context("invalid configuration structure")?;
        file.into_config()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Secret expansion: ${VAR} / ${VAR:-default}
// ─────────────────────────────────────────────────────────────────────────────

/// Recursively expand `${VAR}` placeholders in every string value of a `toml::Value`.
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

/// Replace `${VAR}` (or `${VAR:-default}`) with the environment value. Fails if a required
/// variable is missing (fail-closed: never start with an empty secret).
fn expand_str(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find("${") {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        let end = after
            .find('}')
            .context("unterminated `${` in TOML configuration")?;
        let inner = &after[..end];
        let (name, default) = match inner.split_once(":-") {
            Some((n, d)) => (n.trim(), Some(d)),
            None => (inner.trim(), None),
        };
        if name.is_empty() {
            bail!("`${{}}` with no environment variable name");
        }
        let val = match std::env::var(name) {
            Ok(v) => v,
            Err(_) => default
                .map(str::to_string)
                .with_context(|| format!("required environment variable is not set: {name}"))?,
        };
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

// ─────────────────────────────────────────────────────────────────────────────
// Duration parsing (200ms / 15s / 30m / 1h / 1d), replicated from backend/src/config.rs
// ─────────────────────────────────────────────────────────────────────────────

/// Parse a duration such as `200ms`, `15s`, `30m`, `1h`, `1d`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty duration");
    }
    let (num, unit) = s.split_at(
        s.find(|c: char| c.is_ascii_alphabetic())
            .context("duration with no unit (ms/s/m/h/d)")?,
    );
    let value: u64 = num.trim().parse().context("invalid duration value")?;
    let dur = match unit {
        "ms" => Duration::from_millis(value),
        "s" => Duration::from_secs(value),
        "m" => Duration::from_secs(value * 60),
        "h" => Duration::from_secs(value * 3_600),
        "d" => Duration::from_secs(value * 86_400),
        other => bail!("unknown duration unit: {other}"),
    };
    Ok(dur)
}

/// Parse a duration with a field name for context.
fn dur(s: &str, field: &str) -> Result<Duration> {
    parse_duration(s).with_context(|| format!("invalid duration for {field}: '{s}'"))
}

// ─────────────────────────────────────────────────────────────────────────────
// TOML model (deserialization). Unknown fields are rejected to catch typos.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    main_url: String,
    id: String,
    #[serde(default = "default_token")]
    token: String,
    #[serde(default = "default_interval")]
    interval: String,
    #[serde(default = "default_failure_threshold")]
    failure_threshold: u32,
    alert: AlertSection,
}

fn default_token() -> String {
    "${DATACAT_HEARTBEAT_TOKEN}".to_string()
}
fn default_interval() -> String {
    DEFAULT_INTERVAL.to_string()
}
fn default_failure_threshold() -> u32 {
    DEFAULT_FAILURE_THRESHOLD
}

/// Exactly one self-alert channel must be configured under `[alert.*]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertSection {
    slack: Option<SlackSection>,
    webhook: Option<WebhookSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlackSection {
    /// Slack bot token (`xoxb-…`).
    bot_token: String,
    /// Target channel (e.g. `#alerts`).
    channel: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookSection {
    url: String,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

impl FileConfig {
    fn into_config(self) -> Result<Config> {
        if self.main_url.trim().is_empty() {
            bail!("`main_url` is required");
        }
        if self.id.trim().is_empty() {
            bail!("`id` is required");
        }
        // The serde `default` for `token` (`${DATACAT_HEARTBEAT_TOKEN}`) is filled in *after* the
        // env-expansion pass over the parsed TOML, so expand it here too when the field was omitted.
        let token = expand_str(&self.token).context("expanding `token`")?;
        if token.trim().is_empty() {
            bail!("`token` is required (or set DATACAT_HEARTBEAT_TOKEN)");
        }
        if self.failure_threshold == 0 {
            bail!("`failure_threshold` must be >= 1");
        }

        let alert = match (self.alert.slack, self.alert.webhook) {
            (Some(_), Some(_)) => {
                bail!("configure exactly one self-alert channel: [alert.slack] OR [alert.webhook]")
            }
            (Some(s), None) => {
                if s.bot_token.trim().is_empty() {
                    bail!("[alert.slack].bot_token is required");
                }
                if s.channel.trim().is_empty() {
                    bail!("[alert.slack].channel is required");
                }
                AlertChannel::Slack {
                    bot_token: s.bot_token,
                    channel: s.channel,
                }
            }
            (None, Some(w)) => {
                if w.url.trim().is_empty() {
                    bail!("[alert.webhook].url is required");
                }
                AlertChannel::Webhook {
                    url: w.url,
                    headers: w.headers,
                }
            }
            (None, None) => {
                bail!("a self-alert channel is required: [alert.slack] or [alert.webhook]")
            }
        };

        Ok(Config {
            main_url: self.main_url.trim_end_matches('/').to_string(),
            id: self.id,
            token,
            interval: dur(&self.interval, "interval")?,
            request_timeout: HEARTBEAT_TIMEOUT,
            failure_threshold: self.failure_threshold,
            alert,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_units() {
        assert_eq!(parse_duration("200ms").unwrap(), Duration::from_millis(200));
        assert_eq!(parse_duration("15s").unwrap(), Duration::from_secs(15));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1_800));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3_600));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86_400));
        assert!(parse_duration("12").is_err());
        assert!(parse_duration("12x").is_err());
    }

    #[test]
    fn expands_env_with_default() {
        std::env::set_var("DC_COMP_PRESENT", "secret-value");
        std::env::remove_var("DC_COMP_ABSENT");
        assert_eq!(expand_str("${DC_COMP_PRESENT}").unwrap(), "secret-value");
        assert_eq!(
            expand_str("pre-${DC_COMP_PRESENT}-post").unwrap(),
            "pre-secret-value-post"
        );
        assert_eq!(
            expand_str("${DC_COMP_ABSENT:-fallback}").unwrap(),
            "fallback"
        );
        assert_eq!(expand_str("no placeholder").unwrap(), "no placeholder");
    }

    #[test]
    fn missing_required_env_fails() {
        std::env::remove_var("DC_COMP_MISSING_REQUIRED");
        assert!(expand_str("${DC_COMP_MISSING_REQUIRED}").is_err());
    }

    #[test]
    fn minimal_webhook_config_loads() {
        let raw = r#"
            main_url = "https://datacat.example.com/"
            id = "edge-1"
            token = "static-token"
            [alert.webhook]
            url = "https://hooks.example.com/abc"
        "#;
        let cfg = Config::from_toml_str(raw).unwrap();
        // Trailing slash is trimmed so URL joins are clean.
        assert_eq!(cfg.main_url, "https://datacat.example.com");
        assert_eq!(cfg.id, "edge-1");
        assert_eq!(cfg.interval, Duration::from_secs(30));
        assert_eq!(cfg.failure_threshold, 3);
        assert!(matches!(cfg.alert, AlertChannel::Webhook { .. }));
    }

    #[test]
    fn slack_config_loads_with_overrides() {
        let raw = r##"
            main_url = "https://main"
            id = "edge"
            token = "t"
            interval = "5s"
            failure_threshold = 2
            [alert.slack]
            bot_token = "xoxb-123"
            channel = "#alerts"
        "##;
        let cfg = Config::from_toml_str(raw).unwrap();
        assert_eq!(cfg.interval, Duration::from_secs(5));
        assert_eq!(cfg.failure_threshold, 2);
        match cfg.alert {
            AlertChannel::Slack { bot_token, channel } => {
                assert_eq!(bot_token, "xoxb-123");
                assert_eq!(channel, "#alerts");
            }
            _ => panic!("expected slack channel"),
        }
    }

    #[test]
    fn token_expands_from_env_by_default() {
        std::env::set_var("DATACAT_HEARTBEAT_TOKEN", "env-secret");
        let raw = r#"
            main_url = "https://main"
            id = "edge"
            [alert.webhook]
            url = "https://hook"
        "#;
        let cfg = Config::from_toml_str(raw).unwrap();
        assert_eq!(cfg.token, "env-secret");
    }

    #[test]
    fn requires_main_url_and_id() {
        assert!(Config::from_toml_str(
            r#"id = "x"
               token = "t"
               main_url = ""
               [alert.webhook]
               url = "https://hook""#
        )
        .is_err());
        assert!(Config::from_toml_str(
            r#"main_url = "https://m"
               token = "t"
               id = ""
               [alert.webhook]
               url = "https://hook""#
        )
        .is_err());
    }

    #[test]
    fn requires_exactly_one_channel() {
        // No channel.
        assert!(Config::from_toml_str(
            r#"main_url = "https://m"
               id = "x"
               token = "t""#
        )
        .is_err());
        // Both channels.
        assert!(Config::from_toml_str(
            r##"main_url = "https://m"
               id = "x"
               token = "t"
               [alert.slack]
               bot_token = "b"
               channel = "#a"
               [alert.webhook]
               url = "https://hook""##
        )
        .is_err());
    }

    #[test]
    fn unknown_field_is_rejected() {
        let raw = r#"
            main_url = "https://m"
            id = "x"
            token = "t"
            nope = 1
            [alert.webhook]
            url = "https://hook"
        "#;
        assert!(Config::from_toml_str(raw).is_err());
    }
}
