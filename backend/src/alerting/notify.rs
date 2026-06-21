//! Notifications d'alerte : trait `Notifier` + implémentations Slack (webhook) et e-mail (SMTP).
//!
//! Le moteur d'évaluation est agnostique du canal : il reçoit un `Vec<Arc<dyn Notifier>>` et
//! diffuse chaque transition sur tous les canaux configurés.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use crate::alerting::rules::{Action, Rule};

/// État d'une alerte (transition de la machine à états par règle).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertState {
    /// Transition ok → firing (le seuil vient d'être franchi).
    Firing,
    /// Transition firing → ok (l'alerte est résolue).
    Resolved,
}

impl AlertState {
    pub fn label(&self) -> &'static str {
        match self {
            AlertState::Firing => "FIRING",
            AlertState::Resolved => "RESOLVED",
        }
    }
}

/// Une alerte prête à être notifiée (résultat d'une transition d'état).
#[derive(Debug, Clone)]
pub struct Alert {
    pub rule_name: String,
    pub severity: String,
    pub state: AlertState,
    /// Valeur observée (compte de logs ou agrégat de métrique).
    pub value: f64,
    pub threshold: f64,
    /// Description lisible (ex. `avg(http.server.duration) > 500 over 300s`).
    pub description: String,
    /// Clé du groupe pour les règles `log_group_count` (ex. le message d'erreur identique).
    pub group_key: Option<String>,
}

impl Alert {
    /// Message texte mono-ligne (corps Slack, sujet e-mail).
    pub fn summary(&self) -> String {
        let grp = self
            .group_key
            .as_deref()
            .map(|g| format!(" [{g}]"))
            .unwrap_or_default();
        format!(
            "[{}] {} ({}){} — {} (valeur={:.4}, seuil={:.4})",
            self.state.label(),
            self.rule_name,
            self.severity,
            grp,
            self.description,
            self.value,
            self.threshold
        )
    }

    /// Charge utile JSON (webhook générique).
    pub fn payload(&self) -> serde_json::Value {
        serde_json::json!({
            "rule": self.rule_name,
            "severity": self.severity,
            "state": self.state.label(),
            "value": self.value,
            "threshold": self.threshold,
            "description": self.description,
            "group_key": self.group_key,
            "summary": self.summary(),
        })
    }
}

/// Canal de notification. Implémenté par Slack, e-mail, et (en test) un enregistreur.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync {
    async fn send(&self, alert: &Alert) -> Result<()>;
}

// ── Slack (Web API — bot token) ───────────────────────────────────────────────

const SLACK_POST_MESSAGE_URL: &str = "https://slack.com/api/chat.postMessage";

/// Slack bot credentials: a bot token (`xoxb-…`) and a default channel. Shared by the global and
/// per-project notification config. The legacy incoming-webhook integration is no longer used.
#[derive(Debug, Clone)]
pub struct SlackBot {
    pub token: String,
    pub default_channel: String,
}

/// Posts to a Slack channel through the Web API (`chat.postMessage`) with a bot token.
pub struct SlackNotifier {
    client: reqwest::Client,
    token: String,
    channel: String,
    api_url: String,
}

impl SlackNotifier {
    pub fn new(client: reqwest::Client, token: String, channel: String) -> Self {
        Self {
            client,
            token,
            channel,
            api_url: SLACK_POST_MESSAGE_URL.to_string(),
        }
    }

    /// Same, with an overridable API URL (tests point this at a local mock).
    pub fn with_url(
        client: reqwest::Client,
        token: String,
        channel: String,
        api_url: String,
    ) -> Self {
        Self {
            client,
            token,
            channel,
            api_url,
        }
    }
}

/// Minimal `chat.postMessage` response: Slack always returns HTTP 200, success is `{"ok": …}`.
#[derive(serde::Deserialize)]
struct SlackApiResponse {
    ok: bool,
    #[serde(default)]
    error: Option<String>,
}

#[async_trait::async_trait]
impl Notifier for SlackNotifier {
    async fn send(&self, alert: &Alert) -> Result<()> {
        let resp = self
            .client
            .post(&self.api_url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "channel": self.channel, "text": alert.summary() }))
            .send()
            .await
            .context("POST Slack chat.postMessage")?;
        if !resp.status().is_success() {
            anyhow::bail!("Slack a répondu HTTP {}", resp.status());
        }
        // Slack renvoie 200 même en cas d'échec applicatif : il faut lire `ok`.
        let body: SlackApiResponse = resp.json().await.context("réponse Slack illisible")?;
        if !body.ok {
            anyhow::bail!(
                "Slack chat.postMessage a échoué: {}",
                body.error.as_deref().unwrap_or("erreur inconnue")
            );
        }
        Ok(())
    }
}

// ── E-mail (SMTP via lettre) ──────────────────────────────────────────────────

/// Paramètres SMTP d'envoi d'e-mail d'alerte.
#[derive(Debug, Clone)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub from: String,
    pub to: Vec<String>,
}

pub struct EmailNotifier {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: Mailbox,
    to: Vec<Mailbox>,
}

impl EmailNotifier {
    pub fn new(cfg: &EmailConfig) -> Result<Self> {
        let from: Mailbox = cfg
            .from
            .parse()
            .with_context(|| format!("ALERT_EMAIL_FROM invalide: {}", cfg.from))?;
        let to: Vec<Mailbox> = cfg
            .to
            .iter()
            .map(|addr| {
                addr.parse::<Mailbox>()
                    .with_context(|| format!("ALERT_EMAIL_TO invalide: {addr}"))
            })
            .collect::<Result<_>>()?;
        if to.is_empty() {
            anyhow::bail!("EmailNotifier : aucun destinataire (ALERT_EMAIL_TO)");
        }

        // Relais SMTP avec STARTTLS (rustls) ; chiffrement opportuniste, sans openssl.
        let mut builder = AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.smtp_host)
            .with_context(|| format!("relais SMTP {}", cfg.smtp_host))?
            .port(cfg.smtp_port);
        if let (Some(user), Some(pass)) = (&cfg.username, &cfg.password) {
            builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
        }
        Ok(Self {
            transport: builder.build(),
            from,
            to,
        })
    }

    /// Construit le `Message` lettre (sujet + corps) sans l'envoyer. Testable.
    pub fn build_message(&self, alert: &Alert) -> Result<Message> {
        let mut builder = Message::builder()
            .from(self.from.clone())
            .subject(format!("[Datacat] {}", alert.summary()));
        for rcpt in &self.to {
            builder = builder.to(rcpt.clone());
        }
        builder
            .body(alert.summary())
            .context("construction du message e-mail")
    }
}

#[async_trait::async_trait]
impl Notifier for EmailNotifier {
    async fn send(&self, alert: &Alert) -> Result<()> {
        let message = self.build_message(alert)?;
        self.transport
            .send(message)
            .await
            .context("envoi SMTP de l'alerte")?;
        Ok(())
    }
}

// ── Webhook HTTP générique ────────────────────────────────────────────────────

/// POST la charge utile JSON de l'alerte ([`Alert::payload`]) sur une URL arbitraire, avec
/// des en-têtes optionnels (ex. `Authorization`). Brique de base du système d'actions modulable.
pub struct WebhookNotifier {
    client: reqwest::Client,
    url: String,
    headers: HashMap<String, String>,
}

impl WebhookNotifier {
    pub fn new(client: reqwest::Client, url: String, headers: HashMap<String, String>) -> Self {
        Self {
            client,
            url,
            headers,
        }
    }
}

#[async_trait::async_trait]
impl Notifier for WebhookNotifier {
    async fn send(&self, alert: &Alert) -> Result<()> {
        let mut req = self.client.post(&self.url).json(&alert.payload());
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.context("POST du webhook")?;
        if !resp.status().is_success() {
            anyhow::bail!("webhook a répondu {}", resp.status());
        }
        Ok(())
    }
}

// ── Dispatcher (actions modulables par règle) ─────────────────────────────────

/// Réglages globaux servant de repli pour résoudre les `actions` d'une règle (bot Slack par
/// défaut, configuration SMTP de base). Le client HTTP est partagé entre tous les canaux.
#[derive(Clone, Default)]
pub struct DispatchSettings {
    pub http: reqwest::Client,
    /// Bot Slack global (token + canal par défaut) — repli des actions `slack`.
    pub slack: Option<SlackBot>,
    /// Configuration SMTP de base — repli des actions `email` (le `to` peut être surchargé).
    pub email: Option<EmailConfig>,
}

/// Aiguille chaque règle vers les notifiers à déclencher : ses `actions` résolues si elle en a,
/// sinon les notifiers globaux par défaut. La résolution est faite une fois à la construction.
pub struct Dispatcher {
    /// Notifiers utilisés pour une règle sans `actions`.
    default: Vec<Arc<dyn Notifier>>,
    /// Notifiers résolus par nom de règle (règles avec `actions`).
    per_rule: HashMap<String, Vec<Arc<dyn Notifier>>>,
}

impl Dispatcher {
    /// Dispatcher trivial : un seul jeu de notifiers pour toutes les règles (tests, ou aucune
    /// règle n'utilise `actions`).
    pub fn with_defaults(default: Vec<Arc<dyn Notifier>>) -> Self {
        Self {
            default,
            per_rule: HashMap::new(),
        }
    }

    /// Construit le dispatcher en résolvant les `actions` de chaque règle (repli sur `settings`),
    /// avec `default` comme repli pour les règles sans actions.
    pub fn build(
        rules: &[Rule],
        settings: &DispatchSettings,
        default: Vec<Arc<dyn Notifier>>,
    ) -> Self {
        let mut per_rule = HashMap::new();
        for rule in rules {
            if rule.actions.is_empty() {
                continue;
            }
            let mut notifiers: Vec<Arc<dyn Notifier>> = Vec::new();
            for action in &rule.actions {
                Self::resolve_action(&rule.name, action, settings, &mut notifiers);
            }
            per_rule.insert(rule.name.clone(), notifiers);
        }
        Self { default, per_rule }
    }

    fn resolve_action(
        rule_name: &str,
        action: &Action,
        settings: &DispatchSettings,
        out: &mut Vec<Arc<dyn Notifier>>,
    ) {
        match action {
            Action::Slack { channel } => match &settings.slack {
                Some(bot) => {
                    let chan = channel
                        .clone()
                        .unwrap_or_else(|| bot.default_channel.clone());
                    out.push(Arc::new(SlackNotifier::new(
                        settings.http.clone(),
                        bot.token.clone(),
                        chan,
                    )));
                }
                None => tracing::warn!(
                    rule = %rule_name,
                    "action slack mais aucun bot Slack configuré (bot_token) — ignorée"
                ),
            },
            Action::Email { to } => match &settings.email {
                Some(base) => {
                    let mut cfg = base.clone();
                    if let Some(to) = to {
                        cfg.to = to.clone();
                    }
                    if cfg.to.is_empty() {
                        tracing::warn!(rule = %rule_name, "action email sans destinataire — ignorée");
                        return;
                    }
                    match EmailNotifier::new(&cfg) {
                        Ok(n) => out.push(Arc::new(n)),
                        Err(e) => {
                            tracing::error!(rule = %rule_name, error = %e, "action email invalide — ignorée")
                        }
                    }
                }
                None => {
                    tracing::warn!(rule = %rule_name, "action email mais SMTP non configuré — ignorée")
                }
            },
            Action::Webhook { url, headers } => out.push(Arc::new(WebhookNotifier::new(
                settings.http.clone(),
                url.clone(),
                headers.clone(),
            ))),
        }
    }

    /// Notifiers à déclencher pour une règle : ses actions résolues, sinon les notifiers globaux.
    pub fn for_rule(&self, rule: &Rule) -> &[Arc<dyn Notifier>] {
        self.per_rule
            .get(&rule.name)
            .map(|v| v.as_slice())
            .unwrap_or(&self.default)
    }
}

// ── Enregistreur (tests) ──────────────────────────────────────────────────────

/// Notifier de test : accumule les alertes reçues dans un vecteur partagé.
#[derive(Clone, Default)]
pub struct RecordingNotifier {
    pub alerts: Arc<Mutex<Vec<Alert>>>,
}

impl RecordingNotifier {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn alerts(&self) -> Vec<Alert> {
        self.alerts.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Notifier for RecordingNotifier {
    async fn send(&self, alert: &Alert) -> Result<()> {
        self.alerts.lock().unwrap().push(alert.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_alert() -> Alert {
        Alert {
            rule_name: "latence".into(),
            severity: "critical".into(),
            state: AlertState::Firing,
            value: 742.0,
            threshold: 500.0,
            description: "avg(http.server.duration) > 500 over 300s".into(),
            group_key: None,
        }
    }

    #[test]
    fn summary_is_readable() {
        let s = sample_alert().summary();
        assert!(s.contains("[FIRING]"));
        assert!(s.contains("latence"));
        assert!(s.contains("critical"));
    }

    #[test]
    fn email_message_builds() {
        let notifier = EmailNotifier::new(&EmailConfig {
            smtp_host: "smtp.example.com".into(),
            smtp_port: 587,
            username: Some("u".into()),
            password: Some("p".into()),
            from: "Datacat <alerts@example.com>".into(),
            to: vec!["ops@example.com".into(), "oncall@example.com".into()],
        })
        .unwrap();
        let msg = notifier.build_message(&sample_alert()).unwrap();
        let formatted = String::from_utf8(msg.formatted()).unwrap();
        assert!(formatted.contains("To: ops@example.com"));
        assert!(formatted.contains("oncall@example.com"));
        assert!(formatted.contains("From: Datacat <alerts@example.com>"));
        assert!(formatted.contains("[Datacat]"));
        assert!(formatted.contains("latence"));
    }
}
