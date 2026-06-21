//! Notifications d'alerte : trait `Notifier` + implémentations Slack (webhook) et e-mail (SMTP).
//!
//! Le moteur d'évaluation est agnostique du canal : il reçoit un `Vec<Arc<dyn Notifier>>` et
//! diffuse chaque transition sur tous les canaux configurés.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

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
}

impl Alert {
    /// Message texte mono-ligne (corps Slack, sujet e-mail).
    pub fn summary(&self) -> String {
        format!(
            "[{}] {} ({}) — {} (valeur={:.4}, seuil={:.4})",
            self.state.label(),
            self.rule_name,
            self.severity,
            self.description,
            self.value,
            self.threshold
        )
    }
}

/// Canal de notification. Implémenté par Slack, e-mail, et (en test) un enregistreur.
#[async_trait::async_trait]
pub trait Notifier: Send + Sync {
    async fn send(&self, alert: &Alert) -> Result<()>;
}

// ── Slack (webhook entrant) ───────────────────────────────────────────────────

/// Poste `{ "text": ... }` sur un webhook entrant Slack.
pub struct SlackNotifier {
    client: reqwest::Client,
    webhook_url: String,
}

impl SlackNotifier {
    pub fn new(webhook_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            webhook_url,
        }
    }
}

#[async_trait::async_trait]
impl Notifier for SlackNotifier {
    async fn send(&self, alert: &Alert) -> Result<()> {
        let resp = self
            .client
            .post(&self.webhook_url)
            .json(&serde_json::json!({ "text": alert.summary() }))
            .send()
            .await
            .context("POST du webhook Slack")?;
        if !resp.status().is_success() {
            anyhow::bail!("webhook Slack a répondu {}", resp.status());
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
