//! Self-alert delivery: when this companion cannot reach the Datacat main instance, it raises its
//! *own* alert through the configured channel (Slack or generic webhook). This is the half of the
//! dead-man's-switch the main side cannot cover — a main that is down cannot alert about itself.

use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;

use crate::config::AlertChannel;

const SLACK_POST_MESSAGE_URL: &str = "https://slack.com/api/chat.postMessage";

/// Direction of a self-alert transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertState {
    /// Main is unreachable: the alert is raised.
    Firing,
    /// Main is reachable again: the alert is cleared.
    Resolved,
}

impl AlertState {
    /// Wire label used in webhook payloads.
    pub fn label(self) -> &'static str {
        match self {
            AlertState::Firing => "firing",
            AlertState::Resolved => "resolved",
        }
    }
}

/// A self-alert: the companion id, the transition direction, and a human-readable text.
#[derive(Debug, Clone)]
pub struct SelfAlert {
    pub id: String,
    pub state: AlertState,
    pub text: String,
}

/// An object able to deliver a [`SelfAlert`]. Abstracted as a trait so the agent loop can be unit
/// tested against an in-memory recorder without any network.
#[async_trait]
pub trait AlertSink: Send + Sync {
    async fn send(&self, alert: &SelfAlert) -> Result<()>;
}

/// Build the concrete alert sink for the configured channel.
pub fn build_sink(client: Client, channel: &AlertChannel) -> Box<dyn AlertSink> {
    match channel {
        AlertChannel::Slack { bot_token, channel } => Box::new(SlackSink {
            client,
            token: bot_token.clone(),
            channel: channel.clone(),
            url: SLACK_POST_MESSAGE_URL.to_string(),
        }),
        AlertChannel::Webhook { url, headers } => Box::new(WebhookSink {
            client,
            url: url.clone(),
            headers: headers.clone(),
        }),
    }
}

// ── Slack (Web API — bot token) ───────────────────────────────────────────────

/// Posts to a Slack channel through the Web API (`chat.postMessage`) with a bot token.
pub struct SlackSink {
    client: Client,
    token: String,
    channel: String,
    /// Overridable so tests can point it at a local mock.
    url: String,
}

impl SlackSink {
    /// Construct a Slack sink targeting a custom API URL (used by tests).
    pub fn with_url(client: Client, token: String, channel: String, url: String) -> Self {
        Self {
            client,
            token,
            channel,
            url,
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

#[async_trait]
impl AlertSink for SlackSink {
    async fn send(&self, alert: &SelfAlert) -> Result<()> {
        let resp = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "channel": self.channel, "text": alert.text }))
            .send()
            .await
            .context("POST Slack chat.postMessage")?;
        if !resp.status().is_success() {
            anyhow::bail!("Slack returned HTTP {}", resp.status());
        }
        // Slack returns HTTP 200 even on application failure: the `ok` field must be read.
        let body: SlackApiResponse = resp.json().await.context("unreadable Slack response")?;
        if !body.ok {
            anyhow::bail!(
                "Slack chat.postMessage failed: {}",
                body.error.as_deref().unwrap_or("unknown error")
            );
        }
        Ok(())
    }
}

// ── Generic webhook ───────────────────────────────────────────────────────────

/// Posts a JSON `{"id", "state", "text"}` body to a configured URL, with optional extra headers.
pub struct WebhookSink {
    client: Client,
    url: String,
    headers: std::collections::BTreeMap<String, String>,
}

#[async_trait]
impl AlertSink for WebhookSink {
    async fn send(&self, alert: &SelfAlert) -> Result<()> {
        let mut req = self.client.post(&self.url).json(&serde_json::json!({
            "id": alert.id,
            "state": alert.state.label(),
            "text": alert.text,
        }));
        for (k, v) in &self.headers {
            req = req.header(k, v);
        }
        let resp = req.send().await.context("POST self-alert webhook")?;
        if !resp.status().is_success() {
            anyhow::bail!("self-alert webhook returned HTTP {}", resp.status());
        }
        Ok(())
    }
}
