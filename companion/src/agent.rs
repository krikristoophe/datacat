//! The companion agent: periodically POSTs a heartbeat to the Datacat main instance and, when main
//! stays unreachable for `failure_threshold` consecutive attempts, raises its own alert through the
//! configured sink. On the next successful heartbeat it clears the alert.
//!
//! The state machine (`StateMachine`) is decoupled from I/O so it can be unit tested by feeding it
//! synthetic heartbeat outcomes, while [`Agent::run`] drives the real HTTP loop with `tokio::time`.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tokio::time::{interval, MissedTickBehavior};

use crate::alert::{AlertSink, AlertState, SelfAlert};
use crate::config::Config;

/// Outcome of a single heartbeat attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Beat {
    /// Main acknowledged the heartbeat (HTTP 2xx).
    Ok,
    /// The heartbeat could not be delivered (timeout, connection refused, non-2xx, …).
    Failed,
}

/// Pure consecutive-failure / firing state machine. No I/O: feed it [`Beat`]s, it tells you whether
/// a self-alert transition must be emitted.
#[derive(Debug, Default)]
pub struct StateMachine {
    consecutive_failures: u32,
    firing: bool,
}

impl StateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the agent currently considers main unreachable (alert raised).
    pub fn is_firing(&self) -> bool {
        self.firing
    }

    /// Current consecutive-failure count.
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Apply one heartbeat outcome. Returns `Some(state)` exactly on a transition:
    /// - `Firing` when failures just reached `threshold` and we were not already firing;
    /// - `Resolved` on the first success while firing.
    ///
    /// Returns `None` otherwise (steady state, no notification needed).
    pub fn apply(&mut self, beat: Beat, threshold: u32) -> Option<AlertState> {
        match beat {
            Beat::Ok => {
                self.consecutive_failures = 0;
                if self.firing {
                    self.firing = false;
                    Some(AlertState::Resolved)
                } else {
                    None
                }
            }
            Beat::Failed => {
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
                if !self.firing && self.consecutive_failures >= threshold {
                    self.firing = true;
                    Some(AlertState::Firing)
                } else {
                    None
                }
            }
        }
    }
}

/// The running agent: HTTP client + config + self-alert sink.
pub struct Agent {
    client: Client,
    config: Config,
    sink: Box<dyn AlertSink>,
}

impl Agent {
    /// Build an agent from a resolved [`Config`], constructing the rustls HTTP client and the
    /// concrete self-alert sink.
    pub fn new(config: Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(config.request_timeout)
            .build()
            .context("building HTTP client")?;
        let sink = crate::alert::build_sink(client.clone(), &config.alert);
        Ok(Self {
            client,
            config,
            sink,
        })
    }

    /// Test/explicit constructor with an injected client and sink.
    pub fn with_parts(client: Client, config: Config, sink: Box<dyn AlertSink>) -> Self {
        Self {
            client,
            config,
            sink,
        }
    }

    /// The heartbeat endpoint URL (`{main_url}/v1/heartbeat`).
    fn heartbeat_url(&self) -> String {
        format!("{}/v1/heartbeat", self.config.main_url)
    }

    /// Send a single heartbeat. Returns [`Beat::Ok`] on HTTP 2xx, [`Beat::Failed`] otherwise (a
    /// network error or a non-2xx status). Never errors: an unreachable main is the normal failure
    /// path this agent exists to detect.
    pub async fn heartbeat(&self) -> Beat {
        let res = self
            .client
            .post(self.heartbeat_url())
            .bearer_auth(&self.config.token)
            .json(&serde_json::json!({ "id": self.config.id }))
            .send()
            .await;
        match res {
            Ok(resp) if resp.status().is_success() => Beat::Ok,
            Ok(resp) => {
                tracing::debug!(status = %resp.status(), "heartbeat rejected by main");
                Beat::Failed
            }
            Err(e) => {
                tracing::debug!(error = %e, "heartbeat transport error");
                Beat::Failed
            }
        }
    }

    /// React to one heartbeat outcome: advance the state machine and, on a transition, deliver the
    /// matching self-alert. Returns the transition emitted (if any). Factored out of the loop so it
    /// is independently testable.
    pub async fn process(&self, sm: &mut StateMachine, beat: Beat) -> Option<AlertState> {
        match beat {
            Beat::Ok => tracing::debug!(id = %self.config.id, "heartbeat ok"),
            Beat::Failed => tracing::debug!(
                id = %self.config.id,
                failures = sm.consecutive_failures() + 1,
                "heartbeat failed"
            ),
        }
        let transition = sm.apply(beat, self.config.failure_threshold)?;
        let alert = self.build_alert(transition);
        match transition {
            AlertState::Firing => tracing::warn!(
                id = %self.config.id,
                main_url = %self.config.main_url,
                threshold = self.config.failure_threshold,
                "main unreachable — raising self-alert"
            ),
            AlertState::Resolved => tracing::info!(
                id = %self.config.id,
                main_url = %self.config.main_url,
                "main reachable again — clearing self-alert"
            ),
        }
        if let Err(e) = self.sink.send(&alert).await {
            tracing::error!(error = %e, state = alert.state.label(), "self-alert delivery failed");
        }
        Some(transition)
    }

    /// Compose the human-readable self-alert for a transition.
    fn build_alert(&self, state: AlertState) -> SelfAlert {
        let text = match state {
            AlertState::Firing => format!(
                "Datacat companion '{}' cannot reach Datacat main at {} \
                 ({} consecutive failed heartbeats)",
                self.config.id, self.config.main_url, self.config.failure_threshold
            ),
            AlertState::Resolved => format!(
                "Datacat companion '{}' recovered: Datacat main at {} is reachable again",
                self.config.id, self.config.main_url
            ),
        };
        SelfAlert {
            id: self.config.id.clone(),
            state,
            text,
        }
    }

    /// Run the heartbeat loop until `shutdown` resolves (SIGINT/SIGTERM in `main`). The first
    /// heartbeat is sent immediately, then every `interval`.
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()>) {
        tracing::info!(
            id = %self.config.id,
            main_url = %self.config.main_url,
            interval = ?self.config.interval,
            failure_threshold = self.config.failure_threshold,
            "companion agent started"
        );
        let mut sm = StateMachine::new();
        let mut ticker = interval(self.config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    tracing::info!("companion agent stopping");
                    break;
                }
                _ = ticker.tick() => {
                    let beat = self.heartbeat().await;
                    self.process(&mut sm, beat).await;
                }
            }
        }
    }

    /// Expose the agent's interval (used by `main` for logging / sanity).
    pub fn interval(&self) -> Duration {
        self.config.interval
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_then_resolves_state_machine() {
        let mut sm = StateMachine::new();
        let threshold = 3;

        // First two failures: below threshold, no transition.
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert!(!sm.is_firing());
        assert_eq!(sm.consecutive_failures(), 2);

        // Third failure reaches the threshold → Firing.
        assert_eq!(sm.apply(Beat::Failed, threshold), Some(AlertState::Firing));
        assert!(sm.is_firing());

        // Further failures while firing: no repeated alert.
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert!(sm.is_firing());

        // First success while firing → Resolved, counter cleared.
        assert_eq!(sm.apply(Beat::Ok, threshold), Some(AlertState::Resolved));
        assert!(!sm.is_firing());
        assert_eq!(sm.consecutive_failures(), 0);

        // Steady success: nothing.
        assert_eq!(sm.apply(Beat::Ok, threshold), None);
    }

    #[test]
    fn success_before_threshold_resets_counter() {
        let mut sm = StateMachine::new();
        let threshold = 3;
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        // A success before reaching the threshold must reset and NOT emit a (Resolved) transition
        // since we never fired.
        assert_eq!(sm.apply(Beat::Ok, threshold), None);
        assert_eq!(sm.consecutive_failures(), 0);
        assert!(!sm.is_firing());
        // Now it takes a full `threshold` failures again to fire.
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert_eq!(sm.apply(Beat::Failed, threshold), None);
        assert_eq!(sm.apply(Beat::Failed, threshold), Some(AlertState::Firing));
    }

    #[test]
    fn threshold_of_one_fires_immediately() {
        let mut sm = StateMachine::new();
        assert_eq!(sm.apply(Beat::Failed, 1), Some(AlertState::Firing));
        assert_eq!(sm.apply(Beat::Ok, 1), Some(AlertState::Resolved));
    }
}
