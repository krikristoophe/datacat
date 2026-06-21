//! Companion liveness: remote "companion" nodes send heartbeats to this main instance, and a
//! background monitor raises an alert (through the alerting notifiers) when an expected companion
//! goes silent — and resolves it when the companion returns.
//!
//! This is the **main side** of a bidirectional dead-man's-switch: the standalone `companion`
//! crate runs on the remote node, POSTs heartbeats here, and raises its *own* alert if it cannot
//! reach this instance (since a down main cannot alert about itself). Any number of companions are
//! supported; each is identified by a stable `id`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};

use crate::alerting::{Alert, AlertState, Notifier};

/// In-memory last-seen registry keyed by companion id. Liveness is soft state: on a main restart,
/// companions re-check-in within one `timeout` window.
#[derive(Default)]
pub struct CompanionRegistry {
    last_seen: DashMap<String, DateTime<Utc>>,
}

impl CompanionRegistry {
    pub fn record(&self, id: &str, now: DateTime<Utc>) {
        self.last_seen.insert(id.to_string(), now);
    }

    pub fn last_seen(&self, id: &str) -> Option<DateTime<Utc>> {
        self.last_seen.get(id).map(|v| *v)
    }

    /// `(id, last_seen)` snapshot, for `/stats` / debugging.
    pub fn snapshot(&self) -> Vec<(String, DateTime<Utc>)> {
        self.last_seen
            .iter()
            .map(|e| (e.key().clone(), *e.value()))
            .collect()
    }
}

/// An expected companion and its liveness `timeout` (silence longer than this ⇒ alert).
#[derive(Debug, Clone)]
pub struct ExpectedCompanion {
    pub id: String,
    pub timeout: Duration,
    pub severity: String,
}

/// Background monitor. Every `check_interval`, for each expected companion: if it has never checked
/// in, or has been silent longer than its `timeout`, transition to firing and notify; when it
/// returns, transition to resolved and notify. Reuses the alerting `Notifier`s (Slack/email/webhook).
pub async fn run_monitor_loop(
    registry: Arc<CompanionRegistry>,
    expected: Vec<ExpectedCompanion>,
    notifiers: Vec<Arc<dyn Notifier>>,
    check_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut firing: HashMap<String, bool> = HashMap::new();
    let mut ticker = interval(check_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tracing::info!(companions = expected.len(), "companion monitor started");
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("companion monitor stopped");
                break;
            }
            _ = ticker.tick() => {
                let now = Utc::now();
                for c in &expected {
                    let Some(alert) = evaluate(&registry, c, now, &mut firing) else {
                        continue;
                    };
                    for n in &notifiers {
                        if let Err(e) = n.send(&alert).await {
                            tracing::warn!(companion = %c.id, error = %e, "companion notification failed");
                        }
                    }
                    tracing::info!(companion = %c.id, state = alert.state.label(), "companion liveness transition");
                }
            }
        }
    }
}

/// Pure transition logic (testable): returns an `Alert` only on an ok↔firing transition.
fn evaluate(
    registry: &CompanionRegistry,
    c: &ExpectedCompanion,
    now: DateTime<Utc>,
    firing: &mut HashMap<String, bool>,
) -> Option<Alert> {
    let last = registry.last_seen(&c.id);
    let silent_for = last.map(|t| (now - t).to_std().unwrap_or(Duration::ZERO));
    // Down if never seen, or last heartbeat older than the timeout.
    let down = match silent_for {
        Some(d) => d > c.timeout,
        None => true,
    };
    let was = firing.get(&c.id).copied().unwrap_or(false);
    let transition = match (was, down) {
        (false, true) => Some(AlertState::Firing),
        (true, false) => Some(AlertState::Resolved),
        _ => None,
    };
    firing.insert(c.id.clone(), down);
    let state = transition?;
    let secs = silent_for.map(|d| d.as_secs()).unwrap_or(0);
    let description = match last {
        Some(_) => format!(
            "companion '{}' silent for {}s (timeout {}s)",
            c.id,
            secs,
            c.timeout.as_secs()
        ),
        None => format!("companion '{}' never seen", c.id),
    };
    Some(Alert {
        rule_name: format!("companion/{}", c.id),
        severity: c.severity.clone(),
        state,
        value: secs as f64,
        threshold: c.timeout.as_secs() as f64,
        description,
        group_key: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected(id: &str, timeout_secs: u64) -> ExpectedCompanion {
        ExpectedCompanion {
            id: id.into(),
            timeout: Duration::from_secs(timeout_secs),
            severity: "critical".into(),
        }
    }

    #[test]
    fn fires_when_never_seen_then_resolves_on_heartbeat() {
        let reg = CompanionRegistry::default();
        let c = expected("edge", 60);
        let mut firing = HashMap::new();
        let now = Utc::now();

        // Never seen → firing.
        let a = evaluate(&reg, &c, now, &mut firing).expect("should fire");
        assert_eq!(a.state, AlertState::Firing);
        // Still down → no repeat.
        assert!(evaluate(&reg, &c, now, &mut firing).is_none());

        // Heartbeat arrives → resolved.
        reg.record("edge", now);
        let r = evaluate(&reg, &c, now, &mut firing).expect("should resolve");
        assert_eq!(r.state, AlertState::Resolved);
    }

    #[test]
    fn fires_when_silent_longer_than_timeout() {
        let reg = CompanionRegistry::default();
        let c = expected("edge", 60);
        let mut firing = HashMap::new();
        let now = Utc::now();

        // Last seen 30s ago, timeout 60s → ok (no transition; starts not-firing).
        reg.record("edge", now - chrono::Duration::seconds(30));
        assert!(evaluate(&reg, &c, now, &mut firing).is_none());

        // Last seen 120s ago → firing.
        reg.record("edge", now - chrono::Duration::seconds(120));
        let a = evaluate(&reg, &c, now, &mut firing).expect("should fire");
        assert_eq!(a.state, AlertState::Firing);
        assert!(a.value >= 120.0);
    }
}
