//! Integration tests: spin up a local axum mock standing in for the Datacat main instance and
//! assert the companion agent (1) POSTs the right body + auth to `/v1/heartbeat`, and (2) raises a
//! self-alert after `failure_threshold` consecutive failures when main returns 500.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use datacat_companion::agent::{Agent, Beat, StateMachine};
use datacat_companion::alert::{AlertSink, AlertState, SelfAlert};
use datacat_companion::config::Config;
use reqwest::Client;
use serde_json::Value;
use tokio::net::TcpListener;

/// What the mock main captured about the last heartbeat it received.
#[derive(Default)]
struct Captured {
    count: usize,
    last_auth: Option<String>,
    last_id: Option<String>,
}

#[derive(Clone)]
struct MockState {
    captured: Arc<Mutex<Captured>>,
    /// HTTP status the mock returns for `/v1/heartbeat`.
    status: StatusCode,
}

async fn heartbeat_handler(
    State(state): State<MockState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> StatusCode {
    let mut c = state.captured.lock().unwrap();
    c.count += 1;
    c.last_auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    c.last_id = body.get("id").and_then(|v| v.as_str()).map(str::to_string);
    state.status
}

/// Start a local axum mock returning `status` for `POST /v1/heartbeat`. Returns its base URL and
/// the shared capture buffer.
async fn start_mock(status: StatusCode) -> (String, Arc<Mutex<Captured>>) {
    let captured = Arc::new(Mutex::new(Captured::default()));
    let state = MockState {
        captured: captured.clone(),
        status,
    };
    let app = Router::new()
        .route("/v1/heartbeat", post(heartbeat_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), captured)
}

/// In-memory alert sink that records every delivered self-alert.
#[derive(Clone, Default)]
struct RecordingSink {
    alerts: Arc<Mutex<Vec<(AlertState, String)>>>,
}

#[async_trait]
impl AlertSink for RecordingSink {
    async fn send(&self, alert: &SelfAlert) -> anyhow::Result<()> {
        self.alerts
            .lock()
            .unwrap()
            .push((alert.state, alert.text.clone()));
        Ok(())
    }
}

fn config_for(main_url: &str, threshold: u32) -> Config {
    // A webhook channel keeps the TOML valid; the actual sink is injected separately in tests.
    let toml = format!(
        r#"
        main_url = "{main_url}"
        id = "edge-test"
        token = "test-token"
        interval = "10ms"
        failure_threshold = {threshold}
        [alert.webhook]
        url = "http://127.0.0.1:1/unused"
        "#
    );
    Config::from_toml_str(&toml).unwrap()
}

#[tokio::test]
async fn posts_heartbeat_with_correct_body_and_auth() {
    let (base, captured) = start_mock(StatusCode::NO_CONTENT).await;
    let config = config_for(&base, 3);
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let sink = Box::new(RecordingSink::default());
    let agent = Agent::with_parts(client, config, sink);

    // One direct heartbeat must succeed and hit the mock with the right body + auth.
    let beat = agent.heartbeat().await;
    assert_eq!(beat, Beat::Ok, "204 must be treated as success");

    let c = captured.lock().unwrap();
    assert_eq!(c.count, 1);
    assert_eq!(c.last_auth.as_deref(), Some("Bearer test-token"));
    assert_eq!(c.last_id.as_deref(), Some("edge-test"));
}

#[tokio::test]
async fn self_alert_fires_after_threshold_when_main_errors() {
    let (base, captured) = start_mock(StatusCode::INTERNAL_SERVER_ERROR).await;
    let threshold = 3;
    let config = config_for(&base, threshold);
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let recorder = RecordingSink::default();
    let alerts = recorder.alerts.clone();
    let agent = Agent::with_parts(client, config, Box::new(recorder));

    let mut sm = StateMachine::new();
    // First two failures: below threshold → no alert delivered.
    for _ in 0..2 {
        let beat = agent.heartbeat().await;
        assert_eq!(beat, Beat::Failed, "500 must be a failure");
        assert_eq!(agent.process(&mut sm, beat).await, None);
    }
    assert!(alerts.lock().unwrap().is_empty());

    // Third failure reaches the threshold → Firing self-alert delivered.
    let beat = agent.heartbeat().await;
    assert_eq!(agent.process(&mut sm, beat).await, Some(AlertState::Firing));
    {
        let a = alerts.lock().unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0].0, AlertState::Firing);
        assert!(a[0].1.contains("cannot reach Datacat main"));
        assert!(a[0].1.contains(&base));
    }

    // The mock did receive all three attempts.
    assert_eq!(captured.lock().unwrap().count, 3);
}

#[tokio::test]
async fn self_alert_resolves_when_main_recovers() {
    // Main is down: drive failures past the threshold, then a success must clear the alert.
    let (base, _captured) = start_mock(StatusCode::INTERNAL_SERVER_ERROR).await;
    let config = config_for(&base, 2);
    let client = Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let recorder = RecordingSink::default();
    let alerts = recorder.alerts.clone();
    let agent = Agent::with_parts(client, config, Box::new(recorder));

    let mut sm = StateMachine::new();
    // Two failures → Firing.
    agent.process(&mut sm, Beat::Failed).await;
    assert_eq!(
        agent.process(&mut sm, Beat::Failed).await,
        Some(AlertState::Firing)
    );
    // A subsequent success → Resolved.
    assert_eq!(
        agent.process(&mut sm, Beat::Ok).await,
        Some(AlertState::Resolved)
    );

    let a = alerts.lock().unwrap();
    assert_eq!(a.len(), 2);
    assert_eq!(a[1].0, AlertState::Resolved);
    assert!(a[1].1.contains("recovered"));
}
