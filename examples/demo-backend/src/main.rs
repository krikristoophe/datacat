//! Demo consumer backend for Datacat integration example.
//!
//! Exposes:
//!   GET  /api/analytics-token — signs a short-lived EdDSA JWT for the Datacat SDK
//!   POST /api/action          — simulates a business action and emits an OTLP log to Datacat
//!
//! Config (env vars):
//!   PORT            — HTTP listen port (default: 8091)
//!   DATACAT_URL     — Datacat ingest base URL (default: http://127.0.0.1:8090)
//!   SIGNING_KEY_FILE — path to Ed25519 PKCS#8 PEM private key
//!                      (default: ../../backend/tests/fixtures/ed25519_private.pem)

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::{Method, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tower_http::cors::{Any, CorsLayer};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// JWT claims
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct IngestClaims {
    iss: String,
    aud: String,
    sub: String,
    actor_id: String,
    session_id: String,
    tenant_id: String,
    iat: u64,
    exp: u64,
}

// ---------------------------------------------------------------------------
// Shared application state
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AppState {
    encoding_key: Arc<EncodingKey>,
    datacat_url: String,
    http: Client,
}

// ---------------------------------------------------------------------------
// Helper: sign a JWT for a given actor/session/tenant
// ---------------------------------------------------------------------------

fn sign_token(
    key: &EncodingKey,
    actor_id: &str,
    session_id: &str,
    tenant_id: &str,
) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock error")
        .as_secs();

    let claims = IngestClaims {
        iss: "demo-backend".into(),
        aud: "datacat-ingest".into(),
        sub: actor_id.into(),
        actor_id: actor_id.into(),
        session_id: session_id.into(),
        tenant_id: tenant_id.into(),
        iat: now,
        exp: now + 600, // 10 minutes
    };

    let mut header = Header::new(Algorithm::EdDSA);
    header.kid = Some("2026-06-key-1".into());

    encode(&header, &claims, key).context("JWT signing failed")
}

// ---------------------------------------------------------------------------
// GET /api/analytics-token
//
// Returns a short-lived JWT that the React SDK uses to authenticate event
// ingestion. In a real app this endpoint would be protected by session auth;
// here we issue a demo token for a fixed demo user.
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct TokenResponse {
    token: String,
}

async fn analytics_token(State(state): State<AppState>) -> impl IntoResponse {
    // Demo: fixed actor + tenant, new session each time (client SDK owns session_id in reality)
    let actor_id = "demo-user-1";
    let session_id = "demo-session-fixed"; // real app: propagate from client
    let tenant_id = "demo-tenant";

    match sign_token(&state.encoding_key, actor_id, session_id, tenant_id) {
        Ok(token) => (StatusCode::OK, Json(TokenResponse { token })).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "failed to sign analytics token");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "signing failed" })),
            )
                .into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// POST /api/action  { sessionId, actorId, name }
//
// Simulates a business action, then emits an OTLP log to Datacat for
// correlation.
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ActionRequest {
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(rename = "actorId")]
    actor_id: String,
    name: String,
}

#[derive(Serialize)]
struct ActionResponse {
    ok: bool,
    message: String,
}

async fn handle_action(
    State(state): State<AppState>,
    Json(req): Json<ActionRequest>,
) -> impl IntoResponse {
    let tenant_id = "demo-tenant";

    // Sign a service token for the OTLP log emission
    let token = match sign_token(
        &state.encoding_key,
        &req.actor_id,
        &req.session_id,
        tenant_id,
    ) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "failed to sign service token for log emission");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ActionResponse {
                    ok: false,
                    message: "internal error".into(),
                }),
            )
                .into_response();
        }
    };

    // Emit an OTLP log to Datacat
    let log_body = build_otlp_log(
        &req.session_id,
        &req.actor_id,
        tenant_id,
        &format!("Action '{}' executed by actor {}", req.name, req.actor_id),
    );

    let logs_url = format!("{}/v1/logs", state.datacat_url);
    match state
        .http
        .post(&logs_url)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", token))
        .json(&log_body)
        .send()
        .await
    {
        Ok(resp) => {
            if resp.status().is_success() {
                tracing::info!(
                    session_id = %req.session_id,
                    actor_id = %req.actor_id,
                    action = %req.name,
                    "OTLP log emitted successfully"
                );
            } else {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(status = %status, body = %body, "Datacat /v1/logs rejected log");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to emit OTLP log (network error)");
        }
    }

    (
        StatusCode::OK,
        Json(ActionResponse {
            ok: true,
            message: format!("Action '{}' processed", req.name),
        }),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Build an OTLP ExportLogsServiceRequest JSON payload
// ---------------------------------------------------------------------------

fn build_otlp_log(session_id: &str, actor_id: &str, tenant_id: &str, body: &str) -> Value {
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos() as u64;

    let trace_id = Uuid::new_v4().as_simple().to_string();
    let span_id = &trace_id[..16];

    json!({
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name",  "value": { "stringValue": "demo-backend" } },
                        { "key": "session_id",    "value": { "stringValue": session_id } },
                        { "key": "actor_id",      "value": { "stringValue": actor_id } },
                        { "key": "tenant_id",     "value": { "stringValue": tenant_id } }
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": { "name": "demo-backend.actions" },
                        "logRecords": [
                            {
                                "timeUnixNano": now_ns.to_string(),
                                "observedTimeUnixNano": now_ns.to_string(),
                                "severityNumber": 9,
                                "severityText": "INFO",
                                "body": { "stringValue": body },
                                "traceId": trace_id,
                                "spanId": span_id,
                                "attributes": [
                                    { "key": "session_id", "value": { "stringValue": session_id } },
                                    { "key": "actor_id",   "value": { "stringValue": actor_id } },
                                    { "key": "tenant_id",  "value": { "stringValue": tenant_id } }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    })
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "demo_backend=info,tower_http=info".into()),
        )
        .init();

    let key_path = std::env::var("SIGNING_KEY_FILE").unwrap_or_else(|_| {
        // Default: the shared fixture used by backend tests
        let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_else(|_| ".".into());
        format!("{manifest}/../../backend/tests/fixtures/ed25519_private.pem")
    });

    let pem = std::fs::read_to_string(&key_path)
        .with_context(|| format!("reading signing key from {key_path}"))?;

    let encoding_key =
        EncodingKey::from_ed_pem(pem.as_bytes()).context("parsing Ed25519 PEM private key")?;

    let datacat_url =
        std::env::var("DATACAT_URL").unwrap_or_else(|_| "http://127.0.0.1:8090".into());

    let port: u16 = std::env::var("PORT")
        .unwrap_or_else(|_| "8091".into())
        .parse()
        .context("PORT must be a valid port number")?;

    let state = AppState {
        encoding_key: Arc::new(encoding_key),
        datacat_url,
        http: Client::builder()
            .danger_accept_invalid_certs(false)
            .build()?,
    };

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any)
        .allow_origin(Any);

    let app = Router::new()
        .route("/api/analytics-token", get(analytics_token))
        .route("/api/action", post(handle_action))
        .layer(cors)
        .with_state(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!("demo-backend listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
