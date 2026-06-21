//! Tests d'intégration de l'ingestion des logs techniques OTLP (HTTP → Axum → PostgreSQL).

mod common;

use std::time::Duration;

use chrono::Utc;
use common::*;
use sqlx::PgPool;
use uuid::Uuid;

/// Construit un `ExportLogsServiceRequest` OTLP minimal avec corrélation.
fn otlp_body(session: &str, body: &str, ts: chrono::DateTime<Utc>) -> serde_json::Value {
    let nanos = ts.timestamp_nanos_opt().unwrap() as u64;
    serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": "demo-backend" } },
                { "key": "tenant_id", "value": { "stringValue": "clinic-7" } }
            ]},
            "scopeLogs": [{
                "scope": { "name": "demo.http" },
                "logRecords": [{
                    "timeUnixNano": nanos.to_string(),
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "body": { "stringValue": body },
                    "traceId": "5b8efff798038103d269b633813fc60c",
                    "spanId": "eee19b7ec3c1b174",
                    "attributes": [
                        { "key": "session_id", "value": { "stringValue": session } },
                        { "key": "actor_id", "value": { "stringValue": "user-123" } },
                        { "key": "http.status_code", "value": { "intValue": "200" } }
                    ]
                }]
            }]
        }]
    })
}

#[sqlx::test]
async fn otlp_logs_ingested_and_correlated(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("demo-backend", "svc-session", 600);

    let resp = client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&otlp_body(
            "sess-xyz",
            "user validated planning",
            Utc::now(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "OTLP doit répondre 200");

    assert_eq!(app.wait_logs(1, Duration::from_secs(5)).await, 1);

    let row: LogRow = sqlx::query_as(
        "SELECT service_name, session_id, trace_id, body, severity_number FROM logs LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.service_name.as_deref(), Some("demo-backend"));
    assert_eq!(row.session_id.as_deref(), Some("sess-xyz"));
    assert_eq!(
        row.trace_id.as_deref(),
        Some("5b8efff798038103d269b633813fc60c")
    );
    assert_eq!(row.body.as_deref(), Some("user validated planning"));
    assert_eq!(row.severity_number, Some(9));
}

#[derive(sqlx::FromRow)]
struct LogRow {
    service_name: Option<String>,
    session_id: Option<String>,
    trace_id: Option<String>,
    body: Option<String>,
    severity_number: Option<i16>,
}

#[sqlx::test]
async fn otlp_logs_are_idempotent(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("demo-backend", "svc-session", 600);
    let body = otlp_body("sess-dup", "repeated log line", Utc::now());

    // Même payload renvoyé 3 fois (retry exporter OTLP) → une seule ligne.
    for _ in 0..3 {
        let r = client
            .post(format!("{}/v1/logs", app.base_url))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    app.wait_logs(1, Duration::from_secs(5)).await;
    assert_eq!(app.count_logs().await, 1, "logs dédupliqués");
}

#[sqlx::test]
async fn otlp_logs_require_token(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let r = client
        .post(format!("{}/v1/logs", app.base_url))
        .json(&otlp_body("s", "x", Utc::now()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401, "logs : token requis");
}

#[sqlx::test]
async fn events_and_logs_correlate_by_session(pool: PgPool) {
    // Prouve la corrélation events ↔ logs via session_id (cahier §4.2).
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("user-123", "sess-correlate", 600);
    let now = Utc::now();

    // 1. Un event produit sur la session.
    let ev = event_json(Uuid::new_v4(), "validate_planning", "sess-correlate", now);
    client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(&token)
        .json(&serde_json::json!({ "events": [ev] }))
        .send()
        .await
        .unwrap();

    // 2. Un log technique sur la MÊME session.
    client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&otlp_body(
            "sess-correlate",
            "handled validate_planning",
            now,
        ))
        .send()
        .await
        .unwrap();

    app.wait_total(1, Duration::from_secs(5)).await;
    app.wait_logs(1, Duration::from_secs(5)).await;

    // 3. La jointure sur session_id relie event et log.
    let correlated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM events e JOIN logs l ON e.session_id = l.session_id \
         WHERE e.session_id = 'sess-correlate'",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert!(correlated >= 1, "event et log corrélés par session_id");
}
