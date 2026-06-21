//! Tests d'intégration de la couche de lecture (`/v1/query/*`).

mod common;

use std::time::Duration;

use chrono::Utc;
use common::*;
use sqlx::PgPool;
use uuid::Uuid;

async fn seed_events(app: &TestApp, token: &str, session: &str, names: &[&str]) {
    let client = reqwest::Client::new();
    let base = Utc::now();
    let events: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, n)| {
            event_json(
                Uuid::new_v4(),
                n,
                session,
                base + chrono::Duration::seconds(i as i64),
            )
        })
        .collect();
    let r = client
        .post(format!("{}/v1/events", app.base_url))
        .bearer_auth(token)
        .json(&serde_json::json!({ "events": events }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 202);
}

#[sqlx::test]
async fn query_events_and_journeys(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("actor-1", "svc", 600);
    let client = reqwest::Client::new();

    // Deux sessions suivent le même parcours [open, validate], une autre juste [open].
    seed_events(&app, &token, "s1", &["open", "validate"]).await;
    seed_events(&app, &token, "s2", &["open", "validate"]).await;
    seed_events(&app, &token, "s3", &["open"]).await;
    assert_eq!(app.wait_total(5, Duration::from_secs(5)).await, 5);

    // Recherche d'events par session.
    let body: serde_json::Value = client
        .get(format!("{}/v1/query/events?session=s1", app.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["events"].as_array().unwrap().len(), 2);

    // Séquences de parcours fréquentes.
    let body: serde_json::Value = client
        .get(format!("{}/v1/query/journeys", app.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let journeys = body["journeys"].as_array().unwrap();
    let top = journeys
        .iter()
        .find(|j| j["path"] == serde_json::json!(["open", "validate"]))
        .expect("parcours [open, validate] présent");
    assert_eq!(top["occurrences"], 2, "deux sessions ont suivi ce parcours");
}

#[sqlx::test]
async fn query_logs_search(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("svc", "svc", 600);
    let client = reqwest::Client::new();

    let nanos = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let otlp = serde_json::json!({
        "resourceLogs": [{
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": "billing" } }
            ]},
            "scopeLogs": [{ "logRecords": [{
                "timeUnixNano": nanos.to_string(),
                "severityNumber": 17,
                "severityText": "ERROR",
                "body": { "stringValue": "payment gateway timeout" },
                "attributes": [{ "key": "session_id", "value": { "stringValue": "sess-pay" } }]
            }]}]
        }]
    });
    client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&otlp)
        .send()
        .await
        .unwrap();
    app.wait_logs(1, Duration::from_secs(5)).await;

    // Filtre service + sous-chaîne + sévérité.
    let body: serde_json::Value = client
        .get(format!(
            "{}/v1/query/logs?service=billing&q=timeout&severity_min=17",
            app.base_url
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let logs = body["logs"].as_array().unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0]["service_name"], "billing");
    assert_eq!(logs[0]["session_id"], "sess-pay");
}

#[sqlx::test]
async fn query_trace_by_id(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("api", "svc", 600);
    let client = reqwest::Client::new();

    let trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let start = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let body = serde_json::json!({
        "resourceSpans": [{
            "resource": { "attributes": [{ "key": "service.name", "value": { "stringValue": "api" } }] },
            "scopeSpans": [{ "spans": [
                { "traceId": trace_id, "spanId": "1111111111111111", "name": "root",
                  "startTimeUnixNano": start.to_string(), "endTimeUnixNano": (start+3_000_000).to_string() },
                { "traceId": trace_id, "spanId": "2222222222222222", "parentSpanId": "1111111111111111",
                  "name": "db.query", "startTimeUnixNano": (start+1_000_000).to_string(),
                  "endTimeUnixNano": (start+2_000_000).to_string() }
            ]}]
        }]
    });
    client
        .post(format!("{}/v1/traces", app.base_url))
        .bearer_auth(&token)
        .json(&body)
        .send()
        .await
        .unwrap();
    app.wait_spans(2, Duration::from_secs(5)).await;

    let body: serde_json::Value = client
        .get(format!("{}/v1/query/traces/{trace_id}", app.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["span_count"], 2);
    let spans = body["spans"].as_array().unwrap();
    // Ordonnés par start_time : root puis db.query.
    assert_eq!(spans[0]["name"], "root");
    assert_eq!(spans[1]["name"], "db.query");
    assert_eq!(spans[1]["parent_span_id"], "1111111111111111");
}

#[sqlx::test]
async fn query_requires_auth_when_static(pool: PgPool) {
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| {
            c.query_auth = datacat_ingest::config::LogsAuth::Static("read-secret".into());
        }),
    )
    .await;
    let client = reqwest::Client::new();

    // Sans token → 401.
    let r = client
        .get(format!("{}/v1/query/events", app.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // Bon token → 200.
    let r = client
        .get(format!("{}/v1/query/events", app.base_url))
        .bearer_auth("read-secret")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
}

#[sqlx::test]
async fn query_sql_readonly(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let token = mint_ed("a", "s", 600);
    seed_events(&app, &token, "s1", &["open", "validate"]).await;
    app.wait_total(2, Duration::from_secs(5)).await;
    let client = reqwest::Client::new();
    let url = format!("{}/v1/query/sql", app.base_url);

    // SELECT lecture seule → OK.
    let body: serde_json::Value = client
        .post(&url)
        .json(&serde_json::json!({ "sql": "SELECT count(*)::int AS n FROM events" }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["rows"][0]["n"], 2);

    // Écriture (non SELECT/WITH) → 400.
    let r = client
        .post(&url)
        .json(&serde_json::json!({ "sql": "DELETE FROM events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);

    // Instruction multiple (';') → 400.
    let r = client
        .post(&url)
        .json(&serde_json::json!({ "sql": "SELECT 1; DROP TABLE events" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[sqlx::test]
async fn query_sql_disabled_returns_403(pool: PgPool) {
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| c.query_sql_enabled = false),
    )
    .await;
    let r = reqwest::Client::new()
        .post(format!("{}/v1/query/sql", app.base_url))
        .json(&serde_json::json!({ "sql": "SELECT 1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);
}
