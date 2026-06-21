//! Tests d'intégration de l'ingestion des traces OTLP (HTTP + gRPC → PostgreSQL).

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::*;
use sqlx::PgPool;

const TRACE_ID: &str = "5b8efff798038103d269b633813fc60c";
const SPAN_ID: &str = "eee19b7ec3c1b174";

fn trace_body(
    trace_id: &str,
    span_id: &str,
    session: &str,
    start: DateTime<Utc>,
) -> serde_json::Value {
    let start_n = start.timestamp_nanos_opt().unwrap() as u64;
    let end_n = start_n + 5_000_000; // +5 ms
    serde_json::json!({
        "resourceSpans": [{
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": "api" } }
            ]},
            "scopeSpans": [{
                "spans": [{
                    "traceId": trace_id,
                    "spanId": span_id,
                    "name": "GET /planning",
                    "kind": 2,
                    "startTimeUnixNano": start_n.to_string(),
                    "endTimeUnixNano": end_n.to_string(),
                    "status": { "code": 1 },
                    "attributes": [
                        { "key": "session_id", "value": { "stringValue": session } },
                        { "key": "http.method", "value": { "stringValue": "GET" } }
                    ]
                }]
            }]
        }]
    })
}

#[derive(sqlx::FromRow)]
struct SpanRow {
    trace_id: String,
    name: String,
    service_name: Option<String>,
    session_id: Option<String>,
    status_code: Option<i16>,
    duration_ms: Option<f64>,
}

#[sqlx::test]
async fn otlp_traces_ingested_http(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);

    let r = client
        .post(format!("{}/v1/traces", app.base_url))
        .bearer_auth(&token)
        .json(&trace_body(TRACE_ID, SPAN_ID, "sess-trace", Utc::now()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    assert_eq!(app.wait_spans(1, Duration::from_secs(5)).await, 1);
    let row: SpanRow = sqlx::query_as(
        "SELECT trace_id, name, service_name, session_id, status_code, duration_ms FROM spans LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.trace_id, TRACE_ID);
    assert_eq!(row.name, "GET /planning");
    assert_eq!(row.service_name.as_deref(), Some("api"));
    assert_eq!(row.session_id.as_deref(), Some("sess-trace"));
    assert_eq!(row.status_code, Some(1));
    assert!((row.duration_ms.unwrap() - 5.0).abs() < 0.1);
}

#[sqlx::test]
async fn otlp_traces_idempotent(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);
    let body = trace_body(TRACE_ID, SPAN_ID, "sess-dup", Utc::now());

    for _ in 0..3 {
        let r = client
            .post(format!("{}/v1/traces", app.base_url))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    app.wait_spans(1, Duration::from_secs(5)).await;
    assert_eq!(app.count_spans().await, 1, "span dédupliqué");
}

#[sqlx::test]
async fn logs_and_traces_correlate_by_trace_id(pool: PgPool) {
    // Corrélation logs ↔ traces via trace_id (cœur APM).
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);
    let now = Utc::now();

    // Une trace.
    client
        .post(format!("{}/v1/traces", app.base_url))
        .bearer_auth(&token)
        .json(&trace_body(TRACE_ID, SPAN_ID, "sess-corr", now))
        .send()
        .await
        .unwrap();

    // Un log portant le même trace_id.
    let nanos = now.timestamp_nanos_opt().unwrap() as u64;
    let log = serde_json::json!({
        "resourceLogs": [{
            "scopeLogs": [{ "logRecords": [{
                "timeUnixNano": nanos.to_string(),
                "severityText": "ERROR",
                "body": { "stringValue": "boom in span" },
                "traceId": TRACE_ID,
                "spanId": SPAN_ID
            }]}]
        }]
    });
    client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&log)
        .send()
        .await
        .unwrap();

    app.wait_spans(1, Duration::from_secs(5)).await;
    app.wait_logs(1, Duration::from_secs(5)).await;

    let correlated: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM spans s JOIN logs l ON s.trace_id = l.trace_id WHERE s.trace_id = $1",
    )
    .bind(TRACE_ID)
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert!(correlated >= 1, "log et span corrélés par trace_id");
}

#[sqlx::test]
async fn otlp_grpc_traces_ingested(pool: PgPool) {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{any_value::Value as PV, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| {
            c.logs_auth = datacat_ingest::config::LogsAuth::Static("svc-secret".into());
        }),
    )
    .await;

    fn kv(k: &str, v: &str) -> KeyValue {
        KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(PV::StringValue(v.into())),
            }),
            ..Default::default()
        }
    }

    let start = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "grpc-api")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: (0u8..16).collect(),
                    span_id: (0u8..8).collect(),
                    name: "grpc span".into(),
                    kind: 3,
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 2_000_000,
                    attributes: vec![kv("session_id", "sess-grpc-tr")],
                    status: Some(Status {
                        code: 2,
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut grpc = TraceServiceClient::connect(format!("http://{}", app.grpc_addr))
        .await
        .unwrap();
    let mut request = tonic::Request::new(req);
    request
        .metadata_mut()
        .insert("authorization", "Bearer svc-secret".parse().unwrap());
    grpc.export(request).await.unwrap();

    assert_eq!(app.wait_spans(1, Duration::from_secs(5)).await, 1);
    let row: SpanRow = sqlx::query_as(
        "SELECT trace_id, name, service_name, session_id, status_code, duration_ms FROM spans LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.name, "grpc span");
    assert_eq!(row.service_name.as_deref(), Some("grpc-api"));
    assert_eq!(row.session_id.as_deref(), Some("sess-grpc-tr"));
    assert_eq!(row.status_code, Some(2));
}
