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

#[sqlx::test]
async fn logs_static_service_token(pool: PgPool) {
    // Token de service FIXE (service-à-service) plutôt que JWT par session.
    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| {
            c.logs_auth = datacat_ingest::config::LogsAuth::Static("svc-secret".into());
        }),
    )
    .await;
    let client = reqwest::Client::new();

    // Bon token statique → 200.
    let r = client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth("svc-secret")
        .json(&otlp_body(
            "sess-static",
            "log via static token",
            Utc::now(),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    // Mauvais token → 401.
    let r = client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth("wrong-secret")
        .json(&otlp_body("s", "x", Utc::now()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    app.wait_logs(1, Duration::from_secs(5)).await;
    assert_eq!(app.count_logs().await, 1);
}

#[sqlx::test]
async fn otlp_logs_over_record_size_limit_are_dropped(pool: PgPool) {
    // S-7 : un seul enregistrement surdimensionné est écarté même si la requête entière
    // reste sous `max_payload_bytes`. Un log normal dans la même requête passe quand même.
    use std::sync::atomic::Ordering;

    let app = start_app(
        pool,
        test_config(token_enabled_ed(), |c| {
            c.limits.max_otlp_record_bytes = 512;
        }),
    )
    .await;
    let client = reqwest::Client::new();
    let token = mint_ed("demo-backend", "svc-session", 600);
    let now = Utc::now();

    // Un body de 4 Kio dépasse la limite de 512 octets par enregistrement.
    let huge = "x".repeat(4096);
    let r = client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&otlp_body("sess-huge", &huge, now))
        .send()
        .await
        .unwrap();
    // La requête est acceptée (200) ; l'enregistrement trop gros est silencieusement écarté.
    assert_eq!(r.status(), 200);

    // Un log normal passe.
    let r = client
        .post(format!("{}/v1/logs", app.base_url))
        .bearer_auth(&token)
        .json(&otlp_body("sess-ok", "small log", now))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    assert_eq!(app.wait_logs(1, Duration::from_secs(5)).await, 1);
    // Seul le petit log est persisté ; le gros n'apparaît jamais.
    let big_persisted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM logs WHERE session_id = 'sess-huge'")
            .fetch_one(&app.pool)
            .await
            .unwrap();
    assert_eq!(
        big_persisted, 0,
        "le log surdimensionné ne doit pas être stocké"
    );
    assert_eq!(
        app.logs_metrics
            .dropped_oversized_total
            .load(Ordering::Relaxed),
        1,
        "le compteur dropped_oversized_total doit refléter l'écart"
    );
}

#[sqlx::test]
async fn otlp_grpc_logs_ingested(pool: PgPool) {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{any_value::Value as PV, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;

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

    let nanos = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let req = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "grpc-svc"), kv("tenant_id", "clinic-9")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: nanos,
                    severity_number: 9,
                    severity_text: "INFO".into(),
                    body: Some(AnyValue {
                        value: Some(PV::StringValue("log via grpc".into())),
                    }),
                    attributes: vec![kv("session_id", "sess-grpc"), kv("actor_id", "user-9")],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut grpc = LogsServiceClient::connect(format!("http://{}", app.grpc_addr))
        .await
        .unwrap();
    let mut request = tonic::Request::new(req);
    request
        .metadata_mut()
        .insert("authorization", "Bearer svc-secret".parse().unwrap());
    grpc.export(request).await.unwrap();

    assert_eq!(app.wait_logs(1, Duration::from_secs(5)).await, 1);
    let row: LogRow = sqlx::query_as(
        "SELECT service_name, session_id, trace_id, body, severity_number FROM logs LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.service_name.as_deref(), Some("grpc-svc"));
    assert_eq!(row.session_id.as_deref(), Some("sess-grpc"));
    assert_eq!(row.body.as_deref(), Some("log via grpc"));
    assert_eq!(row.severity_number, Some(9));
}
