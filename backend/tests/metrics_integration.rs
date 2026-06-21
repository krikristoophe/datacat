//! Tests d'intégration de l'ingestion des métriques OTLP (HTTP + gRPC → PostgreSQL).

mod common;

use std::time::Duration;

use chrono::{DateTime, Utc};
use common::*;
use serde_json::Value;
use sqlx::PgPool;

/// Corps OTLP : une gauge (`asDouble`) + un histogram, sur la même resource/scope.
fn metrics_body(service: &str, session: &str, time: DateTime<Utc>) -> serde_json::Value {
    let n = time.timestamp_nanos_opt().unwrap() as u64;
    serde_json::json!({
        "resourceMetrics": [{
            "resource": { "attributes": [
                { "key": "service.name", "value": { "stringValue": service } }
            ]},
            "scopeMetrics": [{
                "scope": { "name": "demo.scope" },
                "metrics": [
                    {
                        "name": "process.cpu.utilization",
                        "unit": "1",
                        "gauge": { "dataPoints": [{
                            "timeUnixNano": n.to_string(),
                            "asDouble": 0.42,
                            "attributes": [
                                { "key": "session_id", "value": { "stringValue": session } }
                            ]
                        }] }
                    },
                    {
                        "name": "http.server.duration",
                        "unit": "ms",
                        "histogram": { "dataPoints": [{
                            "timeUnixNano": n.to_string(),
                            "count": "3",
                            "sum": 600.0,
                            "bucketCounts": ["1", "2", "0"],
                            "explicitBounds": [100.0, 500.0]
                        }] }
                    }
                ]
            }]
        }]
    })
}

#[derive(sqlx::FromRow)]
struct MetricRow {
    metric_name: String,
    metric_type: String,
    unit: Option<String>,
    value_double: Option<f64>,
    count: Option<i64>,
    sum: Option<f64>,
    buckets: Option<Value>,
    service_name: Option<String>,
    session_id: Option<String>,
}

#[sqlx::test]
async fn otlp_metrics_ingested_http(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);

    let r = client
        .post(format!("{}/v1/metrics", app.base_url))
        .bearer_auth(&token)
        .json(&metrics_body("api", "sess-metric", Utc::now()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);

    assert_eq!(app.wait_metrics(2, Duration::from_secs(5)).await, 2);

    let gauge: MetricRow = sqlx::query_as(
        "SELECT metric_name, metric_type, unit, value_double, count, sum, buckets, \
         service_name, session_id FROM metric_points WHERE metric_name = 'process.cpu.utilization'",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(gauge.metric_type, "gauge");
    assert_eq!(gauge.unit.as_deref(), Some("1"));
    assert!((gauge.value_double.unwrap() - 0.42).abs() < 1e-9);
    assert_eq!(gauge.service_name.as_deref(), Some("api"));
    assert_eq!(gauge.session_id.as_deref(), Some("sess-metric"));

    let hist: MetricRow = sqlx::query_as(
        "SELECT metric_name, metric_type, unit, value_double, count, sum, buckets, \
         service_name, session_id FROM metric_points WHERE metric_name = 'http.server.duration'",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(hist.metric_type, "histogram");
    assert_eq!(hist.count, Some(3));
    assert!((hist.sum.unwrap() - 600.0).abs() < 1e-9);
    let b = hist.buckets.unwrap();
    assert_eq!(b["bounds"], serde_json::json!([100.0, 500.0]));
    assert_eq!(b["counts"], serde_json::json!([1, 2, 0]));
}

#[sqlx::test]
async fn otlp_metrics_idempotent(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);
    let body = metrics_body("api", "sess-dup", Utc::now());

    for _ in 0..3 {
        let r = client
            .post(format!("{}/v1/metrics", app.base_url))
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
    }
    app.wait_metrics(2, Duration::from_secs(5)).await;
    assert_eq!(
        app.count_metrics().await,
        2,
        "points de métriques dédupliqués (1 gauge + 1 histogram)"
    );
}

#[sqlx::test]
async fn query_metrics_endpoint(pool: PgPool) {
    let app = start_app(pool, test_config(token_enabled_ed(), |_| {})).await;
    let client = reqwest::Client::new();
    let token = mint_ed("api", "svc", 600);

    client
        .post(format!("{}/v1/metrics", app.base_url))
        .bearer_auth(&token)
        .json(&metrics_body("billing", "sess-q", Utc::now()))
        .send()
        .await
        .unwrap();
    app.wait_metrics(2, Duration::from_secs(5)).await;

    // query_auth = None par défaut en test → pas de token requis.
    let r = client
        .get(format!(
            "{}/v1/query/metrics?name=process.cpu.utilization&service=billing",
            app.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: Value = r.json().await.unwrap();
    let metrics = body["metrics"].as_array().unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0]["metric_name"], "process.cpu.utilization");
    assert_eq!(metrics[0]["service_name"], "billing");
}

#[sqlx::test]
async fn otlp_grpc_metrics_ingested(pool: PgPool) {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{any_value::Value as PV, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric::Data, number_data_point::Value as NumValue, Gauge, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics,
    };
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

    let n = Utc::now().timestamp_nanos_opt().unwrap() as u64;
    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "grpc-api")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "queue.depth".into(),
                    unit: "1".into(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: n,
                            attributes: vec![kv("session_id", "sess-grpc-m")],
                            value: Some(NumValue::AsInt(7)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let mut grpc = MetricsServiceClient::connect(format!("http://{}", app.grpc_addr))
        .await
        .unwrap();
    let mut request = tonic::Request::new(req);
    request
        .metadata_mut()
        .insert("authorization", "Bearer svc-secret".parse().unwrap());
    grpc.export(request).await.unwrap();

    assert_eq!(app.wait_metrics(1, Duration::from_secs(5)).await, 1);
    let row: MetricRow = sqlx::query_as(
        "SELECT metric_name, metric_type, unit, value_double, count, sum, buckets, \
         service_name, session_id FROM metric_points LIMIT 1",
    )
    .fetch_one(&app.pool)
    .await
    .unwrap();
    assert_eq!(row.metric_name, "queue.depth");
    assert_eq!(row.metric_type, "gauge");
    assert_eq!(row.service_name.as_deref(), Some("grpc-api"));
    assert_eq!(row.session_id.as_deref(), Some("sess-grpc-m"));
}
