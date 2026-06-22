//! Service OTLP/gRPC des métriques
//! (`opentelemetry.proto.collector.metrics.v1.MetricsService/Export`). Partage toute la logique
//! d'admission avec le transport HTTP. Le serveur est assemblé dans `crate::grpc`.

use std::time::Instant;

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::metrics::v1::{metric::Data, number_data_point::Value as NumValue};
use tonic::{Request, Response, Status};

use crate::config::ValidationLimits;
use crate::grpc::{app_err_to_status, bearer, request_ip};
use crate::metrics::model::{assemble, buckets_json, MetricFields};
use crate::metrics::{accept_metric_points, authorize_metrics, MetricsParse};
use crate::otlp::nanos_to_dt;
use crate::otlp::proto::attrs_to_map;
use crate::AppState;

/// Implémentation du service OTLP Metrics adossée à l'`AppState`.
pub struct DatacatMetricsService {
    state: AppState,
}

impl DatacatMetricsService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl MetricsService for DatacatMetricsService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let now = Instant::now();
        let ip = request_ip(&request);
        let token = bearer(&request);

        if self.state.anomaly.is_banned(ip, now) {
            return Err(Status::resource_exhausted("IP temporairement bannie"));
        }
        authorize_metrics(&self.state, ip, now, token.as_deref()).map_err(app_err_to_status)?;

        let req = request.into_inner();
        let parse = proto_to_metrics(
            req,
            Utc::now(),
            &self.state.limits,
            self.state.config.max_logs_records,
        );
        let (total, enqueued) =
            accept_metric_points(&self.state, ip, now, parse).map_err(app_err_to_status)?;
        let rejected = (total - enqueued) as i64;

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: (rejected > 0).then(|| ExportMetricsPartialSuccess {
                rejected_data_points: rejected,
                error_message: "back-pressure".into(),
            }),
        }))
    }
}

/// Conversion proto → `StoredMetricPoint` (mêmes champs normalisés que le JSON ⇒ même `point_id`).
fn proto_to_metrics(
    req: ExportMetricsServiceRequest,
    received_at: DateTime<Utc>,
    limits: &ValidationLimits,
    max_records: usize,
) -> MetricsParse {
    let past = received_at - chrono::Duration::from_std(limits.max_past_skew).unwrap();
    let future = received_at + chrono::Duration::from_std(limits.max_future_skew).unwrap();

    let mut out = MetricsParse::default();
    'records: for rm in req.resource_metrics {
        let resource_attrs = rm
            .resource
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or_default();
        let service_name = resource_attrs
            .get("service.name")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        for sm in rm.scope_metrics {
            let scope_name = sm.scope.map(|s| s.name).filter(|s| !s.is_empty());
            for m in sm.metrics {
                if m.name.is_empty() {
                    continue;
                }
                let unit = (!m.unit.is_empty()).then_some(m.unit);

                match m.data {
                    Some(Data::Gauge(g)) => {
                        for p in g.data_points {
                            // Garde-fou mémoire (S-8) : borne le Vec à `max_records + 1`.
                            if out.stored.len() > max_records {
                                break 'records;
                            }
                            push_number(
                                &mut out,
                                received_at,
                                past,
                                future,
                                &m.name,
                                "gauge",
                                unit.as_deref(),
                                p,
                                &service_name,
                                scope_name.as_deref(),
                                &resource_attrs,
                            );
                        }
                    }
                    Some(Data::Sum(s)) => {
                        for p in s.data_points {
                            // Garde-fou mémoire (S-8) : borne le Vec à `max_records + 1`.
                            if out.stored.len() > max_records {
                                break 'records;
                            }
                            push_number(
                                &mut out,
                                received_at,
                                past,
                                future,
                                &m.name,
                                "sum",
                                unit.as_deref(),
                                p,
                                &service_name,
                                scope_name.as_deref(),
                                &resource_attrs,
                            );
                        }
                    }
                    Some(Data::Histogram(h)) => {
                        for p in h.data_points {
                            // Garde-fou mémoire (S-8) : borne le Vec à `max_records + 1`.
                            if out.stored.len() > max_records {
                                break 'records;
                            }
                            let time = nz(p.time_unix_nano).unwrap_or(received_at);
                            if time < past || time > future {
                                out.dropped_skew += 1;
                                continue;
                            }
                            out.stored.push(assemble(MetricFields {
                                received_at,
                                time,
                                metric_name: m.name.clone(),
                                metric_type: "histogram",
                                unit: unit.clone(),
                                value_double: None,
                                value_int: None,
                                count: Some(p.count as i64),
                                sum: p.sum,
                                buckets: Some(buckets_json(&p.explicit_bounds, &p.bucket_counts)),
                                service_name: service_name.clone(),
                                scope_name: scope_name.clone(),
                                attrs: attrs_to_map(&p.attributes),
                                resource_attrs: &resource_attrs,
                            }));
                        }
                    }
                    // summary / exponentialHistogram : non ingérés (documenté).
                    _ => {}
                }
            }
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn push_number(
    out: &mut MetricsParse,
    received_at: DateTime<Utc>,
    past: DateTime<Utc>,
    future: DateTime<Utc>,
    metric_name: &str,
    metric_type: &'static str,
    unit: Option<&str>,
    p: opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
    service_name: &Option<String>,
    scope_name: Option<&str>,
    resource_attrs: &serde_json::Map<String, serde_json::Value>,
) {
    let time = nz(p.time_unix_nano).unwrap_or(received_at);
    if time < past || time > future {
        out.dropped_skew += 1;
        return;
    }
    let (value_double, value_int) = match p.value {
        Some(NumValue::AsDouble(d)) => (Some(d), None),
        Some(NumValue::AsInt(i)) => (None, Some(i)),
        None => (None, None),
    };
    out.stored.push(assemble(MetricFields {
        received_at,
        time,
        metric_name: metric_name.to_string(),
        metric_type,
        unit: unit.map(str::to_string),
        value_double,
        value_int,
        count: None,
        sum: None,
        buckets: None,
        service_name: service_name.clone(),
        scope_name: scope_name.map(str::to_string),
        attrs: attrs_to_map(&p.attributes),
        resource_attrs,
    }));
}

fn nz(nanos: u64) -> Option<DateTime<Utc>> {
    (nanos != 0).then(|| nanos_to_dt(nanos)).flatten()
}
