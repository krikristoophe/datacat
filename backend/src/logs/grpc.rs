//! Service OTLP/gRPC des logs (`opentelemetry.proto.collector.logs.v1.LogsService/Export`).
//! Partage toute la logique d'admission avec le transport HTTP. Le serveur (et l'enregistrement
//! des services) est assemblé dans `crate::grpc`.

use std::time::Instant;

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tonic::{Request, Response, Status};

use crate::config::ValidationLimits;
use crate::grpc::{app_err_to_status, bearer, request_ip};
use crate::logs::model::{assemble_log, LogFields};
use crate::logs::{accept_logs, authorize_logs, LogsParse};
use crate::otlp::nanos_to_dt;
use crate::otlp::proto::{anyvalue_to_string, attrs_to_map, hex_opt};
use crate::AppState;

/// Implémentation du service OTLP Logs adossée à l'`AppState`.
pub struct DatacatLogsService {
    state: AppState,
}

impl DatacatLogsService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl LogsService for DatacatLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let now = Instant::now();
        let ip = request_ip(&request);
        let token = bearer(&request);

        if self.state.anomaly.is_banned(ip, now) {
            return Err(Status::resource_exhausted("IP temporairement bannie"));
        }
        authorize_logs(&self.state, ip, now, token.as_deref()).map_err(app_err_to_status)?;

        let req = request.into_inner();
        let parse = proto_to_logs(req, Utc::now(), &self.state.limits);
        let (total, enqueued) =
            accept_logs(&self.state, ip, now, parse).map_err(app_err_to_status)?;
        let rejected = (total - enqueued) as i64;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: (rejected > 0).then(|| ExportLogsPartialSuccess {
                rejected_log_records: rejected,
                error_message: "back-pressure".into(),
            }),
        }))
    }
}

/// Conversion proto → `StoredLog` (mêmes champs normalisés que le JSON ⇒ même `log_id`).
fn proto_to_logs(
    req: ExportLogsServiceRequest,
    received_at: DateTime<Utc>,
    limits: &ValidationLimits,
) -> LogsParse {
    let past = received_at - chrono::Duration::from_std(limits.max_past_skew).unwrap();
    let future = received_at + chrono::Duration::from_std(limits.max_future_skew).unwrap();

    let mut out = LogsParse::default();
    for rl in req.resource_logs {
        let resource_attrs = rl
            .resource
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or_default();
        let service_name = resource_attrs
            .get("service.name")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        for sl in rl.scope_logs {
            let scope_name = sl.scope.map(|s| s.name).filter(|s| !s.is_empty());
            for r in sl.log_records {
                let log_time = nz(r.time_unix_nano)
                    .or_else(|| nz(r.observed_time_unix_nano))
                    .unwrap_or(received_at);
                if log_time < past || log_time > future {
                    out.dropped_skew += 1;
                    continue;
                }

                out.stored.push(assemble_log(LogFields {
                    received_at,
                    log_time,
                    observed_time: nz(r.observed_time_unix_nano),
                    severity_number: (r.severity_number != 0)
                        .then(|| r.severity_number.clamp(0, 24) as i16),
                    severity_text: opt(r.severity_text),
                    body: r.body.as_ref().map(anyvalue_to_string),
                    service_name: service_name.clone(),
                    scope_name: scope_name.clone(),
                    trace_id: hex_opt(&r.trace_id),
                    span_id: hex_opt(&r.span_id),
                    log_attrs: attrs_to_map(&r.attributes),
                    resource_attrs: &resource_attrs,
                }));
            }
        }
    }
    out
}

fn nz(nanos: u64) -> Option<DateTime<Utc>> {
    (nanos != 0).then(|| nanos_to_dt(nanos)).flatten()
}
fn opt(s: String) -> Option<String> {
    (!s.is_empty()).then_some(s)
}
