//! Transport **OTLP/gRPC** des logs (port 4317 par défaut) — service standard
//! `opentelemetry.proto.collector.logs.v1.LogsService/Export`. Partage toute la logique
//! d'admission avec le transport HTTP (auth de service, rate limit, dédup, enfilage).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use serde_json::{Map, Value};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

use crate::config::ValidationLimits;
use crate::error::AppError;
use crate::logs::model::{assemble_log, nanos_to_dt, LogFields};
use crate::logs::{accept_logs, authorize_logs, LogsParse};
use crate::AppState;

/// Implémentation du service OTLP Logs adossée à l'`AppState`.
pub struct DatacatLogsService {
    state: AppState,
}

#[tonic::async_trait]
impl LogsService for DatacatLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let now = Instant::now();
        let ip = request
            .remote_addr()
            .map(|a| a.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_bearer);

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

/// Sert le gRPC sur `listener` jusqu'au `shutdown`.
pub async fn serve<F>(state: AppState, listener: TcpListener, shutdown: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let incoming = TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(LogsServiceServer::new(DatacatLogsService { state }))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    Ok(())
}

fn parse_bearer(value: &str) -> Option<String> {
    value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn app_err_to_status(e: AppError) -> Status {
    match e {
        AppError::Unauthorized(m) => Status::unauthenticated(m),
        AppError::RateLimited { scope, .. } => {
            Status::resource_exhausted(format!("rate limit: {scope}"))
        }
        AppError::PayloadTooLarge(m) => Status::out_of_range(m),
        AppError::BadRequest { message, .. } => Status::invalid_argument(message),
        AppError::Unavailable(m) => Status::unavailable(m),
        AppError::Internal(_) => Status::internal("erreur interne"),
    }
}

// ── Conversion proto → StoredLog (mêmes champs normalisés que le JSON) ─────────

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
            .map(|r| proto_attrs(&r.attributes))
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
                    body: r.body.as_ref().map(proto_anyvalue_to_string),
                    service_name: service_name.clone(),
                    scope_name: scope_name.clone(),
                    trace_id: hex_opt(&r.trace_id),
                    span_id: hex_opt(&r.span_id),
                    log_attrs: proto_attrs(&r.attributes),
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
fn hex_opt(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn proto_attrs(attrs: &[KeyValue]) -> Map<String, Value> {
    let mut m = Map::new();
    for kv in attrs {
        m.insert(
            kv.key.clone(),
            kv.value
                .as_ref()
                .map(proto_anyvalue_to_json)
                .unwrap_or(Value::Null),
        );
    }
    m
}

fn proto_anyvalue_to_json(v: &AnyValue) -> Value {
    use any_value::Value as P;
    match &v.value {
        Some(P::StringValue(s)) => Value::String(s.clone()),
        Some(P::BoolValue(b)) => Value::Bool(*b),
        Some(P::IntValue(i)) => Value::from(*i),
        Some(P::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(P::ArrayValue(a)) => {
            Value::Array(a.values.iter().map(proto_anyvalue_to_json).collect())
        }
        Some(P::KvlistValue(kv)) => Value::Object(proto_attrs(&kv.values)),
        Some(P::BytesValue(b)) => Value::String(b.iter().map(|x| format!("{x:02x}")).collect()),
        // Variantes OTLP plus récentes (ex. string via table) ou absentes : non corrélées.
        Some(_) => Value::Null,
        None => Value::Null,
    }
}

fn proto_anyvalue_to_string(v: &AnyValue) -> String {
    match proto_anyvalue_to_json(v) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

/// Pratique pour exposer l'adresse écoutée dans les tests.
pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}
