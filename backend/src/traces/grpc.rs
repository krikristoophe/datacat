//! Service OTLP/gRPC des traces (`opentelemetry.proto.collector.trace.v1.TraceService/Export`).

use std::time::Instant;

use chrono::{DateTime, Utc};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::trace::v1::span::{Event, Link};
use serde_json::{json, Value};
use tonic::{Request, Response, Status};

use crate::config::ValidationLimits;
use crate::grpc::{app_err_to_status, bearer, request_ip};
use crate::otlp::nanos_to_dt;
use crate::otlp::proto::{attrs_to_map, hex, hex_opt};
use crate::traces::model::assemble_span;
use crate::traces::{accept_spans, authorize_traces, SpansParse};
use crate::AppState;

/// Implémentation du service OTLP Trace adossée à l'`AppState`.
pub struct DatacatTracesService {
    state: AppState,
}

impl DatacatTracesService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl TraceService for DatacatTracesService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let now = Instant::now();
        let ip = request_ip(&request);
        let token = bearer(&request);

        if self.state.anomaly.is_banned(ip, now) {
            return Err(Status::resource_exhausted("IP temporairement bannie"));
        }
        authorize_traces(&self.state, ip, now, token.as_deref()).map_err(app_err_to_status)?;

        let req = request.into_inner();
        let parse = proto_to_spans(req, Utc::now(), &self.state.limits);
        let (total, enqueued) =
            accept_spans(&self.state, ip, now, parse).map_err(app_err_to_status)?;
        let rejected = (total - enqueued) as i64;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: (rejected > 0).then(|| ExportTracePartialSuccess {
                rejected_spans: rejected,
                error_message: "back-pressure".into(),
            }),
        }))
    }
}

fn proto_to_spans(
    req: ExportTraceServiceRequest,
    received_at: DateTime<Utc>,
    limits: &ValidationLimits,
) -> SpansParse {
    let past = received_at - chrono::Duration::from_std(limits.max_past_skew).unwrap();
    let future = received_at + chrono::Duration::from_std(limits.max_future_skew).unwrap();

    let mut out = SpansParse::default();
    for rs in req.resource_spans {
        let resource_attrs = rs
            .resource
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or_default();
        let service_name = resource_attrs
            .get("service.name")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        for ss in rs.scope_spans {
            let scope_name = ss.scope.map(|s| s.name).filter(|s| !s.is_empty());
            for s in ss.spans {
                let (Some(trace_id), Some(span_id)) = (hex_opt(&s.trace_id), hex_opt(&s.span_id))
                else {
                    out.dropped_invalid += 1;
                    continue;
                };

                let start_time = nz(s.start_time_unix_nano).unwrap_or(received_at);
                if start_time < past || start_time > future {
                    out.dropped_skew += 1;
                    continue;
                }

                let status = s.status.as_ref();
                out.stored.push(assemble_span(
                    received_at,
                    trace_id,
                    span_id,
                    hex_opt(&s.parent_span_id),
                    start_time,
                    nz(s.end_time_unix_nano),
                    s.name,
                    Some((s.kind as i64).clamp(0, 5) as i16),
                    service_name.clone(),
                    scope_name.clone(),
                    status.map(|st| (st.code as i64).clamp(0, 2) as i16),
                    status
                        .map(|st| st.message.clone())
                        .filter(|m| !m.is_empty()),
                    attrs_to_map(&s.attributes),
                    &resource_attrs,
                    proto_events_to_json(&s.events),
                    proto_links_to_json(&s.links),
                ));
            }
        }
    }
    out
}

fn nz(nanos: u64) -> Option<DateTime<Utc>> {
    (nanos != 0).then(|| nanos_to_dt(nanos)).flatten()
}

fn proto_events_to_json(events: &[Event]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|e| {
                json!({
                    "time": nz(e.time_unix_nano).map(|d| d.to_rfc3339()),
                    "name": e.name.clone(),
                    "attributes": Value::Object(attrs_to_map(&e.attributes)),
                })
            })
            .collect(),
    )
}

fn proto_links_to_json(links: &[Link]) -> Value {
    Value::Array(
        links
            .iter()
            .map(|l| {
                json!({
                    "trace_id": hex(&l.trace_id),
                    "span_id": hex(&l.span_id),
                    "attributes": Value::Object(attrs_to_map(&l.attributes)),
                })
            })
            .collect(),
    )
}
