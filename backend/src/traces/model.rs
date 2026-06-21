//! Modèle des traces OpenTelemetry (OTLP) et conversion vers `StoredSpan`.
//!
//! Wire format : **OTLP/HTTP en JSON** (`ExportTraceServiceRequest`). Chaque span est aplati en
//! une ligne, corrélée aux events/logs via tenant/actor/session et reliée par `trace_id`.
//! Idempotence par clé naturelle `(start_time, trace_id, span_id)` — pas de hash nécessaire.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::config::ValidationLimits;
use crate::ingest::{
    push_csv_f64, push_csv_num, push_csv_opt, push_csv_quoted, push_csv_ts, Ingestable,
};
use crate::otlp::json::{attrs_to_map, KeyValue, Resource, Scope, StringOrNum};
use crate::otlp::{correlate, lookup, nanos_to_dt};

// ── Wire format des traces ────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ExportTraceServiceRequest {
    #[serde(default, rename = "resourceSpans")]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceSpans {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(default, rename = "scopeSpans")]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeSpans {
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default)]
    pub spans: Vec<Span>,
}

#[derive(Debug, Deserialize)]
pub struct Span {
    #[serde(default, rename = "traceId")]
    pub trace_id: Option<String>,
    #[serde(default, rename = "spanId")]
    pub span_id: Option<String>,
    #[serde(default, rename = "parentSpanId")]
    pub parent_span_id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub kind: Option<i64>,
    #[serde(default, rename = "startTimeUnixNano")]
    pub start_time_unix_nano: Option<StringOrNum>,
    #[serde(default, rename = "endTimeUnixNano")]
    pub end_time_unix_nano: Option<StringOrNum>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default)]
    pub events: Vec<SpanEvent>,
    #[serde(default)]
    pub links: Vec<SpanLink>,
    #[serde(default)]
    pub status: Option<SpanStatus>,
}

#[derive(Debug, Deserialize)]
pub struct SpanEvent {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<StringOrNum>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct SpanLink {
    #[serde(default, rename = "traceId")]
    pub trace_id: Option<String>,
    #[serde(default, rename = "spanId")]
    pub span_id: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct SpanStatus {
    #[serde(default)]
    pub code: Option<i64>,
    #[serde(default)]
    pub message: Option<String>,
}

// ── Enregistrement persistable ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoredSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub name: String,
    pub kind: Option<i16>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub status_code: Option<i16>,
    pub status_message: Option<String>,
    pub tenant_id: Option<String>,
    pub actor_id: Option<String>,
    pub session_id: Option<String>,
    pub received_at: DateTime<Utc>,
    pub resource_attributes: Value,
    pub span_attributes: Value,
    pub events: Value,
    pub links: Value,
}

#[derive(Debug, Default)]
pub struct SpansParse {
    pub stored: Vec<StoredSpan>,
    pub dropped_skew: u64,
    /// Spans sans `trace_id`/`span_id` exploitables (écartés).
    pub dropped_invalid: u64,
}

/// Aplatit une requête OTLP de traces en `StoredSpan`.
pub fn otlp_to_spans(
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
        let service_name = lookup(&resource_attrs, &["service.name"]);

        for ss in rs.scope_spans {
            let scope_name = ss.scope.and_then(|s| s.name);
            for s in ss.spans {
                let trace_id = s.trace_id.filter(|v| !v.is_empty());
                let span_id = s.span_id.filter(|v| !v.is_empty());
                let (Some(trace_id), Some(span_id)) = (trace_id, span_id) else {
                    out.dropped_invalid += 1;
                    continue;
                };

                let start_time = s
                    .start_time_unix_nano
                    .as_ref()
                    .and_then(|t| t.as_u64())
                    .filter(|&n| n > 0)
                    .and_then(nanos_to_dt)
                    .unwrap_or(received_at);
                if start_time < past || start_time > future {
                    out.dropped_skew += 1;
                    continue;
                }
                let end_time = s
                    .end_time_unix_nano
                    .as_ref()
                    .and_then(|t| t.as_u64())
                    .filter(|&n| n > 0)
                    .and_then(nanos_to_dt);
                let duration_ms = end_time
                    .map(|e| (e - start_time).num_microseconds().unwrap_or(0) as f64 / 1000.0);

                let span_attrs = attrs_to_map(&s.attributes);
                let c = correlate(&span_attrs, &resource_attrs);

                out.stored.push(StoredSpan {
                    trace_id,
                    span_id,
                    parent_span_id: s.parent_span_id.filter(|v| !v.is_empty()),
                    start_time,
                    end_time,
                    duration_ms,
                    name: s.name.unwrap_or_default(),
                    kind: s.kind.map(|k| k.clamp(0, 5) as i16),
                    service_name: service_name.clone(),
                    scope_name: scope_name.clone(),
                    status_code: s
                        .status
                        .as_ref()
                        .and_then(|st| st.code)
                        .map(|c| c.clamp(0, 2) as i16),
                    status_message: s.status.and_then(|st| st.message).filter(|m| !m.is_empty()),
                    tenant_id: c.tenant_id,
                    actor_id: c.actor_id,
                    session_id: c.session_id,
                    received_at,
                    resource_attributes: Value::Object(resource_attrs.clone()),
                    span_attributes: Value::Object(span_attrs),
                    events: events_to_json(&s.events),
                    links: links_to_json(&s.links),
                });
            }
        }
    }
    out
}

fn events_to_json(events: &[SpanEvent]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|e| {
                json!({
                    "time": e.time_unix_nano.as_ref().and_then(|t| t.as_u64()).and_then(nanos_to_dt).map(|d| d.to_rfc3339()),
                    "name": e.name.clone().unwrap_or_default(),
                    "attributes": Value::Object(attrs_to_map(&e.attributes)),
                })
            })
            .collect(),
    )
}

fn links_to_json(links: &[SpanLink]) -> Value {
    Value::Array(
        links
            .iter()
            .map(|l| {
                json!({
                    "trace_id": l.trace_id.clone().unwrap_or_default(),
                    "span_id": l.span_id.clone().unwrap_or_default(),
                    "attributes": Value::Object(attrs_to_map(&l.attributes)),
                })
            })
            .collect(),
    )
}

/// Construit un `StoredSpan` à partir de champs déjà normalisés (utilisé par le transport gRPC).
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble_span(
    received_at: DateTime<Utc>,
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    name: String,
    kind: Option<i16>,
    service_name: Option<String>,
    scope_name: Option<String>,
    status_code: Option<i16>,
    status_message: Option<String>,
    span_attrs: Map<String, Value>,
    resource_attrs: &Map<String, Value>,
    events: Value,
    links: Value,
) -> StoredSpan {
    let c = correlate(&span_attrs, resource_attrs);
    let duration_ms =
        end_time.map(|e| (e - start_time).num_microseconds().unwrap_or(0) as f64 / 1000.0);
    StoredSpan {
        trace_id,
        span_id,
        parent_span_id,
        start_time,
        end_time,
        duration_ms,
        name,
        kind,
        service_name,
        scope_name,
        status_code,
        status_message,
        tenant_id: c.tenant_id,
        actor_id: c.actor_id,
        session_id: c.session_id,
        received_at,
        resource_attributes: Value::Object(resource_attrs.clone()),
        span_attributes: Value::Object(span_attrs),
        events,
        links,
    }
}

// ── Persistance (COPY) ────────────────────────────────────────────────────────

impl Ingestable for StoredSpan {
    fn copy_statement() -> &'static str {
        "COPY spans_staging \
         (trace_id, span_id, parent_span_id, start_time, end_time, duration_ms, name, kind, \
          service_name, scope_name, status_code, status_message, tenant_id, actor_id, session_id, \
          received_at, resource_attributes, span_attributes, events, links) \
         FROM STDIN WITH (FORMAT csv)"
    }
    fn ensure_partitions_statement() -> &'static str {
        "SELECT datacat_ensure_span_partitions_for_staging()"
    }
    fn merge_statement() -> &'static str {
        "SELECT datacat_merge_span_staging()"
    }
    fn staging_table() -> &'static str {
        "spans_staging"
    }
    fn label() -> &'static str {
        "traces"
    }
    fn write_csv_row(&self, out: &mut String) {
        push_csv_quoted(out, &self.trace_id);
        out.push(',');
        push_csv_quoted(out, &self.span_id);
        out.push(',');
        push_csv_opt(out, self.parent_span_id.as_deref());
        out.push(',');
        out.push_str(&self.start_time.to_rfc3339());
        out.push(',');
        push_csv_ts(out, self.end_time);
        out.push(',');
        push_csv_f64(out, self.duration_ms);
        out.push(',');
        push_csv_quoted(out, &self.name);
        out.push(',');
        push_csv_num(out, self.kind.map(i64::from));
        out.push(',');
        push_csv_opt(out, self.service_name.as_deref());
        out.push(',');
        push_csv_opt(out, self.scope_name.as_deref());
        out.push(',');
        push_csv_num(out, self.status_code.map(i64::from));
        out.push(',');
        push_csv_opt(out, self.status_message.as_deref());
        out.push(',');
        push_csv_opt(out, self.tenant_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.actor_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.session_id.as_deref());
        out.push(',');
        out.push_str(&self.received_at.to_rfc3339());
        out.push(',');
        push_csv_quoted(out, &self.resource_attributes.to_string());
        out.push(',');
        push_csv_quoted(out, &self.span_attributes.to_string());
        out.push(',');
        push_csv_quoted(out, &self.events.to_string());
        out.push(',');
        push_csv_quoted(out, &self.links.to_string());
        out.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn limits() -> ValidationLimits {
        ValidationLimits {
            max_batch_events: 500,
            max_payload_bytes: 1_048_576,
            max_properties_bytes: 16_384,
            max_string_len: 200,
            max_json_depth: 16,
            max_past_skew: Duration::from_secs(31 * 86_400),
            max_future_skew: Duration::from_secs(86_400),
        }
    }

    #[test]
    fn flattens_span_and_correlates() {
        let now = Utc::now();
        let start = now.timestamp_nanos_opt().unwrap() as u64;
        let end = start + 5_000_000; // +5 ms
        let req: ExportTraceServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceSpans": [{
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": "api" } }
                ]},
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "5b8efff798038103d269b633813fc60c",
                        "spanId": "eee19b7ec3c1b174",
                        "name": "GET /planning",
                        "kind": 2,
                        "startTimeUnixNano": start.to_string(),
                        "endTimeUnixNano": end.to_string(),
                        "status": { "code": 1 },
                        "attributes": [
                            { "key": "session_id", "value": { "stringValue": "sess-1" } }
                        ]
                    }]
                }]
            }]
        }))
        .unwrap();
        let parsed = otlp_to_spans(req, now, &limits());
        assert_eq!(parsed.stored.len(), 1);
        let s = &parsed.stored[0];
        assert_eq!(s.trace_id, "5b8efff798038103d269b633813fc60c");
        assert_eq!(s.name, "GET /planning");
        assert_eq!(s.kind, Some(2));
        assert_eq!(s.service_name.as_deref(), Some("api"));
        assert_eq!(s.session_id.as_deref(), Some("sess-1"));
        assert_eq!(s.status_code, Some(1));
        assert!((s.duration_ms.unwrap() - 5.0).abs() < 0.01);
    }

    #[test]
    fn drops_span_without_ids() {
        let now = Utc::now();
        let req: ExportTraceServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceSpans": [{ "scopeSpans": [{ "spans": [{ "name": "orphan" }] }] }]
        }))
        .unwrap();
        let parsed = otlp_to_spans(req, now, &limits());
        assert_eq!(parsed.stored.len(), 0);
        assert_eq!(parsed.dropped_invalid, 1);
    }
}
