//! Modèle des logs techniques OpenTelemetry (OTLP) et conversion vers `StoredLog`.
//!
//! Wire format : **OTLP/HTTP en JSON** (`ExportLogsServiceRequest`). Chaque `LogRecord` est
//! aplati en une ligne, corrélée aux events via tenant/actor/session et aux traces via
//! trace_id/span_id (cf. docs/otel-logs.md). Les types OTLP communs (AnyValue, corrélation,
//! horodatage) viennent du module `crate::otlp`.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::ValidationLimits;
use crate::ingest::{push_csv_num, push_csv_opt, push_csv_quoted, push_csv_ts, Ingestable};
use crate::otlp::json::{anyvalue_to_string, attrs_to_map, AnyValue, KeyValue, Resource, Scope};
use crate::otlp::{correlate, lookup, nanos_to_dt};

// ── Wire format des logs ──────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ExportLogsServiceRequest {
    #[serde(default, rename = "resourceLogs")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceLogs {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(default, rename = "scopeLogs")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeLogs {
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default, rename = "logRecords")]
    pub log_records: Vec<LogRecord>,
}

#[derive(Debug, Deserialize)]
pub struct LogRecord {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<crate::otlp::json::StringOrNum>,
    #[serde(default, rename = "observedTimeUnixNano")]
    pub observed_time_unix_nano: Option<crate::otlp::json::StringOrNum>,
    #[serde(default, rename = "severityNumber")]
    pub severity_number: Option<i64>,
    #[serde(default, rename = "severityText")]
    pub severity_text: Option<String>,
    #[serde(default)]
    pub body: Option<AnyValue>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    #[serde(default, rename = "traceId")]
    pub trace_id: Option<String>,
    #[serde(default, rename = "spanId")]
    pub span_id: Option<String>,
}

// ── Enregistrement persistable ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoredLog {
    pub log_id: Uuid,
    pub log_time: DateTime<Utc>,
    pub observed_time: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    pub severity_number: Option<i16>,
    pub severity_text: Option<String>,
    pub body: Option<String>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub tenant_id: Option<String>,
    pub actor_id: Option<String>,
    pub session_id: Option<String>,
    pub resource_attributes: Value,
    pub log_attributes: Value,
}

impl StoredLog {
    /// Taille approximative (octets) du contenu variable de l'enregistrement. Sert au garde-fou
    /// de taille par enregistrement OTLP (S-7) : un seul log surdimensionné ne doit pas passer
    /// même si la requête entière reste sous `max_payload_bytes`.
    pub fn approx_content_bytes(&self) -> usize {
        use crate::otlp::{json_byte_len, opt_len};
        opt_len(&self.body)
            + opt_len(&self.service_name)
            + opt_len(&self.scope_name)
            + opt_len(&self.severity_text)
            + opt_len(&self.trace_id)
            + opt_len(&self.span_id)
            + json_byte_len(&self.resource_attributes)
            + json_byte_len(&self.log_attributes)
    }
}

/// Résultat d'un aplatissement OTLP.
#[derive(Debug, Default)]
pub struct LogsParse {
    pub stored: Vec<StoredLog>,
    pub dropped_skew: u64,
}

/// Aplatit une requête OTLP en `StoredLog`. Les enregistrements hors fenêtre de skew sont
/// écartés (perte tolérée). Le parsing JSON (échec → 400) a lieu en amont.
pub fn otlp_to_logs(
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
        let service_name = lookup(&resource_attrs, &["service.name"]);

        for sl in rl.scope_logs {
            let scope_name = sl.scope.and_then(|s| s.name);
            for r in sl.log_records {
                let log_time = r
                    .time_unix_nano
                    .as_ref()
                    .and_then(|t| t.as_u64())
                    .filter(|&n| n > 0)
                    .and_then(nanos_to_dt)
                    .or_else(|| {
                        r.observed_time_unix_nano
                            .as_ref()
                            .and_then(|t| t.as_u64())
                            .filter(|&n| n > 0)
                            .and_then(nanos_to_dt)
                    })
                    .unwrap_or(received_at);

                if log_time < past || log_time > future {
                    out.dropped_skew += 1;
                    continue;
                }

                out.stored.push(assemble_log(LogFields {
                    received_at,
                    log_time,
                    observed_time: r
                        .observed_time_unix_nano
                        .as_ref()
                        .and_then(|t| t.as_u64())
                        .filter(|&n| n > 0)
                        .and_then(nanos_to_dt),
                    severity_number: r.severity_number.map(|n| n.clamp(0, 24) as i16),
                    severity_text: r.severity_text.clone(),
                    body: r.body.as_ref().map(anyvalue_to_string),
                    service_name: service_name.clone(),
                    scope_name: scope_name.clone(),
                    trace_id: r.trace_id.clone().filter(|s| !s.is_empty()),
                    span_id: r.span_id.clone().filter(|s| !s.is_empty()),
                    log_attrs: attrs_to_map(&r.attributes),
                    resource_attrs: &resource_attrs,
                }));
            }
        }
    }
    out
}

/// Champs normalisés d'un log (indépendants du transport JSON/gRPC).
pub(crate) struct LogFields<'a> {
    pub received_at: DateTime<Utc>,
    pub log_time: DateTime<Utc>,
    pub observed_time: Option<DateTime<Utc>>,
    pub severity_number: Option<i16>,
    pub severity_text: Option<String>,
    pub body: Option<String>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub log_attrs: Map<String, Value>,
    pub resource_attrs: &'a Map<String, Value>,
}

/// Assemble un `StoredLog` (corrélation + `log_id` déterministe). Partagé par les transports
/// JSON et gRPC → même dédup quel que soit OTLP/JSON ou OTLP/gRPC.
pub(crate) fn assemble_log(f: LogFields<'_>) -> StoredLog {
    let c = correlate(&f.log_attrs, f.resource_attrs);
    let log_attributes = Value::Object(f.log_attrs);
    let log_id = dedup_id(
        f.log_time,
        f.service_name.as_deref(),
        f.body.as_deref(),
        f.trace_id.as_deref(),
        f.span_id.as_deref(),
        f.severity_number,
        &log_attributes,
    );

    StoredLog {
        log_id,
        log_time: f.log_time,
        observed_time: f.observed_time,
        received_at: f.received_at,
        severity_number: f.severity_number,
        severity_text: f.severity_text,
        body: f.body,
        service_name: f.service_name,
        scope_name: f.scope_name,
        trace_id: f.trace_id,
        span_id: f.span_id,
        tenant_id: c.tenant_id,
        actor_id: c.actor_id,
        session_id: c.session_id,
        resource_attributes: Value::Object(f.resource_attrs.clone()),
        log_attributes,
    }
}

/// `log_id` déterministe = hash du contenu normalisé (dédup des renvois OTLP identiques).
#[allow(clippy::too_many_arguments)]
fn dedup_id(
    log_time: DateTime<Utc>,
    service_name: Option<&str>,
    body: Option<&str>,
    trace_id: Option<&str>,
    span_id: Option<&str>,
    severity_number: Option<i16>,
    log_attributes: &Value,
) -> Uuid {
    let mut h = Sha256::new();
    h.update(log_time.timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    h.update(service_name.unwrap_or("").as_bytes());
    h.update([0]);
    h.update(body.unwrap_or("").as_bytes());
    h.update([0]);
    h.update(trace_id.unwrap_or("").as_bytes());
    h.update(span_id.unwrap_or("").as_bytes());
    h.update(severity_number.unwrap_or(0).to_le_bytes());
    h.update(log_attributes.to_string().as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

// ── Persistance (COPY) ────────────────────────────────────────────────────────

impl Ingestable for StoredLog {
    fn copy_statement() -> &'static str {
        "COPY logs_staging \
         (log_id, log_time, observed_time, received_at, severity_number, severity_text, body, \
          service_name, scope_name, trace_id, span_id, tenant_id, actor_id, session_id, \
          resource_attributes, log_attributes) \
         FROM STDIN WITH (FORMAT csv)"
    }
    fn ensure_partitions_statement() -> &'static str {
        "SELECT datacat_ensure_log_partitions_for_staging()"
    }
    fn merge_statement() -> &'static str {
        "SELECT datacat_merge_log_staging()"
    }
    fn staging_table() -> &'static str {
        "logs_staging"
    }
    fn label() -> &'static str {
        "logs"
    }
    fn write_csv_row(&self, out: &mut String) {
        out.push_str(&self.log_id.to_string());
        out.push(',');
        out.push_str(&self.log_time.to_rfc3339());
        out.push(',');
        push_csv_ts(out, self.observed_time);
        out.push(',');
        out.push_str(&self.received_at.to_rfc3339());
        out.push(',');
        push_csv_num(out, self.severity_number.map(i64::from));
        out.push(',');
        push_csv_opt(out, self.severity_text.as_deref());
        out.push(',');
        push_csv_opt(out, self.body.as_deref());
        out.push(',');
        push_csv_opt(out, self.service_name.as_deref());
        out.push(',');
        push_csv_opt(out, self.scope_name.as_deref());
        out.push(',');
        push_csv_opt(out, self.trace_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.span_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.tenant_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.actor_id.as_deref());
        out.push(',');
        push_csv_opt(out, self.session_id.as_deref());
        out.push(',');
        push_csv_quoted(out, &self.resource_attributes.to_string());
        out.push(',');
        push_csv_quoted(out, &self.log_attributes.to_string());
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
            max_otlp_record_bytes: 65_536,
        }
    }

    fn sample(now: DateTime<Utc>) -> ExportLogsServiceRequest {
        let nanos = now.timestamp_nanos_opt().unwrap() as u64;
        serde_json::from_value(serde_json::json!({
            "resourceLogs": [{
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": "demo-backend" } },
                    { "key": "tenant_id", "value": { "stringValue": "clinic-7" } }
                ]},
                "scopeLogs": [{
                    "scope": { "name": "demo.scope" },
                    "logRecords": [{
                        "timeUnixNano": nanos.to_string(),
                        "severityNumber": 9,
                        "severityText": "INFO",
                        "body": { "stringValue": "user validated planning" },
                        "traceId": "5b8efff798038103d269b633813fc60c",
                        "spanId": "eee19b7ec3c1b174",
                        "attributes": [
                            { "key": "session_id", "value": { "stringValue": "sess-abc" } },
                            { "key": "actor_id", "value": { "stringValue": "user-123" } },
                            { "key": "http.status", "value": { "intValue": "200" } }
                        ]
                    }]
                }]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn flattens_and_correlates() {
        let now = Utc::now();
        let parsed = otlp_to_logs(sample(now), now, &limits());
        assert_eq!(parsed.stored.len(), 1);
        let l = &parsed.stored[0];
        assert_eq!(l.service_name.as_deref(), Some("demo-backend"));
        assert_eq!(l.session_id.as_deref(), Some("sess-abc"));
        assert_eq!(l.actor_id.as_deref(), Some("user-123"));
        assert_eq!(l.tenant_id.as_deref(), Some("clinic-7"));
        assert_eq!(
            l.trace_id.as_deref(),
            Some("5b8efff798038103d269b633813fc60c")
        );
        assert_eq!(l.body.as_deref(), Some("user validated planning"));
        assert_eq!(l.severity_number, Some(9));
        assert_eq!(l.log_attributes["http.status"], serde_json::json!(200));
    }

    #[test]
    fn dedup_id_is_stable_for_identical_record() {
        let now = Utc::now();
        let a = otlp_to_logs(sample(now), now, &limits());
        let b = otlp_to_logs(sample(now), now, &limits());
        assert_eq!(a.stored[0].log_id, b.stored[0].log_id);
    }

    #[test]
    fn drops_out_of_skew_logs() {
        let now = Utc::now();
        let old = now - chrono::Duration::days(60);
        let nanos = old.timestamp_nanos_opt().unwrap() as u64;
        let req: ExportLogsServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceLogs": [{ "scopeLogs": [{ "logRecords": [{
                "timeUnixNano": nanos.to_string(),
                "body": { "stringValue": "ancien" }
            }]}]}]
        }))
        .unwrap();
        let parsed = otlp_to_logs(req, now, &limits());
        assert_eq!(parsed.stored.len(), 0);
        assert_eq!(parsed.dropped_skew, 1);
    }
}
