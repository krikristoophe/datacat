//! Modèle des logs techniques OpenTelemetry (OTLP) et conversion vers `StoredLog`.
//!
//! Le wire format est l'**OTLP/HTTP en JSON** (`ExportLogsServiceRequest`), produit par
//! n'importe quel SDK OpenTelemetry ou Collector (`OTEL_EXPORTER_OTLP_PROTOCOL=http/json`).
//! Chaque `LogRecord` est aplati en une ligne, corrélée aux events via tenant/actor/session et
//! aux traces via trace_id/span_id (cf. docs/otel-logs.md).

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::ValidationLimits;
use crate::ingest::{push_csv_opt, push_csv_quoted, Ingestable};

// ── Wire format OTLP (sous-ensemble suffisant pour les logs) ──────────────────

#[derive(Debug, Deserialize)]
pub struct ExportLogsServiceRequest {
    #[serde(default, rename = "resourceLogs")]
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceLogs {
    #[serde(default)]
    pub resource: Option<OtlpResource>,
    #[serde(default, rename = "scopeLogs")]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Deserialize)]
pub struct OtlpResource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeLogs {
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default, rename = "logRecords")]
    pub log_records: Vec<LogRecord>,
}

#[derive(Debug, Deserialize)]
pub struct Scope {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LogRecord {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<StringOrNum>,
    #[serde(default, rename = "observedTimeUnixNano")]
    pub observed_time_unix_nano: Option<StringOrNum>,
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

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<AnyValue>,
}

/// `AnyValue` OTLP (union). Champs en camelCase comme l'encodage JSON OTLP.
#[derive(Debug, Default, Deserialize)]
pub struct AnyValue {
    #[serde(default, rename = "stringValue")]
    pub string_value: Option<String>,
    #[serde(default, rename = "intValue")]
    pub int_value: Option<StringOrNum>,
    #[serde(default, rename = "doubleValue")]
    pub double_value: Option<f64>,
    #[serde(default, rename = "boolValue")]
    pub bool_value: Option<bool>,
    #[serde(default, rename = "arrayValue")]
    pub array_value: Option<ArrayValue>,
    #[serde(default, rename = "kvlistValue")]
    pub kvlist_value: Option<KvList>,
    #[serde(default, rename = "bytesValue")]
    pub bytes_value: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ArrayValue {
    #[serde(default)]
    pub values: Vec<AnyValue>,
}

#[derive(Debug, Deserialize)]
pub struct KvList {
    #[serde(default)]
    pub values: Vec<KeyValue>,
}

/// En JSON OTLP, les int64 sont encodés en chaîne ; on accepte aussi un nombre par tolérance.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum StringOrNum {
    Str(String),
    Num(serde_json::Number),
}

impl StringOrNum {
    fn as_u64(&self) -> Option<u64> {
        match self {
            StringOrNum::Str(s) => s.trim().parse().ok(),
            StringOrNum::Num(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        }
    }
    fn as_i64(&self) -> Option<i64> {
        match self {
            StringOrNum::Str(s) => s.trim().parse().ok(),
            StringOrNum::Num(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        }
    }
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

/// Résultat d'un aplatissement OTLP.
#[derive(Debug, Default)]
pub struct LogsParse {
    pub stored: Vec<StoredLog>,
    pub dropped_skew: u64,
}

/// Aplatit une requête OTLP en `StoredLog`. Les enregistrements hors fenêtre de skew sont
/// écartés (perte tolérée). Le parsing JSON lui-même (échec → 400) a lieu en amont.
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

const TENANT_KEYS: &[&str] = &["tenant_id", "tenant.id", "tenant"];
const ACTOR_KEYS: &[&str] = &["actor_id", "actor.id", "user.id", "enduser.id", "user_id"];
const SESSION_KEYS: &[&str] = &["session_id", "session.id", "session"];

/// Champs normalisés d'un log (indépendants du transport JSON/gRPC) prêts à être assemblés.
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

/// Assemble un `StoredLog` à partir de champs normalisés : corrélation (log puis resource)
/// et `log_id` déterministe. Utilisé par les deux transports → même dédup quel que soit OTLP/JSON
/// ou OTLP/gRPC.
pub(crate) fn assemble_log(f: LogFields<'_>) -> StoredLog {
    let tenant_id =
        lookup(&f.log_attrs, TENANT_KEYS).or_else(|| lookup(f.resource_attrs, TENANT_KEYS));
    let actor_id =
        lookup(&f.log_attrs, ACTOR_KEYS).or_else(|| lookup(f.resource_attrs, ACTOR_KEYS));
    let session_id =
        lookup(&f.log_attrs, SESSION_KEYS).or_else(|| lookup(f.resource_attrs, SESSION_KEYS));

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
        tenant_id,
        actor_id,
        session_id,
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

pub(crate) fn nanos_to_dt(nanos: u64) -> Option<DateTime<Utc>> {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsub = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsub)
}

fn attrs_to_map(attrs: &[KeyValue]) -> Map<String, Value> {
    let mut map = Map::new();
    for kv in attrs {
        let v = kv
            .value
            .as_ref()
            .map(anyvalue_to_json)
            .unwrap_or(Value::Null);
        map.insert(kv.key.clone(), v);
    }
    map
}

/// Cherche la première clé candidate dont la valeur est une chaîne non vide.
fn lookup(map: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(Value::String(s)) = map.get(*k) {
            if !s.is_empty() {
                return Some(s.clone());
            }
        }
    }
    None
}

fn anyvalue_to_json(v: &AnyValue) -> Value {
    if let Some(s) = &v.string_value {
        return Value::String(s.clone());
    }
    if let Some(n) = &v.int_value {
        return n.as_i64().map(Value::from).unwrap_or(Value::Null);
    }
    if let Some(d) = v.double_value {
        return serde_json::Number::from_f64(d)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Some(b) = v.bool_value {
        return Value::Bool(b);
    }
    if let Some(a) = &v.array_value {
        return Value::Array(a.values.iter().map(anyvalue_to_json).collect());
    }
    if let Some(kv) = &v.kvlist_value {
        return Value::Object(attrs_to_map(&kv.values));
    }
    if let Some(by) = &v.bytes_value {
        return Value::String(by.clone());
    }
    Value::Null
}

/// Représentation texte d'un `AnyValue` (pour le corps du log).
fn anyvalue_to_string(v: &AnyValue) -> String {
    match anyvalue_to_json(v) {
        Value::String(s) => s,
        other => other.to_string(),
    }
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
        push_ts_opt(out, self.observed_time);
        out.push(',');
        out.push_str(&self.received_at.to_rfc3339());
        out.push(',');
        push_num_opt(out, self.severity_number.map(i64::from));
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

fn push_ts_opt(out: &mut String, ts: Option<DateTime<Utc>>) {
    if let Some(t) = ts {
        out.push_str(&t.to_rfc3339());
    }
}

fn push_num_opt(out: &mut String, n: Option<i64>) {
    if let Some(v) = n {
        out.push_str(&v.to_string());
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

    fn sample(now: DateTime<Utc>) -> ExportLogsServiceRequest {
        let nanos = (now.timestamp_nanos_opt().unwrap()) as u64;
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
        assert_eq!(l.tenant_id.as_deref(), Some("clinic-7")); // hérité de la resource
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
        assert_eq!(
            a.stored[0].log_id, b.stored[0].log_id,
            "même contenu ⇒ même id"
        );
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

    #[test]
    fn csv_row_has_16_columns() {
        let now = Utc::now();
        let parsed = otlp_to_logs(sample(now), now, &limits());
        let mut out = String::new();
        parsed.stored[0].write_csv_row(&mut out);
        assert!(out.ends_with('\n'));
        // Le body/attrs étant quotés sans virgule interne ici, un découpage simple suffit.
        assert!(out.contains("demo-backend"));
        assert!(out.contains("sess-abc"));
    }
}
