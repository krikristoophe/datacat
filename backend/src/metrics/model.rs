//! Modèle des métriques OpenTelemetry (OTLP) et conversion vers `StoredMetricPoint`.
//!
//! Wire format : **OTLP/HTTP en JSON** (`ExportMetricsServiceRequest`). Chaque point de donnée
//! d'une métrique est aplati en une ligne : **gauge** et **sum** via `NumberDataPoint`
//! (`asDouble` / `asInt`), **histogram** via `HistogramDataPoint` (count, sum, bucketCounts,
//! explicitBounds → `buckets` jsonb). Les types `summary` et `exponentialHistogram` ne sont
//! **pas** ingérés (documenté). Corrélation aux events/logs/traces via tenant/actor/session.
//! Idempotence par `point_id` = hash déterministe du contenu (OTLP n'a pas d'id natif).

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::config::ValidationLimits;
use crate::ingest::{push_csv_f64, push_csv_num, push_csv_opt, push_csv_quoted, Ingestable};
use crate::otlp::json::{attrs_to_map, KeyValue, Resource, Scope, StringOrNum};
use crate::otlp::{correlate, lookup, nanos_to_dt};

// ── Wire format des métriques ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ExportMetricsServiceRequest {
    #[serde(default, rename = "resourceMetrics")]
    pub resource_metrics: Vec<ResourceMetrics>,
}

#[derive(Debug, Deserialize)]
pub struct ResourceMetrics {
    #[serde(default)]
    pub resource: Option<Resource>,
    #[serde(default, rename = "scopeMetrics")]
    pub scope_metrics: Vec<ScopeMetrics>,
}

#[derive(Debug, Deserialize)]
pub struct ScopeMetrics {
    #[serde(default)]
    pub scope: Option<Scope>,
    #[serde(default)]
    pub metrics: Vec<Metric>,
}

#[derive(Debug, Deserialize)]
pub struct Metric {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
    #[serde(default)]
    pub gauge: Option<NumberData>,
    #[serde(default)]
    pub sum: Option<NumberData>,
    #[serde(default)]
    pub histogram: Option<HistogramData>,
}

#[derive(Debug, Deserialize)]
pub struct NumberData {
    #[serde(default, rename = "dataPoints")]
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Debug, Deserialize)]
pub struct NumberDataPoint {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<StringOrNum>,
    #[serde(default, rename = "asDouble")]
    pub as_double: Option<f64>,
    #[serde(default, rename = "asInt")]
    pub as_int: Option<StringOrNum>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct HistogramData {
    #[serde(default, rename = "dataPoints")]
    pub data_points: Vec<HistogramDataPoint>,
}

#[derive(Debug, Deserialize)]
pub struct HistogramDataPoint {
    #[serde(default, rename = "timeUnixNano")]
    pub time_unix_nano: Option<StringOrNum>,
    #[serde(default)]
    pub count: Option<StringOrNum>,
    #[serde(default)]
    pub sum: Option<f64>,
    #[serde(default, rename = "bucketCounts")]
    pub bucket_counts: Vec<StringOrNum>,
    #[serde(default, rename = "explicitBounds")]
    pub explicit_bounds: Vec<f64>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

// ── Enregistrement persistable ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StoredMetricPoint {
    pub point_id: Uuid,
    pub time: DateTime<Utc>,
    pub metric_name: String,
    pub metric_type: &'static str,
    pub unit: Option<String>,
    pub value_double: Option<f64>,
    pub value_int: Option<i64>,
    pub count: Option<i64>,
    pub sum: Option<f64>,
    /// Histogram : `{ "bounds": [...], "counts": [...] }` ; `None` pour gauge/sum.
    pub buckets: Option<Value>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub tenant_id: Option<String>,
    pub actor_id: Option<String>,
    pub session_id: Option<String>,
    pub received_at: DateTime<Utc>,
    pub resource_attributes: Value,
    pub attributes: Value,
}

impl StoredMetricPoint {
    /// Taille approximative (octets) du contenu variable du point. Garde-fou de taille par
    /// enregistrement OTLP (S-7) : `buckets`/`attributes` peuvent être volumineux.
    pub fn approx_content_bytes(&self) -> usize {
        use crate::otlp::{json_byte_len, opt_len};
        self.metric_name.len()
            + opt_len(&self.unit)
            + opt_len(&self.service_name)
            + opt_len(&self.scope_name)
            + self.buckets.as_ref().map_or(0, json_byte_len)
            + json_byte_len(&self.resource_attributes)
            + json_byte_len(&self.attributes)
    }
}

/// Résultat d'un aplatissement OTLP de métriques.
#[derive(Debug, Default)]
pub struct MetricsParse {
    pub stored: Vec<StoredMetricPoint>,
    pub dropped_skew: u64,
}

/// Aplatit une requête OTLP de métriques en `StoredMetricPoint`. `summary` et
/// `exponentialHistogram` sont ignorés. Points hors fenêtre de skew écartés (perte tolérée).
pub fn otlp_to_metrics(
    req: ExportMetricsServiceRequest,
    received_at: DateTime<Utc>,
    limits: &ValidationLimits,
) -> MetricsParse {
    let past = received_at - chrono::Duration::from_std(limits.max_past_skew).unwrap();
    let future = received_at + chrono::Duration::from_std(limits.max_future_skew).unwrap();

    let mut out = MetricsParse::default();
    for rm in req.resource_metrics {
        let resource_attrs = rm
            .resource
            .map(|r| attrs_to_map(&r.attributes))
            .unwrap_or_default();
        let service_name = lookup(&resource_attrs, &["service.name"]);

        for sm in rm.scope_metrics {
            let scope_name = sm.scope.and_then(|s| s.name);
            for m in sm.metrics {
                let name = m.name.unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                let unit = m.unit.filter(|u| !u.is_empty());

                // gauge et sum : NumberDataPoint (asDouble / asInt).
                let numbers = m
                    .gauge
                    .map(|g| ("gauge", g.data_points))
                    .into_iter()
                    .chain(m.sum.map(|s| ("sum", s.data_points)));
                for (metric_type, points) in numbers {
                    for p in points {
                        let time = nanos(p.time_unix_nano.as_ref()).unwrap_or(received_at);
                        if time < past || time > future {
                            out.dropped_skew += 1;
                            continue;
                        }
                        let value_int =
                            p.as_int.as_ref().and_then(|v| v.as_u64()).map(|v| v as i64);
                        let attrs = attrs_to_map(&p.attributes);
                        out.stored.push(assemble(MetricFields {
                            received_at,
                            time,
                            metric_name: name.clone(),
                            metric_type,
                            unit: unit.clone(),
                            value_double: p.as_double,
                            value_int,
                            count: None,
                            sum: None,
                            buckets: None,
                            service_name: service_name.clone(),
                            scope_name: scope_name.clone(),
                            attrs,
                            resource_attrs: &resource_attrs,
                        }));
                    }
                }

                // histogram : HistogramDataPoint (count, sum, bucketCounts, explicitBounds).
                if let Some(h) = m.histogram {
                    for p in h.data_points {
                        let time = nanos(p.time_unix_nano.as_ref()).unwrap_or(received_at);
                        if time < past || time > future {
                            out.dropped_skew += 1;
                            continue;
                        }
                        let counts: Vec<u64> =
                            p.bucket_counts.iter().filter_map(|c| c.as_u64()).collect();
                        let attrs = attrs_to_map(&p.attributes);
                        out.stored.push(assemble(MetricFields {
                            received_at,
                            time,
                            metric_name: name.clone(),
                            metric_type: "histogram",
                            unit: unit.clone(),
                            value_double: None,
                            value_int: None,
                            count: p.count.as_ref().and_then(|c| c.as_u64()).map(|c| c as i64),
                            sum: p.sum,
                            buckets: Some(buckets_json(&p.explicit_bounds, &counts)),
                            service_name: service_name.clone(),
                            scope_name: scope_name.clone(),
                            attrs,
                            resource_attrs: &resource_attrs,
                        }));
                    }
                }
            }
        }
    }
    out
}

/// Représentation jsonb d'un histogram : bornes explicites + comptes par bucket.
pub(crate) fn buckets_json(bounds: &[f64], counts: &[u64]) -> Value {
    json!({
        "bounds": bounds,
        "counts": counts,
    })
}

/// Champs normalisés d'un point de métrique (indépendants du transport JSON/gRPC).
pub(crate) struct MetricFields<'a> {
    pub received_at: DateTime<Utc>,
    pub time: DateTime<Utc>,
    pub metric_name: String,
    pub metric_type: &'static str,
    pub unit: Option<String>,
    pub value_double: Option<f64>,
    pub value_int: Option<i64>,
    pub count: Option<i64>,
    pub sum: Option<f64>,
    pub buckets: Option<Value>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub attrs: Map<String, Value>,
    pub resource_attrs: &'a Map<String, Value>,
}

/// Assemble un `StoredMetricPoint` (corrélation + `point_id` déterministe). Partagé par les
/// transports JSON et gRPC → même dédup quel que soit OTLP/JSON ou OTLP/gRPC.
pub(crate) fn assemble(f: MetricFields<'_>) -> StoredMetricPoint {
    let c = correlate(&f.attrs, f.resource_attrs);
    let attributes = Value::Object(f.attrs);
    let point_id = dedup_id(
        f.time,
        &f.metric_name,
        f.metric_type,
        f.service_name.as_deref(),
        f.value_double,
        f.value_int,
        f.count,
        f.sum,
        f.buckets.as_ref(),
        &attributes,
    );

    StoredMetricPoint {
        point_id,
        time: f.time,
        metric_name: f.metric_name,
        metric_type: f.metric_type,
        unit: f.unit,
        value_double: f.value_double,
        value_int: f.value_int,
        count: f.count,
        sum: f.sum,
        buckets: f.buckets,
        service_name: f.service_name,
        scope_name: f.scope_name,
        tenant_id: c.tenant_id,
        actor_id: c.actor_id,
        session_id: c.session_id,
        received_at: f.received_at,
        resource_attributes: Value::Object(f.resource_attrs.clone()),
        attributes,
    }
}

/// `point_id` déterministe = hash du contenu normalisé (dédup des renvois OTLP identiques).
#[allow(clippy::too_many_arguments)]
fn dedup_id(
    time: DateTime<Utc>,
    metric_name: &str,
    metric_type: &str,
    service_name: Option<&str>,
    value_double: Option<f64>,
    value_int: Option<i64>,
    count: Option<i64>,
    sum: Option<f64>,
    buckets: Option<&Value>,
    attributes: &Value,
) -> Uuid {
    let mut h = Sha256::new();
    h.update(time.timestamp_nanos_opt().unwrap_or(0).to_le_bytes());
    h.update(metric_name.as_bytes());
    h.update([0]);
    h.update(metric_type.as_bytes());
    h.update([0]);
    h.update(service_name.unwrap_or("").as_bytes());
    h.update([0]);
    h.update(value_double.unwrap_or(0.0).to_le_bytes());
    h.update(value_int.unwrap_or(0).to_le_bytes());
    h.update(count.unwrap_or(0).to_le_bytes());
    h.update(sum.unwrap_or(0.0).to_le_bytes());
    h.update(
        buckets
            .map(|b| b.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    h.update([0]);
    h.update(attributes.to_string().as_bytes());
    let digest = h.finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    Uuid::from_bytes(bytes)
}

fn nanos(t: Option<&StringOrNum>) -> Option<DateTime<Utc>> {
    t.and_then(|t| t.as_u64())
        .filter(|&n| n > 0)
        .and_then(nanos_to_dt)
}

// ── Persistance (COPY) ────────────────────────────────────────────────────────

impl Ingestable for StoredMetricPoint {
    fn copy_statement() -> &'static str {
        "COPY metric_points_staging \
         (point_id, time, metric_name, metric_type, unit, value_double, value_int, count, sum, \
          buckets, service_name, scope_name, tenant_id, actor_id, session_id, \
          received_at, resource_attributes, attributes) \
         FROM STDIN WITH (FORMAT csv)"
    }
    fn ensure_partitions_statement() -> &'static str {
        "SELECT datacat_ensure_metric_partitions_for_staging()"
    }
    fn merge_statement() -> &'static str {
        "SELECT datacat_merge_metric_staging()"
    }
    fn staging_table() -> &'static str {
        "metric_points_staging"
    }
    fn label() -> &'static str {
        "metrics"
    }
    fn write_csv_row(&self, out: &mut String) {
        out.push_str(&self.point_id.to_string());
        out.push(',');
        out.push_str(&self.time.to_rfc3339());
        out.push(',');
        push_csv_quoted(out, &self.metric_name);
        out.push(',');
        push_csv_quoted(out, self.metric_type);
        out.push(',');
        push_csv_opt(out, self.unit.as_deref());
        out.push(',');
        push_csv_f64(out, self.value_double);
        out.push(',');
        push_csv_num(out, self.value_int);
        out.push(',');
        push_csv_num(out, self.count);
        out.push(',');
        push_csv_f64(out, self.sum);
        out.push(',');
        // `buckets` est un jsonb optionnel : None ⇒ champ vide ⇒ NULL.
        if let Some(b) = &self.buckets {
            push_csv_quoted(out, &b.to_string());
        }
        out.push(',');
        push_csv_opt(out, self.service_name.as_deref());
        out.push(',');
        push_csv_opt(out, self.scope_name.as_deref());
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
        push_csv_quoted(out, &self.attributes.to_string());
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

    fn sample(now: DateTime<Utc>) -> ExportMetricsServiceRequest {
        let nanos = now.timestamp_nanos_opt().unwrap() as u64;
        serde_json::from_value(serde_json::json!({
            "resourceMetrics": [{
                "resource": { "attributes": [
                    { "key": "service.name", "value": { "stringValue": "api" } },
                    { "key": "tenant_id", "value": { "stringValue": "clinic-7" } }
                ]},
                "scopeMetrics": [{
                    "scope": { "name": "demo.scope" },
                    "metrics": [
                        {
                            "name": "process.cpu.utilization",
                            "unit": "1",
                            "gauge": { "dataPoints": [{
                                "timeUnixNano": nanos.to_string(),
                                "asDouble": 0.42,
                                "attributes": [
                                    { "key": "session_id", "value": { "stringValue": "sess-1" } }
                                ]
                            }] }
                        },
                        {
                            "name": "http.server.duration",
                            "unit": "ms",
                            "histogram": { "dataPoints": [{
                                "timeUnixNano": nanos.to_string(),
                                "count": "3",
                                "sum": 600.0,
                                "bucketCounts": ["1", "2", "0"],
                                "explicitBounds": [100.0, 500.0]
                            }] }
                        }
                    ]
                }]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn flattens_gauge_and_histogram() {
        let now = Utc::now();
        let parsed = otlp_to_metrics(sample(now), now, &limits());
        assert_eq!(parsed.stored.len(), 2);

        let gauge = parsed
            .stored
            .iter()
            .find(|p| p.metric_name == "process.cpu.utilization")
            .unwrap();
        assert_eq!(gauge.metric_type, "gauge");
        assert_eq!(gauge.value_double, Some(0.42));
        assert_eq!(gauge.service_name.as_deref(), Some("api"));
        assert_eq!(gauge.tenant_id.as_deref(), Some("clinic-7"));
        assert_eq!(gauge.session_id.as_deref(), Some("sess-1"));
        assert_eq!(gauge.unit.as_deref(), Some("1"));

        let hist = parsed
            .stored
            .iter()
            .find(|p| p.metric_name == "http.server.duration")
            .unwrap();
        assert_eq!(hist.metric_type, "histogram");
        assert_eq!(hist.count, Some(3));
        assert_eq!(hist.sum, Some(600.0));
        let b = hist.buckets.as_ref().unwrap();
        assert_eq!(b["bounds"], serde_json::json!([100.0, 500.0]));
        assert_eq!(b["counts"], serde_json::json!([1, 2, 0]));
    }

    #[test]
    fn flattens_sum_int() {
        let now = Utc::now();
        let nanos = now.timestamp_nanos_opt().unwrap() as u64;
        let req: ExportMetricsServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [{
                "name": "http.server.request.count",
                "sum": { "dataPoints": [{
                    "timeUnixNano": nanos.to_string(),
                    "asInt": "17"
                }] }
            }]}]}]
        }))
        .unwrap();
        let parsed = otlp_to_metrics(req, now, &limits());
        assert_eq!(parsed.stored.len(), 1);
        let p = &parsed.stored[0];
        assert_eq!(p.metric_type, "sum");
        assert_eq!(p.value_int, Some(17));
    }

    #[test]
    fn dedup_id_is_stable_for_identical_point() {
        let now = Utc::now();
        let a = otlp_to_metrics(sample(now), now, &limits());
        let b = otlp_to_metrics(sample(now), now, &limits());
        assert_eq!(a.stored[0].point_id, b.stored[0].point_id);
        assert_eq!(a.stored[1].point_id, b.stored[1].point_id);
    }

    #[test]
    fn ignores_summary_and_exponential_histogram() {
        let now = Utc::now();
        let nanos = now.timestamp_nanos_opt().unwrap() as u64;
        let req: ExportMetricsServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [
                { "name": "ignored.summary", "summary": { "dataPoints": [{ "timeUnixNano": nanos.to_string() }] } },
                { "name": "ignored.exphist", "exponentialHistogram": { "dataPoints": [{ "timeUnixNano": nanos.to_string() }] } }
            ]}]}]
        }))
        .unwrap();
        let parsed = otlp_to_metrics(req, now, &limits());
        assert_eq!(parsed.stored.len(), 0);
    }

    #[test]
    fn drops_out_of_skew_points() {
        let now = Utc::now();
        let old = now - chrono::Duration::days(60);
        let nanos = old.timestamp_nanos_opt().unwrap() as u64;
        let req: ExportMetricsServiceRequest = serde_json::from_value(serde_json::json!({
            "resourceMetrics": [{ "scopeMetrics": [{ "metrics": [{
                "name": "old.gauge",
                "gauge": { "dataPoints": [{ "timeUnixNano": nanos.to_string(), "asDouble": 1.0 }] }
            }]}]}]
        }))
        .unwrap();
        let parsed = otlp_to_metrics(req, now, &limits());
        assert_eq!(parsed.stored.len(), 0);
        assert_eq!(parsed.dropped_skew, 1);
    }
}
