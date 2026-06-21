//! Conversions communes pour OTLP/gRPC (protobuf), partagées par logs / traces / métriques.

use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use serde_json::{Map, Value};

pub fn anyvalue_to_json(v: &AnyValue) -> Value {
    use any_value::Value as P;
    match &v.value {
        Some(P::StringValue(s)) => Value::String(s.clone()),
        Some(P::BoolValue(b)) => Value::Bool(*b),
        Some(P::IntValue(i)) => Value::from(*i),
        Some(P::DoubleValue(d)) => serde_json::Number::from_f64(*d)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(P::ArrayValue(a)) => Value::Array(a.values.iter().map(anyvalue_to_json).collect()),
        Some(P::KvlistValue(kv)) => Value::Object(attrs_to_map(&kv.values)),
        Some(P::BytesValue(b)) => Value::String(hex(b)),
        // Variantes OTLP plus récentes (ex. string via table) ou absentes : non corrélées.
        Some(_) => Value::Null,
        None => Value::Null,
    }
}

pub fn anyvalue_to_string(v: &AnyValue) -> String {
    match anyvalue_to_json(v) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

pub fn attrs_to_map(attrs: &[KeyValue]) -> Map<String, Value> {
    let mut m = Map::new();
    for kv in attrs {
        m.insert(
            kv.key.clone(),
            kv.value
                .as_ref()
                .map(anyvalue_to_json)
                .unwrap_or(Value::Null),
        );
    }
    m
}

/// Encode des octets en hexadécimal ; `None` si vide (trace_id / span_id non renseignés).
pub fn hex_opt(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| hex(bytes))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
