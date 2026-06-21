//! Types communs OTLP/HTTP (JSON) et leurs conversions, partagés par les domaines logs,
//! traces et métriques. Champs en camelCase comme l'encodage JSON OTLP.

use serde::Deserialize;
use serde_json::{Map, Value};

/// Resource OTLP (porteuse de `service.name`, etc.).
#[derive(Debug, Default, Deserialize)]
pub struct Resource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize)]
pub struct Scope {
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KeyValue {
    pub key: String,
    #[serde(default)]
    pub value: Option<AnyValue>,
}

/// `AnyValue` OTLP (union).
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
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            StringOrNum::Str(s) => s.trim().parse().ok(),
            StringOrNum::Num(n) => n.as_u64().or_else(|| n.as_f64().map(|f| f as u64)),
        }
    }
}

pub fn anyvalue_to_json(v: &AnyValue) -> Value {
    if let Some(s) = &v.string_value {
        return Value::String(s.clone());
    }
    if let Some(n) = &v.int_value {
        return n.as_u64().map(Value::from).unwrap_or(Value::Null);
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

/// Représentation texte d'un `AnyValue` (corps de log, message, etc.).
pub fn anyvalue_to_string(v: &AnyValue) -> String {
    match anyvalue_to_json(v) {
        Value::String(s) => s,
        other => other.to_string(),
    }
}

pub fn attrs_to_map(attrs: &[KeyValue]) -> Map<String, Value> {
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
