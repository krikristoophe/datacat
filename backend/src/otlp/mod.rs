//! Éléments communs OpenTelemetry/OTLP partagés par les domaines logs, traces et métriques :
//! types et conversions JSON (`json`) et protobuf (`proto`), extraction des clés de corrélation,
//! conversion des horodatages nano.

pub mod json;
pub mod proto;

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

/// Clés d'attribut candidates pour la corrélation (events ↔ logs ↔ traces).
pub const TENANT_KEYS: &[&str] = &["tenant_id", "tenant.id", "tenant"];
pub const ACTOR_KEYS: &[&str] = &["actor_id", "actor.id", "user.id", "enduser.id", "user_id"];
pub const SESSION_KEYS: &[&str] = &["session_id", "session.id", "session"];

/// Première clé candidate dont la valeur est une chaîne non vide.
pub fn lookup(map: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Some(Value::String(s)) = map.get(*k) {
            if !s.is_empty() {
                return Some(s.clone());
            }
        }
    }
    None
}

/// Identité de corrélation, cherchée d'abord dans les attributs de l'enregistrement,
/// puis dans ceux de la resource.
#[derive(Debug, Default, Clone)]
pub struct Correlation {
    pub tenant_id: Option<String>,
    pub actor_id: Option<String>,
    pub session_id: Option<String>,
}

pub fn correlate(
    record_attrs: &Map<String, Value>,
    resource_attrs: &Map<String, Value>,
) -> Correlation {
    Correlation {
        tenant_id: lookup(record_attrs, TENANT_KEYS)
            .or_else(|| lookup(resource_attrs, TENANT_KEYS)),
        actor_id: lookup(record_attrs, ACTOR_KEYS).or_else(|| lookup(resource_attrs, ACTOR_KEYS)),
        session_id: lookup(record_attrs, SESSION_KEYS)
            .or_else(|| lookup(resource_attrs, SESSION_KEYS)),
    }
}

/// Convertit un horodatage OTLP (nanosecondes depuis l'epoch) en `DateTime<Utc>`.
pub fn nanos_to_dt(nanos: u64) -> Option<DateTime<Utc>> {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsub = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsub)
}
