//! Modèle d'event et validation stricte des entrées (cf. docs/CONTRACT.md §2).

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::config::ValidationLimits;
use crate::ingest::{push_csv_opt, push_csv_quoted, Ingestable};

/// Corps de la requête d'ingestion. `token` est le repli beacon (cf. CONTRACT §1.1).
#[derive(Debug, Deserialize)]
pub struct IngestBody {
    #[serde(default)]
    pub token: Option<String>,
    pub events: Vec<IncomingEvent>,
}

/// Event tel que reçu sur le réseau (avant validation/normalisation).
#[derive(Debug, Deserialize)]
pub struct IncomingEvent {
    pub event_id: Uuid,
    pub event_name: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub actor_id: String,
    pub session_id: String,
    pub timestamp_client: DateTime<Utc>,
    #[serde(default)]
    pub properties: Option<Value>,
}

/// Event validé, prêt à être persisté.
#[derive(Debug, Clone)]
pub struct StoredEvent {
    pub event_id: Uuid,
    pub event_name: String,
    pub tenant_id: Option<String>,
    pub actor_id: String,
    pub session_id: String,
    pub timestamp_client: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub properties: Value,
}

impl Ingestable for StoredEvent {
    fn copy_statement() -> &'static str {
        "COPY events_staging \
         (event_id, event_name, tenant_id, actor_id, session_id, \
          timestamp_client, received_at, properties) \
         FROM STDIN WITH (FORMAT csv)"
    }
    fn ensure_partitions_statement() -> &'static str {
        "SELECT datacat_ensure_partitions_for_staging()"
    }
    fn merge_statement() -> &'static str {
        "SELECT datacat_merge_staging()"
    }
    fn staging_table() -> &'static str {
        "events_staging"
    }
    fn label() -> &'static str {
        "events"
    }
    fn write_csv_row(&self, out: &mut String) {
        out.push_str(&self.event_id.to_string());
        out.push(',');
        push_csv_quoted(out, &self.event_name);
        out.push(',');
        push_csv_opt(out, self.tenant_id.as_deref());
        out.push(',');
        push_csv_quoted(out, &self.actor_id);
        out.push(',');
        push_csv_quoted(out, &self.session_id);
        out.push(',');
        out.push_str(&self.timestamp_client.to_rfc3339());
        out.push(',');
        out.push_str(&self.received_at.to_rfc3339());
        out.push(',');
        let props = serde_json::to_string(&self.properties).unwrap_or_else(|_| "{}".to_string());
        push_csv_quoted(out, &props);
        out.push('\n');
    }
}

/// Résultat de validation d'un event.
pub enum EventCheck {
    /// Event valide et dans la fenêtre temporelle.
    Ok(StoredEvent),
    /// Event valide structurellement mais hors fenêtre de skew → écarté (perte tolérée).
    OutOfSkew,
}

/// Erreur de validation structurelle (→ rejet 400 de toute la requête).
#[derive(Debug)]
pub struct StructuralError(pub String);

/// Valide structurellement un event et, si OK, vérifie la fenêtre temporelle.
///
/// - Erreur structurelle (longueurs, properties) → `Err` → la requête entière est rejetée.
/// - Hors fenêtre de skew → `Ok(EventCheck::OutOfSkew)` → l'event est simplement écarté.
pub fn check_event(
    ev: IncomingEvent,
    received_at: DateTime<Utc>,
    limits: &ValidationLimits,
    index: usize,
) -> Result<EventCheck, StructuralError> {
    let at = |field: &str, msg: &str| StructuralError(format!("events[{index}].{field}: {msg}"));

    check_text(&ev.event_name, "event_name", limits.max_string_len, index)?;
    check_text(&ev.actor_id, "actor_id", limits.max_string_len, index)?;
    check_text(&ev.session_id, "session_id", limits.max_string_len, index)?;
    if let Some(t) = &ev.tenant_id {
        check_text(t, "tenant_id", limits.max_string_len, index)?;
    }

    // properties : objet JSON, taille et profondeur bornées.
    let properties = match ev.properties {
        None | Some(Value::Null) => Value::Object(Default::default()),
        Some(v @ Value::Object(_)) => v,
        Some(_) => return Err(at("properties", "doit être un objet JSON")),
    };
    let serialized_len = serde_json::to_vec(&properties)
        .map_err(|e| at("properties", &format!("non sérialisable: {e}")))?
        .len();
    if serialized_len > limits.max_properties_bytes {
        return Err(at(
            "properties",
            &format!(
                "trop volumineux ({serialized_len} > {} octets)",
                limits.max_properties_bytes
            ),
        ));
    }
    if json_depth(&properties) > limits.max_json_depth {
        return Err(at(
            "properties",
            &format!("profondeur > {}", limits.max_json_depth),
        ));
    }

    // Fenêtre de skew : sémantique, ne fait pas échouer le batch.
    let past_limit = received_at - chrono::Duration::from_std(limits.max_past_skew).unwrap();
    let future_limit = received_at + chrono::Duration::from_std(limits.max_future_skew).unwrap();
    if ev.timestamp_client < past_limit || ev.timestamp_client > future_limit {
        return Ok(EventCheck::OutOfSkew);
    }

    Ok(EventCheck::Ok(StoredEvent {
        event_id: ev.event_id,
        event_name: ev.event_name,
        tenant_id: ev.tenant_id,
        actor_id: ev.actor_id,
        session_id: ev.session_id,
        timestamp_client: ev.timestamp_client,
        received_at,
        properties,
    }))
}

fn check_text(
    value: &str,
    field: &str,
    max_len: usize,
    index: usize,
) -> Result<(), StructuralError> {
    if value.trim().is_empty() {
        return Err(StructuralError(format!("events[{index}].{field}: vide")));
    }
    // Borne en nombre de caractères (pas d'octets) pour rester cohérent multi-langue.
    if value.chars().count() > max_len {
        return Err(StructuralError(format!(
            "events[{index}].{field}: trop long (> {max_len} caractères)"
        )));
    }
    Ok(())
}

/// Profondeur d'imbrication d'une valeur JSON (objets/tableaux).
fn json_depth(value: &Value) -> usize {
    match value {
        Value::Object(map) => 1 + map.values().map(json_depth).max().unwrap_or(0),
        Value::Array(arr) => 1 + arr.iter().map(json_depth).max().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;
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

    fn event(name: &str, ts: DateTime<Utc>, props: Option<Value>) -> IncomingEvent {
        IncomingEvent {
            event_id: Uuid::new_v4(),
            event_name: name.to_string(),
            tenant_id: None,
            actor_id: "actor-1".to_string(),
            session_id: "sess-1".to_string(),
            timestamp_client: ts,
            properties: props,
        }
    }

    #[test]
    fn accepts_valid_event() {
        let now = Utc::now();
        let r = check_event(event("click", now, None), now, &limits(), 0).unwrap();
        assert!(matches!(r, EventCheck::Ok(_)));
    }

    #[test]
    fn rejects_empty_name() {
        let now = Utc::now();
        let r = check_event(event("  ", now, None), now, &limits(), 0);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_oversized_name() {
        let now = Utc::now();
        let big = "x".repeat(201);
        let r = check_event(event(&big, now, None), now, &limits(), 0);
        assert!(r.is_err());
    }

    #[test]
    fn rejects_non_object_properties() {
        let now = Utc::now();
        let r = check_event(
            event("click", now, Some(Value::from(42))),
            now,
            &limits(),
            0,
        );
        assert!(r.is_err());
    }

    #[test]
    fn drops_out_of_skew() {
        let now = Utc::now();
        let old = now - ChronoDuration::days(40);
        let r = check_event(event("click", old, None), now, &limits(), 0).unwrap();
        assert!(matches!(r, EventCheck::OutOfSkew));

        let future = now + ChronoDuration::hours(48);
        let r = check_event(event("click", future, None), now, &limits(), 0).unwrap();
        assert!(matches!(r, EventCheck::OutOfSkew));
    }

    #[test]
    fn rejects_too_deep_properties() {
        let now = Utc::now();
        let mut v = Value::from(1);
        for _ in 0..20 {
            v = serde_json::json!({ "n": v });
        }
        let r = check_event(event("click", now, Some(v)), now, &limits(), 0);
        assert!(r.is_err());
    }

    #[test]
    fn json_depth_basic() {
        assert_eq!(json_depth(&serde_json::json!(1)), 0);
        assert_eq!(json_depth(&serde_json::json!({"a": 1})), 1);
        assert_eq!(json_depth(&serde_json::json!({"a": {"b": 1}})), 2);
        assert_eq!(json_depth(&serde_json::json!([[1]])), 2);
    }
}

#[cfg(test)]
mod csv_tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn ev(name: &str, props: Value, tenant: Option<&str>) -> StoredEvent {
        let ts = Utc.with_ymd_and_hms(2026, 6, 21, 10, 0, 0).unwrap();
        StoredEvent {
            event_id: Uuid::nil(),
            event_name: name.to_string(),
            tenant_id: tenant.map(|s| s.to_string()),
            actor_id: "actor-1".to_string(),
            session_id: "sess-1".to_string(),
            timestamp_client: ts,
            received_at: ts,
            properties: props,
        }
    }

    #[test]
    fn csv_escapes_quotes_and_commas() {
        let mut out = String::new();
        ev("na\"me,with", json!({"a": "x,y\"z"}), Some("t1")).write_csv_row(&mut out);
        assert!(out.contains("\"na\"\"me,with\""), "got: {out}");
        assert!(out.ends_with('\n'));

        // Round-trip CSV (FORMAT csv) → JSON d'origine.
        let json_str = serde_json::to_string(&json!({"a": "x,y\"z"})).unwrap();
        let mut quoted = String::new();
        push_csv_quoted(&mut quoted, &json_str);
        let inner = &quoted[1..quoted.len() - 1];
        let unquoted = inner.replace("\"\"", "\"");
        assert_eq!(unquoted, json_str);
    }

    #[test]
    fn csv_null_tenant_is_empty_field() {
        let mut out = String::new();
        ev("click", json!({}), None).write_csv_row(&mut out);
        let fields: Vec<&str> = out.trim_end().splitn(4, ',').collect();
        assert_eq!(fields[2], ""); // tenant_id NULL
    }
}
