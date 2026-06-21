//! Schémas Arrow des tables Parquet exportées par `datacat-exporter`.
//!
//! Ces schémas sont **identiques** à ceux définis dans `exporter/src/schema.rs`.
//! Ils sont redéfinis ici pour que le crate `reader` soit standalone (pas de
//! dépendance circulaire sur l'exporter).

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Schéma Arrow de la table `events`.
///
/// | Colonne          | Type Arrow                         | Nullable |
/// |------------------|------------------------------------|----------|
/// | event_id         | Utf8                               | non      |
/// | event_name       | Utf8                               | non      |
/// | tenant_id        | Utf8                               | oui      |
/// | actor_id         | Utf8                               | non      |
/// | session_id       | Utf8                               | non      |
/// | timestamp_client | Timestamp(Microsecond, UTC)         | non      |
/// | received_at      | Timestamp(Microsecond, UTC)         | non      |
/// | properties       | Utf8 (JSON sérialisé)              | non      |
pub fn events_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("event_name", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, true),
        Field::new("actor_id", DataType::Utf8, false),
        Field::new("session_id", DataType::Utf8, false),
        Field::new(
            "timestamp_client",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("properties", DataType::Utf8, false),
    ]))
}

/// Schéma Arrow de la table `logs`.
pub fn logs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("log_id", DataType::Utf8, false),
        Field::new(
            "log_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "observed_time",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("severity_number", DataType::Int16, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("tenant_id", DataType::Utf8, true),
        Field::new("actor_id", DataType::Utf8, true),
        Field::new("session_id", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("log_attributes", DataType::Utf8, false),
    ]))
}

/// Retourne le schéma pour un nom de table connu.
pub fn schema_for_table(table: &str) -> anyhow::Result<Arc<Schema>> {
    match table {
        "events" => Ok(events_schema()),
        "logs" => Ok(logs_schema()),
        other => anyhow::bail!("table inconnue : '{other}' (tables valides : events, logs)"),
    }
}
