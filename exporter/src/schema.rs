use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use std::sync::Arc;

/// Arrow schema for the `events` table.
/// - uuid columns → Utf8 (string)
/// - timestamptz  → Timestamp(Microsecond, UTC)
/// - jsonb        → Utf8 (JSON string)
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

/// Arrow schema for the `logs` table.
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
