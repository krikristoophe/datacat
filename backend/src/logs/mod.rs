//! Domaine « logs techniques » : ingestion OTLP/HTTP (OpenTelemetry).

pub mod model;

pub use model::{otlp_to_logs, ExportLogsServiceRequest, LogsParse, StoredLog};
