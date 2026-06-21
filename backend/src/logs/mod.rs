//! Domaine « logs techniques » : ingestion OTLP (OpenTelemetry), transports HTTP et gRPC.

pub mod grpc;
pub mod intake;
pub mod model;

pub use intake::{accept_logs, authorize_logs};
pub use model::{otlp_to_logs, ExportLogsServiceRequest, LogsParse, StoredLog};
