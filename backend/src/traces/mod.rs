//! Domaine « traces » : ingestion OTLP (OpenTelemetry), transports HTTP et gRPC.

pub mod grpc;
pub mod intake;
pub mod model;

pub use intake::{accept_spans, authorize_traces};
pub use model::{otlp_to_spans, ExportTraceServiceRequest, SpansParse, StoredSpan};
