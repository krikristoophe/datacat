//! Domaine « métriques » : ingestion OTLP (OpenTelemetry), transports HTTP et gRPC.

pub mod grpc;
pub mod intake;
pub mod model;

pub use intake::{accept_metric_points, authorize_metrics};
pub use model::{otlp_to_metrics, ExportMetricsServiceRequest, MetricsParse, StoredMetricPoint};
