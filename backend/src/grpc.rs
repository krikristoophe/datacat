//! Serveur OTLP/gRPC : assemble tous les services de télémétrie (logs, traces) sur un même
//! port (4317 par défaut) et fournit les helpers communs (auth, IP, mapping d'erreurs).

use std::net::{IpAddr, Ipv4Addr};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Status};

use crate::error::AppError;
use crate::logs::grpc::DatacatLogsService;
use crate::metrics::grpc::DatacatMetricsService;
use crate::traces::grpc::DatacatTracesService;
use crate::AppState;

/// Sert tous les services OTLP/gRPC sur `listener` jusqu'au `shutdown`.
pub async fn serve<F>(state: AppState, listener: TcpListener, shutdown: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let incoming = TcpListenerStream::new(listener);
    tonic::transport::Server::builder()
        .add_service(LogsServiceServer::new(DatacatLogsService::new(
            state.clone(),
        )))
        .add_service(TraceServiceServer::new(DatacatTracesService::new(
            state.clone(),
        )))
        .add_service(MetricsServiceServer::new(DatacatMetricsService::new(state)))
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;
    Ok(())
}

/// IP du pair gRPC (UNSPECIFIED si indisponible).
pub(crate) fn request_ip<T>(request: &Request<T>) -> IpAddr {
    request
        .remote_addr()
        .map(|a| a.ip())
        .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED))
}

/// Extrait le token de la métadonnée `authorization: Bearer …`.
pub(crate) fn bearer<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|value| {
            value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
}

/// Mappe une `AppError` vers un `Status` gRPC.
pub(crate) fn app_err_to_status(e: AppError) -> Status {
    match e {
        AppError::Unauthorized(m) => Status::unauthenticated(m),
        AppError::Forbidden(m) => Status::permission_denied(m),
        AppError::RateLimited { scope, .. } => {
            Status::resource_exhausted(format!("rate limit: {scope}"))
        }
        AppError::PayloadTooLarge(m) => Status::out_of_range(m),
        AppError::BadRequest { message, .. } => Status::invalid_argument(message),
        AppError::Unavailable(m) => Status::unavailable(m),
        AppError::Internal(_) => Status::internal("erreur interne"),
    }
}
