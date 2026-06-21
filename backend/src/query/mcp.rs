//! Serveur **MCP HTTP** (streamable) intégré : expose la couche de lecture à un agent (Claude).
//! Les outils tapent `crate::query::engine` **en in-process** (aucun saut HTTP). Le service est
//! monté sur `/mcp` (cf. `api/mod.rs`) et protégé par `query_auth`.

use std::sync::Arc;

use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ServerCapabilities, ServerInfo};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler};
use serde_json::{json, Value};

use crate::error::AppError;
use crate::query::engine::{
    self, EventsParams, JourneysParams, LogsParams, MetricsParams, SqlParams,
};
use crate::AppState;

/// Serveur MCP adossé à l'`AppState` (accès lecture).
#[derive(Clone)]
pub struct DatacatMcp {
    state: AppState,
    // Lu par le code généré par `#[tool_handler]` (faux positif dead_code sur certaines versions).
    #[allow(dead_code)]
    tool_router: ToolRouter<Self>,
}

impl DatacatMcp {
    pub fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }

    fn ok(value: Value) -> Result<CallToolResult, ErrorData> {
        let text = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
        Ok(CallToolResult::success(vec![Content::text(text)]))
    }
}

fn to_err(e: AppError) -> ErrorData {
    match e {
        AppError::BadRequest { message, .. } | AppError::PayloadTooLarge(message) => {
            ErrorData::invalid_params(message, None)
        }
        AppError::Forbidden(m) | AppError::Unauthorized(m) | AppError::Unavailable(m) => {
            ErrorData::invalid_request(m, None)
        }
        AppError::RateLimited { scope, .. } => {
            ErrorData::invalid_request(format!("rate limit: {scope}"), None)
        }
        AppError::Internal(_) => ErrorData::internal_error("erreur interne", None),
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TraceParams {
    /// trace_id hexadécimal.
    pub trace_id: String,
}

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
pub struct NoParams {}

#[tool_router]
impl DatacatMcp {
    #[tool(
        description = "Recherche des logs techniques. Filtres : service, session, trace_id, severity_min (1..24), q (sous-chaîne du corps), from/to (RFC3339), limit."
    )]
    async fn search_logs(
        &self,
        Parameters(p): Parameters<LogsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Self::ok(
            engine::search_logs(&self.state.pool, &p)
                .await
                .map_err(to_err)?,
        )
    }

    #[tool(description = "Récupère tous les spans d'une trace (par trace_id), ordonnés par début.")]
    async fn get_trace(
        &self,
        Parameters(p): Parameters<TraceParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Self::ok(
            engine::get_trace(&self.state.pool, &p.trace_id)
                .await
                .map_err(to_err)?,
        )
    }

    #[tool(
        description = "Recherche des events produit. Filtres : actor, session, tenant, name (event_name), from/to (RFC3339), limit."
    )]
    async fn search_events(
        &self,
        Parameters(p): Parameters<EventsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Self::ok(
            engine::search_events(&self.state.pool, &p)
                .await
                .map_err(to_err)?,
        )
    }

    #[tool(
        description = "Séquences de parcours les plus fréquentes par session (suite ordonnée d'events). Utile pour générer des tests E2E fidèles à l'usage réel. Filtres : actor, tenant, limit."
    )]
    async fn frequent_journeys(
        &self,
        Parameters(p): Parameters<JourneysParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Self::ok(
            engine::frequent_journeys(&self.state.pool, &p)
                .await
                .map_err(to_err)?,
        )
    }

    #[tool(
        description = "Recherche des points de métriques. Filtres : name (metric_name), service, from/to (RFC3339), limit."
    )]
    async fn search_metrics(
        &self,
        Parameters(p): Parameters<MetricsParams>,
    ) -> Result<CallToolResult, ErrorData> {
        Self::ok(
            engine::search_metrics(&self.state.pool, &p)
                .await
                .map_err(to_err)?,
        )
    }

    #[tool(
        description = "Exécute une requête SQL EN LECTURE SEULE (SELECT/WITH, instruction unique) sur events/logs/spans/metric_points — pour de l'analyse ad-hoc (agrégats, corrélation). Nécessite QUERY_SQL_ENABLED côté serveur."
    )]
    async fn run_read_sql(
        &self,
        Parameters(p): Parameters<SqlParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let c = &self.state.config;
        Self::ok(
            engine::run_read_sql(
                &self.state.pool,
                c.query_sql_enabled,
                c.query_sql_timeout,
                c.query_sql_max_rows,
                &p,
            )
            .await
            .map_err(to_err)?,
        )
    }

    #[tool(
        description = "Statistiques d'ingestion par domaine (events, logs, traces, metrics) : volumes reçus/insérés, déduplication, pertes."
    )]
    async fn ingest_stats(
        &self,
        Parameters(_): Parameters<NoParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let s = &self.state;
        Self::ok(json!({
            "events": s.events.metrics.snapshot(),
            "logs": s.logs.metrics.snapshot(),
            "traces": s.spans.metrics.snapshot(),
            "metrics": s.metric_points.metrics.snapshot(),
        }))
    }
}

#[tool_handler]
impl ServerHandler for DatacatMcp {
    fn get_info(&self) -> ServerInfo {
        let mut info = ServerInfo::default();
        info.capabilities = ServerCapabilities::builder().enable_tools().build();
        info.server_info.name = "datacat".into();
        info.server_info.version = env!("CARGO_PKG_VERSION").into();
        info.instructions = Some(
            "Accès LECTURE aux données Datacat (logs, traces, events, métriques, parcours). \
             Utilise ces outils pour debugger, analyser l'usage réel, corréler events↔logs↔traces \
             et générer/mettre à jour des tests E2E."
                .into(),
        );
        info
    }
}

/// Construit le service HTTP MCP (streamable) à monter dans axum (`/mcp`).
pub fn service(state: AppState) -> StreamableHttpService<DatacatMcp, LocalSessionManager> {
    StreamableHttpService::new(
        move || Ok(DatacatMcp::new(state.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    )
}

#[cfg(test)]
mod tests {
    use super::DatacatMcp;

    #[test]
    fn registers_all_tools() {
        let router = DatacatMcp::tool_router();
        for name in [
            "search_logs",
            "get_trace",
            "search_events",
            "frequent_journeys",
            "search_metrics",
            "run_read_sql",
            "ingest_stats",
        ] {
            assert!(router.has_route(name), "outil MCP manquant: {name}");
        }
        assert_eq!(router.list_all().len(), 7);
    }
}
