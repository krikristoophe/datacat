//! Moteur de requête de la couche de lecture, **partagé** par les handlers REST (`/v1/query/*`)
//! et le serveur MCP. Chaque fonction prend des paramètres typés et renvoie un `serde_json::Value`
//! (même forme de réponse quel que soit le transport).
//!
//! Les bornes temporelles sont des chaînes RFC3339 (pratique pour un agent et pour générer un
//! schéma JSON sans dépendance chrono côté schemars).

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder};

use crate::error::AppError;

fn db_err(e: sqlx::Error) -> AppError {
    AppError::Internal(e.into())
}

fn clamp_limit(limit: Option<i64>, default: i64, max: i64) -> i64 {
    limit.unwrap_or(default).clamp(1, max)
}

fn parse_ts(value: &Option<String>) -> Result<Option<DateTime<Utc>>, AppError> {
    match value {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => DateTime::parse_from_rfc3339(s.trim())
            .map(|d| Some(d.with_timezone(&Utc)))
            .map_err(|e| AppError::bad_request(format!("horodatage RFC3339 invalide '{s}': {e}"))),
    }
}

// ── Logs ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct LogsParams {
    /// Filtre sur le nom du service (`service.name`).
    pub service: Option<String>,
    pub session: Option<String>,
    pub trace_id: Option<String>,
    /// Sévérité OTLP minimale (1..24 ; ex. 17 = ERROR).
    pub severity_min: Option<i16>,
    /// Sous-chaîne recherchée dans le corps du log (ILIKE).
    pub q: Option<String>,
    /// Borne basse RFC3339 (ex. 2026-06-21T10:00:00Z).
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
struct LogRow {
    log_time: DateTime<Utc>,
    severity_number: Option<i16>,
    severity_text: Option<String>,
    service_name: Option<String>,
    body: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    session_id: Option<String>,
    actor_id: Option<String>,
    tenant_id: Option<String>,
}

pub async fn search_logs(pool: &PgPool, p: &LogsParams) -> Result<Value, AppError> {
    let (from, to) = (parse_ts(&p.from)?, parse_ts(&p.to)?);
    let limit = clamp_limit(p.limit, 100, 1000);
    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT log_time, severity_number, severity_text, service_name, body, \
         trace_id, span_id, session_id, actor_id, tenant_id FROM logs WHERE true",
    );
    if let Some(s) = &p.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(s) = &p.session {
        qb.push(" AND session_id = ").push_bind(s.clone());
    }
    if let Some(t) = &p.trace_id {
        qb.push(" AND trace_id = ").push_bind(t.clone());
    }
    if let Some(sv) = p.severity_min {
        qb.push(" AND severity_number >= ").push_bind(sv);
    }
    if let Some(text) = &p.q {
        qb.push(" AND body ILIKE ").push_bind(format!("%{text}%"));
    }
    if let Some(f) = from {
        qb.push(" AND log_time >= ").push_bind(f);
    }
    if let Some(t) = to {
        qb.push(" AND log_time <= ").push_bind(t);
    }
    qb.push(" ORDER BY log_time DESC LIMIT ").push_bind(limit);
    let rows: Vec<LogRow> = qb.build_query_as().fetch_all(pool).await.map_err(db_err)?;
    Ok(json!({ "logs": rows }))
}

// ── Traces ────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, FromRow)]
struct SpanRow {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    start_time: DateTime<Utc>,
    end_time: Option<DateTime<Utc>>,
    duration_ms: Option<f64>,
    name: String,
    kind: Option<i16>,
    service_name: Option<String>,
    status_code: Option<i16>,
    status_message: Option<String>,
    session_id: Option<String>,
    span_attributes: Value,
}

pub async fn get_trace(pool: &PgPool, trace_id: &str) -> Result<Value, AppError> {
    let spans: Vec<SpanRow> = sqlx::query_as(
        "SELECT trace_id, span_id, parent_span_id, start_time, end_time, duration_ms, name, \
         kind, service_name, status_code, status_message, session_id, span_attributes \
         FROM spans WHERE trace_id = $1 ORDER BY start_time",
    )
    .bind(trace_id)
    .fetch_all(pool)
    .await
    .map_err(db_err)?;
    Ok(json!({ "trace_id": trace_id, "span_count": spans.len(), "spans": spans }))
}

// ── Events ────────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct EventsParams {
    pub actor: Option<String>,
    pub session: Option<String>,
    pub tenant: Option<String>,
    /// Nom métier de l'event (`event_name`).
    pub name: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
struct EventRow {
    event_id: uuid::Uuid,
    event_name: String,
    actor_id: String,
    session_id: String,
    tenant_id: Option<String>,
    timestamp_client: DateTime<Utc>,
    properties: Value,
}

pub async fn search_events(pool: &PgPool, p: &EventsParams) -> Result<Value, AppError> {
    let (from, to) = (parse_ts(&p.from)?, parse_ts(&p.to)?);
    let limit = clamp_limit(p.limit, 100, 1000);
    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT event_id, event_name, actor_id, session_id, tenant_id, timestamp_client, \
         properties FROM events WHERE true",
    );
    if let Some(a) = &p.actor {
        qb.push(" AND actor_id = ").push_bind(a.clone());
    }
    if let Some(s) = &p.session {
        qb.push(" AND session_id = ").push_bind(s.clone());
    }
    if let Some(t) = &p.tenant {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    if let Some(n) = &p.name {
        qb.push(" AND event_name = ").push_bind(n.clone());
    }
    if let Some(f) = from {
        qb.push(" AND timestamp_client >= ").push_bind(f);
    }
    if let Some(t) = to {
        qb.push(" AND timestamp_client <= ").push_bind(t);
    }
    qb.push(" ORDER BY timestamp_client DESC LIMIT ")
        .push_bind(limit);
    let rows: Vec<EventRow> = qb.build_query_as().fetch_all(pool).await.map_err(db_err)?;
    Ok(json!({ "events": rows }))
}

// ── Parcours ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct JourneysParams {
    pub actor: Option<String>,
    pub tenant: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
struct JourneyRow {
    path: Vec<String>,
    occurrences: i64,
}

pub async fn frequent_journeys(pool: &PgPool, p: &JourneysParams) -> Result<Value, AppError> {
    let limit = clamp_limit(p.limit, 20, 200);
    let mut qb = QueryBuilder::<Postgres>::new(
        "WITH seq AS (\
           SELECT session_id, array_agg(event_name ORDER BY timestamp_client, event_id) AS path \
           FROM events WHERE true",
    );
    if let Some(a) = &p.actor {
        qb.push(" AND actor_id = ").push_bind(a.clone());
    }
    if let Some(t) = &p.tenant {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    qb.push(
        " GROUP BY session_id) \
         SELECT path, count(*) AS occurrences FROM seq \
         GROUP BY path ORDER BY occurrences DESC, path LIMIT ",
    )
    .push_bind(limit);
    let rows: Vec<JourneyRow> = qb.build_query_as().fetch_all(pool).await.map_err(db_err)?;
    Ok(json!({ "journeys": rows }))
}

// ── Métriques ─────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize, JsonSchema)]
pub struct MetricsParams {
    /// Nom de la métrique (`metric_name`).
    pub name: Option<String>,
    pub service: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
struct MetricRow {
    time: DateTime<Utc>,
    metric_name: String,
    metric_type: String,
    unit: Option<String>,
    value_double: Option<f64>,
    value_int: Option<i64>,
    count: Option<i64>,
    sum: Option<f64>,
    buckets: Option<Value>,
    service_name: Option<String>,
    scope_name: Option<String>,
    attributes: Value,
}

pub async fn search_metrics(pool: &PgPool, p: &MetricsParams) -> Result<Value, AppError> {
    let (from, to) = (parse_ts(&p.from)?, parse_ts(&p.to)?);
    let limit = clamp_limit(p.limit, 100, 1000);
    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT time, metric_name, metric_type, unit, value_double, value_int, count, sum, \
         buckets, service_name, scope_name, attributes FROM metric_points WHERE true",
    );
    if let Some(n) = &p.name {
        qb.push(" AND metric_name = ").push_bind(n.clone());
    }
    if let Some(s) = &p.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(f) = from {
        qb.push(" AND time >= ").push_bind(f);
    }
    if let Some(t) = to {
        qb.push(" AND time <= ").push_bind(t);
    }
    qb.push(" ORDER BY time DESC LIMIT ").push_bind(limit);
    let rows: Vec<MetricRow> = qb.build_query_as().fetch_all(pool).await.map_err(db_err)?;
    Ok(json!({ "metrics": rows }))
}
