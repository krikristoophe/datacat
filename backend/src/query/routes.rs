//! Handlers de lecture (`/v1/query/*`). Lecture seule, authentifiés par token de lecture.

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap};
use axum::response::IntoResponse;
use axum::Json;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{FromRow, Postgres, QueryBuilder};

use crate::error::{AppError, AppResult};
use crate::security::check_service_token;
use crate::AppState;

/// Authentifie une requête de lecture (`query_auth`).
fn authorize_query(state: &AppState, headers: &HeaderMap) -> AppResult<()> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
                .map(str::trim)
                .filter(|t| !t.is_empty())
        });
    check_service_token(&state.config.query_auth, &state.verifier, token)
        .map_err(AppError::Unauthorized)
}

fn clamp_limit(limit: Option<i64>, default: i64, max: i64) -> i64 {
    limit.unwrap_or(default).clamp(1, max)
}

fn db_err(e: sqlx::Error) -> AppError {
    AppError::Internal(e.into())
}

// ── Logs ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    pub service: Option<String>,
    pub session: Option<String>,
    pub trace_id: Option<String>,
    pub severity_min: Option<i16>,
    /// Sous-chaîne recherchée dans le corps (ILIKE).
    pub q: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct LogRow {
    pub log_time: DateTime<Utc>,
    pub severity_number: Option<i16>,
    pub severity_text: Option<String>,
    pub service_name: Option<String>,
    pub body: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub session_id: Option<String>,
    pub actor_id: Option<String>,
    pub tenant_id: Option<String>,
}

/// `GET /v1/query/logs` — recherche de logs.
pub async fn query_logs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<LogsQuery>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    let limit = clamp_limit(q.limit, 100, 1000);

    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT log_time, severity_number, severity_text, service_name, body, \
         trace_id, span_id, session_id, actor_id, tenant_id FROM logs WHERE true",
    );
    if let Some(s) = &q.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(s) = &q.session {
        qb.push(" AND session_id = ").push_bind(s.clone());
    }
    if let Some(t) = &q.trace_id {
        qb.push(" AND trace_id = ").push_bind(t.clone());
    }
    if let Some(sv) = q.severity_min {
        qb.push(" AND severity_number >= ").push_bind(sv);
    }
    if let Some(text) = &q.q {
        qb.push(" AND body ILIKE ").push_bind(format!("%{text}%"));
    }
    if let Some(f) = q.from {
        qb.push(" AND log_time >= ").push_bind(f);
    }
    if let Some(t) = q.to {
        qb.push(" AND log_time <= ").push_bind(t);
    }
    qb.push(" ORDER BY log_time DESC LIMIT ").push_bind(limit);

    let rows: Vec<LogRow> = qb
        .build_query_as()
        .fetch_all(&state.pool)
        .await
        .map_err(db_err)?;
    Ok(Json(json!({ "logs": rows })))
}

// ── Traces ────────────────────────────────────────────────────────────────────

#[derive(Debug, Serialize, FromRow)]
pub struct SpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub duration_ms: Option<f64>,
    pub name: String,
    pub kind: Option<i16>,
    pub service_name: Option<String>,
    pub status_code: Option<i16>,
    pub status_message: Option<String>,
    pub session_id: Option<String>,
    pub span_attributes: Value,
}

/// `GET /v1/query/traces/{trace_id}` — tous les spans d'une trace, ordonnés.
pub async fn query_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;

    let spans: Vec<SpanRow> = sqlx::query_as(
        "SELECT trace_id, span_id, parent_span_id, start_time, end_time, duration_ms, name, \
         kind, service_name, status_code, status_message, session_id, span_attributes \
         FROM spans WHERE trace_id = $1 ORDER BY start_time",
    )
    .bind(&trace_id)
    .fetch_all(&state.pool)
    .await
    .map_err(db_err)?;

    Ok(Json(
        json!({ "trace_id": trace_id, "span_count": spans.len(), "spans": spans }),
    ))
}

// ── Events ────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    pub name: Option<String>,
    pub service: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct MetricRow {
    pub time: DateTime<Utc>,
    pub metric_name: String,
    pub metric_type: String,
    pub unit: Option<String>,
    pub value_double: Option<f64>,
    pub value_int: Option<i64>,
    pub count: Option<i64>,
    pub sum: Option<f64>,
    pub buckets: Option<Value>,
    pub service_name: Option<String>,
    pub scope_name: Option<String>,
    pub attributes: Value,
}

/// `GET /v1/query/metrics` — recherche de points de métriques (par nom / service / fenêtre).
pub async fn query_metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<MetricsQuery>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    let limit = clamp_limit(q.limit, 100, 1000);

    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT time, metric_name, metric_type, unit, value_double, value_int, count, sum, \
         buckets, service_name, scope_name, attributes FROM metric_points WHERE true",
    );
    if let Some(n) = &q.name {
        qb.push(" AND metric_name = ").push_bind(n.clone());
    }
    if let Some(s) = &q.service {
        qb.push(" AND service_name = ").push_bind(s.clone());
    }
    if let Some(f) = q.from {
        qb.push(" AND time >= ").push_bind(f);
    }
    if let Some(t) = q.to {
        qb.push(" AND time <= ").push_bind(t);
    }
    qb.push(" ORDER BY time DESC LIMIT ").push_bind(limit);

    let rows: Vec<MetricRow> = qb
        .build_query_as()
        .fetch_all(&state.pool)
        .await
        .map_err(db_err)?;
    Ok(Json(json!({ "metrics": rows })))
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub actor: Option<String>,
    pub session: Option<String>,
    pub tenant: Option<String>,
    pub name: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct EventRow {
    pub event_id: uuid::Uuid,
    pub event_name: String,
    pub actor_id: String,
    pub session_id: String,
    pub tenant_id: Option<String>,
    pub timestamp_client: DateTime<Utc>,
    pub properties: Value,
}

/// `GET /v1/query/events` — recherche d'events.
pub async fn query_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<EventsQuery>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    let limit = clamp_limit(q.limit, 100, 1000);

    let mut qb = QueryBuilder::<Postgres>::new(
        "SELECT event_id, event_name, actor_id, session_id, tenant_id, timestamp_client, \
         properties FROM events WHERE true",
    );
    if let Some(a) = &q.actor {
        qb.push(" AND actor_id = ").push_bind(a.clone());
    }
    if let Some(s) = &q.session {
        qb.push(" AND session_id = ").push_bind(s.clone());
    }
    if let Some(t) = &q.tenant {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    if let Some(n) = &q.name {
        qb.push(" AND event_name = ").push_bind(n.clone());
    }
    if let Some(f) = q.from {
        qb.push(" AND timestamp_client >= ").push_bind(f);
    }
    if let Some(t) = q.to {
        qb.push(" AND timestamp_client <= ").push_bind(t);
    }
    qb.push(" ORDER BY timestamp_client DESC LIMIT ")
        .push_bind(limit);

    let rows: Vec<EventRow> = qb
        .build_query_as()
        .fetch_all(&state.pool)
        .await
        .map_err(db_err)?;
    Ok(Json(json!({ "events": rows })))
}

// ── Parcours (séquences d'events par session) ─────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct JourneysQuery {
    pub actor: Option<String>,
    pub tenant: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, FromRow)]
pub struct JourneyRow {
    /// Séquence ordonnée d'`event_name` au sein d'une session.
    pub path: Vec<String>,
    /// Nombre de sessions ayant suivi exactement cette séquence.
    pub occurrences: i64,
}

/// `GET /v1/query/journeys` — séquences de parcours les plus fréquentes (par session).
/// Cœur du besoin de génération de tests E2E à partir de l'usage réel.
pub async fn query_journeys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<JourneysQuery>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    let limit = clamp_limit(q.limit, 20, 200);

    let mut qb = QueryBuilder::<Postgres>::new(
        "WITH seq AS (\
           SELECT session_id, array_agg(event_name ORDER BY timestamp_client, event_id) AS path \
           FROM events WHERE true",
    );
    if let Some(a) = &q.actor {
        qb.push(" AND actor_id = ").push_bind(a.clone());
    }
    if let Some(t) = &q.tenant {
        qb.push(" AND tenant_id = ").push_bind(t.clone());
    }
    qb.push(
        " GROUP BY session_id) \
         SELECT path, count(*) AS occurrences FROM seq \
         GROUP BY path ORDER BY occurrences DESC, path LIMIT ",
    )
    .push_bind(limit);

    let rows: Vec<JourneyRow> = qb
        .build_query_as()
        .fetch_all(&state.pool)
        .await
        .map_err(db_err)?;
    Ok(Json(json!({ "journeys": rows })))
}

// ── SQL ad-hoc (lecture seule) ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SqlRequest {
    pub sql: String,
    pub limit: Option<i64>,
}

/// `POST /v1/query/sql` — SQL **lecture seule** ad-hoc (debug/analyse, ex. via MCP).
///
/// Désactivé par défaut (`QUERY_SQL_ENABLED`). Défenses : seules les requêtes `SELECT`/`WITH`
/// sont acceptées (instruction unique, `;` interdit), exécutées dans une transaction
/// `READ ONLY` avec `statement_timeout`, et encapsulées `SELECT to_jsonb(...) FROM (<sql>) LIMIT n`
/// (la transaction READ ONLY bloque toute écriture/DDL par défense en profondeur).
pub async fn query_sql(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<SqlRequest>,
) -> AppResult<impl IntoResponse> {
    authorize_query(&state, &headers)?;
    if !state.config.query_sql_enabled {
        return Err(AppError::Forbidden(
            "endpoint SQL lecture seule désactivé (QUERY_SQL_ENABLED)".into(),
        ));
    }

    let sql = req.sql.trim().trim_end_matches(';').trim();
    let lowered = sql.to_ascii_lowercase();
    if !(lowered.starts_with("select") || lowered.starts_with("with")) {
        return Err(AppError::bad_request(
            "seules les requêtes SELECT/WITH sont autorisées",
        ));
    }
    if sql.contains(';') {
        return Err(AppError::bad_request(
            "requête unique : le caractère ';' est interdit",
        ));
    }

    let max = state.config.query_sql_max_rows;
    let limit = clamp_limit(req.limit, max, max);
    let timeout_ms = state.config.query_sql_timeout.as_millis() as i64;
    let wrapped = format!("SELECT to_jsonb(t) FROM ({sql}) AS t LIMIT {}", limit + 1);

    let mut tx = state.pool.begin().await.map_err(db_err)?;
    sqlx::query("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    sqlx::query(&format!("SET LOCAL statement_timeout = {timeout_ms}"))
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;
    let result = sqlx::query_scalar::<_, Value>(&wrapped)
        .fetch_all(&mut *tx)
        .await;
    let _ = tx.rollback().await;

    let mut rows = result.map_err(|e| match e {
        sqlx::Error::Database(db) => AppError::bad_request(format!("erreur SQL: {}", db.message())),
        other => AppError::Internal(other.into()),
    })?;
    let truncated = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    Ok(Json(
        json!({ "row_count": rows.len(), "truncated": truncated, "rows": rows }),
    ))
}
