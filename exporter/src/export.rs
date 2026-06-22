use anyhow::Context;
use arrow::array::{ArrayRef, Int16Array, StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use chrono::{NaiveDate, TimeZone, Utc};
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use sqlx::Row;
use std::sync::Arc;
use tracing::{debug, info};

use crate::schema;

/// Number of rows fetched from PostgreSQL per batch.
const BATCH_SIZE: usize = 10_000;

// ─────────────────────────────────────────────────────────────────────────────
// Events export
// ─────────────────────────────────────────────────────────────────────────────

/// Row fetched from PostgreSQL for `events`.
struct EventRow {
    event_id: String,
    event_name: String,
    tenant_id: Option<String>,
    actor_id: String,
    session_id: String,
    timestamp_client_us: i64,
    received_at_us: i64,
    properties: String,
}

/// Export all events for `date` (UTC) to S3.
///
/// S3 path: `{prefix}events/date=YYYY-MM-DD/part-0.parquet`
///
/// Idempotence: the destination object is always overwritten.  Re-running the
/// export for the same day produces byte-for-byte identical output given
/// identical data in PostgreSQL (deterministic row order via ORDER BY).
pub async fn export_events(
    pool: &sqlx::PgPool,
    store: &Arc<dyn ObjectStore>,
    date: NaiveDate,
    _bucket: &str,
    prefix: Option<&str>,
) -> anyhow::Result<usize> {
    validate_prefix(prefix)?;
    let day_start = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
    let day_end = Utc.from_utc_datetime(
        &(date + chrono::Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );

    let schema = schema::events_schema();
    let props = writer_properties();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
        .context("creating ArrowWriter")?;

    let mut total_rows: usize = 0;
    let mut offset: i64 = 0;

    loop {
        let rows: Vec<EventRow> = sqlx::query(
            "SELECT
                event_id::text,
                event_name,
                tenant_id,
                actor_id,
                session_id,
                EXTRACT(EPOCH FROM timestamp_client)::bigint * 1_000_000
                    + (EXTRACT(MICROSECONDS FROM timestamp_client)::bigint % 1_000_000) AS timestamp_client_us,
                EXTRACT(EPOCH FROM received_at)::bigint * 1_000_000
                    + (EXTRACT(MICROSECONDS FROM received_at)::bigint % 1_000_000) AS received_at_us,
                properties::text
            FROM events
            WHERE timestamp_client >= $1
              AND timestamp_client <  $2
            ORDER BY timestamp_client, event_id
            LIMIT $3 OFFSET $4",
        )
        .bind(day_start)
        .bind(day_end)
        .bind(BATCH_SIZE as i64)
        .bind(offset)
        .fetch_all(pool)
        .await
        .context("querying events")?
        .into_iter()
        .map(|r| EventRow {
            event_id: r.get::<String, _>("event_id"),
            event_name: r.get::<String, _>("event_name"),
            tenant_id: r.get::<Option<String>, _>("tenant_id"),
            actor_id: r.get::<String, _>("actor_id"),
            session_id: r.get::<String, _>("session_id"),
            timestamp_client_us: r.get::<i64, _>("timestamp_client_us"),
            received_at_us: r.get::<i64, _>("received_at_us"),
            properties: r.get::<String, _>("properties"),
        })
        .collect();

        let n = rows.len();
        if n == 0 {
            break;
        }

        debug!(batch_offset = offset, batch_size = n, "fetched batch");

        let batch = events_to_record_batch(&schema, &rows)?;
        writer.write(&batch).context("writing record batch")?;

        total_rows += n;
        offset += n as i64;

        if n < BATCH_SIZE {
            break;
        }
    }

    writer.close().context("closing parquet writer")?;

    // Hive-style partition path
    let key = hive_path(prefix, "events", date, 0);
    let path = Path::from(key.as_str());

    store
        .put(&path, Bytes::from(buf).into())
        .await
        .with_context(|| format!("uploading to S3 at {key}"))?;

    info!(path = %key, rows = total_rows, "uploaded events parquet");

    Ok(total_rows)
}

fn events_to_record_batch(
    schema: &Arc<arrow::datatypes::Schema>,
    rows: &[EventRow],
) -> anyhow::Result<RecordBatch> {
    let n = rows.len();

    let mut event_id_b = StringBuilder::with_capacity(n, n * 36);
    let mut event_name_b = StringBuilder::with_capacity(n, n * 20);
    let mut tenant_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut actor_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut session_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut ts_client_b: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut received_at_b: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut properties_b = StringBuilder::with_capacity(n, n * 50);

    for r in rows {
        event_id_b.append_value(&r.event_id);
        event_name_b.append_value(&r.event_name);
        match &r.tenant_id {
            Some(v) => tenant_id_b.append_value(v),
            None => tenant_id_b.append_null(),
        }
        actor_id_b.append_value(&r.actor_id);
        session_id_b.append_value(&r.session_id);
        ts_client_b.push(Some(r.timestamp_client_us));
        received_at_b.push(Some(r.received_at_us));
        properties_b.append_value(&r.properties);
    }

    let ts_client_arr: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(ts_client_b).with_timezone("UTC".to_string()));
    let received_at_arr: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(received_at_b).with_timezone("UTC".to_string()));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(event_id_b.finish()),
            Arc::new(event_name_b.finish()),
            Arc::new(tenant_id_b.finish()),
            Arc::new(actor_id_b.finish()),
            Arc::new(session_id_b.finish()),
            ts_client_arr,
            received_at_arr,
            Arc::new(properties_b.finish()),
        ],
    )
    .context("building RecordBatch for events")?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// Logs export
// ─────────────────────────────────────────────────────────────────────────────

struct LogRow {
    log_id: String,
    log_time_us: i64,
    observed_time_us: Option<i64>,
    received_at_us: i64,
    severity_number: Option<i16>,
    severity_text: Option<String>,
    body: Option<String>,
    service_name: Option<String>,
    scope_name: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    tenant_id: Option<String>,
    actor_id: Option<String>,
    session_id: Option<String>,
    resource_attributes: String,
    log_attributes: String,
}

/// Export all logs for `date` (UTC) to S3.
///
/// S3 path: `{prefix}logs/date=YYYY-MM-DD/part-0.parquet`
pub async fn export_logs(
    pool: &sqlx::PgPool,
    store: &Arc<dyn ObjectStore>,
    date: NaiveDate,
    _bucket: &str,
    prefix: Option<&str>,
) -> anyhow::Result<usize> {
    validate_prefix(prefix)?;
    let day_start = Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0).unwrap());
    let day_end = Utc.from_utc_datetime(
        &(date + chrono::Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );

    let schema = schema::logs_schema();
    let props = writer_properties();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
        .context("creating ArrowWriter for logs")?;

    let mut total_rows: usize = 0;
    let mut offset: i64 = 0;

    loop {
        let rows: Vec<LogRow> = sqlx::query(
            "SELECT
                log_id::text,
                EXTRACT(EPOCH FROM log_time)::bigint * 1_000_000
                    + (EXTRACT(MICROSECONDS FROM log_time)::bigint % 1_000_000) AS log_time_us,
                CASE WHEN observed_time IS NOT NULL THEN
                    EXTRACT(EPOCH FROM observed_time)::bigint * 1_000_000
                    + (EXTRACT(MICROSECONDS FROM observed_time)::bigint % 1_000_000)
                END AS observed_time_us,
                EXTRACT(EPOCH FROM received_at)::bigint * 1_000_000
                    + (EXTRACT(MICROSECONDS FROM received_at)::bigint % 1_000_000) AS received_at_us,
                severity_number,
                severity_text,
                body,
                service_name,
                scope_name,
                trace_id,
                span_id,
                tenant_id,
                actor_id,
                session_id,
                resource_attributes::text,
                log_attributes::text
            FROM logs
            WHERE log_time >= $1
              AND log_time <  $2
            ORDER BY log_time, log_id
            LIMIT $3 OFFSET $4",
        )
        .bind(day_start)
        .bind(day_end)
        .bind(BATCH_SIZE as i64)
        .bind(offset)
        .fetch_all(pool)
        .await
        .context("querying logs")?
        .into_iter()
        .map(|r| LogRow {
            log_id: r.get::<String, _>("log_id"),
            log_time_us: r.get::<i64, _>("log_time_us"),
            observed_time_us: r.get::<Option<i64>, _>("observed_time_us"),
            received_at_us: r.get::<i64, _>("received_at_us"),
            severity_number: r.get::<Option<i16>, _>("severity_number"),
            severity_text: r.get::<Option<String>, _>("severity_text"),
            body: r.get::<Option<String>, _>("body"),
            service_name: r.get::<Option<String>, _>("service_name"),
            scope_name: r.get::<Option<String>, _>("scope_name"),
            trace_id: r.get::<Option<String>, _>("trace_id"),
            span_id: r.get::<Option<String>, _>("span_id"),
            tenant_id: r.get::<Option<String>, _>("tenant_id"),
            actor_id: r.get::<Option<String>, _>("actor_id"),
            session_id: r.get::<Option<String>, _>("session_id"),
            resource_attributes: r.get::<String, _>("resource_attributes"),
            log_attributes: r.get::<String, _>("log_attributes"),
        })
        .collect();

        let n = rows.len();
        if n == 0 {
            break;
        }

        debug!(batch_offset = offset, batch_size = n, "fetched log batch");

        let batch = logs_to_record_batch(&schema, &rows)?;
        writer.write(&batch).context("writing log record batch")?;

        total_rows += n;
        offset += n as i64;

        if n < BATCH_SIZE {
            break;
        }
    }

    writer.close().context("closing parquet writer for logs")?;

    let key = hive_path(prefix, "logs", date, 0);
    let path = Path::from(key.as_str());

    store
        .put(&path, Bytes::from(buf).into())
        .await
        .with_context(|| format!("uploading logs to S3 at {key}"))?;

    info!(path = %key, rows = total_rows, "uploaded logs parquet");

    Ok(total_rows)
}

fn logs_to_record_batch(
    schema: &Arc<arrow::datatypes::Schema>,
    rows: &[LogRow],
) -> anyhow::Result<RecordBatch> {
    let n = rows.len();

    let mut log_id_b = StringBuilder::with_capacity(n, n * 36);
    let mut log_time_b: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut observed_time_b: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut received_at_b: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut severity_number_b: Vec<Option<i16>> = Vec::with_capacity(n);
    let mut severity_text_b = StringBuilder::with_capacity(n, n * 10);
    let mut body_b = StringBuilder::with_capacity(n, n * 50);
    let mut service_name_b = StringBuilder::with_capacity(n, n * 20);
    let mut scope_name_b = StringBuilder::with_capacity(n, n * 20);
    let mut trace_id_b = StringBuilder::with_capacity(n, n * 32);
    let mut span_id_b = StringBuilder::with_capacity(n, n * 16);
    let mut tenant_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut actor_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut session_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut resource_attributes_b = StringBuilder::with_capacity(n, n * 50);
    let mut log_attributes_b = StringBuilder::with_capacity(n, n * 50);

    for r in rows {
        log_id_b.append_value(&r.log_id);
        log_time_b.push(Some(r.log_time_us));
        observed_time_b.push(r.observed_time_us);
        received_at_b.push(Some(r.received_at_us));
        severity_number_b.push(r.severity_number);
        match &r.severity_text {
            Some(v) => severity_text_b.append_value(v),
            None => severity_text_b.append_null(),
        }
        match &r.body {
            Some(v) => body_b.append_value(v),
            None => body_b.append_null(),
        }
        match &r.service_name {
            Some(v) => service_name_b.append_value(v),
            None => service_name_b.append_null(),
        }
        match &r.scope_name {
            Some(v) => scope_name_b.append_value(v),
            None => scope_name_b.append_null(),
        }
        match &r.trace_id {
            Some(v) => trace_id_b.append_value(v),
            None => trace_id_b.append_null(),
        }
        match &r.span_id {
            Some(v) => span_id_b.append_value(v),
            None => span_id_b.append_null(),
        }
        match &r.tenant_id {
            Some(v) => tenant_id_b.append_value(v),
            None => tenant_id_b.append_null(),
        }
        match &r.actor_id {
            Some(v) => actor_id_b.append_value(v),
            None => actor_id_b.append_null(),
        }
        match &r.session_id {
            Some(v) => session_id_b.append_value(v),
            None => session_id_b.append_null(),
        }
        resource_attributes_b.append_value(&r.resource_attributes);
        log_attributes_b.append_value(&r.log_attributes);
    }

    let log_time_arr: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(log_time_b).with_timezone("UTC".to_string()));
    let observed_time_arr: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(observed_time_b).with_timezone("UTC".to_string()));
    let received_at_arr: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(received_at_b).with_timezone("UTC".to_string()));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(log_id_b.finish()),
            log_time_arr,
            observed_time_arr,
            received_at_arr,
            Arc::new(Int16Array::from(severity_number_b)),
            Arc::new(severity_text_b.finish()),
            Arc::new(body_b.finish()),
            Arc::new(service_name_b.finish()),
            Arc::new(scope_name_b.finish()),
            Arc::new(trace_id_b.finish()),
            Arc::new(span_id_b.finish()),
            Arc::new(tenant_id_b.finish()),
            Arc::new(actor_id_b.finish()),
            Arc::new(session_id_b.finish()),
            Arc::new(resource_attributes_b.finish()),
            Arc::new(log_attributes_b.finish()),
        ],
    )
    .context("building RecordBatch for logs")?;

    Ok(batch)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Validate an operator-supplied S3 key prefix (S-12). Even though S3 keys are flat strings,
/// a prefix with `..` segments, a leading `/`, backslashes or control characters can produce
/// surprising keys on filesystem-backed object stores (MinIO on local disk) — reject them so a
/// misconfiguration fails closed instead of writing outside the intended layout.
fn validate_prefix(prefix: Option<&str>) -> anyhow::Result<()> {
    let Some(p) = prefix.filter(|p| !p.is_empty()) else {
        return Ok(());
    };
    if p.starts_with('/') || p.contains('\\') {
        anyhow::bail!(
            "invalid export prefix {p:?}: must be a relative S3 key (no leading '/' or '\\')"
        );
    }
    if p.split('/').any(|seg| seg == "..") {
        anyhow::bail!("invalid export prefix {p:?}: must not contain '..' path segments");
    }
    if p.chars().any(|c| c.is_control()) {
        anyhow::bail!("invalid export prefix {p:?}: must not contain control characters");
    }
    Ok(())
}

/// Build the Hive-partition-style S3 key.
///
/// Format: `[prefix/]<table>/date=YYYY-MM-DD/part-<n>.parquet`
///
/// The `part-<n>` numbering matches Iceberg/Spark conventions.
fn hive_path(prefix: Option<&str>, table: &str, date: NaiveDate, part: u32) -> String {
    let date_str = date.format("%Y-%m-%d").to_string();
    match prefix {
        Some(p) if !p.is_empty() => {
            format!("{p}/{table}/date={date_str}/part-{part:04}.parquet")
        }
        _ => format!("{table}/date={date_str}/part-{part:04}.parquet"),
    }
}

/// Parquet writer properties: zstd compression, single row group per batch.
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3 is valid"),
        ))
        .build()
}

// Suppress "never used" warning for TimeUnit import used in schema module
#[allow(dead_code)]
fn _use_time_unit(_: TimeUnit) {}

#[cfg(test)]
mod tests {
    use super::{hive_path, validate_prefix};
    use chrono::NaiveDate;

    #[test]
    fn valid_prefixes_accepted() {
        assert!(validate_prefix(None).is_ok());
        assert!(validate_prefix(Some("")).is_ok());
        assert!(validate_prefix(Some("cold")).is_ok());
        assert!(validate_prefix(Some("tenant-a/cold")).is_ok());
    }

    #[test]
    fn traversal_and_absolute_prefixes_rejected() {
        assert!(validate_prefix(Some("../escape")).is_err());
        assert!(validate_prefix(Some("a/../../b")).is_err());
        assert!(validate_prefix(Some("/absolute")).is_err());
        assert!(validate_prefix(Some("a\\b")).is_err());
        assert!(validate_prefix(Some("a\nb")).is_err());
    }

    #[test]
    fn hive_path_layout() {
        let date = NaiveDate::from_ymd_opt(2026, 6, 15).unwrap();
        assert_eq!(
            hive_path(None, "events", date, 0),
            "events/date=2026-06-15/part-0000.parquet"
        );
        assert_eq!(
            hive_path(Some("cold"), "logs", date, 1),
            "cold/logs/date=2026-06-15/part-0001.parquet"
        );
    }
}
