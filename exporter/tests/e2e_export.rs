//! End-to-end integration test for the Parquet export pipeline.
//!
//! Infrastructure prerequisites (managed by `run-tests.sh`):
//!   - MinIO running on localhost:9100 (API) / 9101 (console)
//!     with MINIO_ROOT_USER=minioadmin / MINIO_ROOT_PASSWORD=minioadmin
//!   - PostgreSQL on localhost:55432 (user/pass/db = datacat)
//!
//! The test:
//!   1. Creates an isolated PostgreSQL database `datacat_export_test`.
//!   2. Creates a minimal `events` table + partition for the test date.
//!   3. Inserts 1 000 synthetic events.
//!   4. Creates a MinIO bucket.
//!   5. Runs the export.
//!   6. Reads back the Parquet from MinIO via object_store + parquet reader.
//!   7. Asserts row count and spot-checks columns.
//!   8. Cleans up (drops the test DB, the bucket objects are ephemeral).

use anyhow::Context;
use arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use bytes::Bytes;
use chrono::{NaiveDate, TimeZone, Utc};
use object_store::aws::AmazonS3Builder;
use object_store::{path::Path, ObjectStore};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

// ─── Constants ───────────────────────────────────────────────────────────────

const PG_ADMIN_URL: &str = "postgres://datacat:datacat@localhost:55432/datacat";
const TEST_DB: &str = "datacat_export_test";
const MINIO_ENDPOINT: &str = "http://localhost:9100";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const TEST_BUCKET: &str = "datacat-test";
const TEST_DATE: &str = "2024-06-15";
const N_EVENTS: usize = 1_000;

// ─── Test ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_export_events_end_to_end() -> anyhow::Result<()> {
    // ── 1. Create isolated PostgreSQL DB ──────────────────────────────────────
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(PG_ADMIN_URL)
        .await
        .context("connecting to admin PostgreSQL")?;

    // Drop + recreate for a clean slate (idempotent if previous run crashed).
    sqlx::query(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{TEST_DB}'"
    ))
    .execute(&admin_pool)
    .await
    .ok();

    sqlx::query(&format!("DROP DATABASE IF EXISTS {TEST_DB}"))
        .execute(&admin_pool)
        .await
        .context("drop test db")?;

    sqlx::query(&format!("CREATE DATABASE {TEST_DB}"))
        .execute(&admin_pool)
        .await
        .context("create test db")?;

    // ── 2. Apply schema to test DB ────────────────────────────────────────────
    let test_db_url = format!("postgres://datacat:datacat@localhost:55432/{TEST_DB}");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&test_db_url)
        .await
        .context("connecting to test PostgreSQL DB")?;

    // Minimal events table (partitioned, same DDL as production)
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS events (
            event_id         uuid        NOT NULL,
            event_name       text        NOT NULL,
            tenant_id        text,
            actor_id         text        NOT NULL,
            session_id       text        NOT NULL,
            timestamp_client timestamptz NOT NULL,
            received_at      timestamptz NOT NULL DEFAULT now(),
            properties       jsonb       NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (timestamp_client, event_id)
        ) PARTITION BY RANGE (timestamp_client)",
    )
    .execute(&pool)
    .await
    .context("create events table")?;

    // Create the daily partition for our test date
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS events_p20240615
         PARTITION OF events
         FOR VALUES FROM ('2024-06-15 00:00:00+00') TO ('2024-06-16 00:00:00+00')",
    )
    .execute(&pool)
    .await
    .context("create partition")?;

    // ── 3. Insert 1 000 events ────────────────────────────────────────────────
    let date = NaiveDate::parse_from_str(TEST_DATE, "%Y-%m-%d").unwrap();
    let base_ts = Utc.from_utc_datetime(&date.and_hms_opt(8, 0, 0).unwrap());

    let mut tx = pool.begin().await?;
    for i in 0..N_EVENTS {
        let ts = base_ts + chrono::Duration::seconds(i as i64);
        let event_id = uuid::Uuid::new_v4();
        let event_name = format!("test_event_{}", i % 10);
        let actor_id = format!("actor-{}", i % 100);
        let session_id = format!("session-{}", i % 50);
        let properties = serde_json::json!({
            "index": i,
            "label": format!("label-{i}"),
        });

        sqlx::query(
            "INSERT INTO events (event_id, event_name, actor_id, session_id, timestamp_client, received_at, properties)
             VALUES ($1, $2, $3, $4, $5, $5, $6)",
        )
        .bind(event_id)
        .bind(&event_name)
        .bind(&actor_id)
        .bind(&session_id)
        .bind(ts)
        .bind(serde_json::Value::Object(
            properties.as_object().unwrap().clone(),
        ))
        .execute(&mut *tx)
        .await
        .with_context(|| format!("inserting event {i}"))?;
    }
    tx.commit().await?;

    eprintln!("Inserted {N_EVENTS} events into {TEST_DB}");

    // ── 4. MinIO: create bucket ───────────────────────────────────────────────
    let store: Arc<dyn ObjectStore> = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(TEST_BUCKET)
            .with_region("us-east-1")
            .with_endpoint(MINIO_ENDPOINT)
            .with_access_key_id(MINIO_ACCESS_KEY)
            .with_secret_access_key(MINIO_SECRET_KEY)
            .with_allow_http(true)
            .build()
            .context("building MinIO object store")?,
    );

    // Create bucket via MinIO admin API (mc alias + mb) — we call it via reqwest.
    create_minio_bucket(TEST_BUCKET).await?;

    // ── 5. Run the export ─────────────────────────────────────────────────────
    let cfg = datacat_exporter::config::Config {
        database_url: test_db_url.clone(),
        s3_endpoint: Some(MINIO_ENDPOINT.to_string()),
        s3_region: "us-east-1".to_string(),
        aws_access_key_id: Some(MINIO_ACCESS_KEY.to_string()),
        aws_secret_access_key: Some(MINIO_SECRET_KEY.to_string()),
        s3_allow_http: true,
    };

    let store_export = datacat_exporter::config::build_object_store(&cfg, TEST_BUCKET)?;

    let rows_written =
        datacat_exporter::export::export_events(&pool, &store_export, date, TEST_BUCKET, None)
            .await
            .context("running export_events")?;

    eprintln!("export_events returned rows_written = {rows_written}");
    assert_eq!(rows_written, N_EVENTS, "rows_written must match inserted");

    // ── 6. Read back Parquet from MinIO ───────────────────────────────────────
    let parquet_key = format!("events/date={TEST_DATE}/part-0000.parquet");
    let path = Path::from(parquet_key.as_str());

    let get_result = store
        .get(&path)
        .await
        .with_context(|| format!("getting {parquet_key} from MinIO"))?;

    let parquet_bytes: Bytes = get_result
        .bytes()
        .await
        .context("reading parquet bytes from MinIO")?;

    eprintln!(
        "Downloaded {} bytes from s3://{TEST_BUCKET}/{parquet_key}",
        parquet_bytes.len()
    );

    // ── 7. Verify content ─────────────────────────────────────────────────────
    let builder = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)
        .context("creating parquet reader")?;

    let parquet_schema = builder.schema().clone();
    eprintln!("Parquet schema: {parquet_schema}");

    let reader = builder.build().context("building parquet reader")?;

    let mut total_read: usize = 0;
    let mut first_batch_checked = false;

    for batch in reader {
        let batch = batch.context("reading record batch")?;
        total_read += batch.num_rows();

        if !first_batch_checked {
            first_batch_checked = true;

            // Check event_id column is Utf8 (non-null)
            let event_id_col = batch
                .column_by_name("event_id")
                .expect("event_id column missing");
            let event_id_arr = event_id_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("event_id must be StringArray");
            assert!(!event_id_arr.is_null(0), "event_id[0] must not be null");
            let first_event_id = event_id_arr.value(0);
            assert_eq!(first_event_id.len(), 36, "event_id must be UUID string");
            eprintln!("first event_id = {first_event_id}");

            // Check event_name
            let event_name_col = batch
                .column_by_name("event_name")
                .expect("event_name column missing");
            let event_name_arr = event_name_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("event_name must be StringArray");
            let first_event_name = event_name_arr.value(0);
            assert!(
                first_event_name.starts_with("test_event_"),
                "unexpected event_name: {first_event_name}"
            );
            eprintln!("first event_name = {first_event_name}");

            // Check timestamp_client is Timestamp(Micros, UTC)
            let ts_col = batch
                .column_by_name("timestamp_client")
                .expect("timestamp_client missing");
            let ts_arr = ts_col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("timestamp_client must be TimestampMicrosecondArray");
            assert!(!ts_arr.is_null(0), "timestamp_client[0] must not be null");
            eprintln!("first timestamp_client (micros) = {}", ts_arr.value(0));

            // Check properties is valid JSON string
            let props_col = batch
                .column_by_name("properties")
                .expect("properties column missing");
            let props_arr = props_col
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("properties must be StringArray");
            let props_str = props_arr.value(0);
            let _props_json: serde_json::Value =
                serde_json::from_str(props_str).expect("properties must be valid JSON");
            eprintln!("first properties = {props_str}");
        }
    }

    eprintln!("Total rows read from Parquet: {total_read}");
    assert_eq!(
        total_read, N_EVENTS,
        "must read back exactly {N_EVENTS} rows from Parquet"
    );

    // ── 8. Idempotence check: re-run export, same result ─────────────────────
    let rows_written2 =
        datacat_exporter::export::export_events(&pool, &store_export, date, TEST_BUCKET, None)
            .await
            .context("re-running export_events for idempotence check")?;
    assert_eq!(
        rows_written2, N_EVENTS,
        "idempotent re-run must produce same row count"
    );
    eprintln!("Idempotence check passed: re-run produced {rows_written2} rows");

    // ── Cleanup ───────────────────────────────────────────────────────────────
    pool.close().await;
    sqlx::query(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{TEST_DB}'"
    ))
    .execute(&admin_pool)
    .await
    .ok();
    sqlx::query(&format!("DROP DATABASE IF EXISTS {TEST_DB}"))
        .execute(&admin_pool)
        .await
        .context("drop test db at teardown")?;

    eprintln!("Test database {TEST_DB} dropped. Test passed.");

    Ok(())
}

// ─── MinIO bucket creation ────────────────────────────────────────────────────

/// Create a bucket in MinIO using the S3 API (PutBucket).
/// Tolerates "BucketAlreadyOwnedByYou" (idempotent).
async fn create_minio_bucket(bucket: &str) -> anyhow::Result<()> {
    // Use reqwest with AWS Signature V4 is complex; instead we use object_store's
    // create_multipart_upload workaround or simply the AWS S3 CreateBucket API via
    // a raw HTTP PUT with correct auth headers via the aws-sigv4 style.
    //
    // Simpler: use the MinIO `mc` CLI if available, else build the store client
    // and tolerate the "bucket already exists" path by uploading an empty object
    // and catching errors.
    //
    // Actual approach: we just attempt to list the bucket; if it fails, we issue
    // a CreateBucket via reqwest with proper V4 signing (we rely on object_store).
    //
    // Easiest path compatible with object_store: call `store.list(None)` — if the
    // bucket doesn't exist object_store returns an error; we then use mc to create it.
    // Instead, we use the MinIO admin API (non-standard S3).
    //
    // Decision: use `aws` CLI (or mc) if available, else use the S3 CreateBucket
    // request via reqwest with manual basic-auth-style MinIO compatibility.
    //
    // For robustness in the test we call the MinIO S3 CreateBucket endpoint directly.

    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;

    // Format date/time for AWS SigV4
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dt = chrono::DateTime::from_timestamp(now as i64, 0).unwrap();
    let amz_date = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = dt.format("%Y%m%d").to_string();

    let region = "us-east-1";
    let service = "s3";
    let method = "PUT";
    let host = "localhost:9100".to_string();
    let uri = format!("/{bucket}");

    // Payload hash (empty body)
    let payload_hash = {
        let mut hasher = Sha256::new();
        hasher.update(b"");
        format!("{:x}", hasher.finalize())
    };

    // Canonical headers
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    // Canonical request
    let canonical_request =
        format!("{method}\n{uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");

    // String to sign
    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let canonical_request_hash = {
        let mut hasher = Sha256::new();
        hasher.update(canonical_request.as_bytes());
        format!("{:x}", hasher.finalize())
    };
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}");

    // Signing key
    type HmacSha256 = Hmac<Sha256>;
    let signing_key = {
        let k_date = {
            let mut mac =
                HmacSha256::new_from_slice(format!("AWS4{MINIO_SECRET_KEY}").as_bytes()).unwrap();
            mac.update(date_stamp.as_bytes());
            mac.finalize().into_bytes()
        };
        let k_region = {
            let mut mac = HmacSha256::new_from_slice(&k_date).unwrap();
            mac.update(region.as_bytes());
            mac.finalize().into_bytes()
        };
        let k_service = {
            let mut mac = HmacSha256::new_from_slice(&k_region).unwrap();
            mac.update(service.as_bytes());
            mac.finalize().into_bytes()
        };
        let mut mac = HmacSha256::new_from_slice(&k_service).unwrap();
        mac.update(b"aws4_request");
        mac.finalize().into_bytes()
    };

    let signature = {
        let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
        mac.update(string_to_sign.as_bytes());
        format!("{:x}", mac.finalize().into_bytes())
    };

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_ACCESS_KEY}/{credential_scope},SignedHeaders={signed_headers},Signature={signature}"
    );

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{MINIO_ENDPOINT}/{bucket}"))
        .header("host", &host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("authorization", &authorization)
        .send()
        .await
        .context("creating MinIO bucket")?;

    let status = resp.status();
    if status.is_success() || status.as_u16() == 409 {
        // 409 = BucketAlreadyOwnedByYou — acceptable
        eprintln!("MinIO bucket '{bucket}' ready (status={status})");
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("CreateBucket failed: {status} — {body}");
    }
}
