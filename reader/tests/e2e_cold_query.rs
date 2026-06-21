//! Test e2e : écriture de Parquet sur MinIO → requête DataFusion → vérification.
//!
//! Infrastructure gérée par `reader/run-tests.sh` :
//!   - MinIO sur `localhost:9200` (API) / `9201` (console)
//!     MINIO_ROOT_USER=minioadmin / MINIO_ROOT_PASSWORD=minioadmin
//!
//! Ce test :
//!   1. Construit des RecordBatch d'events de test (Arrow/Parquet, sans PG).
//!   2. Les écrit sur MinIO dans le layout Hive attendu.
//!   3. Enregistre la table dans DataFusion via `ColdReader`.
//!   4. Exécute `SELECT count(*)` + `GROUP BY event_name` et vérifie les résultats.
//!   5. Exécute la requête "séquences par session" et vérifie le format.

use anyhow::Context;
use arrow::array::{Int64Array, StringArray, StringBuilder, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datacat_reader::{config::build_object_store, ColdConfig, ColdReader};
use object_store::{path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

// ─── Constantes ──────────────────────────────────────────────────────────────

const MINIO_ENDPOINT: &str = "http://localhost:9200";
const MINIO_ACCESS_KEY: &str = "minioadmin";
const MINIO_SECRET_KEY: &str = "minioadmin";
const TEST_BUCKET: &str = "datacat-reader-test";
const TEST_DATE: &str = "2024-08-20";
const N_EVENTS: usize = 300;
/// Nombre de noms d'events distincts dans les données de test.
const N_EVENT_NAMES: usize = 5;
/// Nombre de sessions distinctes.
const N_SESSIONS: usize = 10;

// ─── Helper : config ColdReader ───────────────────────────────────────────────

fn test_config() -> ColdConfig {
    ColdConfig {
        s3_endpoint: Some(MINIO_ENDPOINT.to_string()),
        s3_region: "us-east-1".to_string(),
        s3_bucket: TEST_BUCKET.to_string(),
        aws_access_key_id: MINIO_ACCESS_KEY.to_string(),
        aws_secret_access_key: MINIO_SECRET_KEY.to_string(),
        s3_allow_http: true,
        s3_prefix: None,
    }
}

// ─── Helper : génération Parquet ─────────────────────────────────────────────

/// Génère N_EVENTS lignes d'events synthétiques et les sérialise en Parquet.
fn make_events_parquet() -> anyhow::Result<Bytes> {
    let schema = datacat_reader::schema::events_schema();
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3"),
        ))
        .build();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).context("ArrowWriter")?;

    let n = N_EVENTS;
    // timestamp de base : 2024-08-20T08:00:00 UTC en microsecondes
    let base_us: i64 = 1_724_140_800_000_000;

    let mut event_id_b = StringBuilder::with_capacity(n, n * 36);
    let mut event_name_b = StringBuilder::with_capacity(n, n * 20);
    let mut tenant_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut actor_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut session_id_b = StringBuilder::with_capacity(n, n * 10);
    let mut ts_client: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut received_at: Vec<Option<i64>> = Vec::with_capacity(n);
    let mut properties_b = StringBuilder::with_capacity(n, n * 30);

    for i in 0..n {
        event_id_b.append_value(format!("{:0>36}", i));
        event_name_b.append_value(format!("event_{}", i % N_EVENT_NAMES));
        tenant_id_b.append_value("tenant-1");
        actor_id_b.append_value(format!("actor-{}", i % 20));
        session_id_b.append_value(format!("session-{}", i % N_SESSIONS));
        ts_client.push(Some(base_us + i as i64 * 1_000_000));
        received_at.push(Some(base_us + i as i64 * 1_000_000 + 500));
        properties_b.append_value(format!(r#"{{"idx":{i}}}"#));
    }

    let ts_client_arr = Arc::new(
        TimestampMicrosecondArray::from(ts_client).with_timezone("UTC".to_string()),
    );
    let received_at_arr = Arc::new(
        TimestampMicrosecondArray::from(received_at).with_timezone("UTC".to_string()),
    );

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
    .context("building test RecordBatch")?;

    writer.write(&batch).context("writing batch to Parquet")?;
    writer.close().context("closing Parquet writer")?;

    Ok(Bytes::from(buf))
}

// ─── Helper : création bucket MinIO ──────────────────────────────────────────

/// Crée un bucket MinIO via l'API S3 (SigV4 manuelle).
async fn create_minio_bucket(bucket: &str) -> anyhow::Result<()> {
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha256};
    use std::time::SystemTime;
    type HmacSha256 = Hmac<Sha256>;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let dt = chrono::DateTime::from_timestamp(now as i64, 0).unwrap();
    let amz_date = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = dt.format("%Y%m%d").to_string();

    let region = "us-east-1";
    let service = "s3";
    let host = "localhost:9200";
    let uri = format!("/{bucket}");
    let payload_hash = {
        let mut h = Sha256::new();
        h.update(b"");
        format!("{:x}", h.finalize())
    };
    let canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical_request = format!(
        "PUT\n{uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let credential_scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let cr_hash = {
        let mut h = Sha256::new();
        h.update(canonical_request.as_bytes());
        format!("{:x}", h.finalize())
    };
    let string_to_sign =
        format!("AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{cr_hash}");

    let k_date = {
        let mut m =
            HmacSha256::new_from_slice(format!("AWS4{MINIO_SECRET_KEY}").as_bytes()).unwrap();
        m.update(date_stamp.as_bytes());
        m.finalize().into_bytes()
    };
    let k_region = {
        let mut m = HmacSha256::new_from_slice(&k_date).unwrap();
        m.update(region.as_bytes());
        m.finalize().into_bytes()
    };
    let k_service = {
        let mut m = HmacSha256::new_from_slice(&k_region).unwrap();
        m.update(service.as_bytes());
        m.finalize().into_bytes()
    };
    let signing_key = {
        let mut m = HmacSha256::new_from_slice(&k_service).unwrap();
        m.update(b"aws4_request");
        m.finalize().into_bytes()
    };
    let signature = {
        let mut m = HmacSha256::new_from_slice(&signing_key).unwrap();
        m.update(string_to_sign.as_bytes());
        format!("{:x}", m.finalize().into_bytes())
    };

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={MINIO_ACCESS_KEY}/{credential_scope},\
         SignedHeaders={signed_headers},Signature={signature}"
    );

    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{MINIO_ENDPOINT}/{bucket}"))
        .header("host", host)
        .header("x-amz-date", &amz_date)
        .header("x-amz-content-sha256", &payload_hash)
        .header("authorization", &authorization)
        .send()
        .await
        .context("MinIO CreateBucket request")?;

    let status = resp.status();
    if status.is_success() || status.as_u16() == 409 {
        eprintln!("MinIO bucket '{bucket}' ready (HTTP {status})");
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("CreateBucket failed: {status} — {body}");
    }
}

// ─── Test principal ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cold_query_end_to_end() -> anyhow::Result<()> {
    // ── 1. Génère le Parquet en mémoire ──────────────────────────────────────
    let parquet_bytes = make_events_parquet().context("generating test Parquet")?;
    eprintln!(
        "Generated Parquet: {} bytes for {N_EVENTS} events",
        parquet_bytes.len()
    );

    // ── 2. Crée le bucket MinIO ───────────────────────────────────────────────
    create_minio_bucket(TEST_BUCKET).await?;

    // ── 3. Upload du Parquet vers MinIO ───────────────────────────────────────
    let cfg = test_config();
    let store = build_object_store(&cfg).context("building object store for upload")?;

    let s3_key = format!("events/date={TEST_DATE}/part-0000.parquet");
    let path = Path::from(s3_key.as_str());

    store
        .put(&path, parquet_bytes.clone().into())
        .await
        .with_context(|| format!("uploading Parquet to s3://{TEST_BUCKET}/{s3_key}"))?;

    eprintln!("Uploaded to s3://{TEST_BUCKET}/{s3_key}");

    // ── 4. Crée le ColdReader ─────────────────────────────────────────────────
    let reader = ColdReader::new(cfg).await.context("creating ColdReader")?;

    // ── 5. Requête : COUNT(*) ─────────────────────────────────────────────────
    eprintln!("Running: SELECT count(*) FROM events");
    let batches = reader
        .query_date("events", TEST_DATE, "SELECT count(*) AS total FROM events")
        .await
        .context("count(*) query")?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "COUNT(*) must return 1 row");

    let count_col = batches[0]
        .column_by_name("total")
        .expect("column 'total' missing");
    let count_val = count_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("total must be Int64Array")
        .value(0);
    eprintln!("COUNT(*) = {count_val}");
    assert_eq!(
        count_val, N_EVENTS as i64,
        "COUNT(*) must equal {N_EVENTS}"
    );

    // ── 6. Requête : GROUP BY event_name ─────────────────────────────────────
    eprintln!("Running: GROUP BY event_name");
    let batches2 = reader
        .query_date(
            "events",
            TEST_DATE,
            "SELECT event_name, count(*) AS n \
             FROM events \
             GROUP BY event_name \
             ORDER BY event_name",
        )
        .await
        .context("GROUP BY event_name query")?;

    let group_rows: usize = batches2.iter().map(|b| b.num_rows()).sum();
    eprintln!("GROUP BY returned {group_rows} rows (expected {N_EVENT_NAMES})");
    assert_eq!(
        group_rows, N_EVENT_NAMES,
        "GROUP BY must return exactly {N_EVENT_NAMES} distinct event_names"
    );

    // Vérifie que chaque nom commence par "event_"
    let first_batch = &batches2[0];
    let event_name_col = first_batch
        .column_by_name("event_name")
        .expect("event_name column missing");
    let event_name_arr = event_name_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("event_name must be StringArray");

    let n_col = first_batch
        .column_by_name("n")
        .expect("n column missing");
    let n_arr = n_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("n must be Int64Array");

    let mut total_from_groups: i64 = 0;
    for i in 0..group_rows {
        let name = event_name_arr.value(i);
        let count = n_arr.value(i);
        eprintln!("  {name}: {count}");
        assert!(name.starts_with("event_"), "unexpected event_name: {name}");
        total_from_groups += count;
    }
    assert_eq!(
        total_from_groups, N_EVENTS as i64,
        "sum of group counts must equal total events"
    );
    eprintln!("Sum of group counts = {total_from_groups} ✓");

    // ── 7. Requête : séquences par session ────────────────────────────────────
    // GROUP BY session + ORDER BY timestamp_client pour reconstituer les
    // séquences d'events par session (utile pour la génération de tests E2E).
    eprintln!("Running: sequences by session");
    let batches3 = reader
        .query_date(
            "events",
            TEST_DATE,
            "SELECT session_id, \
                    count(*) AS n_events, \
                    min(timestamp_client) AS first_event_at, \
                    max(timestamp_client) AS last_event_at \
             FROM events \
             GROUP BY session_id \
             ORDER BY session_id",
        )
        .await
        .context("sequences by session query")?;

    let session_rows: usize = batches3.iter().map(|b| b.num_rows()).sum();
    eprintln!("Sessions: {session_rows} (expected {N_SESSIONS})");
    assert_eq!(
        session_rows, N_SESSIONS,
        "must find exactly {N_SESSIONS} distinct sessions"
    );

    let sess_batch = &batches3[0];
    let session_id_col = sess_batch
        .column_by_name("session_id")
        .expect("session_id column missing");
    let session_id_arr = session_id_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("session_id must be StringArray");

    eprintln!("Session sequences:");
    for i in 0..session_rows {
        let sid = session_id_arr.value(i);
        eprintln!("  {sid}");
        assert!(sid.starts_with("session-"), "unexpected session_id: {sid}");
    }

    eprintln!("All assertions passed. Test PASSED.");
    Ok(())
}
