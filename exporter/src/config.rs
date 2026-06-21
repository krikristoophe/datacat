use anyhow::{bail, Context};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;

/// Runtime configuration loaded from environment variables.
#[derive(Debug)]
pub struct Config {
    pub database_url: String,
    pub s3_endpoint: Option<String>,
    pub s3_region: String,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    /// Set to "true" to allow HTTP (needed for local MinIO without TLS).
    pub s3_allow_http: bool,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set")?;

        let s3_endpoint = std::env::var("S3_ENDPOINT").ok();
        let s3_region = std::env::var("S3_REGION").unwrap_or_else(|_| "eu-west-1".to_string());
        let aws_access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
        let aws_secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();
        let s3_allow_http = std::env::var("S3_ALLOW_HTTP")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);

        Ok(Self {
            database_url,
            s3_endpoint,
            s3_region,
            aws_access_key_id,
            aws_secret_access_key,
            s3_allow_http,
        })
    }
}

/// Build an `ObjectStore` pointing at the given bucket, configured from `Config`.
pub fn build_object_store(cfg: &Config, bucket: &str) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(&cfg.s3_region);

    if let Some(endpoint) = &cfg.s3_endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if let Some(key_id) = &cfg.aws_access_key_id {
        builder = builder.with_access_key_id(key_id);
    } else {
        bail!("AWS_ACCESS_KEY_ID must be set");
    }

    if let Some(secret) = &cfg.aws_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    } else {
        bail!("AWS_SECRET_ACCESS_KEY must be set");
    }

    if cfg.s3_allow_http {
        builder = builder.with_allow_http(true);
    }

    let store = builder.build().context("building S3 object store")?;

    Ok(Arc::new(store))
}
