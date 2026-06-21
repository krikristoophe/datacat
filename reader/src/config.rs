//! Configuration chargée depuis les variables d'environnement.
//!
//! Les variables sont **identiques** à celles de `datacat-exporter` pour
//! faciliter le déploiement dans le même environnement.

use anyhow::Context;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;

/// Configuration runtime du lecteur froid.
#[derive(Debug, Clone)]
pub struct ColdConfig {
    /// URL de l'endpoint S3-compatible (ex. `http://localhost:9200`).
    /// `None` = AWS S3 public.
    pub s3_endpoint: Option<String>,
    /// Région AWS / MinIO (défaut : `eu-west-1`).
    pub s3_region: String,
    /// Nom du bucket S3.
    pub s3_bucket: String,
    /// Access key ID AWS / MinIO.
    pub aws_access_key_id: String,
    /// Secret access key AWS / MinIO.
    pub aws_secret_access_key: String,
    /// `true` pour autoriser HTTP (MinIO local sans TLS).
    pub s3_allow_http: bool,
    /// Préfixe optionnel à l'intérieur du bucket (ex. `prod/`).
    /// `None` = racine du bucket.
    pub s3_prefix: Option<String>,
}

impl ColdConfig {
    /// Charge la config depuis les variables d'environnement.
    ///
    /// Variables reconnues :
    /// - `S3_ENDPOINT` (optionnel)
    /// - `S3_REGION` (défaut : `eu-west-1`)
    /// - `S3_BUCKET` (obligatoire)
    /// - `AWS_ACCESS_KEY_ID` (obligatoire)
    /// - `AWS_SECRET_ACCESS_KEY` (obligatoire)
    /// - `S3_ALLOW_HTTP` (`true`/`1` pour activer)
    /// - `S3_PREFIX` (optionnel)
    pub fn from_env() -> anyhow::Result<Self> {
        let s3_endpoint = std::env::var("S3_ENDPOINT").ok();
        let s3_region = std::env::var("S3_REGION").unwrap_or_else(|_| "eu-west-1".to_string());
        let s3_bucket = std::env::var("S3_BUCKET").context("S3_BUCKET must be set")?;
        let aws_access_key_id =
            std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID must be set")?;
        let aws_secret_access_key =
            std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY must be set")?;
        let s3_allow_http = std::env::var("S3_ALLOW_HTTP")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        let s3_prefix = std::env::var("S3_PREFIX").ok();

        Ok(Self {
            s3_endpoint,
            s3_region,
            s3_bucket,
            aws_access_key_id,
            aws_secret_access_key,
            s3_allow_http,
            s3_prefix,
        })
    }
}

/// Construit un `ObjectStore` pointant sur le bucket configuré.
pub fn build_object_store(cfg: &ColdConfig) -> anyhow::Result<Arc<dyn ObjectStore>> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&cfg.s3_bucket)
        .with_region(&cfg.s3_region)
        .with_access_key_id(&cfg.aws_access_key_id)
        .with_secret_access_key(&cfg.aws_secret_access_key);

    if let Some(endpoint) = &cfg.s3_endpoint {
        builder = builder.with_endpoint(endpoint);
    }

    if cfg.s3_allow_http {
        builder = builder.with_allow_http(true);
    }

    let store = builder.build().context("building S3 object store")?;
    Ok(Arc::new(store))
}
