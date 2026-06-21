//! Moteur de requête analytique : DataFusion sur Parquet S3.
//!
//! [`ColdReader`] enregistre les fichiers Parquet S3 comme tables DataFusion
//! via `ListingTable` et exécute des requêtes SQL arbitraires.

use anyhow::Context;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

use crate::config::{build_object_store, ColdConfig};
use crate::schema::schema_for_table;

/// Moteur de lecture analytique sur le stockage froid.
///
/// Un `ColdReader` maintient un contexte DataFusion configuré avec
/// l'object store S3.  Il peut enregistrer plusieurs tables et exécuter
/// des requêtes SQL sur celles-ci.
pub struct ColdReader {
    ctx: SessionContext,
    cfg: ColdConfig,
    store: Arc<dyn ObjectStore>,
}

impl ColdReader {
    /// Crée un nouveau `ColdReader` à partir de la configuration.
    pub async fn new(cfg: ColdConfig) -> anyhow::Result<Self> {
        let store = build_object_store(&cfg)?;
        let ctx = SessionContext::new();

        // Enregistre l'object store dans le contexte DataFusion
        // sous le schéma URL s3://<bucket>/
        let s3_url = format!("s3://{}/", cfg.s3_bucket);
        let url = Url::parse(&s3_url).context("parsing S3 bucket URL")?;
        ctx.register_object_store(&url, Arc::clone(&store));

        Ok(Self { ctx, cfg, store })
    }

    /// Enregistre une table Parquet S3 dans le contexte DataFusion.
    ///
    /// `table` : `"events"` ou `"logs"`.
    /// `date_prefix` : filtre optionnel sur la partition Hive.
    ///   - `None`  → toutes les partitions (`events/`)
    ///   - `Some("2024-06-15")` → `events/date=2024-06-15/`
    ///   - `Some("2024-06")` → `events/date=2024-06` (préfixe de chemin)
    ///
    /// La table est enregistrée sous son nom dans le contexte SQL.
    pub async fn register_table(
        &self,
        table: &str,
        date_prefix: Option<&str>,
    ) -> anyhow::Result<()> {
        let _schema = schema_for_table(table)?;

        // Construit le chemin S3 : s3://<bucket>/[prefix/]<table>/[date=...]
        let base = match &self.cfg.s3_prefix {
            Some(p) => format!("{p}/{table}/"),
            None => format!("{table}/"),
        };

        let path = match date_prefix {
            Some(d) => format!("{base}date={d}/"),
            None => base,
        };

        let table_url = ListingTableUrl::parse(format!("s3://{}/{path}", self.cfg.s3_bucket))
            .with_context(|| format!("parsing listing URL for table '{table}'"))?;

        let file_format = Arc::new(ParquetFormat::new());
        let listing_opts = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_collect_stat(true);

        // Infère le schéma depuis les fichiers S3 réels
        let inferred_schema = listing_opts
            .infer_schema(&self.ctx.state(), &table_url)
            .await
            .with_context(|| format!("inferring schema for table '{table}'"))?;

        let listing_cfg = ListingTableConfig::new(table_url)
            .with_listing_options(listing_opts)
            .with_schema(inferred_schema);

        let listing_table = Arc::new(
            ListingTable::try_new(listing_cfg)
                .with_context(|| format!("creating ListingTable for '{table}'"))?,
        );

        self.ctx
            .register_table(table, listing_table)
            .with_context(|| format!("registering table '{table}' in DataFusion context"))?;

        tracing::info!(table, "table registered");
        Ok(())
    }

    /// Enregistre la table `table` puis exécute la requête SQL `sql`.
    ///
    /// Retourne les [`RecordBatch`] résultants.
    pub async fn query(&self, table: &str, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        self.register_table(table, None).await?;
        self.execute_sql(sql).await
    }

    /// Enregistre la table `table` pour une date précise (`YYYY-MM-DD`)
    /// puis exécute `sql`.
    pub async fn query_date(
        &self,
        table: &str,
        date: &str,
        sql: &str,
    ) -> anyhow::Result<Vec<RecordBatch>> {
        self.register_table(table, Some(date)).await?;
        self.execute_sql(sql).await
    }

    /// Exécute une requête SQL **sans** enregistrer de table supplémentaire.
    /// Les tables doivent avoir été enregistrées au préalable via
    /// [`register_table`](Self::register_table).
    pub async fn execute_sql(&self, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .with_context(|| format!("parsing SQL: {sql}"))?;

        let batches = df.collect().await.context("executing SQL query")?;
        Ok(batches)
    }

    /// Accès direct à l'object store (utile pour les tests).
    pub fn object_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }
}
