//! Connexion PostgreSQL, migrations et gestion des partitions.

mod partitions;
pub use partitions::{drain_staging, ensure_partition_window, purge_old_partitions};

use std::time::Duration;

use anyhow::{Context, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Executor, PgPool};

/// Ouvre le pool de connexions. Toutes les connexions sont forcées en UTC (cohérence des
/// bornes de partition et des horodatages).
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                conn.execute("SET TIME ZONE 'UTC'").await?;
                conn.execute("SET application_name = 'datacat-ingest'")
                    .await?;
                Ok(())
            })
        })
        .connect(database_url)
        .await
        .context("connexion PostgreSQL")
}

/// Applique les migrations versionnées du dossier `migrations/`.
pub async fn run_migrations(pool: &PgPool) -> Result<()> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .context("application des migrations")
}
