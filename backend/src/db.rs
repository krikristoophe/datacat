//! Connexion PostgreSQL, migrations et gestion des partitions.

use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
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

/// Garantit l'existence des partitions journalières pour `[today - past_days, today + future_days]`.
pub async fn ensure_partition_window(
    pool: &PgPool,
    past_days: i64,
    future_days: i64,
) -> Result<()> {
    let today = Utc::now().date_naive();
    for offset in -past_days..=future_days {
        let day = today + chrono::Duration::days(offset);
        sqlx::query("SELECT datacat_ensure_partition($1)")
            .bind(day)
            .execute(pool)
            .await
            .with_context(|| format!("création de la partition du {day}"))?;
    }
    Ok(())
}

/// Purge de la rétention : DROP des partitions plus anciennes que `retention_days`.
/// Retourne le nombre de partitions supprimées.
pub async fn purge_old_partitions(pool: &PgPool, retention_days: i64) -> Result<i64> {
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(retention_days);
    let dropped: i32 = sqlx::query_scalar("SELECT datacat_drop_partitions_before($1)")
        .bind(cutoff)
        .fetch_one(pool)
        .await
        .context("purge des partitions")?;
    Ok(dropped as i64)
}

/// Fusionne tout résidu de staging (ex. après un crash) et retourne le nombre de lignes insérées.
pub async fn drain_staging(pool: &PgPool) -> Result<u64> {
    sqlx::query("SELECT datacat_ensure_partitions_for_staging()")
        .execute(pool)
        .await?;
    let inserted: i64 = sqlx::query_scalar("SELECT datacat_merge_staging()")
        .fetch_one(pool)
        .await?;
    Ok(inserted.max(0) as u64)
}
