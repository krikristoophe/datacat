//! Création anticipée et purge (par `DROP PARTITION`) des partitions journalières,
//! et drain du staging au démarrage. SQL dynamique côté base (cf. migrations/0002_functions.sql).

use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::PgPool;

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

// ── Variantes pour le domaine « logs » (cf. migrations/0003_logs.sql) ──────────

/// Garantit les partitions journalières de `logs` pour `[today - past_days, today + future_days]`.
pub async fn ensure_log_partition_window(
    pool: &PgPool,
    past_days: i64,
    future_days: i64,
) -> Result<()> {
    let today = Utc::now().date_naive();
    for offset in -past_days..=future_days {
        let day = today + chrono::Duration::days(offset);
        sqlx::query("SELECT datacat_ensure_log_partition($1)")
            .bind(day)
            .execute(pool)
            .await
            .with_context(|| format!("création de la partition de logs du {day}"))?;
    }
    Ok(())
}

/// Purge de rétention des partitions de `logs` (DROP PARTITION).
pub async fn purge_old_log_partitions(pool: &PgPool, retention_days: i64) -> Result<i64> {
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(retention_days);
    let dropped: i32 = sqlx::query_scalar("SELECT datacat_drop_log_partitions_before($1)")
        .bind(cutoff)
        .fetch_one(pool)
        .await
        .context("purge des partitions de logs")?;
    Ok(dropped as i64)
}

/// Drain du staging de logs au démarrage.
pub async fn drain_log_staging(pool: &PgPool) -> Result<u64> {
    sqlx::query("SELECT datacat_ensure_log_partitions_for_staging()")
        .execute(pool)
        .await?;
    let inserted: i64 = sqlx::query_scalar("SELECT datacat_merge_log_staging()")
        .fetch_one(pool)
        .await?;
    Ok(inserted.max(0) as u64)
}

// ── Variantes pour le domaine « traces » (cf. migrations/0004_traces.sql) ──────

pub async fn ensure_span_partition_window(
    pool: &PgPool,
    past_days: i64,
    future_days: i64,
) -> Result<()> {
    let today = Utc::now().date_naive();
    for offset in -past_days..=future_days {
        let day = today + chrono::Duration::days(offset);
        sqlx::query("SELECT datacat_ensure_span_partition($1)")
            .bind(day)
            .execute(pool)
            .await
            .with_context(|| format!("création de la partition de spans du {day}"))?;
    }
    Ok(())
}

pub async fn purge_old_span_partitions(pool: &PgPool, retention_days: i64) -> Result<i64> {
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(retention_days);
    let dropped: i32 = sqlx::query_scalar("SELECT datacat_drop_span_partitions_before($1)")
        .bind(cutoff)
        .fetch_one(pool)
        .await
        .context("purge des partitions de spans")?;
    Ok(dropped as i64)
}

pub async fn drain_span_staging(pool: &PgPool) -> Result<u64> {
    sqlx::query("SELECT datacat_ensure_span_partitions_for_staging()")
        .execute(pool)
        .await?;
    let inserted: i64 = sqlx::query_scalar("SELECT datacat_merge_span_staging()")
        .fetch_one(pool)
        .await?;
    Ok(inserted.max(0) as u64)
}
