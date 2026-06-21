//! Ingestion asynchrone : micro-batch en mémoire → `COPY` → merge idempotent.
//!
//! L'API HTTP se contente d'enfiler les events validés (acquittement immédiat). Une tâche de
//! fond unique accumule un micro-batch et l'écrit via `COPY` dans la table de staging UNLOGGED,
//! puis fusionne de façon idempotente vers `events` (cf. migrations/0002_functions.sql).
//! Un seul writer ⇒ pas de contention sur le staging ; `COPY` sature l'écriture sans le coût
//! des INSERT ligne à ligne.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use crate::config::Config;
use crate::model::StoredEvent;

/// Compteurs d'observabilité (exposés via `/stats`, journalisés à chaque flush).
#[derive(Default)]
pub struct IngestMetrics {
    /// Events acceptés pour écriture (enfilés). N'est pas le nombre d'insertions (cf. dédup).
    pub received_total: AtomicU64,
    /// Events réellement insérés après déduplication.
    pub inserted_total: AtomicU64,
    /// Events écartés car hors fenêtre de skew (perte tolérée).
    pub dropped_skew_total: AtomicU64,
    /// Events perdus car la file était saturée (back-pressure, perte tolérée).
    pub dropped_channel_full_total: AtomicU64,
    /// Flushes en échec (batch perdu).
    pub flush_failures_total: AtomicU64,
    /// Nombre de flushes réussis.
    pub flushes_total: AtomicU64,
}

impl IngestMetrics {
    pub fn snapshot(&self) -> serde_json::Value {
        json!({
            "received_total": self.received_total.load(Ordering::Relaxed),
            "inserted_total": self.inserted_total.load(Ordering::Relaxed),
            "deduplicated_total":
                self.received_total.load(Ordering::Relaxed)
                    .saturating_sub(self.inserted_total.load(Ordering::Relaxed)),
            "dropped_skew_total": self.dropped_skew_total.load(Ordering::Relaxed),
            "dropped_channel_full_total": self.dropped_channel_full_total.load(Ordering::Relaxed),
            "flush_failures_total": self.flush_failures_total.load(Ordering::Relaxed),
            "flushes_total": self.flushes_total.load(Ordering::Relaxed),
        })
    }
}

/// Point d'entrée d'enfilage utilisé par le handler HTTP.
#[derive(Clone)]
pub struct Ingestor {
    tx: mpsc::Sender<Vec<StoredEvent>>,
    pub metrics: Arc<IngestMetrics>,
}

impl Ingestor {
    /// Enfile un batch sans bloquer. Retourne le nombre d'events réellement enfilés
    /// (0 si la file est saturée → perte tolérée).
    pub fn try_enqueue(&self, batch: Vec<StoredEvent>) -> usize {
        let n = batch.len();
        match self.tx.try_send(batch) {
            Ok(()) => {
                self.metrics
                    .received_total
                    .fetch_add(n as u64, Ordering::Relaxed);
                n
            }
            Err(_) => {
                self.metrics
                    .dropped_channel_full_total
                    .fetch_add(n as u64, Ordering::Relaxed);
                0
            }
        }
    }
}

/// Poignée d'arrêt propre du batcher.
pub struct BatcherHandle {
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl BatcherHandle {
    /// Signale l'arrêt, draine la file et effectue un dernier flush.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

/// Démarre la tâche de batching et retourne l'`Ingestor` + la poignée d'arrêt.
pub fn spawn(pool: PgPool, cfg: &Config, metrics: Arc<IngestMetrics>) -> (Ingestor, BatcherHandle) {
    let (tx, rx) = mpsc::channel(cfg.channel_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let ingestor = Ingestor {
        tx,
        metrics: Arc::clone(&metrics),
    };
    let join = tokio::spawn(batcher_loop(
        pool,
        rx,
        shutdown_rx,
        metrics,
        cfg.flush_interval,
        cfg.flush_batch_size,
    ));

    (ingestor, BatcherHandle { shutdown_tx, join })
}

async fn batcher_loop(
    pool: PgPool,
    mut rx: mpsc::Receiver<Vec<StoredEvent>>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<IngestMetrics>,
    flush_interval: std::time::Duration,
    flush_batch_size: usize,
) {
    let mut buf: Vec<StoredEvent> = Vec::with_capacity(flush_batch_size);
    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => {
                while let Ok(batch) = rx.try_recv() {
                    buf.extend(batch);
                    if buf.len() >= flush_batch_size {
                        flush(&pool, &mut buf, &metrics).await;
                    }
                }
                flush(&pool, &mut buf, &metrics).await;
                break;
            }
            maybe = rx.recv() => {
                match maybe {
                    Some(batch) => {
                        buf.extend(batch);
                        if buf.len() >= flush_batch_size {
                            flush(&pool, &mut buf, &metrics).await;
                        }
                    }
                    None => {
                        flush(&pool, &mut buf, &metrics).await;
                        break;
                    }
                }
            }
            _ = ticker.tick() => {
                if !buf.is_empty() {
                    flush(&pool, &mut buf, &metrics).await;
                }
            }
        }
    }
    tracing::info!("batcher arrêté proprement");
}

async fn flush(pool: &PgPool, buf: &mut Vec<StoredEvent>, metrics: &IngestMetrics) {
    if buf.is_empty() {
        return;
    }
    let n = buf.len();
    match flush_inner(pool, buf).await {
        Ok(inserted) => {
            metrics
                .inserted_total
                .fetch_add(inserted, Ordering::Relaxed);
            metrics.flushes_total.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(events = n, inserted, "flush écrit");
        }
        Err(e) => {
            metrics.flush_failures_total.fetch_add(1, Ordering::Relaxed);
            tracing::error!(error = %e, events = n, "échec du flush — batch perdu (tolérance §2)");
            // Reset défensif du staging pour éviter un poison-loop.
            let _ = sqlx::query("TRUNCATE events_staging").execute(pool).await;
        }
    }
    buf.clear();
}

/// COPY du micro-batch dans le staging, garantie des partitions, merge idempotent.
/// Retourne le nombre de lignes réellement insérées (après déduplication).
async fn flush_inner(pool: &PgPool, buf: &[StoredEvent]) -> Result<u64> {
    let mut conn = pool.acquire().await?;

    let mut csv = String::with_capacity(buf.len() * 160);
    for e in buf {
        write_csv_row(&mut csv, e)?;
    }

    {
        let mut copy = conn
            .copy_in_raw(
                "COPY events_staging \
                 (event_id, event_name, tenant_id, actor_id, session_id, \
                  timestamp_client, received_at, properties) \
                 FROM STDIN WITH (FORMAT csv)",
            )
            .await?;
        copy.send(csv.as_bytes()).await?;
        copy.finish().await?;
    }

    sqlx::query("SELECT datacat_ensure_partitions_for_staging()")
        .execute(&mut *conn)
        .await?;
    let inserted: i64 = sqlx::query_scalar("SELECT datacat_merge_staging()")
        .fetch_one(&mut *conn)
        .await?;

    Ok(inserted.max(0) as u64)
}

/// Écrit une ligne au format CSV PostgreSQL (FORMAT csv : seul `"` est spécial, doublé).
fn write_csv_row(out: &mut String, e: &StoredEvent) -> Result<()> {
    out.push_str(&e.event_id.to_string());
    out.push(',');
    push_csv_quoted(out, &e.event_name);
    out.push(',');
    if let Some(t) = &e.tenant_id {
        push_csv_quoted(out, t); // None ⇒ champ vide non quoté ⇒ NULL
    }
    out.push(',');
    push_csv_quoted(out, &e.actor_id);
    out.push(',');
    push_csv_quoted(out, &e.session_id);
    out.push(',');
    out.push_str(&e.timestamp_client.to_rfc3339());
    out.push(',');
    out.push_str(&e.received_at.to_rfc3339());
    out.push(',');
    let props = serde_json::to_string(&e.properties)?;
    push_csv_quoted(out, &props);
    out.push('\n');
    Ok(())
}

fn push_csv_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn ev(name: &str, props: serde_json::Value, tenant: Option<&str>) -> StoredEvent {
        let ts = Utc.with_ymd_and_hms(2026, 6, 21, 10, 0, 0).unwrap();
        StoredEvent {
            event_id: Uuid::nil(),
            event_name: name.to_string(),
            tenant_id: tenant.map(|s| s.to_string()),
            actor_id: "actor-1".to_string(),
            session_id: "sess-1".to_string(),
            timestamp_client: ts,
            received_at: ts,
            properties: props,
        }
    }

    #[test]
    fn csv_escapes_quotes_and_commas() {
        let mut out = String::new();
        write_csv_row(
            &mut out,
            &ev("na\"me,with", json!({"a": "x,y\"z"}), Some("t1")),
        )
        .unwrap();
        // Le nom contenant guillemet+virgule est quoté et le `"` est doublé.
        assert!(out.contains("\"na\"\"me,with\""), "got: {out}");
        assert!(out.ends_with('\n'));

        // Round-trip : ré-appliquer les règles CSV PostgreSQL (FORMAT csv) doit redonner
        // exactement le JSON sérialisé d'origine.
        let json_str = serde_json::to_string(&json!({"a": "x,y\"z"})).unwrap();
        let mut quoted = String::new();
        push_csv_quoted(&mut quoted, &json_str);
        // Désquote : retire les guillemets externes, dédouble les `"` internes.
        let inner = &quoted[1..quoted.len() - 1];
        let unquoted = inner.replace("\"\"", "\"");
        assert_eq!(unquoted, json_str);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&unquoted).unwrap(),
            json!({"a": "x,y\"z"})
        );
    }

    #[test]
    fn csv_null_tenant_is_empty_field() {
        let mut out = String::new();
        write_csv_row(&mut out, &ev("click", json!({}), None)).unwrap();
        // event_id,"event_name",,"actor"... → tenant vide entre deux virgules.
        let fields: Vec<&str> = out.trim_end().splitn(4, ',').collect();
        assert_eq!(fields[2], ""); // tenant_id NULL
    }
}
