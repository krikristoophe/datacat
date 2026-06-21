//! Ingestion asynchrone générique : micro-batch en mémoire → `COPY` → merge idempotent.
//!
//! Le mécanisme est partagé par tous les domaines (events produit, logs techniques) via le
//! trait [`Ingestable`]. L'API HTTP enfile les enregistrements validés (acquittement immédiat) ;
//! une tâche de fond unique par domaine accumule un micro-batch et l'écrit via `COPY` dans une
//! table de staging `UNLOGGED`, puis fusionne de façon idempotente vers la table cible. Un seul
//! writer par domaine ⇒ pas de contention sur le staging ; `COPY` sature l'écriture.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;
use sqlx::PgPool;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

/// Un enregistrement persistable par micro-batch + COPY (events, logs, …).
pub trait Ingestable: Send + Sync + 'static {
    /// `COPY <staging> (...) FROM STDIN WITH (FORMAT csv)`.
    fn copy_statement() -> &'static str;
    /// Requête garantissant l'existence des partitions des lignes en staging.
    fn ensure_partitions_statement() -> &'static str;
    /// Requête de merge idempotent (retourne un `bigint` = lignes réellement insérées).
    fn merge_statement() -> &'static str;
    /// Nom de la table de staging (reset défensif en cas d'échec de flush).
    fn staging_table() -> &'static str;
    /// Libellé court pour la journalisation.
    fn label() -> &'static str;
    /// Sérialise cet enregistrement en une ligne CSV (FORMAT csv PostgreSQL).
    fn write_csv_row(&self, out: &mut String);
}

/// Échappe une valeur texte au format CSV PostgreSQL (FORMAT csv : seul `"` est spécial, doublé).
pub fn push_csv_quoted(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        if c == '"' {
            out.push('"');
        }
        out.push(c);
    }
    out.push('"');
}

/// Pousse un champ texte optionnel : `None` ⇒ champ vide non quoté ⇒ NULL côté COPY.
pub fn push_csv_opt(out: &mut String, value: Option<&str>) {
    if let Some(v) = value {
        push_csv_quoted(out, v);
    }
}

/// Pousse un horodatage optionnel en RFC3339 (champ vide ⇒ NULL).
pub fn push_csv_ts(out: &mut String, ts: Option<chrono::DateTime<chrono::Utc>>) {
    if let Some(t) = ts {
        out.push_str(&t.to_rfc3339());
    }
}

/// Pousse un entier optionnel (champ vide ⇒ NULL).
pub fn push_csv_num(out: &mut String, n: Option<i64>) {
    if let Some(v) = n {
        out.push_str(&v.to_string());
    }
}

/// Pousse un flottant optionnel (champ vide ⇒ NULL).
pub fn push_csv_f64(out: &mut String, n: Option<f64>) {
    if let Some(v) = n {
        out.push_str(&v.to_string());
    }
}

/// Compteurs d'observabilité d'un domaine (exposés via `/stats`, journalisés à chaque flush).
#[derive(Default)]
pub struct IngestMetrics {
    /// Enregistrements acceptés pour écriture (enfilés). Pas le nombre d'insertions (cf. dédup).
    pub received_total: AtomicU64,
    /// Enregistrements réellement insérés après déduplication.
    pub inserted_total: AtomicU64,
    /// Enregistrements écartés car hors fenêtre de skew (perte tolérée).
    pub dropped_skew_total: AtomicU64,
    /// Enregistrements perdus car la file était saturée (back-pressure, perte tolérée).
    pub dropped_channel_full_total: AtomicU64,
    /// Flushes en échec (batch perdu).
    pub flush_failures_total: AtomicU64,
    /// Nombre de flushes réussis.
    pub flushes_total: AtomicU64,
}

impl IngestMetrics {
    pub fn snapshot(&self) -> serde_json::Value {
        let received = self.received_total.load(Ordering::Relaxed);
        let inserted = self.inserted_total.load(Ordering::Relaxed);
        json!({
            "received_total": received,
            "inserted_total": inserted,
            "deduplicated_total": received.saturating_sub(inserted),
            "dropped_skew_total": self.dropped_skew_total.load(Ordering::Relaxed),
            "dropped_channel_full_total": self.dropped_channel_full_total.load(Ordering::Relaxed),
            "flush_failures_total": self.flush_failures_total.load(Ordering::Relaxed),
            "flushes_total": self.flushes_total.load(Ordering::Relaxed),
        })
    }
}

/// Point d'entrée d'enfilage (utilisé par les handlers HTTP). Générique sur le type d'enregistrement.
pub struct Ingestor<T: Ingestable> {
    tx: mpsc::Sender<Vec<T>>,
    pub metrics: Arc<IngestMetrics>,
}

impl<T: Ingestable> Clone for Ingestor<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }
}

impl<T: Ingestable> Ingestor<T> {
    /// Enfile un batch sans bloquer. Retourne le nombre d'enregistrements réellement enfilés
    /// (0 si la file est saturée → perte tolérée).
    pub fn try_enqueue(&self, batch: Vec<T>) -> usize {
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

/// Poignée d'arrêt propre d'un batcher.
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

/// Démarre la tâche de batching d'un domaine et retourne l'`Ingestor` + la poignée d'arrêt.
pub fn spawn<T: Ingestable>(
    pool: PgPool,
    flush_interval: Duration,
    flush_batch_size: usize,
    channel_capacity: usize,
    metrics: Arc<IngestMetrics>,
) -> (Ingestor<T>, BatcherHandle) {
    let (tx, rx) = mpsc::channel(channel_capacity);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let ingestor = Ingestor {
        tx,
        metrics: Arc::clone(&metrics),
    };
    let join = tokio::spawn(batcher_loop::<T>(
        pool,
        rx,
        shutdown_rx,
        metrics,
        flush_interval,
        flush_batch_size,
    ));

    (ingestor, BatcherHandle { shutdown_tx, join })
}

async fn batcher_loop<T: Ingestable>(
    pool: PgPool,
    mut rx: mpsc::Receiver<Vec<T>>,
    mut shutdown_rx: watch::Receiver<bool>,
    metrics: Arc<IngestMetrics>,
    flush_interval: Duration,
    flush_batch_size: usize,
) {
    let mut buf: Vec<T> = Vec::with_capacity(flush_batch_size);
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
    tracing::info!(domain = T::label(), "batcher arrêté proprement");
}

async fn flush<T: Ingestable>(pool: &PgPool, buf: &mut Vec<T>, metrics: &IngestMetrics) {
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
            tracing::debug!(domain = T::label(), records = n, inserted, "flush écrit");
        }
        Err(e) => {
            metrics.flush_failures_total.fetch_add(1, Ordering::Relaxed);
            tracing::error!(domain = T::label(), error = %e, records = n, "échec du flush — batch perdu (tolérance §2)");
            // Reset défensif du staging pour éviter un poison-loop.
            let _ = sqlx::query(&format!("TRUNCATE {}", T::staging_table()))
                .execute(pool)
                .await;
        }
    }
    buf.clear();
}

/// COPY du micro-batch dans le staging, garantie des partitions, merge idempotent.
/// Retourne le nombre de lignes réellement insérées (après déduplication).
async fn flush_inner<T: Ingestable>(pool: &PgPool, buf: &[T]) -> Result<u64> {
    let mut conn = pool.acquire().await?;

    let mut csv = String::with_capacity(buf.len() * 160);
    for r in buf {
        r.write_csv_row(&mut csv);
    }

    {
        let mut copy = conn.copy_in_raw(T::copy_statement()).await?;
        copy.send(csv.as_bytes()).await?;
        copy.finish().await?;
    }

    sqlx::query(T::ensure_partitions_statement())
        .execute(&mut *conn)
        .await?;
    let inserted: i64 = sqlx::query_scalar(T::merge_statement())
        .fetch_one(&mut *conn)
        .await?;

    Ok(inserted.max(0) as u64)
}
