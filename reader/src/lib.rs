//! Datacat — moteur de lecture analytique sur le stockage froid.
//!
//! Ce crate expose un moteur de requête SQL (Apache DataFusion) qui lit les
//! fichiers Parquet exportés sur S3-compatible (MinIO / AWS S3) par le crate
//! `datacat-exporter`.
//!
//! # Layout S3
//! ```text
//! <bucket>/events/date=YYYY-MM-DD/part-0000.parquet
//! <bucket>/logs/date=YYYY-MM-DD/part-0000.parquet
//! ```
//!
//! # Exemple rapide
//! ```rust,no_run
//! use datacat_reader::{ColdConfig, ColdReader};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let cfg = ColdConfig::from_env()?;
//! let reader = ColdReader::new(cfg).await?;
//!
//! // Requête simple
//! let batches = reader.query(
//!     "events",
//!     "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC",
//! ).await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod engine;
pub mod output;
pub mod schema;

pub use config::ColdConfig;
pub use engine::ColdReader;
pub use output::OutputFormat;
