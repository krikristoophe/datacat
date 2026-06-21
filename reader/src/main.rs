//! `datacat-query-cold` — CLI de requête analytique sur le stockage froid.
//!
//! # Usage
//! ```text
//! datacat-query-cold --table events \
//!   --sql "SELECT event_name, count(*) AS n FROM events GROUP BY event_name ORDER BY n DESC"
//!
//! datacat-query-cold --table events --date 2024-06-15 \
//!   --sql "SELECT session_id, count(*) AS n FROM events GROUP BY session_id" \
//!   --format json
//! ```
//!
//! # Variables d'environnement
//! - `S3_ENDPOINT`          : URL de l'endpoint (MinIO local : `http://localhost:9200`)
//! - `S3_REGION`            : région (défaut : `eu-west-1`)
//! - `S3_BUCKET`            : bucket S3 (obligatoire)
//! - `AWS_ACCESS_KEY_ID`    : access key (obligatoire)
//! - `AWS_SECRET_ACCESS_KEY`: secret key (obligatoire)
//! - `S3_ALLOW_HTTP`        : `true` pour MinIO sans TLS
//! - `S3_PREFIX`            : préfixe dans le bucket (optionnel)

use anyhow::Context;
use clap::Parser;
use datacat_reader::{ColdConfig, ColdReader, OutputFormat};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(
    name = "datacat-query-cold",
    about = "Requête SQL analytique sur le stockage froid Parquet/S3 (DataFusion)",
    version
)]
struct Cli {
    /// Nom de la table à interroger : `events` ou `logs`.
    #[arg(long, short = 't')]
    table: String,

    /// Requête SQL DataFusion à exécuter.
    /// La table doit être référencée par son nom (même valeur que `--table`).
    #[arg(long, short = 's')]
    sql: String,

    /// Filtre optionnel sur la date (partition Hive).
    /// Format : `YYYY-MM-DD` pour un jour exact, `YYYY-MM` pour un mois (préfixe).
    /// Si absent, toutes les partitions disponibles sont scannées.
    #[arg(long, short = 'd')]
    date: Option<String>,

    /// Format de sortie.
    #[arg(long, short = 'f', value_enum, default_value_t = OutputFormat::Table)]
    format: OutputFormat,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise le logger (RUST_LOG=info par défaut)
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let cli = Cli::parse();

    let cfg = ColdConfig::from_env().context("loading config from environment")?;
    let reader = ColdReader::new(cfg).await.context("initialising ColdReader")?;

    let batches = if let Some(date) = &cli.date {
        reader
            .query_date(&cli.table, date, &cli.sql)
            .await
            .with_context(|| {
                format!(
                    "executing query on table '{}' for date '{}'",
                    cli.table, date
                )
            })?
    } else {
        reader
            .query(&cli.table, &cli.sql)
            .await
            .with_context(|| format!("executing query on table '{}'", cli.table))?
    };

    let n_rows = datacat_reader::output::total_rows(&batches);
    datacat_reader::output::print_batches(&batches, cli.format)
        .context("printing query results")?;

    tracing::info!(rows = n_rows, "query complete");
    Ok(())
}
