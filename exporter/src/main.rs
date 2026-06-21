use anyhow::Context;
use clap::{Parser, ValueEnum};
use tracing::info;

mod config;
mod export;
mod schema;

#[derive(Debug, Clone, ValueEnum)]
pub enum Table {
    Events,
    Logs,
}

#[derive(Debug, Parser)]
#[command(
    name = "datacat-export",
    about = "Export Datacat events/logs from PostgreSQL to Parquet on S3-compatible storage"
)]
pub struct Cli {
    /// Table to export
    #[arg(long, value_enum)]
    pub table: Table,

    /// Date to export (YYYY-MM-DD UTC)
    #[arg(long)]
    pub date: String,

    /// S3 bucket name (overrides S3_BUCKET env)
    #[arg(long, env = "S3_BUCKET")]
    pub bucket: String,

    /// S3 key prefix (default: table name)
    #[arg(long, env = "S3_PREFIX", default_value = "")]
    pub prefix: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "datacat_exporter=info".parse().unwrap()),
        )
        .init();

    let cli = Cli::parse();
    let cfg = config::Config::from_env().context("loading config from environment")?;

    let date = chrono::NaiveDate::parse_from_str(&cli.date, "%Y-%m-%d")
        .with_context(|| format!("invalid date: {}", cli.date))?;

    info!(
        table = ?cli.table,
        date = %date,
        bucket = %cli.bucket,
        "starting export"
    );

    let store = config::build_object_store(&cfg, &cli.bucket)?;

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect(&cfg.database_url)
        .await
        .context("connecting to PostgreSQL")?;

    let prefix = if cli.prefix.is_empty() {
        None
    } else {
        Some(cli.prefix.clone())
    };

    let rows_written = match cli.table {
        Table::Events => {
            export::export_events(&db_pool, &store, date, &cli.bucket, prefix.as_deref()).await?
        }
        Table::Logs => {
            export::export_logs(&db_pool, &store, date, &cli.bucket, prefix.as_deref()).await?
        }
    };

    info!(rows_written, "export complete");

    Ok(())
}
