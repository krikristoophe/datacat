//! Formatage et affichage des résultats de requête DataFusion.

use anyhow::Context;
use arrow::json::writer::JsonArray;
use arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::record_batch::RecordBatch;

/// Format de sortie des résultats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum OutputFormat {
    /// Tableau ASCII lisible (défaut).
    #[default]
    Table,
    /// Tableau JSON (une ligne, tableau d'objets).
    Json,
}

/// Affiche les `RecordBatch` sur stdout selon le format choisi.
pub fn print_batches(batches: &[RecordBatch], format: OutputFormat) -> anyhow::Result<()> {
    match format {
        OutputFormat::Table => {
            let display = pretty_format_batches(batches).context("formatting batches as table")?;
            println!("{display}");
        }
        OutputFormat::Json => {
            let mut buf = Vec::new();
            {
                let mut writer = arrow::json::writer::Writer::<_, JsonArray>::new(&mut buf);
                for batch in batches {
                    writer.write(batch).context("writing batch to JSON")?;
                }
                writer.finish().context("finishing JSON writer")?;
            }
            let s = String::from_utf8(buf).context("JSON output is not valid UTF-8")?;
            println!("{s}");
        }
    }
    Ok(())
}

/// Compte le nombre total de lignes dans des `RecordBatch`.
pub fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
