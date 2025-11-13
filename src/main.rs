use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tracing::{info, warn};
use ndbc_data::NdbcData;

/// Simple CLI to download, parse, and save NOAA NDBC standard met data (last ~45 days) to Parquet.
#[derive(Parser, Debug)]
#[command(name = "ndbc-data", version, about = "Fetch NDBC realtime standard meteorological data and save as Parquet")] 
struct Args {
    /// Station identifiers to retrieve (e.g., 42040, 46042, FPKA2)
    #[arg(required = true)]
    stations: Vec<String>,

    /// Output directory for Parquet files (default: ./data)
    #[arg(short, long, default_value = "data")]
    out_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();
    let args = Args::parse();

    // Initialize core library with output directory
    let core = NdbcData::new(args.out_dir)?;

    // Fetch fresh station metadata every run.
    core.fetch_station_metadata().await?;

    // Process each requested station.
    let mut successes = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();

    for station in &args.stations {
        match core.fetch_and_save_station(station).await {
            Ok(_) => successes += 1,
            Err(e) => {
                warn!(station = %station, error = %e, "failed to process station");
                failures.push((station.clone(), format!("{}", e)));
            }
        }
    }

    info!(%successes, failures = failures.len(), "done");
    if !failures.is_empty() {
        eprintln!("Warnings:");
        for (st, err) in failures {
            eprintln!("- {}: {}", st, err);
        }
    }
    Ok(())
}

fn setup_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(false)
        .try_init();
}
