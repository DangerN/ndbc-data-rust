use anyhow::{anyhow, Context, Result};
use clap::Parser;
use polars::prelude::*;
use quick_xml::Reader as XmlReader;
use quick_xml::events::Event;
use reqwest::StatusCode;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use time::{Date, Time as Tm, OffsetDateTime, UtcOffset};
use tracing::{info, warn};

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

    // Ensure data directory exists and is gitignored.
    ensure_data_dir(&args.out_dir)?;

    // Fetch fresh station metadata every run.
    fetch_station_metadata().await?;

    // Process each requested station.
    let client = reqwest::Client::builder().user_agent("ndbc-data-rust/0.1").build()?;
    let mut successes = 0usize;
    let mut failures: Vec<(String, String)> = Vec::new();

    for station in &args.stations {
        match fetch_and_save_station(&client, station, &args.out_dir).await {
            Ok(_) => {
                successes += 1;
            }
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

fn ensure_data_dir(dir: &Path) -> Result<()> {
    if !dir.exists() {
        fs::create_dir_all(dir).with_context(|| format!("creating data dir {}", dir.display()))?;
    }
    // Ensure .gitignore has /data
    let gi = Path::new(".gitignore");
    let rule = format!("/{}\n", dir.display());
    if gi.exists() {
        let txt = fs::read_to_string(gi).unwrap_or_default();
        if !txt.contains(&rule) {
            let mut new_txt = txt;
            if !new_txt.ends_with('\n') { new_txt.push('\n'); }
            new_txt.push_str(&rule);
            fs::write(gi, new_txt).context("updating .gitignore")?;
        }
    } else {
        fs::write(gi, rule).context("creating .gitignore")?;
    }
    Ok(())
}

async fn fetch_station_metadata() -> Result<()> {
    let url = "https://www.ndbc.noaa.gov/metadata/stationmetadata.xml";
    info!(%url, "downloading station metadata");
    let xml = reqwest::get(url).await?.error_for_status()?.bytes().await?;

    // Minimal parse to ensure it's the station metadata (fresh each run as required)
    let mut reader = XmlReader::from_reader(xml.as_ref());
    reader.trim_text(true);
    let mut buf = Vec::new();
    let mut ok = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                if e.name().as_ref() == b"stations" { ok = true; break; }
            }
            Ok(Event::Eof) => break,
            Err(e) => return Err(anyhow!("station metadata parse error: {}", e)),
            _ => {}
        }
    }
    if !ok { return Err(anyhow!("unexpected station metadata content")); }
    info!("station metadata retrieved");
    Ok(())
}

async fn fetch_and_save_station(client: &reqwest::Client, station: &str, out_dir: &Path) -> Result<()> {
    let url = format!("https://www.ndbc.noaa.gov/data/realtime2/{}.txt", station);
    info!(station = %station, %url, "downloading realtime data");
    let resp = client.get(&url).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
        return Err(anyhow!("data unavailable (404)"));
    }
    let text = resp.error_for_status()?.text().await?;
    if text.trim().is_empty() {
        return Err(anyhow!("empty data"));
    }

    let mut df = parse_std_met_to_df(&text).with_context(|| format!("parsing standard met data for {}", station))?;

    if df.height() == 0 {
        return Err(anyhow!("no standard met rows found"));
    }

    // Add a new column with the station id for every row
    let station_vals: Vec<String> = std::iter::repeat(station.to_string()).take(df.height()).collect();
    let station_series = Series::new("station_id".into(), station_vals);
    df = df.hstack(&[station_series])?;

    let out_path = out_dir.join(format!("{}.parquet", station));
    info!(file = %out_path.display(), rows = df.height(), cols = df.width(), "writing parquet");
    let file = std::fs::File::create(&out_path)?;
    ParquetWriter::new(file).finish(&mut df)?;
    Ok(())
}

fn parse_std_met_to_df(text: &str) -> Result<DataFrame> {
    // Identify standard met header (first group of two comment lines starting with #YY and #yr)
    let mut lines = text.lines().peekable();
    let mut header_cols: Vec<String> = Vec::new();

    while let Some(line) = lines.next() {
        let l = line.trim_start();
        if l.starts_with('#') {
            let header = l.trim_start_matches('#').trim_start();
            // We expect this to be the names header; verify it contains WDIR and WSPD at least.
            let tokens: Vec<&str> = header.split_whitespace().collect();
            if tokens.len() >= 5 && tokens[0].ends_with("YY") && tokens[1] == "MM" && tokens[2] == "DD" {
                // Consume the next units line if present
                if let Some(next) = lines.peek() {
                    if next.trim_start().starts_with('#') { let _ = lines.next(); }
                }
                header_cols = tokens.into_iter().map(|s| s.to_string()).collect();
                break;
            }
        }
    }

    if header_cols.is_empty() {
        return Ok(DataFrame::empty());
    }

    // Map column name to index after the time fields (first 5 positions are date/time)
    // Header includes time fields too; build indices accordingly
    let mut col_idx: HashMap<String, usize> = HashMap::new();
    for (i, name) in header_cols.iter().enumerate() {
        col_idx.insert(name.clone(), i);
    }

    // We'll capture a subset of known standard met columns if present
    let wanted = [
        "WDIR","WSPD","GST","WVHT","DPD","APD","MWD","PRES","ATMP","WTMP","DEWP","VIS","PTDY","TIDE"
    ];

    let mut times: Vec<i64> = Vec::new(); // as milliseconds since epoch
    let mut cols: HashMap<&'static str, Vec<Option<f64>>> = HashMap::new();
    for w in wanted.iter() { cols.insert(w, Vec::new()); }

    // Read data lines until next comment header or EOF
    for line in lines {
        let l = line.trim();
        if l.is_empty() { continue; }
        if l.starts_with('#') { break; }
        let toks: Vec<&str> = l.split_whitespace().collect();
        if toks.len() < 5 { continue; }

        // Time components may be 4-digit year in first token or two-digit.
        let year_s = toks[0];
        let year: i32 = year_s.parse().unwrap_or_else(|_| 0);
        let year = if year >= 1000 { year } else { 2000 + year };
        let month: u8 = toks.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
        let day: u8 = toks.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
        let hour: u8 = toks.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
        let minute: u8 = toks.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);
        let date = Date::from_calendar_date(year, time::Month::try_from(month).unwrap_or(time::Month::January), day).ok();
        let time = Tm::from_hms(hour, minute, 0).ok();
        if let (Some(d), Some(t)) = (date, time) {
            let odt = OffsetDateTime::new_utc(d, t).to_offset(UtcOffset::UTC);
            let ms: i64 = (odt.unix_timestamp() * 1000)
                .saturating_add((odt.millisecond() as i64));
            times.push(ms); // milliseconds
        } else {
            // Skip malformed line
            continue;
        }

        for w in wanted.iter() {
            let idx = match col_idx.get(*w) { Some(i) => *i, None => usize::MAX };
            if idx == usize::MAX { cols.get_mut(w).unwrap().push(None); continue; }
            // Get token at same position as header index
            let v = toks.get(idx).copied().unwrap_or("MM");
            let val = if v == "MM" { None } else { v.parse::<f64>().ok() };
            cols.get_mut(w).unwrap().push(val);
        }
    }

    // Build Polars columns
    let mut series: Vec<Series> = Vec::new();
    let ts = Series::new("time_ms".into(), times);
    // Cast to Datetime[ms]
    let ts = ts.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))?;
    series.push(ts);

    for w in wanted.iter() {
        let v = cols.remove(w).unwrap_or_default();
        let s = Series::new((*w).to_lowercase().into(), v);
        series.push(s);
    }

    let df = DataFrame::new(series)?;
    Ok(df)
}
