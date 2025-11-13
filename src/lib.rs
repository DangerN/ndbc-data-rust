use anyhow::{anyhow, Context, Result};
use polars::prelude::*;
use quick_xml::events::Event;
use quick_xml::Reader as XmlReader;
use reqwest::StatusCode;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use time::{Date, Time as Tm};
use tracing::info;

/// Core library for downloading, parsing, and saving NOAA NDBC standard met data.
///
/// Holds shared resources (HTTP client and output directory) and provides
/// async methods to fetch metadata and process stations.
pub struct NdbcData {
    client: reqwest::Client,
    out_dir: PathBuf,
    // Map of station id -> (latitude, longitude) for stations with met data
    station_meta: HashMap<String, (f64, f64)>,
}

impl NdbcData {
    /// Create a new instance and ensure the output directory exists and is gitignored.
    pub fn new(out_dir: impl Into<PathBuf>) -> Result<Self> {
        let out_dir = out_dir.into();
        ensure_data_dir(&out_dir)?;
        let client = reqwest::Client::builder()
            .user_agent("ndbc-data-rust/0.1")
            .build()?;
        Ok(Self { client, out_dir, station_meta: HashMap::new() })
    }

    /// Download and lightly-validate the station metadata XML.
    pub async fn fetch_station_metadata(&mut self) -> Result<()> {
        let url = "https://www.ndbc.noaa.gov/metadata/stationmetadata.xml";
        info!(%url, "downloading station metadata");
        let xml = self.client.get(url).send().await?.error_for_status()?.bytes().await?;

        // Parse and populate station -> (lat, lon) for met-enabled stations
        let mut reader = XmlReader::from_reader(xml.as_ref());
        reader.trim_text(true);
        let mut buf = Vec::new();
        let mut in_stations = false;
        let mut current_station_id: Option<String> = None;
        let mut picked_lat_lon: Option<(f64, f64)> = None;

        loop {
            match reader.read_event_into(&mut buf) {
                Ok(Event::Start(e)) => {
                    let name = e.name();
                    if name.as_ref() == b"stations" {
                        in_stations = true;
                    } else if in_stations && name.as_ref() == b"station" {
                        // Start new station
                        current_station_id = e
                            .attributes()
                            .filter_map(|a| a.ok())
                            .find(|a| a.key.as_ref() == b"id")
                            .and_then(|a| String::from_utf8(a.value.to_vec()).ok());
                        picked_lat_lon = None;
                    } else if in_stations && name.as_ref() == b"history" {
                        // Consider this history as a candidate for current position if met="y"
                        let mut met = None::<String>;
                        let mut stop = None::<String>;
                        let mut lat = None::<f64>;
                        let mut lng = None::<f64>;
                        for attr in e.attributes().filter_map(|a| a.ok()) {
                            let key = attr.key;
                            let val = String::from_utf8_lossy(&attr.value).to_string();
                            match key.as_ref() {
                                b"met" => met = Some(val),
                                b"stop" => stop = Some(val),
                                b"lat" => lat = val.parse().ok(),
                                b"lng" => lng = val.parse().ok(),
                                _ => {}
                            }
                        }
                        let met_yes = met.as_deref() == Some("y");
                        let is_current = stop.as_deref().map(|s| s.is_empty()).unwrap_or(true);
                        if met_yes {
                            if let (Some(la), Some(lo)) = (lat, lng) {
                                // Prefer the current entry (stop empty). If not set yet, set. If we already set and current is false, keep existing.
                                if picked_lat_lon.is_none() || is_current {
                                    picked_lat_lon = Some((la, lo));
                                }
                            }
                        }
                    }
                }
                Ok(Event::End(e)) => {
                    if e.name().as_ref() == b"station" {
                        if let (Some(id), Some(ll)) = (current_station_id.take(), picked_lat_lon.take()) {
                            self.station_meta.insert(id, ll);
                        }
                    } else if e.name().as_ref() == b"stations" {
                        // finished
                        break;
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => return Err(anyhow!("station metadata parse error: {}", e)),
                _ => {}
            }
        }

        if self.station_meta.is_empty() {
            return Err(anyhow!("no stations with met data found in metadata"));
        }
        info!(count = self.station_meta.len(), "station metadata retrieved");
        Ok(())
    }

    /// Fetch realtime data for a station, parse, and save as Parquet into the configured output directory.
    pub async fn fetch_and_save_station(&self, station: &str) -> Result<()> {
        let url = format!("https://www.ndbc.noaa.gov/data/realtime2/{}.txt", station);
        info!(station = %station, %url, "downloading realtime data");
        let resp = self.client.get(&url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Err(anyhow!("data unavailable (404)"));
        }
        let text = resp.error_for_status()?.text().await?;
        if text.trim().is_empty() {
            return Err(anyhow!("empty data"));
        }

        let mut df = parse_std_met_to_df(&text)
            .with_context(|| format!("parsing standard met data for {}", station))?;

        if df.height() == 0 {
            return Err(anyhow!("no standard met rows found"));
        }

        // Add a new column with the station id for every row
        let station_vals: Vec<String> = std::iter::repeat(station.to_string()).take(df.height()).collect();
        let station_series = Series::new("station_id".into(), station_vals);
        // Latitude/Longitude from metadata, if available
        let (lat_opt, lon_opt) = self
            .station_meta
            .get(station)
            .cloned()
            .map(|(la, lo)| (Some(la), Some(lo)))
            .unwrap_or((None, None));
        let lat_series: Series = Series::new(
            "latitude".into(),
            std::iter::repeat(lat_opt).take(df.height()).collect::<Vec<Option<f64>>>(),
        );
        let lon_series: Series = Series::new(
            "longitude".into(),
            std::iter::repeat(lon_opt).take(df.height()).collect::<Vec<Option<f64>>>(),
        );
        df = df.hstack(&[station_series, lat_series, lon_series])?;

        let out_path = self.out_dir.join(format!("{}.parquet", station));
        info!(file = %out_path.display(), rows = df.height(), cols = df.width(), "writing parquet");
        let file = std::fs::File::create(&out_path)?;
        ParquetWriter::new(file).finish(&mut df)?;
        Ok(())
    }
}

impl NdbcData {
    /// Return all station IDs that have met data in the loaded metadata.
    pub fn all_station_ids(&self) -> Vec<String> {
        let mut v: Vec<String> = self.station_meta.keys().cloned().collect();
        v.sort();
        v
    }
}

fn ensure_data_dir(dir: &Path) -> Result<()> {
    if !dir.exists() {
        fs::create_dir_all(dir).with_context(|| format!("creating data dir {}", dir.display()))?;
    }
    // Ensure .gitignore has /data (or the provided dir name)
    let gi = Path::new(".gitignore");
    let rule = format!("/{}\n", dir.display());
    if gi.exists() {
        let txt = fs::read_to_string(gi).unwrap_or_default();
        if !txt.contains(&rule) {
            let mut new_txt = txt;
            if !new_txt.ends_with('\n') {
                new_txt.push('\n');
            }
            new_txt.push_str(&rule);
            fs::write(gi, new_txt).context("updating .gitignore")?;
        }
    } else {
        fs::write(gi, rule).context("creating .gitignore")?;
    }
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
                    if next.trim_start().starts_with('#') {
                        let _ = lines.next();
                    }
                }
                header_cols = tokens.into_iter().map(|s| s.to_string()).collect();
                break;
            }
        }
    }

    if header_cols.is_empty() {
        return Ok(DataFrame::empty());
    }

    // Map column name to index after the time fields (first positions include date/time)
    let mut col_idx: HashMap<String, usize> = HashMap::new();
    for (i, name) in header_cols.iter().enumerate() {
        col_idx.insert(name.clone(), i);
    }

    // We'll capture a subset of known standard met columns if present
    let wanted = [
        "WDIR", "WSPD", "GST", "WVHT", "DPD", "APD", "MWD", "PRES", "ATMP", "WTMP", "DEWP", "VIS", "PTDY", "TIDE",
    ];

    let mut times: Vec<i64> = Vec::new(); // as milliseconds since epoch
    let mut cols: HashMap<&'static str, Vec<Option<f64>>> = HashMap::new();
    for w in wanted.iter() {
        cols.insert(w, Vec::new());
    }

    // Read data lines until next comment header or EOF
    for line in lines {
        let l = line.trim();
        if l.is_empty() {
            continue;
        }
        if l.starts_with('#') {
            break;
        }
        let toks: Vec<&str> = l.split_whitespace().collect();
        if toks.len() < 5 {
            continue;
        }

        // Time components may be 4-digit year in first token or two-digit.
        let year_s = toks[0];
        let year: i32 = year_s.parse().unwrap_or_else(|_| 0);
        let year = if year >= 1000 { year } else { 2000 + year };
        let month: u8 = toks.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
        let day: u8 = toks.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
        let hour: u8 = toks.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
        let minute: u8 = toks.get(4).and_then(|s| s.parse().ok()).unwrap_or(0);

        let date = Date::from_calendar_date(year, month.try_into().unwrap_or(time::Month::January), day.into())
            .unwrap_or_else(|_| Date::from_calendar_date(2000, time::Month::January, 1).unwrap());
        let time = Tm::from_hms(hour, minute, 0).unwrap_or_else(|_| Tm::from_hms(0, 0, 0).unwrap());
        let dt = date.with_time(time).assume_utc();
        // Convert to milliseconds since epoch as i64
        let ts_ms: i64 = dt.unix_timestamp() * 1000 + (dt.millisecond() as i64);
        times.push(ts_ms);

        for &w in wanted.iter() {
            let idx_opt = col_idx.get(w).cloned();
            if let Some(idx) = idx_opt {
                let val = toks.get(idx).and_then(|s| match *s {
                    "MM" | "NaN" => None,
                    other => other.parse::<f64>().ok(),
                });
                cols.get_mut(w).unwrap().push(val);
            } else {
                cols.get_mut(w).unwrap().push(None);
            }
        }
    }

    // Build DataFrame
    let mut series: Vec<Series> = Vec::new();
    let time_series = Series::new("time_ms".into(), times);
    series.push(time_series);
    for &w in wanted.iter() {
        let vals = cols.remove(w).unwrap();
        let s = Series::new(w.into(), vals);
        series.push(s);
    }

    let df = DataFrame::new(series)?;
    Ok(df)
}
