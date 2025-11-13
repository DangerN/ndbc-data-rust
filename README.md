ndbc-data (Rust)

### Usage

```
cargo build
cargo run -- 42040 46042
./target/debug/ndbc-data 42040 46042
./target/debug/ndbc-data --out-dir my_data 42040
```

Outputs are written as Parquet files named after each station identifier, for example: `data/42040.parquet`, `data/46042.parquet`.

### Overview

`ndbc-data` is a small command-line tool that downloads, parses, and saves NOAA NDBC realtime Standard Meteorological observations (generally the last ~45 days) to Parquet format using the Polars dataframe library.

Key behavior and guarantees:
- Fresh metadata every run: retrieves the station metadata XML on each invocation to ensure freshness.
- Station selection: pass one or more station IDs as positional arguments (e.g., `42040`, `46042`, `FPKA2`).
- Graceful handling: if a station’s realtime standard meteorological data are unavailable (404, empty file, or header not found), the tool prints a warning and continues with the next station.
- Output management: saves Parquet files under `./data` by default and automatically ensures that directory is listed in `.gitignore`.

### Design rationale

- Single-file simplicity: implementation is kept in `src/main.rs` to minimize project overhead and make the core logic easy to review.
- Robust text parsing: NDBC realtime files are space-delimited and can vary slightly in spacing. The parser auto-detects the standard meteorological header (the `#YY MM DD hh mm ...` line) and uses token positions derived from that header, making it resilient to alignment changes.
- Clear null handling: missing values denoted by `MM` are mapped to nulls in the dataframe.
- Portable networking: `reqwest` is configured with `rustls-tls`, avoiding OpenSSL requirements for easier setup on most systems.
- Column subset: focuses on the commonly present Standard Meteorological fields to keep the output concise while covering typical analysis needs.

### What gets parsed and saved

- Time column: `time_ms` as Polars `Datetime[ms]` (UTC).
- Standard Meteorological columns (lowercased in output when present):
  - `wdir, wspd, gst, wvht, dpd, apd, mwd, pres, atmp, wtmp, dewp, vis, ptdy, tide`

### File locations and naming

- Default output directory: `./data/`
- File naming: `<station_id>.parquet` (e.g., `42040.parquet`)
- VCS hygiene: the tool ensures the chosen output directory (default `data/`) is listed in `.gitignore`.

### Examples

```
cargo run -- 42040 46042
cargo run -- --out-dir my_data 42040
./target/debug/ndbc-data 41009 42040
```

### Warnings you might see

- `data unavailable (404)`: The realtime file for the station could not be found.
- `empty data`: The file exists but contains no rows.
- `no standard met rows found`: The parser could not locate the standard meteorological header/data in the file.

### Dependencies and notable crates

- `clap` for CLI argument parsing
- `reqwest` (with `rustls-tls`) for HTTP
- `quick-xml` to validate that fresh station metadata was retrieved
- `polars` to build dataframes and write Parquet files
- `time` for UTC datetime handling
- `tracing` for structured logs

### Project layout

- `src/main.rs` — entire implementation (CLI, downloads, parsing, Parquet write)
- `data/` — default output location for Parquet files (auto-ignored by git)
- `data-samples/` — sample inputs and references

### Notes and limitations

- Scope is limited to realtime Standard Meteorological data (roughly last 45 days) from NDBC.
- Only a common subset of columns is parsed. Additional groups (waves spectra, currents, etc.) are out of scope.
- The parser uses whitespace tokenization keyed off the header; it is not strict fixed-width parsing. If strict fixed-width is desired, the implementation can be adapted (the `fixed_width` crate is available in `Cargo.toml`).
- Station metadata is fetched and minimally validated (root tag) but is not currently joined into the dataframe.

### Building

```
cargo build --release
./target/release/ndbc-data 42040
```

### Troubleshooting

- SSL/TLS issues: the binary uses Rustls; if you encounter TLS errors behind corporate proxies, try setting standard proxy environment variables (e.g., `HTTPS_PROXY`).
- Slow downloads or timeouts: temporary network issues to NDBC can occur; try again.
- Missing columns: some stations do not report all fields; those columns will contain nulls.

### License

This project is provided as-is for demonstration purposes. See repository terms if added later.
