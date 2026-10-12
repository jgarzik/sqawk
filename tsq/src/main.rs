//! tsq -- the tool that beats up sqawk.
//!
//! * `tsq gen`: deterministic multi-table test data (foreign keys, realistic
//!   distributions, optional edge-case cells, every file layout sqawk
//!   reads).
//!
//! Everything random comes from one ChaCha8 stream per seed (`rng.rs`), so a
//! seed reproduces the same data on every OS.

#![forbid(unsafe_code)]

mod data;
mod gen;
mod rng;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "tsq",
    version,
    about = "Test data generator for sqawk",
    long_about = "Test data generator for sqawk.\n\n\
                  gen writes deterministic multi-table CSV/TSV data. The same seed gives \
                  the same data on every OS."
)]
struct Cli {
    /// Random seed (full 64 bits; printed when chosen from the clock)
    #[arg(long, short = 's', global = true)]
    seed: Option<u64>,

    /// Progress messages
    #[arg(long, short = 'v', global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FileFormat {
    Csv,
    Tsv,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Write customers, products, orders, order_items and reviews tables
    Gen {
        /// Output directory (data/ and metadata.json go here)
        #[arg(long, short = 'o')]
        output_dir: PathBuf,
        /// Base row count: customers; the other tables scale from it
        #[arg(long, short = 'r', default_value_t = 1000)]
        rows: usize,
        /// Probability that a non-key cell is replaced by an edge value
        #[arg(long, default_value_t = 0.0)]
        edge_rate: f64,
        /// Field delimiter: csv (comma) or tsv (tab)
        #[arg(long, value_enum, default_value_t = FileFormat::Csv)]
        format: FileFormat,
        /// CRLF line endings
        #[arg(long)]
        crlf: bool,
        /// Start each file with a UTF-8 byte order mark
        #[arg(long)]
        bom: bool,
        /// Write no header line (read with sqawk --no-header)
        #[arg(long)]
        no_header: bool,
        /// Probability that a row leaves off its trailing empty fields
        #[arg(long, default_value_t = 0.0)]
        ragged_rate: f64,
    },
}

fn clock_seed() -> u64 {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    t ^ u64::from(std::process::id()).rotate_left(32)
}

fn real_main() -> Result<i32> {
    let cli = Cli::parse();
    let seed = match cli.seed {
        Some(s) => s,
        None => {
            let s = clock_seed();
            eprintln!("tsq: seed {s}");
            s
        }
    };
    match cli.command {
        Command::Gen {
            output_dir,
            rows,
            edge_rate,
            format,
            crlf,
            bom,
            no_header,
            ragged_rate,
        } => {
            if !(0.0..=1.0).contains(&edge_rate) || !(0.0..=1.0).contains(&ragged_rate) {
                bail!("rates must be between 0 and 1");
            }
            let opts = gen::GenOptions {
                output_dir,
                rows,
                edge_rate,
                format: data::Format {
                    delimiter: match format {
                        FileFormat::Csv => b',',
                        FileFormat::Tsv => b'\t',
                    },
                    crlf,
                    bom,
                    header: !no_header,
                    ragged_rate,
                },
            };
            gen::run(seed, &opts, cli.verbose)?;
            Ok(0)
        }
    }
}

fn main() -> ExitCode {
    match real_main() {
        Ok(0) => ExitCode::SUCCESS,
        Ok(c) => ExitCode::from(c as u8),
        Err(e) => {
            eprintln!("tsq: {e:#}");
            ExitCode::from(2)
        }
    }
}
