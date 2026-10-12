//! tsq -- the tool that beats up sqawk.
//!
//! * `tsq gen`: deterministic multi-table test data (foreign keys, realistic
//!   distributions, optional edge-case cells, every file layout sqawk
//!   reads);
//! * `tsq diff`: random SQL run through sqawk and through SQLite (rusqlite's
//!   bundled build) holding the same values; every result set compared byte
//!   for byte; failures shrunk to minimal reproductions;
//! * `tsq corpus`: hand-written query files judged the same way;
//! * `tsq wb`: random files and DML run with `--write`; every file checked
//!   byte for byte against the oracle's final tables rendered by the
//!   writeback rules (and, on Unix, killed mid-write);
//! * `tsq cli`: command-line and REPL surface fuzzing (exit statuses,
//!   one-line errors, no panics, hangs or stray files);
//! * `tsq stress`: performance workloads with wall time and peak RSS,
//!   compared against a saved baseline.
//!
//! Everything random comes from one ChaCha8 stream per seed (`rng.rs`), so a
//! seed reproduces the same data and queries on every OS.

#![forbid(unsafe_code)]

mod clifuzz;
mod corpus;
mod data;
mod diff;
mod gen;
mod rng;
mod run;
mod stress;
mod wb;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Result};
use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "tsq",
    version,
    about = "Test data generator, differential tester and fuzzer for sqawk",
    long_about = "Test data generator, differential tester and fuzzer for sqawk.\n\n\
                  gen writes deterministic multi-table CSV/TSV data; diff runs random SQL \
                  through sqawk and SQLite and compares the answers byte for byte; corpus \
                  runs hand-written query files the same way; wb checks --write output \
                  against SQLite's final tables; cli fuzzes the command line and the REPL; \
                  stress times performance workloads. The same seed gives the same data \
                  and queries on every OS."
)]
struct Cli {
    /// The sqawk binary (default: $SQAWK, else sqawk next to tsq)
    #[arg(long, global = true)]
    sqawk: Option<PathBuf>,

    /// Random seed (full 64 bits; printed when chosen from the clock)
    #[arg(long, short = 's', global = true)]
    seed: Option<u64>,

    /// Progress and every non-passing result, documented ones included
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
    /// Random SQL through sqawk and SQLite; compare every result set
    Diff {
        /// Number of generated tests
        #[arg(long, short = 'n', default_value_t = 1000)]
        queries: usize,
        /// Table size: small (12 customers), large (100000) or a row count
        #[arg(long, default_value = "small")]
        scale: diff::Scale,
        /// Tables: gen (tsq gen's), edge (adversarial values) or harness
        #[arg(long, default_value = "gen")]
        tables: String,
        /// Edge-value rate for --tables gen
        #[arg(long, default_value_t = 0.0)]
        edge_rate: f64,
        /// Run every test even after a finding
        #[arg(long)]
        keep_going: bool,
        /// Shrink each finding to a minimal reproduction
        #[arg(long)]
        shrink: bool,
        /// Directory for shrunk reproductions (default ./tsq-repro)
        #[arg(long)]
        out: Option<PathBuf>,
        /// Parallel jobs (default: available cores, at most 8 at large scale)
        #[arg(long, short = 'j')]
        jobs: Option<usize>,
        /// Per-test time limit in seconds, for each engine
        #[arg(long, default_value_t = 10)]
        timeout: u64,
        /// Run only test number N
        #[arg(long)]
        only: Option<usize>,
        /// Print the generated scripts instead of running them
        #[arg(long)]
        print: bool,
    },
    /// Run hand-written corpus files (one test per line)
    Corpus {
        /// Corpus files
        #[arg(required = true)]
        files: Vec<PathBuf>,
        /// Table set (overrides the files' `-- tsq-tables:` line)
        #[arg(long)]
        tables: Option<String>,
        /// Parallel jobs (default: available cores)
        #[arg(long, short = 'j')]
        jobs: Option<usize>,
        /// Per-test time limit in seconds
        #[arg(long, default_value_t = 10)]
        timeout: u64,
    },
    /// Writeback fuzzer: random files and DML with --write, checked against
    /// the oracle byte for byte (plus kill-during-write trials on Unix)
    Wb {
        /// Number of iterations
        #[arg(long, short = 'n', default_value_t = 300)]
        iterations: usize,
        /// Run only iteration N (prints its files' flags and script)
        #[arg(long)]
        only: Option<usize>,
        /// Probability that an iteration adds a kill-during-write trial
        /// (Unix only)
        #[arg(long, default_value_t = 0.04)]
        kill_rate: f64,
        /// Per-run time limit in seconds
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
    /// Command-line and REPL surface fuzzer: random flags, operands, REPL
    /// scripts, malformed files, garbage and huge SQL, closed stdout
    Cli {
        /// Number of iterations
        #[arg(long, short = 'n', default_value_t = 300)]
        iterations: usize,
        /// Run only iteration N (prints its command line)
        #[arg(long)]
        only: Option<usize>,
        /// Per-run time limit in seconds (a run past it is a hang)
        #[arg(long, default_value_t = 30)]
        timeout: u64,
    },
    /// Performance workloads on generated data: wall time and peak RSS
    Stress {
        /// Rows of the largest table (order_items), 1000000 to 10000000
        #[arg(long, short = 'r', default_value_t = 1_000_000)]
        rows: usize,
        /// Comma-separated workload names (default: all; see --list)
        #[arg(long)]
        workloads: Option<String>,
        /// Print the workload names and exit
        #[arg(long)]
        list: bool,
        /// Write the results as JSON to this file
        #[arg(long)]
        json: Option<PathBuf>,
        /// Compare with an earlier --json file; exit 1 on a regression
        #[arg(long)]
        baseline: Option<PathBuf>,
        /// Allowed regression in percent (time and peak RSS)
        #[arg(long, default_value_t = 25.0)]
        tolerance: f64,
        /// Runs per workload; the best time and the lowest peak RSS count
        #[arg(long, default_value_t = 1)]
        repeat: usize,
        /// Per-run time limit in seconds
        #[arg(long, default_value_t = 1800)]
        timeout: u64,
        /// Directory for the generated data, kept and reused by later runs
        /// with the same seed and size (default: a temp dir, removed)
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
}

fn clock_seed() -> u64 {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    t ^ u64::from(std::process::id()).rotate_left(32)
}

fn cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn real_main() -> Result<i32> {
    let cli = Cli::parse();
    let uses_seed = !matches!(cli.command, Command::Corpus { .. });
    let seed = match cli.seed {
        Some(s) => s,
        None => {
            let s = clock_seed();
            if uses_seed {
                eprintln!("tsq: seed {s}");
            }
            s
        }
    };
    let sqawk = run::locate_sqawk(cli.sqawk.as_deref());
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
        Command::Diff {
            queries,
            scale,
            tables,
            edge_rate,
            keep_going,
            shrink,
            out,
            jobs,
            timeout,
            only,
            print,
        } => {
            let diff::Scale::Rows(rows) = scale;
            let jobs = jobs.unwrap_or_else(|| {
                if rows >= 5000 {
                    cores().min(8)
                } else {
                    cores()
                }
            });
            let opts = diff::DiffOptions {
                queries,
                scale,
                tables,
                edge_rate,
                keep_going,
                shrink,
                out,
                jobs,
                timeout: Duration::from_secs(timeout),
                only,
                print,
            };
            diff::run(seed, &sqawk, &opts, cli.verbose)
        }
        Command::Corpus {
            files,
            tables,
            jobs,
            timeout,
        } => {
            let opts = corpus::CorpusOptions {
                files,
                tables,
                jobs: jobs.unwrap_or_else(cores),
                timeout: Duration::from_secs(timeout),
                keep_going: true,
            };
            corpus::run(&sqawk, &opts, cli.verbose)
        }
        Command::Wb {
            iterations,
            only,
            kill_rate,
            timeout,
        } => {
            let opts = wb::WbOptions {
                iterations,
                timeout: Duration::from_secs(timeout),
                kill_rate,
            };
            match only {
                Some(n) => wb::run_one(seed, n, &sqawk, &opts),
                None => wb::run(seed, &sqawk, &opts, cli.verbose),
            }
        }
        Command::Cli {
            iterations,
            only,
            timeout,
        } => {
            let opts = clifuzz::CliOptions {
                iterations,
                only,
                timeout: Duration::from_secs(timeout),
            };
            clifuzz::run(seed, &sqawk, &opts, cli.verbose)
        }
        Command::Stress {
            rows,
            workloads,
            list,
            json,
            baseline,
            tolerance,
            repeat,
            timeout,
            data_dir,
        } => {
            if list {
                for w in stress::WORKLOADS {
                    println!("{:<16} {}", w.name, w.about);
                }
                return Ok(0);
            }
            let opts = stress::StressOptions {
                rows,
                workloads,
                json,
                baseline,
                tolerance,
                repeat: repeat.max(1),
                timeout: Duration::from_secs(timeout),
                data_dir,
            };
            stress::run(seed, &sqawk, &opts, cli.verbose)
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
