use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::compare::Temperature;

#[derive(Parser, Debug)]
#[command(author, version, about = "Portable data lake benchmark runner")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Run {
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        /// Run only the specified query numbers (1-based, repeatable).
        /// e.g. --query 7 --query 29  runs only q07 and q29.
        /// Overrides selected_queries in the config file.
        #[arg(long = "query", value_name = "N")]
        queries: Vec<usize>,
        /// Run only the named scenarios (repeatable or comma-separated).
        /// e.g. --scenario sirius-hot --scenario polars-gpu-hot
        /// Unknown names are a hard error.
        #[arg(long = "scenario", value_name = "NAME", value_delimiter = ',')]
        scenarios: Vec<String>,
    },
    Report {
        #[arg(long)]
        results: PathBuf,
    },
    Compare {
        #[arg(long)]
        left: PathBuf,
        #[arg(long)]
        right: PathBuf,
        #[arg(long)]
        left_scenario: Option<String>,
        #[arg(long)]
        right_scenario: Option<String>,
        #[arg(long, default_value = "best")]
        temperature: Temperature,
        #[arg(long, default_value_t = 15.0)]
        warning_pct: f64,
        #[arg(long)]
        tui: bool,
    },
    ProbeCapacity {
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        #[arg(long)]
        scenario: String,
        #[arg(long)]
        query: Option<usize>,
        #[arg(long, default_value_t = 100_000)]
        probe_rows: usize,
        #[arg(long, default_value_t = 3.0)]
        overhead_multiplier: f64,
    },
    Debug {
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        #[arg(long)]
        query: usize,
        #[arg(long)]
        scenario: Option<String>,
        #[arg(long)]
        engine: Option<String>,
        #[arg(long)]
        dataset: Option<String>,
    },
    List {
        /// Path to a results directory or results.json file.
        /// Defaults to ./results if it exists, otherwise the current directory.
        #[arg(long)]
        results: Option<PathBuf>,
    },
    /// Delete one or more scenario result directories and rebuild the merged results.json.
    Delete {
        /// Names of scenarios to delete (repeatable: --scenario foo --scenario bar).
        #[arg(long = "scenario", required = true)]
        scenarios: Vec<String>,
        /// Path to the runner config (to locate output_dir and rebuild the index).
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        /// Skip the confirmation prompt.
        #[arg(long, default_value_t = false)]
        yes: bool,
    },
    /// Scan an output directory and reconstruct per-scenario results.json files
    /// from existing profile JSON files. Useful after upgrading or when the
    /// top-level results.json was overwritten by a re-run.
    Rebuild {
        /// Path to the runner config (to resolve engine/dataset/tags/queries).
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        /// Force overwrite of scenario results.json files that already exist.
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    /// Upload Lakebench results into the shared qlens_runs warehouse table.
    UploadWarehouse {
        /// Path to the runner config (to resolve warehouse settings and S3 auth).
        #[arg(long, default_value = "runner.toml")]
        config: PathBuf,
        /// Path to a results.json file. Defaults to <output_dir>/results.json from the config.
        #[arg(long)]
        results: Option<PathBuf>,
    },
}
