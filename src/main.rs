mod cli;
mod compare;
mod config;
mod display;
mod list;
mod probe;
mod profile;
mod report;
mod runner;
mod warehouse;

use anyhow::Result;
use clap::Parser;

use crate::cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Run { config, queries, scenarios } => {
            runner::run_suite_filtered(&config, &queries, &scenarios)
        }
        Commands::Report { results } => report::render_report_command(&results),
        Commands::Compare {
            left,
            right,
            left_scenario,
            right_scenario,
            temperature,
            warning_pct,
            tui,
        } => compare::compare_command(
            &left,
            left_scenario.as_deref(),
            &right,
            right_scenario.as_deref(),
            temperature,
            warning_pct,
            tui,
        ),
        Commands::ProbeCapacity {
            config,
            scenario,
            query,
            probe_rows,
            overhead_multiplier,
        } => probe::probe_capacity_command(
            &config,
            &scenario,
            query,
            probe_rows,
            overhead_multiplier,
        ),
        Commands::Debug {
            config,
            query,
            scenario,
            engine,
            dataset,
        } => runner::debug_shell_command(
            &config,
            query,
            scenario.as_deref(),
            engine.as_deref(),
            dataset.as_deref(),
        ),
        Commands::List { results } => list::list_results(results.as_deref()),
        Commands::Delete { scenarios, config, yes } => runner::delete_command(&config, &scenarios, yes),
        Commands::Rebuild { config, force } => runner::rebuild_command(&config, force),
        Commands::UploadWarehouse { config, results } => {
            warehouse::upload_command(&config, results.as_deref())
        }
    }
}
