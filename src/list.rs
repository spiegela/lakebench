use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::config::SuiteResults;
use crate::display::{colorize, AnsiColor};

/// Resolve the results path from an optional argument.
/// Priority: explicit arg → ./results (if it exists) → current dir.
fn resolve_results_path(arg: Option<&Path>) -> PathBuf {
    if let Some(p) = arg {
        return p.to_owned();
    }
    let default_results = PathBuf::from("results");
    if default_results.is_dir() {
        return default_results;
    }
    PathBuf::from(".")
}

/// Load all QueryExecutions by scanning for per-scenario results.json files under
/// `dir`, or falling back to a single results.json in the directory itself.
fn load_all_runs(dir: &Path) -> Result<Vec<(String, SuiteResults)>> {
    // If the path itself is a results.json, just load that.
    if dir.is_file() {
        let text = fs::read_to_string(dir)
            .with_context(|| format!("failed to read {}", dir.display()))?;
        let sr: SuiteResults = serde_json::from_str(&text)
            .with_context(|| format!("failed to parse {}", dir.display()))?;
        let label = dir.parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("results")
            .to_string();
        return Ok(vec![(label, sr)]);
    }

    // Scan immediate subdirectories for results.json files (per-scenario layout).
    let read_dir = fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?;
    let mut entries: Vec<_> = read_dir.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());

    let mut found: Vec<(String, SuiteResults)> = Vec::new();
    for entry in &entries {
        if !entry.file_type().map_or(false, |ft| ft.is_dir()) {
            continue;
        }
        let scenario_results_path = entry.path().join("results.json");
        if !scenario_results_path.exists() {
            continue;
        }
        if let Some(text) = fs::read_to_string(&scenario_results_path).ok() {
            if let Ok(sr) = serde_json::from_str::<SuiteResults>(&text) {
                let label = entry.file_name().to_string_lossy().into_owned();
                found.push((label, sr));
            }
        }
    }

    // Fall back to a top-level results.json if no per-scenario files were found.
    if found.is_empty() {
        let top_level = dir.join("results.json");
        if top_level.exists() {
            if let Ok(text) = fs::read_to_string(&top_level) {
                if let Ok(sr) = serde_json::from_str::<SuiteResults>(&text) {
                    found.push((
                        dir.file_name()
                            .and_then(|n| n.to_str())
                            .unwrap_or("results")
                            .to_string(),
                        sr,
                    ));
                }
            }
        }
    }

    Ok(found)
}

pub fn list_results(results_arg: Option<&Path>) -> Result<()> {
    let path = resolve_results_path(results_arg);
    let all_sources = load_all_runs(&path)?;

    if all_sources.is_empty() {
        println!("No results found under {}", path.display());
        return Ok(());
    }

    // Aggregate per (scenario, engine, dataset) across all loaded sources.
    let mut groups: BTreeMap<(String, String, String), GroupStats> = BTreeMap::new();
    for (_label, sr) in &all_sources {
        for run in &sr.runs {
            let key = (
                run.scenario.clone(),
                run.engine.clone(),
                run.dataset.clone(),
            );
            let entry = groups.entry(key).or_default();

            entry.total_runs += 1;
            if run.process.success {
                entry.successful_runs += 1;
            }
            entry.total_elapsed_seconds += run.process.elapsed_seconds;

            for warm in &run.warm_runs {
                if let Some(ps) = &warm.profile_summary {
                    if let Some(v) = ps.cpu_time_seconds {
                        *entry.total_cpu_seconds.get_or_insert(0.0) += v;
                    }
                    if let Some(v) = ps.total_bytes_read {
                        *entry.total_bytes_read.get_or_insert(0.0) += v;
                    }
                    if let Some(v) = ps.total_files_read {
                        *entry.total_files_read.get_or_insert(0.0) += v;
                    }
                }
            }
        }
    }

    if groups.is_empty() {
        println!("No runs found under {}", path.display());
        return Ok(());
    }

    // Column headers
    const HDR_SCENARIO: &str = "SCENARIO";
    const HDR_ENGINE: &str = "ENGINE";
    const HDR_DATASET: &str = "DATASET";
    const HDR_RUNS: &str = "OK/TOTAL";
    const HDR_ELAPSED: &str = "ELAPSED";
    const HDR_CPU: &str = "CPU";
    const HDR_BYTES: &str = "BYTES READ";
    const HDR_FILES: &str = "FILES";

    // Compute column widths from data + header minimums.
    let w_scenario = groups
        .keys()
        .map(|(s, _, _)| s.len())
        .max()
        .unwrap_or(0)
        .max(HDR_SCENARIO.len());
    let w_engine = groups
        .keys()
        .map(|(_, e, _)| e.len())
        .max()
        .unwrap_or(0)
        .max(HDR_ENGINE.len());
    let w_dataset = groups
        .keys()
        .map(|(_, _, d)| d.len())
        .max()
        .unwrap_or(0)
        .max(HDR_DATASET.len());
    let w_runs = HDR_RUNS.len().max(7); // "999/999"
    let w_elapsed = HDR_ELAPSED.len().max(8);
    let w_cpu = HDR_CPU.len().max(8);
    let w_bytes = HDR_BYTES.len().max(9);
    let w_files = HDR_FILES.len().max(8);

    let sep = colorize(
        &format!(
            "+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+",
            "-".repeat(w_scenario),
            "-".repeat(w_engine),
            "-".repeat(w_dataset),
            "-".repeat(w_runs),
            "-".repeat(w_elapsed),
            "-".repeat(w_cpu),
            "-".repeat(w_bytes),
            "-".repeat(w_files),
        ),
        AnsiColor::Blue,
    );

    println!();
    println!(
        "{}",
        colorize(
            &format!("Results: {}", path.display()),
            AnsiColor::BlueBold
        )
    );
    println!("{sep}");
    println!(
        "| {} | {} | {} | {} | {} | {} | {} | {} |",
        colorize(
            &format!("{HDR_SCENARIO:<w_scenario$}"),
            AnsiColor::CyanBold
        ),
        colorize(&format!("{HDR_ENGINE:<w_engine$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_DATASET:<w_dataset$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_RUNS:>w_runs$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_ELAPSED:>w_elapsed$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_CPU:>w_cpu$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_BYTES:>w_bytes$}"), AnsiColor::CyanBold),
        colorize(&format!("{HDR_FILES:>w_files$}"), AnsiColor::CyanBold),
    );
    println!("{sep}");

    for ((scenario, engine, dataset), stats) in &groups {
        let runs_str = format!("{}/{}", stats.successful_runs, stats.total_runs);
        let elapsed_str = format_seconds(stats.total_elapsed_seconds);
        let cpu_str = stats
            .total_cpu_seconds
            .map(format_seconds)
            .unwrap_or_else(|| "n/a".to_string());
        let bytes_str = stats
            .total_bytes_read
            .map(format_bytes)
            .unwrap_or_else(|| "n/a".to_string());
        let files_str = stats
            .total_files_read
            .map(|v| format!("{v:.0}"))
            .unwrap_or_else(|| "n/a".to_string());

        let runs_color = if stats.successful_runs == stats.total_runs {
            AnsiColor::Green
        } else if stats.successful_runs == 0 {
            AnsiColor::Red
        } else {
            AnsiColor::Yellow
        };

        println!(
            "| {:<w_scenario$} | {:<w_engine$} | {:<w_dataset$} | {} | {:>w_elapsed$} | {:>w_cpu$} | {:>w_bytes$} | {:>w_files$} |",
            scenario,
            engine,
            dataset,
            colorize(&format!("{runs_str:>w_runs$}"), runs_color),
            elapsed_str,
            cpu_str,
            bytes_str,
            files_str,
        );
    }
    println!("{sep}");
    println!();

    Ok(())
}

#[derive(Default)]
struct GroupStats {
    total_runs: usize,
    successful_runs: usize,
    total_elapsed_seconds: f64,
    total_cpu_seconds: Option<f64>,
    total_bytes_read: Option<f64>,
    total_files_read: Option<f64>,
}

fn format_seconds(value: f64) -> String {
    if value >= 3600.0 {
        let h = (value / 3600.0) as u64;
        let m = ((value % 3600.0) / 60.0) as u64;
        format!("{h}h{m:02}m")
    } else if value >= 60.0 {
        let m = (value / 60.0) as u64;
        let s = (value % 60.0) as u64;
        format!("{m}m{s:02}s")
    } else {
        format!("{value:.1}s")
    }
}

fn format_bytes(value: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut scaled = value;
    let mut unit_idx = 0usize;
    while scaled >= 1024.0 && unit_idx < UNITS.len() - 1 {
        scaled /= 1024.0;
        unit_idx += 1;
    }
    format!("{scaled:.1}{}", UNITS[unit_idx])
}
