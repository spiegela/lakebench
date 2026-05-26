use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;

use crate::config::{
    Config, DatasetConfig, EngineBackend, EngineConfig, PolarsFormat, PolarsPreload, ProcessResult,
    ProfileTemperature, QueryExecution, QueryExecutionMode, S3Config, S3UrlStyle, ScenarioConfig,
    SuiteConfig, SuiteResults, WarmRunResult,
};
use crate::display::{
    colorize, print_debug_query_sql, print_failure_log, print_query_banner,
    print_run_status, print_scenario_banner, print_warm_run_status, AnsiColor,
};
use crate::probe::estimate_sample_rows_for_scenario;
use crate::profile::parse_profile_summary;
use crate::report::build_report;
use crate::warehouse::maybe_upload_suite_results;

#[derive(Debug, Clone)]
struct BoundScenario<'a> {
    scenario: &'a ScenarioConfig,
    engine: &'a EngineConfig,
    dataset: &'a DatasetConfig,
}

pub fn run_suite_filtered(config_path: &Path, query_filter: &[usize], scenario_filter: &[String]) -> Result<()> {
    let config_text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: Config = toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;

    let queries = parse_queries(
        &fs::read_to_string(&config.suite.queries_file).with_context(|| {
            format!(
                "failed to read queries file {}",
                config.suite.queries_file.display()
            )
        })?,
    );

    fs::create_dir_all(&config.suite.output_dir).with_context(|| {
        format!(
            "failed to create output directory {}",
            config.suite.output_dir.display()
        )
    })?;

    let engine_map: HashMap<_, _> = config
        .engines
        .iter()
        .map(|e| (e.name.as_str(), e))
        .collect();
    let dataset_map: HashMap<_, _> = config
        .datasets
        .iter()
        .map(|d| (d.name.as_str(), d))
        .collect();

    // Validate and apply --scenario filter.  Unknown names are a hard error so
    // typos don't silently produce empty runs.
    if !scenario_filter.is_empty() {
        let known: std::collections::HashSet<&str> =
            config.scenarios.iter().map(|s| s.name.as_str()).collect();
        let unknown: Vec<&str> = scenario_filter
            .iter()
            .map(|s| s.as_str())
            .filter(|s| !known.contains(s))
            .collect();
        if !unknown.is_empty() {
            bail!(
                "unknown scenario(s): {}\nknown: {}",
                unknown.join(", "),
                known.into_iter().collect::<Vec<_>>().join(", ")
            );
        }
    }

    // CLI --query flags take precedence over selected_queries in the config.
    let selected: Option<Vec<usize>> = if !query_filter.is_empty() {
        Some(query_filter.to_vec())
    } else if !config.suite.selected_queries.is_empty() {
        Some(config.suite.selected_queries.iter().copied().collect())
    } else {
        None
    };

    for scenario in &config.scenarios {
        if !scenario_filter.is_empty() && !scenario_filter.contains(&scenario.name) {
            continue;
        }
        let bound = bind_scenario(scenario, &engine_map, &dataset_map)?;
        let warm_runs = scenario.warm_runs.unwrap_or(config.suite.warm_runs);
        let cold_runs = scenario.cold_runs.unwrap_or(config.suite.cold_runs);
        let selected_query_indices = selected
            .clone()
            .unwrap_or_else(|| (1..=queries.len()).collect::<Vec<_>>());
        let sample_rows =
            resolve_sample_rows(&config, scenario.name.as_str(), &selected_query_indices)?;

        if warm_runs == 0 {
            bail!("scenario {} configured with warm_runs = 0", scenario.name);
        }
        if cold_runs == 0 {
            bail!("scenario {} configured with cold_runs = 0", scenario.name);
        }

        print_scenario_banner(&bound.scenario.name, &bound.engine.name, &bound.dataset.name, warm_runs, cold_runs, sample_rows);

        let mut scenario_runs = Vec::new();
        for (offset, query_sql) in queries.iter().enumerate() {
            let query_index = offset + 1;
            if let Some(ref selected_queries) = selected {
                if !selected_queries.contains(&query_index) {
                    continue;
                }
            }

            for cold_run in 1..=cold_runs {
                print_query_banner(&bound.engine.name, query_index, cold_run, warm_runs);
                let execution = execute_query(
                    &config.suite,
                    &bound,
                    query_index,
                    query_sql,
                    cold_run,
                    warm_runs,
                    sample_rows,
                )?;
                scenario_runs.push(execution);
            }
        }

        // Write per-scenario results.json so re-runs replace only that scenario
        let scenario_dir = config.suite.output_dir.join(&scenario.name);
        let scenario_results = SuiteResults {
            queries_file: config.suite.queries_file.clone(),
            generated_from: config_path.to_path_buf(),
            runs: scenario_runs,
        };
        let scenario_results_path = scenario_dir.join("results.json");
        fs::write(&scenario_results_path, serde_json::to_vec_pretty(&scenario_results)?)
            .with_context(|| format!("failed to write {}", scenario_results_path.display()))?;
    }

    // Build merged top-level results.json from all scenario dirs so previously-run
    // scenarios that weren't part of this invocation are preserved.
    let suite_results = load_suite_results_from_dir(&config.suite.output_dir, &config.suite.queries_file, config_path)?;

    let results_path = config.suite.output_dir.join("results.json");
    fs::write(&results_path, serde_json::to_vec_pretty(&suite_results)?)
        .with_context(|| format!("failed to write {}", results_path.display()))?;

    let report = build_report(&suite_results);
    let report_path = config.suite.output_dir.join("report.md");
    fs::write(&report_path, report)
        .with_context(|| format!("failed to write {}", report_path.display()))?;

    maybe_upload_suite_results(&config, &suite_results)?;

    println!("results: {}", results_path.display());
    println!("report:  {}", report_path.display());
    Ok(())
}

/// Scan an output directory for per-scenario `results.json` files and merge them
/// into a single `SuiteResults`. Scenario dirs are identified as immediate
/// subdirectories that contain a `results.json` file.
pub fn load_suite_results_from_dir(
    output_dir: &Path,
    queries_file: &Path,
    generated_from: &Path,
) -> Result<SuiteResults> {
    let mut all_runs = Vec::new();

    let read_dir = fs::read_dir(output_dir)
        .with_context(|| format!("failed to read directory {}", output_dir.display()))?;

    let mut entries: Vec<_> = read_dir.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        if !entry.file_type().map_or(false, |ft| ft.is_dir()) {
            continue;
        }
        let scenario_results_path = entry.path().join("results.json");
        if !scenario_results_path.exists() {
            continue;
        }
        match fs::read_to_string(&scenario_results_path)
            .ok()
            .and_then(|text| serde_json::from_str::<SuiteResults>(&text).ok())
        {
            Some(sr) => all_runs.extend(sr.runs),
            None => eprintln!(
                "warning: could not parse {}",
                scenario_results_path.display()
            ),
        }
    }

    Ok(SuiteResults {
        queries_file: queries_file.to_path_buf(),
        generated_from: generated_from.to_path_buf(),
        runs: all_runs,
    })
}

/// Delete scenario directories by name and rebuild the merged results.json.
pub fn delete_command(config_path: &Path, scenarios: &[String], yes: bool) -> Result<()> {
    let config_text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: Config = toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;

    let output_dir = &config.suite.output_dir;

    // Validate that every requested scenario dir actually exists.
    let mut targets = Vec::new();
    let mut missing = Vec::new();
    for name in scenarios {
        let dir = output_dir.join(name);
        if dir.is_dir() {
            targets.push((name.clone(), dir));
        } else {
            missing.push(name.clone());
        }
    }
    if !missing.is_empty() {
        for m in &missing {
            eprintln!("not found: {}/{}", output_dir.display(), m);
        }
        anyhow::bail!("one or more scenario directories not found");
    }

    // Print what will be removed and confirm unless --yes.
    println!("The following scenario directories will be permanently deleted:");
    for (name, dir) in &targets {
        println!("  {} ({})", name, dir.display());
    }
    if !yes {
        eprint!("Continue? [y/N] ");
        use std::io::{self, BufRead};
        let line = io::stdin().lock().lines().next()
            .and_then(|l| l.ok())
            .unwrap_or_default();
        if !matches!(line.trim(), "y" | "Y" | "yes" | "YES") {
            println!("Aborted.");
            return Ok(());
        }
    }

    for (name, dir) in &targets {
        fs::remove_dir_all(dir)
            .with_context(|| format!("failed to remove {}", dir.display()))?;
        println!("deleted: {name}");
    }

    // Rebuild merged results.json from whatever scenario dirs remain.
    let suite_results =
        load_suite_results_from_dir(output_dir, &config.suite.queries_file, config_path)?;
    let results_path = output_dir.join("results.json");
    fs::write(&results_path, serde_json::to_vec_pretty(&suite_results)?)
        .with_context(|| format!("failed to write {}", results_path.display()))?;
    println!("merged:  {}", results_path.display());

    Ok(())
}

/// Scan the output directory for scenario subdirectories that have profile JSON
/// files but no `results.json`, and reconstruct them from available data.
/// Metadata (engine, dataset, tags, query SQL) is resolved from the config file.
/// Afterwards the top-level `results.json` is rebuilt from all scenario files.
pub fn rebuild_command(config_path: &Path, force: bool) -> Result<()> {
    let config_text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: Config = toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;

    let queries_text = fs::read_to_string(&config.suite.queries_file).with_context(|| {
        format!(
            "failed to read queries file {}",
            config.suite.queries_file.display()
        )
    })?;
    let queries = parse_queries(&queries_text);

    // Sorted engine/dataset name lists used for inference when a dir isn't in config.
    // Sorted longest-first so more specific names (e.g. "polars-cpu") match before shorter ones.
    let mut engine_names: Vec<&str> = config.engines.iter().map(|e| e.name.as_str()).collect();
    engine_names.sort_by_key(|n| std::cmp::Reverse(n.len()));
    let mut dataset_names: Vec<&str> = config.datasets.iter().map(|d| d.name.as_str()).collect();
    dataset_names.sort_by_key(|n| std::cmp::Reverse(n.len()));

    let output_dir = &config.suite.output_dir;
    let entries = {
        let mut v: Vec<_> = fs::read_dir(output_dir)
            .with_context(|| format!("failed to read {}", output_dir.display()))?
            .filter_map(|e| e.ok())
            .collect();
        v.sort_by_key(|e| e.file_name());
        v
    };

    let mut rebuilt = 0usize;
    let mut skipped = 0usize;

    for entry in &entries {
        if !entry.file_type().map_or(false, |ft| ft.is_dir()) {
            continue;
        }
        let scenario_dir = entry.path();
        let scenario_name = entry.file_name().to_string_lossy().into_owned();

        let scenario_results_path = scenario_dir.join("results.json");
        if scenario_results_path.exists() && !force {
            skipped += 1;
            continue;
        }

        // Resolve engine, dataset, and tags for this scenario directory.
        // Priority: exact match in config scenarios → infer from dir name → fallback to "unknown".
        let (engine_name, dataset_name, tags) =
            if let Some(s) = config.scenarios.iter().find(|s| s.name == scenario_name) {
                (s.engine.clone(), s.dataset.clone(), s.tags.clone())
            } else {
                // Infer by matching known engine and dataset names against the directory name.
                let inferred_engine = engine_names
                    .iter()
                    .find(|&&e| scenario_name.starts_with(e))
                    .map(|&e| e.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                // Dataset name appears after the engine prefix in the scenario name.
                let remainder = scenario_name
                    .strip_prefix(&inferred_engine)
                    .unwrap_or(&scenario_name)
                    .trim_start_matches('-');
                let inferred_dataset = dataset_names
                    .iter()
                    .find(|&&d| remainder.starts_with(d))
                    .map(|&d| d.to_string())
                    .unwrap_or_else(|| remainder.to_string());

                eprintln!(
                    "note: '{scenario_name}' not in config scenarios — inferred engine='{inferred_engine}' dataset='{inferred_dataset}'"
                );
                (inferred_engine, inferred_dataset, vec![])
            };

        let mut runs = Vec::new();

        // Scan q{nn} subdirectories.
        let mut q_entries: Vec<_> = fs::read_dir(&scenario_dir)
            .with_context(|| format!("failed to read {}", scenario_dir.display()))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_type().map_or(false, |ft| ft.is_dir())
                    && e.file_name().to_string_lossy().starts_with('q')
            })
            .collect();
        q_entries.sort_by_key(|e| e.file_name());

        for q_entry in &q_entries {
            let q_name = q_entry.file_name().to_string_lossy().into_owned();
            let query_index: usize = q_name.trim_start_matches('q').parse().unwrap_or(0);
            if query_index == 0 {
                continue;
            }
            let query_sql = queries
                .get(query_index.saturating_sub(1))
                .cloned()
                .unwrap_or_default();

            // Scan cold{nn} subdirectories within each q dir.
            let q_dir = q_entry.path();
            let mut cold_entries: Vec<_> = fs::read_dir(&q_dir)
                .with_context(|| format!("failed to read {}", q_dir.display()))?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_type().map_or(false, |ft| ft.is_dir())
                        && e.file_name().to_string_lossy().starts_with("cold")
                })
                .collect();
            cold_entries.sort_by_key(|e| e.file_name());

            for cold_entry in &cold_entries {
                let cold_name = cold_entry.file_name().to_string_lossy().into_owned();
                let cold_run: usize = cold_name.trim_start_matches("cold").parse().unwrap_or(0);
                if cold_run == 0 {
                    continue;
                }
                let run_dir = cold_entry.path();

                let stdout_path = run_dir.join("stdout.log");
                let stderr_path = run_dir.join("stderr.log");

                // Scan warm{nn}.json files in this cold run dir.
                let mut warm_entries: Vec<_> = fs::read_dir(&run_dir)
                    .with_context(|| format!("failed to read {}", run_dir.display()))?
                    .filter_map(|e| e.ok())
                    .filter(|e| {
                        e.file_type().map_or(false, |ft| ft.is_file())
                            && {
                                let name = e.file_name().to_string_lossy().into_owned();
                                // Match warm{N}.json but not warm{N}-request.json or other suffixed variants.
                                name.starts_with("warm") && name.ends_with(".json") && !name.contains('-')
                            }
                    })
                    .collect();
                warm_entries.sort_by_key(|e| e.file_name());

                let mut warm_runs: Vec<WarmRunResult> = Vec::new();
                let mut total_latency = 0.0f64;

                for (idx, warm_entry) in warm_entries.iter().enumerate() {
                    let warm_run_num = idx + 1;
                    let profile_path = warm_entry.path();
                    let profile_summary = parse_profile_summary(&profile_path).ok();
                    if let Some(ref ps) = profile_summary {
                        total_latency += ps.latency_seconds.unwrap_or(0.0);
                    }
                    let temperature = Some(if warm_run_num == 1 {
                        ProfileTemperature::Cold
                    } else {
                        ProfileTemperature::Warm
                    });
                    warm_runs.push(WarmRunResult {
                        warm_run: warm_run_num,
                        temperature,
                        profile_path,
                        profile_summary,
                    });
                }

                let success = warm_runs.iter().any(|w| w.profile_summary.is_some());

                runs.push(QueryExecution {
                    scenario: scenario_name.clone(),
                    engine: engine_name.clone(),
                    dataset: dataset_name.clone(),
                    query_index,
                    query_sql: query_sql.clone(),
                    cold_run,
                    tags: tags.clone(),
                    process: ProcessResult {
                        status_code: None,
                        success,
                        elapsed_seconds: total_latency,
                        stdout_path,
                        stderr_path,
                        command: vec![],
                    },
                    warm_runs,
                });
            }
        }

        if runs.is_empty() {
            continue;
        }

        let scenario_results = SuiteResults {
            queries_file: config.suite.queries_file.clone(),
            generated_from: config_path.to_path_buf(),
            runs,
        };
        fs::write(
            &scenario_results_path,
            serde_json::to_vec_pretty(&scenario_results)?,
        )
        .with_context(|| format!("failed to write {}", scenario_results_path.display()))?;
        println!("rebuilt: {}", scenario_results_path.display());
        rebuilt += 1;
    }

    println!("{rebuilt} scenario(s) rebuilt, {skipped} skipped (already have results.json)");

    // Rebuild the merged top-level results.json.
    let suite_results =
        load_suite_results_from_dir(output_dir, &config.suite.queries_file, config_path)?;
    let results_path = output_dir.join("results.json");
    fs::write(&results_path, serde_json::to_vec_pretty(&suite_results)?)
        .with_context(|| format!("failed to write {}", results_path.display()))?;
    println!("merged:  {}", results_path.display());

    Ok(())
}

pub fn debug_shell_command(
    config_path: &Path,
    query_index: usize,
    scenario_name: Option<&str>,
    engine_name: Option<&str>,
    dataset_name: Option<&str>,
) -> Result<()> {
    let config_text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: Config = toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;
    let queries = parse_queries(
        &fs::read_to_string(&config.suite.queries_file).with_context(|| {
            format!(
                "failed to read queries file {}",
                config.suite.queries_file.display()
            )
        })?,
    );
    let query_sql = queries
        .get(query_index.saturating_sub(1))
        .ok_or_else(|| anyhow!("query {} is out of range", query_index))?;

    let engine_map: HashMap<_, _> = config
        .engines
        .iter()
        .map(|e| (e.name.as_str(), e))
        .collect();
    let dataset_map: HashMap<_, _> = config
        .datasets
        .iter()
        .map(|d| (d.name.as_str(), d))
        .collect();

    let synthetic_scenario;
    let scenario_ref = if let Some(name) = scenario_name {
        config
            .scenarios
            .iter()
            .find(|scenario| scenario.name == name)
            .ok_or_else(|| anyhow!("unknown scenario {}", name))?
    } else {
        let engine = engine_name.ok_or_else(|| {
            anyhow!("pass either --scenario or both --engine and --dataset")
        })?;
        let dataset = dataset_name.ok_or_else(|| {
            anyhow!("pass either --scenario or both --engine and --dataset")
        })?;
        synthetic_scenario = ScenarioConfig {
            name: format!("debug-{engine}-{dataset}"),
            engine: engine.to_string(),
            dataset: dataset.to_string(),
            warm_runs: None,
            cold_runs: None,
            setup_sql: Vec::new(),
            tags: vec!["debug".to_string()],
        };
        &synthetic_scenario
    };
    let bound = bind_scenario(scenario_ref, &engine_map, &dataset_map)?;
    let sample_rows = if scenario_name.is_some() {
        resolve_sample_rows(&config, scenario_ref.name.as_str(), &[query_index])?
    } else {
        let sample = bound.dataset.sample.as_ref();
        if sample.and_then(|value| value.target_bytes).is_some() {
            println!(
                "{} {}",
                colorize("  note:", AnsiColor::Yellow),
                "byte-target sampling requires --scenario so the probe can reuse scenario-specific setup; continuing without sample.target_bytes"
            );
        }
        sample.and_then(|value| value.rows)
    };
    let debug_dir = prepare_debug_dir(&config.suite.output_dir, &bound.scenario.name, query_index)?;

    println!(
        "{}",
        colorize(
            &format!(
                "Debug shell | scenario={} | engine={} | dataset={} | query=q{}{}",
                bound.scenario.name,
                bound.engine.name,
                bound.dataset.name,
                query_index,
                sample_rows
                    .map(|rows| format!(" | sample_rows={rows}"))
                    .unwrap_or_default()
            ),
            AnsiColor::MagentaBold,
        )
    );
    print_debug_query_sql(query_sql);

    match bound.engine.backend {
        EngineBackend::Duckdb => launch_duckdb_debug_shell(
            &config.suite,
            &bound,
            query_index,
            query_sql,
            sample_rows,
            &debug_dir,
        ),
        EngineBackend::Polars => launch_polars_debug_shell(
            &config.suite,
            &bound,
            query_index,
            query_sql,
            sample_rows,
            &debug_dir,
        ),
    }
}

fn bind_scenario<'a>(
    scenario: &'a ScenarioConfig,
    engines: &HashMap<&str, &'a EngineConfig>,
    datasets: &HashMap<&str, &'a DatasetConfig>,
) -> Result<BoundScenario<'a>> {
    let engine = engines
        .get(scenario.engine.as_str())
        .copied()
        .ok_or_else(|| anyhow!("unknown engine {}", scenario.engine))?;
    let dataset = datasets
        .get(scenario.dataset.as_str())
        .copied()
        .ok_or_else(|| anyhow!("unknown dataset {}", scenario.dataset))?;
    Ok(BoundScenario {
        scenario,
        engine,
        dataset,
    })
}

fn prepare_debug_dir(output_dir: &Path, scenario_name: &str, query_index: usize) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let dir = output_dir
        .join("debug")
        .join(scenario_name)
        .join(format!("q{:02}-{stamp}", query_index));
    fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    Ok(dir)
}

fn launch_duckdb_debug_shell(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    query_index: usize,
    query_sql: &str,
    sample_rows: Option<usize>,
    debug_dir: &Path,
) -> Result<()> {
    let init_path = debug_dir.join("init.sql");
    let query_path = debug_dir.join(format!("q{:02}.sql", query_index));

    let mut init_commands = vec![
        ".timer on".to_string(),
        ".headers on".to_string(),
        ".mode duckbox".to_string(),
    ];
    init_commands.extend(duckdb_builtin_macros().into_iter().map(|sql| ensure_sql_terminator(&sql)));
    init_commands.extend(suite.setup_sql.iter().map(|sql| ensure_sql_terminator(sql)));
    init_commands.extend(duckdb_s3_setup_sql(suite).into_iter().map(|sql| ensure_sql_terminator(&sql)));
    init_commands.extend(
        bound
            .engine
            .setup_sql
            .iter()
            .map(|sql| ensure_sql_terminator(sql)),
    );
    init_commands.extend(
        bound
            .dataset
            .setup_sql
            .iter()
            .map(|sql| ensure_sql_terminator(sql)),
    );
    init_commands.extend(
        bound
            .scenario
            .setup_sql
            .iter()
            .map(|sql| ensure_sql_terminator(sql)),
    );
    init_commands.push(format!(
        "CREATE OR REPLACE VIEW {} AS {};",
        bound.engine.table_name,
        sampled_duckdb_table_sql(bound.dataset, sample_rows)
    ));

    fs::write(&init_path, init_commands.join("\n"))
        .with_context(|| format!("failed to write {}", init_path.display()))?;
    let query_to_run = duckdb_query_sql_for_engine(bound.engine, query_sql);
    fs::write(&query_path, format!("{}\n", query_to_run.trim()))
        .with_context(|| format!("failed to write {}", query_path.display()))?;

    println!(
        "{} {}",
        colorize("  init:", AnsiColor::Yellow),
        init_path.display()
    );
    println!(
        "{} {}",
        colorize("  query:", AnsiColor::Yellow),
        query_path.display()
    );
    println!(
        "{} {}",
        colorize("  tip:", AnsiColor::Yellow),
        "if the preflight fails, rerun with .read <query file> after adjusting setup"
    );

    let preload_query = run_duckdb_debug_preflight(suite, bound, &init_path, &query_path)?;
    launch_duckdb_interactive_shell(suite, bound, &init_path, preload_query, &query_path)
}

fn run_duckdb_debug_preflight(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    init_path: &Path,
    query_path: &Path,
) -> Result<bool> {
    let mut command = Command::new(&bound.engine.binary);
    command
        .arg(&bound.engine.database)
        .arg("-init")
        .arg(init_path)
        .arg("-cmd")
        .arg(format!(".read {}", query_path.display()))
        .arg("-no-stdin");
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());

    let output = command.output().with_context(|| {
        format!(
            "failed to launch duckdb debug preflight {} for scenario {}",
            bound.engine.binary.display(),
            bound.scenario.name
        )
    })?;

    if output.status.success() {
        println!(
            "{} {}",
            colorize("  preflight:", AnsiColor::Green),
            "query executed successfully; opening shell with the query preloaded"
        );
        return Ok(true);
    }

    println!(
        "{} {}",
        colorize("  preflight:", AnsiColor::Red),
        "query failed; opening shell without auto-running it so you can debug"
    );
    if !output.stdout.is_empty() {
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        print_failure_log(&output.stderr);
    }
    Ok(false)
}

fn launch_duckdb_interactive_shell(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    init_path: &Path,
    preload_query: bool,
    query_path: &Path,
) -> Result<()> {
    let mut command = Command::new(&bound.engine.binary);
    command
        .arg(&bound.engine.database)
        .arg("-interactive")
        .arg("-init")
        .arg(init_path);
    if preload_query {
        command.arg("-cmd").arg(format!(".read {}", query_path.display()));
    }
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());
    command.stdin(std::process::Stdio::inherit());
    command.stdout(std::process::Stdio::inherit());
    command.stderr(std::process::Stdio::inherit());

    let status = command.status().with_context(|| {
        format!(
            "failed to launch debug shell {} for scenario {}",
            bound.engine.binary.display(),
            bound.scenario.name
        )
    })?;

    if !status.success() {
        bail!("duckdb debug shell exited with status {:?}", status.code());
    }
    Ok(())
}

fn launch_polars_debug_shell(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    query_index: usize,
    query_sql: &str,
    sample_rows: Option<usize>,
    debug_dir: &Path,
) -> Result<()> {
    let script_path = debug_dir.join("polars_debug.py");
    let request_path = debug_dir.join("request.json");
    let query_path = debug_dir.join(format!("q{:02}.sql", query_index));

    let dataset_expr = sampled_polars_expr(bound.dataset, sample_rows)?;
    // Debug shell: single profile path in a one-element list; no preload.
    let request = PolarsRequest {
        dataset_expr,
        query: query_sql.to_string(),
        warm_profile_paths: vec![debug_dir.join("profile.json").to_string_lossy().to_string()],
        table_name: bound.engine.table_name.clone(),
        collect_engine: bound.engine.polars_collect_engine.clone(),
        storage_options: polars_s3_storage_options(suite),
        iceberg_storage_options: polars_iceberg_storage_options(suite),
        preload: "none".to_string(),
        raise_on_fail: false,
    };
    fs::write(&script_path, POLARS_DEBUG_SCRIPT)
        .with_context(|| format!("failed to write {}", script_path.display()))?;
    fs::write(&request_path, serde_json::to_vec_pretty(&request)?)
        .with_context(|| format!("failed to write {}", request_path.display()))?;
    fs::write(&query_path, format!("{}\n", query_sql.trim()))
        .with_context(|| format!("failed to write {}", query_path.display()))?;

    println!(
        "{} {}",
        colorize("  script:", AnsiColor::Yellow),
        script_path.display()
    );
    println!(
        "{} {}",
        colorize("  query:", AnsiColor::Yellow),
        query_path.display()
    );
    println!(
        "{} {}",
        colorize("  helpers:", AnsiColor::Yellow),
        "run(), run_sql(...), profile(), dataset, ctx, query_text"
    );

    let mut command = Command::new(&bound.engine.binary);
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    command.arg("-i").arg(&script_path).arg(&request_path);
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());
    command.stdin(std::process::Stdio::inherit());
    command.stdout(std::process::Stdio::inherit());
    command.stderr(std::process::Stdio::inherit());

    let status = command.status().with_context(|| {
        format!(
            "failed to launch polars debug shell {} for scenario {}",
            bound.engine.binary.display(),
            bound.scenario.name
        )
    })?;
    if !status.success() {
        bail!("polars debug shell exited with status {:?}", status.code());
    }
    Ok(())
}

fn execute_query(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    query_index: usize,
    query_sql: &str,
    cold_run: usize,
    warm_runs: usize,
    sample_rows: Option<usize>,
) -> Result<QueryExecution> {
    match bound.engine.backend {
        EngineBackend::Duckdb => execute_duckdb_query(
            suite,
            bound,
            query_index,
            query_sql,
            cold_run,
            warm_runs,
            sample_rows,
        ),
        EngineBackend::Polars => execute_polars_query(
            suite,
            bound,
            query_index,
            query_sql,
            cold_run,
            warm_runs,
            sample_rows,
        ),
    }
}

fn execute_duckdb_query(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    query_index: usize,
    query_sql: &str,
    cold_run: usize,
    warm_runs: usize,
    sample_rows: Option<usize>,
) -> Result<QueryExecution> {
    let run_dir = suite
        .output_dir
        .join(&bound.scenario.name)
        .join(format!("q{:02}", query_index))
        .join(format!("cold{:02}", cold_run));
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let stdout_path = run_dir.join("stdout.log");
    let stderr_path = run_dir.join("stderr.log");

    let mut sql_commands = Vec::new();
    sql_commands.push(format!(
        "PRAGMA enable_profiling = '{}'",
        suite.profiling_mode
    ));
    sql_commands.extend(duckdb_builtin_macros());
    sql_commands.extend(suite.setup_sql.iter().cloned());
    sql_commands.extend(duckdb_s3_setup_sql(suite));
    sql_commands.extend(bound.engine.setup_sql.iter().cloned());
    sql_commands.extend(bound.dataset.setup_sql.iter().cloned());
    sql_commands.extend(bound.scenario.setup_sql.iter().cloned());
    // Create a non-TEMP view so the table is visible inside CALL gpu_execution('...')
    // (Sirius's gpu_execution context cannot see TEMP-schema objects).
    sql_commands.push(format!(
        "CREATE OR REPLACE VIEW {} AS {}",
        bound.engine.table_name,
        sampled_duckdb_table_sql(bound.dataset, sample_rows)
    ));

    // Preload SQL runs after table view creation but before profiling begins.
    // For Sirius this typically materialises data into the GPU buffer manager.
    let has_preload = !bound.engine.preload_sql.is_empty();
    // preload_warms_gpu defaults to true: typical preload wraps a warming
    // gpu_execution call so every iteration is Warm.  Set false in TOML when
    // preload only materialises a CPU table (no GPU access); then warm_run=1
    // is still GPU-cold.
    let preload_warms_gpu = bound.engine.preload_warms_gpu.unwrap_or(true);
    if has_preload {
        sql_commands.extend(bound.engine.preload_sql.iter().cloned());
    }

    let mut warm_run_results = Vec::new();
    for warm_run in 1..=warm_runs {
        let profile_path = run_dir.join(format!("warm{:02}.json", warm_run));
        sql_commands.push(format!(
            "PRAGMA profiling_output = '{}'",
            escape_sql_string(&profile_path.to_string_lossy())
        ));
        sql_commands.push(duckdb_query_sql_for_engine(bound.engine, query_sql));
        let temperature = if has_preload && preload_warms_gpu {
            ProfileTemperature::Warm
        } else if warm_run == 1 {
            ProfileTemperature::Cold
        } else {
            ProfileTemperature::Warm
        };
        warm_run_results.push(WarmRunResult {
            warm_run,
            temperature: Some(temperature),
            profile_path,
            profile_summary: None,
        });
    }

    let mut command = Command::new(&bound.engine.binary);
    command.arg(&bound.engine.database);
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    for sql in &sql_commands {
        command.arg("-c").arg(sql);
    }
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());

    let rendered_command = render_command(&command);
    let start = Instant::now();
    let output = command.output().with_context(|| {
        format!(
            "failed to launch {} for scenario {} query {}",
            bound.engine.binary.display(),
            bound.scenario.name,
            query_index
        )
    })?;
    let elapsed_seconds = start.elapsed().as_secs_f64();

    fs::write(&stdout_path, &output.stdout)
        .with_context(|| format!("failed to write {}", stdout_path.display()))?;
    fs::write(&stderr_path, &output.stderr)
        .with_context(|| format!("failed to write {}", stderr_path.display()))?;
    print_run_status(
        output.status.success(),
        &bound.scenario.name,
        query_index,
        cold_run,
        Some("process"),
        elapsed_seconds,
        &stderr_path,
        &output.stderr,
    );
    // SIGKILL / SIGTERM leaves stderr empty (the process never gets to flush)
    // — print a hint so the user knows to check memory limits / OOM logs.
    diagnose_signal_exit(&output, &bound.scenario.name, query_index, cold_run);

    for warm_run in &mut warm_run_results {
        if warm_run.profile_path.exists() {
            warm_run.profile_summary = Some(parse_profile_summary(&warm_run.profile_path)?);
        }
    }

    // Print one status line per warm iteration so progress is visible per-run.
    for wr in &warm_run_results {
        let latency = wr.profile_summary
            .as_ref()
            .and_then(|s| s.latency_seconds)
            .unwrap_or(0.0);
        let temp = match wr.temperature {
            Some(ProfileTemperature::Cold) => "cold",
            _ => "warm",
        };
        print_warm_run_status(
            wr.profile_summary.is_some(),
            &bound.scenario.name,
            query_index,
            cold_run,
            wr.warm_run,
            temp,
            latency,
        );
    }

    Ok(QueryExecution {
        scenario: bound.scenario.name.clone(),
        engine: bound.engine.name.clone(),
        dataset: bound.dataset.name.clone(),
        query_index,
        query_sql: query_sql.to_string(),
        cold_run,
        tags: bound.scenario.tags.clone(),
        process: ProcessResult {
            status_code: output.status.code(),
            success: output.status.success(),
            elapsed_seconds,
            stdout_path,
            stderr_path,
            command: rendered_command,
        },
        warm_runs: warm_run_results,
    })
}

fn execute_polars_query(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    query_index: usize,
    query_sql: &str,
    cold_run: usize,
    warm_runs: usize,
    sample_rows: Option<usize>,
) -> Result<QueryExecution> {
    let run_dir = suite
        .output_dir
        .join(&bound.scenario.name)
        .join(format!("q{:02}", query_index))
        .join(format!("cold{:02}", cold_run));
    fs::create_dir_all(&run_dir)
        .with_context(|| format!("failed to create {}", run_dir.display()))?;

    let stdout_path = run_dir.join("stdout.log");
    let stderr_path = run_dir.join("stderr.log");
    let request_path = run_dir.join("request.json");

    let dataset_expr = sampled_polars_expr(bound.dataset, sample_rows)?;

    // Collect all warm-run profile paths up front; the Python script loops over them.
    let mut warm_run_results = Vec::new();
    let mut warm_profile_paths = Vec::new();
    let has_preload = bound.engine.polars_preload
        .map(|p| p != PolarsPreload::None)
        .unwrap_or(false);
    let preload_warms_gpu = bound.engine.preload_warms_gpu.unwrap_or(true);
    for warm_run in 1..=warm_runs {
        let profile_path = run_dir.join(format!("warm{:02}.json", warm_run));
        warm_profile_paths.push(profile_path.to_string_lossy().to_string());
        let temperature = if has_preload && preload_warms_gpu {
            ProfileTemperature::Warm
        } else if warm_run == 1 {
            ProfileTemperature::Cold
        } else {
            ProfileTemperature::Warm
        };
        warm_run_results.push(WarmRunResult {
            warm_run,
            temperature: Some(temperature),
            profile_path,
            profile_summary: None,
        });
    }

    let preload_str = match bound.engine.polars_preload.unwrap_or(PolarsPreload::None) {
        PolarsPreload::None    => "none",
        PolarsPreload::Collect => "collect",
        PolarsPreload::Warmup  => "warmup",
    };

    // raise_on_fail defaults to true when the engine is explicitly "gpu".
    let raise_on_fail = bound.engine.polars_raise_on_fail
        || bound.engine.polars_collect_engine.as_deref() == Some("gpu");

    let request = PolarsRequest {
        dataset_expr,
        query: query_sql.to_string(),
        warm_profile_paths,
        table_name: bound.engine.table_name.clone(),
        collect_engine: bound.engine.polars_collect_engine.clone(),
        storage_options: polars_s3_storage_options(suite),
        iceberg_storage_options: polars_iceberg_storage_options(suite),
        preload: preload_str.to_string(),
        raise_on_fail,
    };
    fs::write(&request_path, serde_json::to_vec_pretty(&request)?)
        .with_context(|| format!("failed to write {}", request_path.display()))?;

    let mut command = Command::new(&bound.engine.binary);
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    command
        .arg("-c")
        .arg(POLARS_RUNNER_SCRIPT)
        .arg(&request_path);
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());

    let rendered_command = render_command(&command);
    let start = Instant::now();
    let output = command.output().with_context(|| {
        format!(
            "failed to launch {} for scenario {} query {}",
            bound.engine.binary.display(),
            bound.scenario.name,
            query_index
        )
    })?;
    let elapsed_seconds = start.elapsed().as_secs_f64();

    fs::write(&stdout_path, &output.stdout)
        .with_context(|| format!("failed to write {}", stdout_path.display()))?;
    fs::write(&stderr_path, &output.stderr)
        .with_context(|| format!("failed to write {}", stderr_path.display()))?;
    print_run_status(
        output.status.success(),
        &bound.scenario.name,
        query_index,
        cold_run,
        Some("process"),
        elapsed_seconds,
        &stderr_path,
        &output.stderr,
    );
    // SIGKILL / SIGTERM leaves stderr empty (the process never gets to flush)
    // — print a hint so the user knows to check memory limits / OOM logs.
    diagnose_signal_exit(&output, &bound.scenario.name, query_index, cold_run);

    for warm_run in &mut warm_run_results {
        if warm_run.profile_path.exists() {
            warm_run.profile_summary = Some(parse_profile_summary(&warm_run.profile_path)?);
        }
    }

    // Print one status line per warm iteration so progress is visible per-run.
    for wr in &warm_run_results {
        let latency = wr.profile_summary
            .as_ref()
            .and_then(|s| s.latency_seconds)
            .unwrap_or(0.0);
        let temp = match wr.temperature {
            Some(ProfileTemperature::Cold) => "cold",
            _ => "warm",
        };
        print_warm_run_status(
            wr.profile_summary.is_some(),
            &bound.scenario.name,
            query_index,
            cold_run,
            wr.warm_run,
            temp,
            latency,
        );
    }

    Ok(QueryExecution {
        scenario: bound.scenario.name.clone(),
        engine: bound.engine.name.clone(),
        dataset: bound.dataset.name.clone(),
        query_index,
        query_sql: query_sql.to_string(),
        cold_run,
        tags: bound.scenario.tags.clone(),
        process: ProcessResult {
            status_code: output.status.code(),
            success: output.status.success(),
            elapsed_seconds,
            stdout_path,
            stderr_path,
            command: rendered_command,
        },
        warm_runs: warm_run_results,
    })
}

#[derive(Serialize)]
struct PolarsRequest {
    dataset_expr: String,
    query: String,
    /// All profile output paths for this cold-run, one per warm iteration.
    /// The Python script loops over them and writes each profile in sequence.
    warm_profile_paths: Vec<String>,
    table_name: String,
    collect_engine: Option<String>,
    storage_options: Option<HashMap<String, String>>,
    iceberg_storage_options: Option<HashMap<String, String>>,
    /// "none" | "collect" | "warmup"
    preload: String,
    /// When true (GPU engine) passes raise_on_fail=True so CPU fallback is an error.
    raise_on_fail: bool,
}

fn sampled_duckdb_table_sql(dataset: &DatasetConfig, sample_rows: Option<usize>) -> String {
    if let Some(rows) = sample_rows {
        format!(
            "SELECT * FROM ({}) AS sampled_dataset LIMIT {}",
            dataset.table_sql, rows
        )
    } else {
        dataset.table_sql.clone()
    }
}

fn duckdb_query_sql_for_engine(engine: &EngineConfig, query_sql: &str) -> String {
    match engine.query_execution_mode {
        QueryExecutionMode::Direct => query_sql.to_string(),
        QueryExecutionMode::GpuExecution => {
            // Strip trailing semicolons from the inner SQL: Sirius parses the
            // string itself and an inner `;` produces a parse error / silent
            // failure depending on the build.
            let inner = query_sql.trim().trim_end_matches(';').trim_end();
            format!("CALL gpu_execution('{}');", escape_sql_string(inner))
        }
    }
}

fn sampled_polars_expr(dataset: &DatasetConfig, sample_rows: Option<usize>) -> Result<String> {
    let base = if let Some(expr) = dataset.polars_lazyframe_py.as_ref() {
        expr.clone()
    } else {
        let format = dataset.polars_format.ok_or_else(|| {
            anyhow!(
                "dataset {} requires either polars_lazyframe_py or both polars_format and polars_source for the polars backend",
                dataset.name
            )
        })?;
        let source = dataset.polars_source.as_ref().ok_or_else(|| {
            anyhow!(
                "dataset {} requires polars_source when polars_format is set",
                dataset.name
            )
        })?;
        let format_name = match format {
            PolarsFormat::Parquet => "parquet",
            PolarsFormat::Delta => "delta",
            PolarsFormat::Iceberg => "iceberg",
        };
        format!("lakebench_scan({format_name:?}, {source:?})")
    };

    Ok(if let Some(rows) = sample_rows {
        format!("({base}).limit({rows})")
    } else {
        base
    })
}

fn resolve_sample_rows(
    config: &Config,
    scenario_name: &str,
    query_indices: &[usize],
) -> Result<Option<usize>> {
    let scenario = config
        .scenarios
        .iter()
        .find(|scenario| scenario.name == scenario_name)
        .ok_or_else(|| anyhow!("unknown scenario {}", scenario_name))?;
    let dataset = config
        .datasets
        .iter()
        .find(|dataset| dataset.name == scenario.dataset)
        .ok_or_else(|| anyhow!("unknown dataset {}", scenario.dataset))?;

    let Some(sample) = dataset.sample.as_ref() else {
        return Ok(None);
    };

    if let Some(rows) = sample.rows {
        return Ok(Some(rows));
    }

    if sample.target_bytes.is_some() {
        let estimate = estimate_sample_rows_for_scenario(config, scenario_name, query_indices)?;
        println!(
            "resolved sample rows for scenario={} target_bytes={} -> {} rows (limiting query=q{}, avg_row_bytes={:.2}, overhead={:.2}x)",
            scenario_name,
            sample.target_bytes.unwrap_or_default(),
            estimate.sample_rows,
            estimate.limiting_query_index,
            estimate.avg_row_bytes,
            estimate.overhead_multiplier,
        );
        return Ok(Some(estimate.sample_rows));
    }

    Ok(None)
}

pub fn parse_queries(text: &str) -> Vec<String> {
    let mut queries = Vec::new();
    let mut current = String::new();

    for ch in text.chars() {
        current.push(ch);
        if ch == ';' {
            let query = current.trim();
            if !query.is_empty() {
                queries.push(query.to_string());
            }
            current.clear();
        }
    }

    let trailing = current.trim();
    if !trailing.is_empty() {
        queries.push(trailing.to_string());
    }

    queries
}

/// When a subprocess exits via signal (SIGKILL=9 is the OOM killer's hammer,
/// SIGTERM=15 is a graceful kill, SIGSEGV=11 is a crash), stderr is usually
/// empty because the process never got to flush.  Print a clear hint so the
/// user knows where to look.
fn diagnose_signal_exit(
    output: &std::process::Output,
    scenario_name: &str,
    query_index: usize,
    cold_run: usize,
) {
    if output.status.success() {
        return;
    }
    // On Unix, signaled exits report exit code = None and signal = Some(N).
    // The shell convention is exit code 128+N; some platforms also surface
    // that directly via .code(). Cover both.
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        output.status.signal()
    };
    #[cfg(not(unix))]
    let signal: Option<i32> = None;

    let signal_from_code = output.status.code().and_then(|c| {
        if c > 128 && c < 128 + 64 { Some(c - 128) } else { None }
    });
    let sig = signal.or(signal_from_code);
    let Some(sig) = sig else { return; };

    let (name, hint) = match sig {
        9 => ("SIGKILL",
              "likely OOM-killed (or Slurm cgroup memory limit hit).  \
               Check `dmesg | tail` or `journalctl -k | grep -i oom`, and \
               verify your allocation has enough RAM (a full ClickBench \
               materialise needs ~70 GB).  Consider sampling, or have \
               sirius-cold skip materialisation."),
        15 => ("SIGTERM", "process was asked to terminate (job timeout?)"),
        11 => ("SIGSEGV", "segfault — likely an engine bug; capture core dump"),
        n  => return eprintln!(
            "  note: scenario={scenario_name} query=q{query_index} cold_run={cold_run} \
             exited via signal {n} (stderr/stdout may be empty)"
        ),
    };
    eprintln!(
        "  {} scenario={scenario_name} query=q{query_index} cold_run={cold_run} \
         signal={name} — {hint}",
        colorize("note:", AnsiColor::Yellow)
    );
}

fn render_command(command: &Command) -> Vec<String> {
    let mut rendered = Vec::new();
    rendered.push(command.get_program().to_string_lossy().to_string());
    for arg in command.get_args() {
        rendered.push(sanitize_command_arg(&arg.to_string_lossy()));
    }
    rendered
}

fn ensure_sql_terminator(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        trimmed.to_string()
    } else {
        format!("{trimmed};")
    }
}

fn sanitize_command_arg(arg: &str) -> String {
    for key in ["s3_access_key_id", "s3_secret_access_key", "s3_session_token"] {
        if arg.to_ascii_lowercase().contains(key) {
            return redact_sql_assignment(arg);
        }
    }
    arg.to_string()
}

fn redact_sql_assignment(arg: &str) -> String {
    let Some(start) = arg.find('\'') else {
        return arg.to_string();
    };
    let rest = &arg[start + 1..];
    let Some(end_offset) = rest.find('\'') else {
        return arg.to_string();
    };
    format!("{}'<redacted>'{}", &arg[..start], &rest[end_offset + 1..])
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Built-in DuckDB macros that lakebench always injects before user setup_sql.
///
/// These are emitted first so that user setup_sql can override them if needed.
/// Macros here should be type-safe no-ops when the column already has the
/// expected type, so they're safe to apply unconditionally in `table_sql`.
pub(crate) fn duckdb_builtin_macros() -> Vec<String> {
    vec![
        // toDate(d): coerce an INTEGER days-since-epoch column to DATE.
        // Delta Lake and some Parquet files store EventDate as int32 without a
        // Parquet logical type annotation, so DuckDB reads it as INTEGER.
        // Usage in table_sql:
        //   SELECT * REPLACE (toDate(EventDate) AS EventDate) FROM delta_scan(...)
        "CREATE OR REPLACE MACRO toDate(d) AS \
         CASE WHEN starts_with(typeof(d), 'DATE') THEN d \
              ELSE make_date(CAST(d AS INTEGER)) END"
            .to_string(),
    ]
}

pub(crate) fn duckdb_s3_setup_sql(suite: &SuiteConfig) -> Vec<String> {
    let Some(s3) = suite.s3.as_ref() else {
        return Vec::new();
    };

    // ── Legacy httpfs SET commands (read by parquet/iceberg scans) ──────────
    let mut commands = vec![
        format!(
            "SET s3_access_key_id='{}'",
            escape_sql_string(&s3.access_key_id)
        ),
        format!(
            "SET s3_secret_access_key='{}'",
            escape_sql_string(&s3.secret_access_key)
        ),
    ];

    if let Some(session_token) = &s3.session_token {
        commands.push(format!(
            "SET s3_session_token='{}'",
            escape_sql_string(session_token)
        ));
    }
    if let Some(region) = &s3.region {
        commands.push(format!("SET s3_region='{}'", escape_sql_string(region)));
    }
    if let Some(endpoint) = &s3.endpoint {
        commands.push(format!("SET s3_endpoint='{}'", escape_sql_string(endpoint)));
    }
    if let Some(url_style) = s3.url_style {
        commands.push(format!(
            "SET s3_url_style='{}'",
            match url_style {
                S3UrlStyle::Path => "path",
                S3UrlStyle::Vhost => "vhost",
            }
        ));
    }
    if let Some(use_ssl) = s3.use_ssl {
        commands.push(format!(
            "SET s3_use_ssl={}",
            if use_ssl { "true" } else { "false" }
        ));
    }

    // ── Secrets API (read by the delta extension / DeltaKernel) ─────────────
    // DuckDB's delta_scan uses DeltaKernel which bypasses the httpfs SET
    // settings and instead reads credentials from DuckDB's secrets store.
    // Without a secret, DeltaKernel falls through to its AWS credential chain
    // which eventually tries the EC2 IMDS endpoint (169.254.169.254) and
    // times out when not running on EC2.
    let mut secret_parts = vec![
        "TYPE S3".to_string(),
        format!("KEY_ID '{}'", escape_sql_string(&s3.access_key_id)),
        format!("SECRET '{}'", escape_sql_string(&s3.secret_access_key)),
    ];
    if let Some(session_token) = &s3.session_token {
        secret_parts.push(format!(
            "SESSION_TOKEN '{}'",
            escape_sql_string(session_token)
        ));
    }
    if let Some(region) = &s3.region {
        secret_parts.push(format!("REGION '{}'", escape_sql_string(region)));
    }
    if let Some(endpoint) = &s3.endpoint {
        secret_parts.push(format!("ENDPOINT '{}'", escape_sql_string(endpoint)));
        // For custom endpoints (e.g. MinIO) the URL style is almost always path.
        let style = s3.url_style.map_or("path", |u| match u {
            S3UrlStyle::Path => "path",
            S3UrlStyle::Vhost => "vhost",
        });
        secret_parts.push(format!("URL_STYLE '{style}'"));
        // Custom endpoints typically don't use TLS unless explicitly requested.
        let use_ssl = s3.use_ssl.unwrap_or(false);
        secret_parts.push(format!("USE_SSL {}", if use_ssl { "true" } else { "false" }));
    }
    commands.push(format!(
        "CREATE OR REPLACE SECRET __lakebench_s3 ({})",
        secret_parts.join(", ")
    ));

    commands
}

pub(crate) fn apply_s3_env(command: &mut Command, s3: Option<&S3Config>) {
    for key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_STS_REGIONAL_ENDPOINTS",
    ] {
        command.env_remove(key);
    }
    command.env("AWS_EC2_METADATA_DISABLED", "true");
    command.env("AWS_SDK_LOAD_CONFIG", "0");

    let Some(s3) = s3 else {
        return;
    };

    command.env("AWS_ACCESS_KEY_ID", &s3.access_key_id);
    command.env("AWS_SECRET_ACCESS_KEY", &s3.secret_access_key);
    if let Some(session_token) = &s3.session_token {
        command.env("AWS_SESSION_TOKEN", session_token);
    }
    if let Some(region) = &s3.region {
        command.env("AWS_REGION", region);
        command.env("AWS_DEFAULT_REGION", region);
    }
    if let Some(endpoint) = &s3.endpoint {
        command.env("AWS_ENDPOINT_URL", endpoint);
        command.env("AWS_ENDPOINT_URL_S3", endpoint);
    }
}

fn polars_s3_storage_options(suite: &SuiteConfig) -> Option<HashMap<String, String>> {
    let s3 = suite.s3.as_ref()?;
    let mut options = HashMap::new();
    options.insert("aws_access_key_id".to_string(), s3.access_key_id.clone());
    options.insert(
        "aws_secret_access_key".to_string(),
        s3.secret_access_key.clone(),
    );
    if let Some(session_token) = &s3.session_token {
        options.insert("aws_session_token".to_string(), session_token.clone());
    }
    if let Some(region) = &s3.region {
        options.insert("region".to_string(), region.clone());
    }
    if let Some(endpoint) = &s3.endpoint {
        options.insert("endpoint".to_string(), endpoint.clone());
    }
    if let Some(url_style) = s3.url_style {
        options.insert(
            "virtual_hosted_style_request".to_string(),
            match url_style {
                S3UrlStyle::Path => "false".to_string(),
                S3UrlStyle::Vhost => "true".to_string(),
            },
        );
    }
    if let Some(use_ssl) = s3.use_ssl {
        options.insert(
            "allow_http".to_string(),
            if use_ssl { "false" } else { "true" }.to_string(),
        );
    }
    (!options.is_empty()).then_some(options)
}

fn polars_iceberg_storage_options(suite: &SuiteConfig) -> Option<HashMap<String, String>> {
    let s3 = suite.s3.as_ref()?;
    let mut options = HashMap::new();
    options.insert("s3.access-key-id".to_string(), s3.access_key_id.clone());
    options.insert(
        "s3.secret-access-key".to_string(),
        s3.secret_access_key.clone(),
    );
    if let Some(session_token) = &s3.session_token {
        options.insert("s3.session-token".to_string(), session_token.clone());
    }
    if let Some(region) = &s3.region {
        options.insert("s3.region".to_string(), region.clone());
    }
    if let Some(endpoint) = &s3.endpoint {
        options.insert("s3.endpoint".to_string(), endpoint.clone());
    }
    if let Some(url_style) = s3.url_style {
        options.insert(
            "s3.force-virtual-addressing".to_string(),
            match url_style {
                S3UrlStyle::Path => "false".to_string(),
                S3UrlStyle::Vhost => "true".to_string(),
            },
        );
    }
    if let Some(use_ssl) = s3.use_ssl {
        options.insert(
            "s3.scheme".to_string(),
            if use_ssl { "https" } else { "http" }.to_string(),
        );
    }
    Some(options)
}

const POLARS_RUNNER_SCRIPT: &str = include_str!("scripts/polars_runner.py");
const POLARS_DEBUG_SCRIPT: &str = include_str!("scripts/polars_debug.py");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SuiteConfig;
    use std::path::PathBuf;

    #[test]
    fn parses_queries_split_by_semicolon() {
        let queries = parse_queries("select 1;\nselect 2;\n");
        assert_eq!(queries, vec!["select 1;", "select 2;"]);
    }

    #[test]
    fn builds_duckdb_s3_setup_sql() {
        let suite = SuiteConfig {
            queries_file: PathBuf::from("queries.sql"),
            output_dir: PathBuf::from("results/local"),
            profiling_mode: "json".to_string(),
            warm_runs: 1,
            cold_runs: 1,
            setup_sql: Vec::new(),
            selected_queries: Vec::new(),
            s3: Some(crate::config::S3Config {
                access_key_id: "AKIA_TEST".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: Some("token".to_string()),
                region: Some("us-east-1".to_string()),
                endpoint: Some("http://minio:9000".to_string()),
                url_style: Some(crate::config::S3UrlStyle::Path),
                use_ssl: Some(false),
            }),
            warehouse: None,
        };

        let sql = duckdb_s3_setup_sql(&suite);
        assert!(sql.iter().any(|item| item.contains("s3_access_key_id")));
        assert!(sql.iter().any(|item| item.contains("s3_secret_access_key")));
        assert!(sql.iter().any(|item| item.contains("s3_session_token")));
        assert!(sql.iter().any(|item| item.contains("s3_region")));
        assert!(sql.iter().any(|item| item.contains("s3_endpoint")));
        assert!(sql.iter().any(|item| item.contains("s3_url_style='path'")));
        assert!(sql.iter().any(|item| item.contains("s3_use_ssl=false")));
    }

    #[test]
    fn polars_s3_storage_options_include_explicit_keys() {
        let suite = SuiteConfig {
            queries_file: PathBuf::from("queries.sql"),
            output_dir: PathBuf::from("results/local"),
            profiling_mode: "json".to_string(),
            warm_runs: 1,
            cold_runs: 1,
            setup_sql: Vec::new(),
            selected_queries: Vec::new(),
            s3: Some(crate::config::S3Config {
                access_key_id: "AKIA_TEST".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                region: Some("eu-west-1".to_string()),
                endpoint: Some("http://localhost:9000".to_string()),
                url_style: Some(crate::config::S3UrlStyle::Path),
                use_ssl: Some(false),
            }),
            warehouse: None,
        };

        let options = polars_s3_storage_options(&suite).unwrap();
        assert_eq!(options.get("aws_access_key_id"), Some(&"AKIA_TEST".to_string()));
        assert_eq!(
            options.get("aws_secret_access_key"),
            Some(&"secret".to_string())
        );
        assert_eq!(options.get("region"), Some(&"eu-west-1".to_string()));
        assert_eq!(
            options.get("endpoint"),
            Some(&"http://localhost:9000".to_string())
        );
        assert_eq!(
            options.get("virtual_hosted_style_request"),
            Some(&"false".to_string())
        );
        assert_eq!(options.get("allow_http"), Some(&"true".to_string()));
    }

    #[test]
    fn redacts_sensitive_s3_sql_from_rendered_command() {
        let rendered = sanitize_command_arg("SET s3_secret_access_key='super-secret'");
        assert_eq!(rendered, "SET s3_secret_access_key='<redacted>'");
    }

    #[test]
    fn keeps_sensitive_s3_redaction_targeted() {
        let rendered = sanitize_command_arg("SET s3_region='us-east-1'");
        assert_eq!(rendered, "SET s3_region='us-east-1'");
    }

    #[test]
    fn colorize_supports_bold_codes() {
        let rendered = colorize("hello", AnsiColor::CyanBold);
        assert_eq!(rendered, "\u{1b}[1;36mhello\u{1b}[0m");
    }

    #[test]
    fn highlight_sql_colors_keywords() {
        use crate::display::highlight_sql;
        let rendered = highlight_sql("SELECT count(*) FROM hits WHERE UserID = 42");
        assert!(rendered.contains("\u{1b}[1;34mSELECT\u{1b}[0m"));
        assert!(rendered.contains("\u{1b}[1;34mFROM\u{1b}[0m"));
        assert!(rendered.contains("\u{1b}[32m42\u{1b}[0m"));
    }

    #[test]
    fn polars_debug_script_includes_helper_history() {
        let suite = SuiteConfig {
            queries_file: PathBuf::from("queries.sql"),
            output_dir: PathBuf::from("results/local"),
            profiling_mode: "json".to_string(),
            warm_runs: 1,
            cold_runs: 1,
            setup_sql: Vec::new(),
            selected_queries: Vec::new(),
            s3: Some(crate::config::S3Config {
                access_key_id: "AKIA_TEST".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                region: Some("eu-west-1".to_string()),
                endpoint: None,
                url_style: None,
                use_ssl: Some(true),
            }),
            warehouse: None,
        };
        let scenario = ScenarioConfig {
            name: "debug".to_string(),
            engine: "polars".to_string(),
            dataset: "sample".to_string(),
            warm_runs: None,
            cold_runs: None,
            setup_sql: Vec::new(),
            tags: Vec::new(),
        };
        let engine = EngineConfig {
            name: "polars".to_string(),
            backend: EngineBackend::Polars,
            query_execution_mode: QueryExecutionMode::Direct,
            binary: PathBuf::from("python3"),
            args: Vec::new(),
            env: HashMap::new().into_iter().collect(),
            setup_sql: Vec::new(),
            preload_sql: Vec::new(),
            database: ":memory:".to_string(),
            table_name: "hits".to_string(),
            polars_collect_engine: Some("auto".to_string()),
            polars_preload: None,
            polars_raise_on_fail: false,
            preload_warms_gpu: None,
        };
        let dataset = DatasetConfig {
            name: "sample".to_string(),
            table_sql: "SELECT 1".to_string(),
            polars_lazyframe_py: Some("pl.LazyFrame({'x':[1]})".to_string()),
            polars_source: None,
            polars_format: None,
            sample: None,
            setup_sql: Vec::new(),
        };
        let bound = BoundScenario {
            scenario: &scenario,
            engine: &engine,
            dataset: &dataset,
        };

        // The debug script is now a static file; verify it contains the expected helpers.
        let script = POLARS_DEBUG_SCRIPT;
        assert!(script.contains("def run():"));
        assert!(script.contains("def profile("));
        assert!(script.contains("readline.add_history(\"run()\")"));
        assert!(script.contains("_credential_provider_fn"));
        assert!(script.contains("AWS_REGION"));
        assert!(script.contains("_can_use_as_provider"));
        assert!(script.contains("lakebench polars debug env:"));
        assert!(script.contains("AWS_EC2_METADATA_DISABLED"));
        // Suppress unused-variable warnings for the test fixtures no longer needed.
        let _ = (&suite, &bound);
    }

    #[test]
    fn sampled_polars_expr_prefers_format_aware_scan() {
        let dataset = DatasetConfig {
            name: "iceberg".to_string(),
            table_sql: "SELECT 1".to_string(),
            polars_lazyframe_py: None,
            polars_source: Some("s3://bucket/path/metadata/00001.metadata.json".to_string()),
            polars_format: Some(PolarsFormat::Iceberg),
            sample: None,
            setup_sql: Vec::new(),
        };

        let expr = sampled_polars_expr(&dataset, Some(10)).unwrap();
        assert!(expr.contains("lakebench_scan(\"iceberg\""));
        assert!(expr.contains(".limit(10)"));
    }

    #[test]
    fn ensure_sql_terminator_adds_missing_semicolon() {
        assert_eq!(ensure_sql_terminator("LOAD httpfs"), "LOAD httpfs;");
        assert_eq!(ensure_sql_terminator("SELECT 1;"), "SELECT 1;");
    }

    #[test]
    fn wraps_duckdb_query_for_gpu_execution_mode() {
        let mut engine = EngineConfig {
            name: "siriusdb".to_string(),
            backend: EngineBackend::Duckdb,
            query_execution_mode: QueryExecutionMode::GpuExecution,
            binary: PathBuf::from("duckdb"),
            args: Vec::new(),
            env: Default::default(),
            setup_sql: Vec::new(),
            preload_sql: Vec::new(),
            database: ":memory:".to_string(),
            table_name: "hits".to_string(),
            polars_collect_engine: None,
            polars_preload: None,
            polars_raise_on_fail: false,
            preload_warms_gpu: None,
        };

        assert_eq!(
            duckdb_query_sql_for_engine(&engine, "SELECT 'gpu' AS mode;"),
            "CALL gpu_execution('SELECT ''gpu'' AS mode');"
        );

        engine.query_execution_mode = QueryExecutionMode::Direct;
        assert_eq!(
            duckdb_query_sql_for_engine(&engine, "SELECT 1;"),
            "SELECT 1;"
        );
    }
}
