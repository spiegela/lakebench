use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::json;

use crate::config::{Config, EngineBackend, ProfileTemperature, QueryExecution, SuiteResults};
use crate::runner::apply_s3_env;

const QLENS_UPLOAD_SCRIPT: &str = include_str!("scripts/qlens_runs_upload.py");
const MAX_EXPERIMENT_ID_LEN: usize = 48;

#[derive(Debug, Serialize)]
struct WarehouseRow {
    experiment_id: String,
    run_id: String,
    table_details: String,
    query: String,
    ts_start: i64,
    ts_end: i64,
    execution_time_ms: i64,
    success: bool,
    error_message: String,
    spark_conf: String,
    runtime_info: String,
    query_index: i32,
    temperature: String,
    query_name: String,
}

pub fn upload_command(config_path: &Path, results_override: Option<&Path>) -> Result<()> {
    let config = load_config(config_path)?;
    let results_path = match results_override {
        Some(path) => path.to_path_buf(),
        None => config.suite.output_dir.join("results.json"),
    };
    let suite_results = load_results(&results_path)?;
    let uploaded = upload_suite_results(&config, &suite_results)?;
    println!(
        "warehouse: uploaded {} qlens_runs row(s) to {}",
        uploaded,
        config
            .suite
            .warehouse
            .as_ref()
            .map(|w| w.qlens_runs_table_path.as_str())
            .unwrap_or("<warehouse disabled>")
    );
    Ok(())
}

pub fn maybe_upload_suite_results(config: &Config, results: &SuiteResults) -> Result<()> {
    let Some(warehouse) = config.suite.warehouse.as_ref() else {
        return Ok(());
    };
    if !warehouse.enabled {
        return Ok(());
    }
    let uploaded = upload_suite_results(config, results)?;
    println!(
        "warehouse: uploaded {} qlens_runs row(s) to {}",
        uploaded, warehouse.qlens_runs_table_path
    );
    Ok(())
}

fn upload_suite_results(config: &Config, results: &SuiteResults) -> Result<usize> {
    let warehouse = config
        .suite
        .warehouse
        .as_ref()
        .ok_or_else(|| anyhow!("suite.warehouse is not configured"))?;

    let engine_backends: HashMap<_, _> = config
        .engines
        .iter()
        .map(|engine| (engine.name.as_str(), engine.backend))
        .collect();

    let rows = build_rows(config, results, &engine_backends)?;
    if rows.is_empty() {
        return Ok(0);
    }

    let upload_dir = config.suite.output_dir.join("warehouse-upload");
    fs::create_dir_all(&upload_dir)
        .with_context(|| format!("failed to create {}", upload_dir.display()))?;

    let rows_path = upload_dir.join("qlens_runs_rows.json");
    let script_path = upload_dir.join("qlens_runs_upload.py");
    fs::write(&rows_path, serde_json::to_vec_pretty(&rows)?)
        .with_context(|| format!("failed to write {}", rows_path.display()))?;
    fs::write(&script_path, QLENS_UPLOAD_SCRIPT)
        .with_context(|| format!("failed to write {}", script_path.display()))?;

    let mut command = Command::new(&warehouse.python);
    command
        .arg(&script_path)
        .arg(&warehouse.qlens_runs_table_path)
        .arg(&rows_path);

    let upload_s3 = warehouse.s3.as_ref().or(config.suite.s3.as_ref());
    apply_s3_env(&mut command, upload_s3);

    let output = command.output().with_context(|| {
        format!(
            "failed to launch warehouse uploader {}",
            warehouse.python.display()
        )
    })?;
    if !output.status.success() {
        bail!(
            "warehouse upload failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    if !output.stdout.is_empty() {
        print!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    Ok(rows.len())
}

fn build_rows(
    config: &Config,
    results: &SuiteResults,
    engine_backends: &HashMap<&str, EngineBackend>,
) -> Result<Vec<WarehouseRow>> {
    let mut rows = Vec::new();
    for execution in &results.runs {
        let scenario_experiment_id = experiment_id_for(config, execution)?;
        let backend = engine_backends
            .get(execution.engine.as_str())
            .copied()
            .unwrap_or(EngineBackend::Duckdb);
        let backend_name = match backend {
            EngineBackend::Duckdb => "duckdb",
            EngineBackend::Polars => "polars",
        };

        for warm_run in &execution.warm_runs {
            let temperature = match warm_run.effective_temperature() {
                ProfileTemperature::Cold => "cold",
                ProfileTemperature::Warm => "warm",
            };
            let execution_time_ms =
                infer_execution_time_ms(warm_run, execution) as i64;
            let ts_end = infer_end_timestamp_ms(warm_run, execution)?;
            let ts_start = ts_end.saturating_sub(execution_time_ms);
            let success = warm_run.profile_path.exists() && warm_run.profile_summary.is_some();
            let query_name = format!("q{:02}", execution.query_index);

            let table_details = json!({
                "engine": execution.engine,
                "dataset": execution.dataset,
                "scenario": execution.scenario,
                "tags": execution.tags,
                "queries_file": results.queries_file.display().to_string(),
            })
            .to_string();

            let spark_conf = json!({
                "lakebench.engine": execution.engine,
                "lakebench.backend": backend_name,
                "lakebench.dataset": execution.dataset,
                "lakebench.scenario": execution.scenario,
                "lakebench.output_dir": config.suite.output_dir.display().to_string(),
            })
            .to_string();

            let runtime_info = json!({
                "runner": "lakebench",
                "engine": execution.engine,
                "backend": backend_name,
                "dataset": execution.dataset,
                "scenario": execution.scenario,
                "temperature": temperature,
                "spark_user": std::env::var("USER").ok(),
                "app_name": format!("lakebench-{}", execution.scenario),
                "platform": format!("{}-{}", std::env::consts::OS, std::env::consts::ARCH),
                "osName": std::env::consts::OS,
                "qlens_version": format!("lakebench-{}", env!("CARGO_PKG_VERSION")),
            })
            .to_string();

            rows.push(WarehouseRow {
                experiment_id: scenario_experiment_id.clone(),
                run_id: format!(
                    "{}-q{:02}-cold{:02}-run{:02}",
                    sanitize_identifier(&execution.scenario),
                    execution.query_index,
                    execution.cold_run,
                    warm_run.warm_run
                ),
                table_details,
                query: execution.query_sql.clone(),
                ts_start,
                ts_end,
                execution_time_ms,
                success,
                error_message: if success {
                    String::new()
                } else {
                    summarize_error(&execution.process.stderr_path)
                },
                spark_conf,
                runtime_info,
                query_index: execution.query_index as i32,
                temperature: temperature.to_string(),
                query_name,
            });
        }
    }
    Ok(rows)
}

fn experiment_id_for(config: &Config, execution: &QueryExecution) -> Result<String> {
    let warehouse = config
        .suite
        .warehouse
        .as_ref()
        .ok_or_else(|| anyhow!("suite.warehouse is not configured"))?;
    let output_name = config
        .suite
        .output_dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("lakebench");
    let rendered = warehouse
        .experiment_id_template
        .replace("{output}", output_name)
        .replace("{scenario}", &execution.scenario)
        .replace("{engine}", &execution.engine)
        .replace("{dataset}", &execution.dataset);
    Ok(truncate_identifier(&sanitize_identifier(&rendered)))
}

fn sanitize_identifier(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut last_dash = false;
    for ch in value.chars() {
        let keep = ch.is_ascii_alphanumeric();
        if keep {
            out.push(ch.to_ascii_lowercase());
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

fn truncate_identifier(value: &str) -> String {
    if value.len() <= MAX_EXPERIMENT_ID_LEN {
        return value.to_string();
    }
    value.chars().take(MAX_EXPERIMENT_ID_LEN).collect()
}

fn infer_execution_time_ms(warm_run: &crate::config::WarmRunResult, execution: &QueryExecution) -> u64 {
    if let Some(summary) = &warm_run.profile_summary {
        if let Some(latency) = summary.latency_seconds {
            return (latency * 1000.0).round() as u64;
        }
    }
    let runs = execution.warm_runs.len().max(1) as f64;
    (execution.process.elapsed_seconds * 1000.0 / runs).round() as u64
}

fn infer_end_timestamp_ms(
    warm_run: &crate::config::WarmRunResult,
    execution: &QueryExecution,
) -> Result<i64> {
    for path in [&warm_run.profile_path, &execution.process.stderr_path, &execution.process.stdout_path] {
        if let Ok(metadata) = fs::metadata(path) {
            if let Ok(modified) = metadata.modified() {
                return Ok(system_time_ms(modified));
            }
        }
    }
    Ok(system_time_ms(SystemTime::now()))
}

fn system_time_ms(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn summarize_error(stderr_path: &Path) -> String {
    match fs::read_to_string(stderr_path) {
        Ok(text) => text
            .lines()
            .rev()
            .find(|line| !line.trim().is_empty())
            .unwrap_or("execution failed")
            .trim()
            .to_string(),
        Err(_) => "execution failed".to_string(),
    }
}

fn load_config(path: &Path) -> Result<Config> {
    let config_text = fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn load_results(path: &Path) -> Result<SuiteResults> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("failed to parse {}", path.display()))
}
