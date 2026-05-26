use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};

use crate::config::{Config, DatasetConfig, EngineBackend, EngineConfig, ScenarioConfig, SuiteConfig};
use crate::runner::{apply_s3_env, duckdb_s3_setup_sql, parse_queries};

#[derive(Debug, Clone)]
struct BoundScenario<'a> {
    scenario: &'a ScenarioConfig,
    engine: &'a EngineConfig,
    dataset: &'a DatasetConfig,
}

#[derive(Debug, Clone)]
struct SchemaColumn {
    name: String,
    data_type: String,
}

#[derive(Debug, Clone)]
struct ProbeEstimate {
    query_index: usize,
    columns: Vec<SchemaColumn>,
    sample_rows: usize,
    avg_row_bytes: f64,
    full_rows: usize,
    raw_full_bytes: f64,
    working_set_bytes: f64,
}

pub struct SampleEstimate {
    pub sample_rows: usize,
    pub limiting_query_index: usize,
    pub avg_row_bytes: f64,
    pub overhead_multiplier: f64,
}

pub fn probe_capacity_command(
    config_path: &Path,
    scenario_name: &str,
    query_index: Option<usize>,
    probe_rows: usize,
    overhead_multiplier: f64,
) -> Result<()> {
    let config_text = fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config {}", config_path.display()))?;
    let config: Config = toml::from_str(&config_text)
        .with_context(|| format!("failed to parse config {}", config_path.display()))?;

    if probe_rows == 0 {
        bail!("probe_rows must be greater than 0");
    }
    if overhead_multiplier <= 0.0 {
        bail!("overhead_multiplier must be greater than 0");
    }

    let queries = parse_queries(
        &fs::read_to_string(&config.suite.queries_file).with_context(|| {
            format!(
                "failed to read queries file {}",
                config.suite.queries_file.display()
            )
        })?,
    );

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
    let scenario = config
        .scenarios
        .iter()
        .find(|scenario| scenario.name == scenario_name)
        .ok_or_else(|| anyhow!("unknown scenario {}", scenario_name))?;
    let bound = bind_scenario(scenario, &engine_map, &dataset_map)?;

    let probe_bound = bind_probe_scenario(&config, &bound)?;

    let probe_dir = config
        .suite
        .output_dir
        .join("probe-capacity")
        .join(&bound.scenario.name);
    fs::create_dir_all(&probe_dir)
        .with_context(|| format!("failed to create {}", probe_dir.display()))?;

    let schema = load_schema(&config.suite, &probe_bound, &probe_dir)?;
    let full_rows = load_total_rows(&config.suite, &probe_bound, &probe_dir)?;

    let selected_queries = match query_index {
        Some(index) => vec![index],
        None => (1..=queries.len()).collect::<Vec<_>>(),
    };

    let mut estimates = Vec::new();
    for query_index in selected_queries {
        let query_sql = queries
            .get(query_index - 1)
            .ok_or_else(|| anyhow!("query {} is out of range", query_index))?;
        let referenced_columns = referenced_columns(query_sql, &schema);
        let columns = if referenced_columns.is_empty() {
            schema.clone()
        } else {
            referenced_columns
        };
        let sample_rows_observed =
            load_sample_row_count(&config.suite, &probe_bound, &probe_dir, &columns, probe_rows, query_index)?;
        let approx_payload_bytes =
            load_approx_payload_bytes(&config.suite, &probe_bound, &probe_dir, &columns, probe_rows, query_index)?;
        let avg_row_bytes = if sample_rows_observed > 0 {
            approx_payload_bytes / sample_rows_observed as f64
        } else {
            0.0
        };
        let raw_full_bytes = avg_row_bytes * full_rows as f64;
        let working_set_bytes = raw_full_bytes * overhead_multiplier;

        estimates.push(ProbeEstimate {
            query_index,
            columns,
            sample_rows: sample_rows_observed,
            avg_row_bytes,
            full_rows,
            raw_full_bytes,
            working_set_bytes,
        });
    }

    let report = render_probe_report(
        &probe_bound,
        query_index,
        probe_rows,
        overhead_multiplier,
        &estimates,
    );
    println!("{report}");

    let report_path = probe_dir.join("capacity-report.md");
    fs::write(&report_path, &report)
        .with_context(|| format!("failed to write {}", report_path.display()))?;

    Ok(())
}

pub fn estimate_sample_rows_for_scenario(
    config: &Config,
    scenario_name: &str,
    query_indices: &[usize],
) -> Result<SampleEstimate> {
    let queries = parse_queries(
        &fs::read_to_string(&config.suite.queries_file).with_context(|| {
            format!(
                "failed to read queries file {}",
                config.suite.queries_file.display()
            )
        })?,
    );

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
    let scenario = config
        .scenarios
        .iter()
        .find(|scenario| scenario.name == scenario_name)
        .ok_or_else(|| anyhow!("unknown scenario {}", scenario_name))?;
    let bound = bind_scenario(scenario, &engine_map, &dataset_map)?;
    let sample = bound
        .dataset
        .sample
        .as_ref()
        .ok_or_else(|| anyhow!("dataset {} has no sample config", bound.dataset.name))?;
    let target_bytes = sample.target_bytes.ok_or_else(|| {
        anyhow!(
            "dataset {} does not define sample.target_bytes",
            bound.dataset.name
        )
    })?;

    let probe_bound = bind_probe_scenario(config, &bound)?;

    let probe_dir = config
        .suite
        .output_dir
        .join("probe-capacity")
        .join(&bound.scenario.name);
    fs::create_dir_all(&probe_dir)
        .with_context(|| format!("failed to create {}", probe_dir.display()))?;

    let schema = load_schema(&config.suite, &probe_bound, &probe_dir)?;

    let selected_queries = if query_indices.is_empty() {
        (1..=queries.len()).collect::<Vec<_>>()
    } else {
        query_indices.to_vec()
    };

    let mut worst: Option<SampleEstimate> = None;
    for query_index in selected_queries {
        let query_sql = queries
            .get(query_index - 1)
            .ok_or_else(|| anyhow!("query {} is out of range", query_index))?;
        let referenced_columns = referenced_columns(query_sql, &schema);
        let columns = if referenced_columns.is_empty() {
            schema.clone()
        } else {
            referenced_columns
        };
        let sample_rows_observed = load_sample_row_count(
            &config.suite,
            &probe_bound,
            &probe_dir,
            &columns,
            sample.probe_rows,
            query_index,
        )?;
        let approx_payload_bytes = load_approx_payload_bytes(
            &config.suite,
            &probe_bound,
            &probe_dir,
            &columns,
            sample.probe_rows,
            query_index,
        )?;
        let avg_row_bytes = if sample_rows_observed > 0 {
            approx_payload_bytes / sample_rows_observed as f64
        } else {
            0.0
        };
        let sample_rows = if avg_row_bytes > 0.0 {
            ((target_bytes as f64) / (avg_row_bytes * sample.overhead_multiplier)).floor() as usize
        } else {
            0
        };

        let candidate = SampleEstimate {
            sample_rows,
            limiting_query_index: query_index,
            avg_row_bytes,
            overhead_multiplier: sample.overhead_multiplier,
        };
        match &worst {
            Some(existing) if candidate.sample_rows >= existing.sample_rows => {}
            _ => worst = Some(candidate),
        }
    }

    worst.ok_or_else(|| anyhow!("no queries were available for byte-driven sampling"))
}

fn bind_probe_scenario<'a>(
    config: &'a Config,
    bound: &BoundScenario<'a>,
) -> Result<BoundScenario<'a>> {
    if bound.engine.backend == EngineBackend::Duckdb {
        return Ok(bound.clone());
    }

    let probe_engine = config
        .engines
        .iter()
        .find(|engine| engine.backend == EngineBackend::Duckdb)
        .ok_or_else(|| {
            anyhow!(
                "scenario {} uses {:?}, and no duckdb-compatible engine is available for byte-driven probing",
                bound.scenario.name,
                bound.engine.backend
            )
        })?;

    Ok(BoundScenario {
        scenario: bound.scenario,
        engine: probe_engine,
        dataset: bound.dataset,
    })
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

fn load_schema(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    probe_dir: &Path,
) -> Result<Vec<SchemaColumn>> {
    let schema_path = probe_dir.join("schema.csv");
    let sql = format!(
        "COPY (DESCRIBE SELECT * FROM {} LIMIT 0) TO '{}' (FORMAT CSV, HEADER)",
        hits_relation_sql(bound),
        escape_sql_string(&schema_path.to_string_lossy())
    );
    run_duckdb_copy(suite, bound, &[sql])?;
    parse_schema_csv(&schema_path)
}

fn load_total_rows(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    probe_dir: &Path,
) -> Result<usize> {
    let count_path = probe_dir.join("row-count.csv");
    let sql = format!(
        "COPY (SELECT COUNT(*) AS total_rows FROM {}) TO '{}' (FORMAT CSV, HEADER)",
        hits_relation_sql(bound),
        escape_sql_string(&count_path.to_string_lossy())
    );
    run_duckdb_copy(suite, bound, &[sql])?;
    let csv = fs::read_to_string(&count_path)
        .with_context(|| format!("failed to read {}", count_path.display()))?;
    let value = csv
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow!("unexpected row-count output in {}", count_path.display()))?;
    value
        .trim()
        .parse::<usize>()
        .with_context(|| format!("failed to parse row count from {}", count_path.display()))
}

fn load_sample_row_count(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    probe_dir: &Path,
    columns: &[SchemaColumn],
    probe_rows: usize,
    query_index: usize,
) -> Result<usize> {
    let path = probe_dir.join(format!("q{:02}-sample-rows.csv", query_index));
    let select_list = projection_list(columns);
    let sql = format!(
        "COPY (SELECT COUNT(*) AS sample_rows FROM (SELECT {} FROM {} LIMIT {}) AS probe) TO '{}' (FORMAT CSV, HEADER)",
        select_list,
        hits_relation_sql(bound),
        probe_rows,
        escape_sql_string(&path.to_string_lossy())
    );
    run_duckdb_copy(suite, bound, &[sql])?;
    let csv =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let value = csv
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow!("unexpected sample row output in {}", path.display()))?;
    value
        .trim()
        .parse::<usize>()
        .with_context(|| format!("failed to parse sample row count from {}", path.display()))
}

fn load_approx_payload_bytes(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    probe_dir: &Path,
    columns: &[SchemaColumn],
    probe_rows: usize,
    query_index: usize,
) -> Result<f64> {
    let path = probe_dir.join(format!("q{:02}-approx-bytes.csv", query_index));
    let select_list = projection_list(columns);
    let sql = format!(
        "COPY (WITH probe AS (SELECT {} FROM {} LIMIT {}) SELECT {} AS approx_payload_bytes FROM probe) TO '{}' (FORMAT CSV, HEADER)",
        select_list,
        hits_relation_sql(bound),
        probe_rows,
        aggregate_payload_expr(columns),
        escape_sql_string(&path.to_string_lossy())
    );
    run_duckdb_copy(suite, bound, &[sql])?;
    let csv =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    let value = csv
        .lines()
        .nth(1)
        .ok_or_else(|| anyhow!("unexpected approx bytes output in {}", path.display()))?;
    value
        .trim()
        .parse::<f64>()
        .with_context(|| format!("failed to parse approx bytes from {}", path.display()))
}

fn render_probe_report(
    bound: &BoundScenario<'_>,
    query_index: Option<usize>,
    probe_rows: usize,
    overhead_multiplier: f64,
    estimates: &[ProbeEstimate],
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Scenario: {}", bound.scenario.name));
    lines.push(format!("Engine: {}", bound.engine.name));
    lines.push(format!("Dataset: {}", bound.dataset.name));
    lines.push(format!("Probe rows: {}", probe_rows));
    lines.push(format!("Overhead multiplier: {:.2}x", overhead_multiplier));
    lines.push(String::new());

    if let Some(query_index) = query_index {
        lines.push(format!("Capacity estimate for q{}:", query_index));
    } else {
        lines.push("Capacity estimate across selected queries:".to_string());
    }

    for estimate in estimates {
        lines.push(String::new());
        lines.push(format!("  Query: q{}", estimate.query_index));
        lines.push(format!(
            "  Referenced columns: {}",
            estimate
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
        lines.push(format!("  Sample rows observed: {}", estimate.sample_rows));
        lines.push(format!(
            "  Avg row bytes: {}",
            format_bytes(estimate.avg_row_bytes)
        ));
        lines.push(format!(
            "  Estimated raw dataset bytes: {}",
            format_bytes(estimate.raw_full_bytes)
        ));
        lines.push(format!(
            "  Estimated working set bytes: {}",
            format_bytes(estimate.working_set_bytes)
        ));
        lines.push(format!("  Total rows in dataset: {}", estimate.full_rows));
        lines.push("  Recommended max sample rows:".to_string());
        for budget_gb in [8_u64, 16, 24, 48] {
            let budget_bytes = budget_gb as f64 * 1024.0 * 1024.0 * 1024.0;
            let rows = if estimate.avg_row_bytes > 0.0 {
                (budget_bytes / (estimate.avg_row_bytes * overhead_multiplier)).floor() as u64
            } else {
                0
            };
            lines.push(format!("    {} GB GPU: {} rows", budget_gb, rows));
        }
    }

    if estimates.len() > 1 {
        let worst = estimates
            .iter()
            .max_by(|left, right| left.working_set_bytes.total_cmp(&right.working_set_bytes));
        if let Some(worst) = worst {
            lines.push(String::new());
            lines.push(format!(
                "Worst-case query: q{} at {}",
                worst.query_index,
                format_bytes(worst.working_set_bytes)
            ));
        }
    }

    lines.join("\n")
}

fn referenced_columns(query_sql: &str, schema: &[SchemaColumn]) -> Vec<SchemaColumn> {
    let tokens = tokenize_identifiers(query_sql);
    let mut columns = Vec::new();
    for column in schema {
        if tokens.contains(&column.name.to_ascii_lowercase()) {
            columns.push(column.clone());
        }
    }
    columns
}

fn tokenize_identifiers(text: &str) -> HashSet<String> {
    let mut tokens = HashSet::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            current.push(ch.to_ascii_lowercase());
        } else if !current.is_empty() {
            tokens.insert(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        tokens.insert(current);
    }
    tokens
}

fn projection_list(columns: &[SchemaColumn]) -> String {
    columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn aggregate_payload_expr(columns: &[SchemaColumn]) -> String {
    columns
        .iter()
        .map(column_payload_expr)
        .collect::<Vec<_>>()
        .join(" + ")
}

fn column_payload_expr(column: &SchemaColumn) -> String {
    let quoted = quote_identifier(&column.name);
    let type_name = column.data_type.to_ascii_uppercase();

    if is_variable_width_type(&type_name) {
        format!(
            "COALESCE(SUM(CASE WHEN {quoted} IS NULL THEN 0 ELSE OCTET_LENGTH(CAST({quoted} AS BLOB)) + 8 END), 0)"
        )
    } else {
        let width = fixed_width_bytes(&type_name).unwrap_or(16);
        format!("COALESCE(SUM(CASE WHEN {quoted} IS NULL THEN 0 ELSE {width} END), 0)")
    }
}

fn fixed_width_bytes(type_name: &str) -> Option<usize> {
    if type_name.starts_with("BOOLEAN") || type_name.starts_with("TINYINT") {
        Some(1)
    } else if type_name.starts_with("SMALLINT") {
        Some(2)
    } else if type_name.starts_with("INTEGER")
        || type_name.starts_with("DATE")
        || type_name.starts_with("FLOAT")
        || type_name.starts_with("REAL")
    {
        Some(4)
    } else if type_name.starts_with("BIGINT")
        || type_name.starts_with("DOUBLE")
        || type_name.starts_with("TIMESTAMP")
        || type_name.starts_with("TIME")
        || type_name.starts_with("INTERVAL")
    {
        Some(8)
    } else if type_name.starts_with("HUGEINT")
        || type_name.starts_with("UUID")
        || type_name.starts_with("DECIMAL")
    {
        Some(16)
    } else {
        None
    }
}

fn is_variable_width_type(type_name: &str) -> bool {
    type_name.starts_with("VARCHAR")
        || type_name.starts_with("BLOB")
        || type_name.starts_with("JSON")
        || type_name.starts_with("LIST")
        || type_name.starts_with("MAP")
        || type_name.starts_with("STRUCT")
        || type_name.starts_with("UNION")
        || type_name.starts_with("ARRAY")
}

fn parse_schema_csv(path: &Path) -> Result<Vec<SchemaColumn>> {
    let text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut rows = text.lines();
    let _header = rows
        .next()
        .ok_or_else(|| anyhow!("schema output {} was empty", path.display()))?;
    let mut columns = Vec::new();
    for line in rows {
        if line.trim().is_empty() {
            continue;
        }
        let parts = parse_csv_line(line);
        if parts.len() < 2 {
            continue;
        }
        columns.push(SchemaColumn {
            name: parts[0].clone(),
            data_type: parts[1].clone(),
        });
    }
    Ok(columns)
}

fn parse_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    current.push('"');
                    let _ = chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                values.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    values.push(current);
    values
}

fn run_duckdb_copy(
    suite: &SuiteConfig,
    bound: &BoundScenario<'_>,
    extra_sql: &[String],
) -> Result<()> {
    let mut command = Command::new(&bound.engine.binary);
    command.arg(&bound.engine.database);
    for arg in &bound.engine.args {
        command.arg(arg);
    }
    for sql in &suite.setup_sql {
        command.arg("-c").arg(sql);
    }
    for sql in duckdb_s3_setup_sql(suite) {
        command.arg("-c").arg(sql);
    }
    for sql in &bound.engine.setup_sql {
        command.arg("-c").arg(sql);
    }
    for sql in &bound.dataset.setup_sql {
        command.arg("-c").arg(sql);
    }
    for sql in &bound.scenario.setup_sql {
        command.arg("-c").arg(sql);
    }
    for sql in extra_sql {
        command.arg("-c").arg(sql);
    }
    for (key, value) in &bound.engine.env {
        command.env(key, value);
    }
    apply_s3_env(&mut command, suite.s3.as_ref());

    let output = command.output().with_context(|| {
        format!(
            "failed to launch {} for capacity probe on scenario {}",
            bound.engine.binary.display(),
            bound.scenario.name
        )
    })?;
    if !output.status.success() {
        bail!(
            "capacity probe failed for scenario {}:\nstdout:\n{}\nstderr:\n{}",
            bound.scenario.name,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

fn hits_relation_sql(bound: &BoundScenario<'_>) -> String {
    format!(
        "({}) AS {}",
        bound.dataset.table_sql,
        quote_identifier(&bound.engine.table_name)
    )
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn tokenizes_identifiers() {
        let tokens = tokenize_identifiers("SELECT UserID, COUNT(*) FROM hits WHERE URL LIKE '%x%'");
        assert!(tokens.contains("userid"));
        assert!(tokens.contains("url"));
    }

    #[test]
    fn parses_csv_with_quotes() {
        let values = parse_csv_line("\"UserID\",\"VARCHAR\",YES");
        assert_eq!(values[0], "UserID");
        assert_eq!(values[1], "VARCHAR");
    }

    #[test]
    fn polars_scenario_uses_duckdb_probe_engine() {
        let config: Config = toml::from_str(
            r#"
            [suite]
            queries_file = "queries.sql"
            output_dir = "results/local"

            [[engines]]
            name = "duckdb"
            backend = "duckdb"
            binary = "duckdb"

            [[engines]]
            name = "polars-gpu"
            backend = "polars"
            binary = "python3"

            [[datasets]]
            name = "sampled"
            table_sql = "SELECT 1 AS x"
            polars_lazyframe_py = "pl.LazyFrame({'x': [1]})"
            sample = { target_bytes = "2GiB" }

            [[scenarios]]
            name = "polars-scenario"
            engine = "polars-gpu"
            dataset = "sampled"
            "#,
        )
        .expect("config should parse");

        let engine_map: HashMap<_, _> = config
            .engines
            .iter()
            .map(|engine| (engine.name.as_str(), engine))
            .collect();
        let dataset_map: HashMap<_, _> = config
            .datasets
            .iter()
            .map(|dataset| (dataset.name.as_str(), dataset))
            .collect();
        let scenario = config
            .scenarios
            .iter()
            .find(|scenario| scenario.name == "polars-scenario")
            .unwrap();
        let bound = bind_scenario(scenario, &engine_map, &dataset_map).unwrap();
        let probe_bound = bind_probe_scenario(&config, &bound).unwrap();

        assert_eq!(probe_bound.engine.name, "duckdb");
    }
}
