use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

use crate::config::{ProfileTemperature, SuiteResults};

#[derive(Debug, Default)]
struct ScenarioAggregate {
    cold_latency: Vec<f64>,
    warm_latency: Vec<f64>,
    cold_cpu: Vec<f64>,
    warm_cpu: Vec<f64>,
    peak_buffer_memory: Vec<f64>,
    rows_scanned: Vec<f64>,
}

pub fn render_report_command(results_path: &Path) -> Result<()> {
    let results_text = fs::read_to_string(results_path)
        .with_context(|| format!("failed to read {}", results_path.display()))?;
    let suite_results: SuiteResults = serde_json::from_str(&results_text)
        .with_context(|| format!("failed to parse {}", results_path.display()))?;

    let report = build_report(&suite_results);
    let report_path = results_path.with_file_name("report.md");
    fs::write(&report_path, report)
        .with_context(|| format!("failed to write {}", report_path.display()))?;
    println!("report: {}", report_path.display());
    Ok(())
}

pub fn build_report(results: &SuiteResults) -> String {
    let mut lines = Vec::new();
    lines.push("# Benchmark Report".to_string());
    lines.push(String::new());
    lines.push(format!(
        "- queries file: `{}`",
        results.queries_file.display()
    ));
    lines.push(format!(
        "- result count: `{}` process executions",
        results.runs.len()
    ));
    lines.push(String::new());

    // Top-of-report cold-vs-hot summary, auto-paired by scenario name suffix.
    append_cold_vs_hot_summary(&mut lines, results);

    let mut scenario_names = results
        .runs
        .iter()
        .map(|run| run.scenario.clone())
        .collect::<Vec<_>>();
    scenario_names.sort();
    scenario_names.dedup();

    lines.push("## Scenario Overview".to_string());
    lines.push(String::new());
    lines.push("| Scenario | Query Runs | Successful | Median Cold Latency (s) | Median Warm Latency (s) | Median CPU Time (s) |".to_string());
    lines.push("|---|---:|---:|---:|---:|---:|".to_string());

    for scenario in &scenario_names {
        let executions = results
            .runs
            .iter()
            .filter(|run| &run.scenario == scenario)
            .collect::<Vec<_>>();

        let successful = executions.iter().filter(|run| run.process.success).count();
        let mut cold_latencies = Vec::new();
        let mut warm_latencies = Vec::new();
        let mut cpu_times = Vec::new();
        for execution in executions.iter().copied() {
            for warm_run in &execution.warm_runs {
                if let Some(summary) = &warm_run.profile_summary {
                    if let Some(value) = summary.latency_seconds {
                        match warm_run.effective_temperature() {
                            ProfileTemperature::Cold => cold_latencies.push(value),
                            ProfileTemperature::Warm => warm_latencies.push(value),
                        }
                    }
                    if let Some(value) = summary.cpu_time_seconds {
                        cpu_times.push(value);
                    }
                }
            }
        }

        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            scenario,
            executions.len(),
            successful,
            format_optional(median(&cold_latencies)),
            format_optional(median(&warm_latencies)),
            format_optional(median(&cpu_times)),
        ));
    }

    // Preload-effectiveness check: for hot scenarios, did the first measured
    // iteration land close to the steady state, or is it still slow?
    append_preload_effectiveness(&mut lines, results);

    let mut query_indices = results
        .runs
        .iter()
        .map(|run| run.query_index)
        .collect::<Vec<_>>();
    query_indices.sort();
    query_indices.dedup();

    for query_index in query_indices {
        let query_runs = results
            .runs
            .iter()
            .filter(|run| run.query_index == query_index)
            .collect::<Vec<_>>();
        let query_sql = query_runs
            .first()
            .map(|run| run.query_sql.as_str())
            .unwrap_or_default();

        lines.push(String::new());
        lines.push(format!("## Query {}", query_index));
        lines.push(String::new());
        lines.push("```sql".to_string());
        lines.push(query_sql.to_string());
        lines.push("```".to_string());
        lines.push(String::new());
        lines.push("| Scenario | Cold Median (s) | Warm Median (s) | CPU Median (s) | Peak Buffer Median (bytes) | Rows Scanned Median |".to_string());
        lines.push("|---|---:|---:|---:|---:|---:|".to_string());

        let mut per_scenario = BTreeMap::<String, ScenarioAggregate>::new();
        for execution in query_runs {
            let aggregate = per_scenario.entry(execution.scenario.clone()).or_default();
            for warm_run in &execution.warm_runs {
                if let Some(summary) = &warm_run.profile_summary {
                    if let Some(value) = summary.latency_seconds {
                        match warm_run.effective_temperature() {
                            ProfileTemperature::Cold => aggregate.cold_latency.push(value),
                            ProfileTemperature::Warm => aggregate.warm_latency.push(value),
                        }
                    }
                    if let Some(value) = summary.cpu_time_seconds {
                        match warm_run.effective_temperature() {
                            ProfileTemperature::Cold => aggregate.cold_cpu.push(value),
                            ProfileTemperature::Warm => aggregate.warm_cpu.push(value),
                        }
                    }
                    if let Some(value) = summary.peak_buffer_memory_bytes {
                        aggregate.peak_buffer_memory.push(value);
                    }
                    if let Some(value) = summary.cumulative_rows_scanned {
                        aggregate.rows_scanned.push(value);
                    }
                }
            }
        }

        for (scenario, aggregate) in per_scenario {
            lines.push(format!(
                "| {} | {} | {} | {} | {} | {} |",
                scenario,
                format_optional(median(&aggregate.cold_latency)),
                format_optional(median(&aggregate.warm_latency)),
                format_optional(
                    median(&aggregate.warm_cpu).or_else(|| median(&aggregate.cold_cpu))
                ),
                format_optional(median(&aggregate.peak_buffer_memory)),
                format_optional(median(&aggregate.rows_scanned)),
            ));
        }
    }

    lines.push(String::new());
    lines.join("\n")
}

/// Auto-pairs scenarios whose names contain `-cold-` against those that
/// substitute `-hot-`, and emits a Cold-vs-Hot summary.  For each pair:
///
///   - **GPFS Cold** = geomean across queries of the per-query median of
///     `temperature=Cold` iterations (warm_run=1) in the `-cold-` scenario.
///   - **Process Warm** = geomean across queries of `temperature=Warm`
///     iterations in the `-cold-` scenario (process state but no preload).
///   - **GPU Hot** = geomean across queries of all iterations in the
///     `-hot-` scenario (preload absorbed cold).
///   - **Speedup** = GPFS Cold / GPU Hot.
fn append_cold_vs_hot_summary(lines: &mut Vec<String>, results: &SuiteResults) {
    // Discover scenario names and pair them.
    let mut names: Vec<String> =
        results.runs.iter().map(|r| r.scenario.clone()).collect();
    names.sort();
    names.dedup();

    let mut pairs: Vec<(String, String, String)> = Vec::new(); // (label, cold, hot)
    for name in &names {
        if let Some(pos) = name.find("-cold-") {
            let hot_name = format!("{}-hot-{}", &name[..pos], &name[pos + 6..]);
            if names.iter().any(|n| n == &hot_name) {
                // Derive a "family + dataset" label by stripping the cold/hot middle.
                let prefix = &name[..pos];
                let suffix = &name[pos + 6..];
                let label = format!("{prefix} / {suffix}");
                pairs.push((label, name.clone(), hot_name));
            }
        }
    }

    if pairs.is_empty() {
        return;
    }

    lines.push("## Cold vs Hot Summary".to_string());
    lines.push(String::new());
    lines.push(
        "Geomean of per-query median latency (seconds), with speedup vs GPFS cold."
            .to_string(),
    );
    lines.push(String::new());
    lines.push(
        "| Engine / Dataset | GPFS Cold (s) | Process Warm (s) | GPU Hot (s) | Hot vs Cold | Hot vs Warm |"
            .to_string(),
    );
    lines.push("|---|---:|---:|---:|---:|---:|".to_string());

    for (label, cold_name, hot_name) in pairs {
        let cold_geo = geomean_per_query(
            results,
            &cold_name,
            |t| matches!(t, ProfileTemperature::Cold),
        );
        let warm_geo = geomean_per_query(
            results,
            &cold_name,
            |t| matches!(t, ProfileTemperature::Warm),
        );
        let hot_geo = geomean_per_query(results, &hot_name, |_| true);

        let speedup_cold = match (cold_geo, hot_geo) {
            (Some(c), Some(h)) if h > 0.0 => Some(c / h),
            _ => None,
        };
        let speedup_warm = match (warm_geo, hot_geo) {
            (Some(w), Some(h)) if h > 0.0 => Some(w / h),
            _ => None,
        };

        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} |",
            label,
            format_optional(cold_geo),
            format_optional(warm_geo),
            format_optional(hot_geo),
            format_speedup(speedup_cold),
            format_speedup(speedup_warm),
        ));
    }
    lines.push(String::new());
}

/// For scenarios with a preload (heuristic: name contains "-hot-"), compare
/// the first measured warm_run of each cold_run against the rest.  If the
/// first iteration is much slower than the steady state, the preload didn't
/// load enough data and the cache is filling on the first real query.
fn append_preload_effectiveness(lines: &mut Vec<String>, results: &SuiteResults) {
    let mut hot_scenarios: Vec<String> = results
        .runs
        .iter()
        .filter(|r| r.scenario.contains("-hot-"))
        .map(|r| r.scenario.clone())
        .collect();
    hot_scenarios.sort();
    hot_scenarios.dedup();

    if hot_scenarios.is_empty() {
        return;
    }

    lines.push(String::new());
    lines.push("## Preload Effectiveness".to_string());
    lines.push(String::new());
    lines.push(
        "For hot scenarios, the first measured iteration of each cold_run vs the rest. \
         If first ≈ rest, the preload is working; if first is much slower, the preload \
         isn't loading enough data and the cache is filling on the first real query."
            .to_string(),
    );
    lines.push(String::new());
    lines.push(
        "| Scenario | First-Iter Median (s) | Steady-State Median (s) | Ratio | n_first / n_steady |"
            .to_string(),
    );
    lines.push("|---|---:|---:|---:|---:|".to_string());

    for scenario in hot_scenarios {
        let mut first_iter = Vec::new();
        let mut steady = Vec::new();
        for run in results.runs.iter().filter(|r| r.scenario == scenario) {
            for warm in &run.warm_runs {
                if let Some(summary) = &warm.profile_summary {
                    if let Some(latency) = summary.latency_seconds {
                        if warm.warm_run == 1 {
                            first_iter.push(latency);
                        } else {
                            steady.push(latency);
                        }
                    }
                }
            }
        }
        let first_med = median(&first_iter);
        let steady_med = median(&steady);
        let ratio = match (first_med, steady_med) {
            (Some(f), Some(s)) if s > 0.0 => Some(f / s),
            _ => None,
        };
        lines.push(format!(
            "| {} | {} | {} | {} | {} / {} |",
            scenario,
            format_optional(first_med),
            format_optional(steady_med),
            format_speedup(ratio),
            first_iter.len(),
            steady.len(),
        ));
    }
    lines.push(String::new());
}

/// Compute geomean across queries of the per-query median latency, filtered
/// to iterations whose effective temperature satisfies the predicate.
fn geomean_per_query<F>(
    results: &SuiteResults,
    scenario: &str,
    temperature_filter: F,
) -> Option<f64>
where
    F: Fn(ProfileTemperature) -> bool,
{
    let mut per_query: BTreeMap<usize, Vec<f64>> = BTreeMap::new();
    for run in results.runs.iter().filter(|r| r.scenario == scenario) {
        for warm in &run.warm_runs {
            if !temperature_filter(warm.effective_temperature()) {
                continue;
            }
            if let Some(summary) = &warm.profile_summary {
                if let Some(latency) = summary.latency_seconds {
                    per_query.entry(run.query_index).or_default().push(latency);
                }
            }
        }
    }
    let medians: Vec<f64> = per_query
        .values()
        .filter_map(|v| median(v))
        .filter(|v| v.is_finite() && *v > 0.0)
        .collect();
    if medians.is_empty() {
        return None;
    }
    let log_sum: f64 = medians.iter().map(|v| v.ln()).sum();
    Some((log_sum / medians.len() as f64).exp())
}

fn format_speedup(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.2}x"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    let mut copy = values.to_vec();
    copy.sort_by(f64::total_cmp);
    let mid = copy.len() / 2;
    if copy.len() % 2 == 0 {
        Some((copy[mid - 1] + copy[mid]) / 2.0)
    } else {
        Some(copy[mid])
    }
}

fn format_optional(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:.6}"))
        .unwrap_or_else(|| "n/a".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ProcessResult, QueryExecution, WarmRunResult};
    use crate::profile::ProfileSummary;
    use std::path::PathBuf;

    fn make_summary(latency: f64) -> ProfileSummary {
        ProfileSummary {
            latency_seconds: Some(latency),
            ..Default::default()
        }
    }

    fn make_execution(
        scenario: &str,
        query_index: usize,
        cold_run: usize,
        warm_iters: &[(usize, ProfileTemperature, f64)],
    ) -> QueryExecution {
        let warm_runs = warm_iters
            .iter()
            .map(|(idx, temp, latency)| WarmRunResult {
                warm_run: *idx,
                temperature: Some(*temp),
                profile_path: PathBuf::from(format!("warm{:02}.json", idx)),
                profile_summary: Some(make_summary(*latency)),
            })
            .collect();
        QueryExecution {
            scenario: scenario.to_string(),
            engine: "ignored".to_string(),
            dataset: "ignored".to_string(),
            query_index,
            query_sql: "SELECT 1".to_string(),
            cold_run,
            tags: vec![],
            process: ProcessResult {
                status_code: Some(0),
                success: true,
                elapsed_seconds: 0.0,
                stdout_path: PathBuf::from("stdout.log"),
                stderr_path: PathBuf::from("stderr.log"),
                command: vec![],
            },
            warm_runs,
        }
    }

    #[test]
    fn cold_hot_summary_pairs_scenarios_and_computes_speedup() {
        // Construct two queries × two scenarios.  Cold-from-GPFS is 10x slower
        // than hot; process-warm is 2x slower than hot.
        let mut runs = Vec::new();
        for q in [1, 2] {
            // sirius-cold: warm_run=1 → cold=10s, warm_run=2..3 → warm=2s
            runs.push(make_execution(
                "sirius-cold-delta-qbeast",
                q,
                1,
                &[
                    (1, ProfileTemperature::Cold, 10.0),
                    (2, ProfileTemperature::Warm, 2.0),
                    (3, ProfileTemperature::Warm, 2.0),
                ],
            ));
            // sirius-hot: all Warm at 1.0s (preload absorbed cold)
            runs.push(make_execution(
                "sirius-hot-delta-qbeast",
                q,
                1,
                &[
                    (1, ProfileTemperature::Warm, 1.0),
                    (2, ProfileTemperature::Warm, 1.0),
                    (3, ProfileTemperature::Warm, 1.0),
                ],
            ));
        }
        let results = SuiteResults {
            queries_file: PathBuf::from("queries.sql"),
            generated_from: PathBuf::from("runner.toml"),
            runs,
        };

        let report = build_report(&results);

        // Header section present
        assert!(report.contains("## Cold vs Hot Summary"));
        // The auto-derived label strips the cold/hot middle
        assert!(report.contains("sirius / delta-qbeast"));
        // Speedup vs cold: 10 / 1 = 10.00x
        assert!(
            report.contains("10.00x"),
            "expected hot-vs-cold speedup of 10.00x in report:\n{report}"
        );
        // Speedup vs warm: 2 / 1 = 2.00x
        assert!(
            report.contains("2.00x"),
            "expected hot-vs-warm speedup of 2.00x in report:\n{report}"
        );
        // GPFS Cold geomean = 10.0
        assert!(report.contains("10.000000"));
    }

    #[test]
    fn preload_effectiveness_section_only_emitted_when_hot_scenarios_exist() {
        let runs_cold_only = vec![make_execution(
            "duckdb-delta-qbeast",
            1,
            1,
            &[(1, ProfileTemperature::Cold, 5.0)],
        )];
        let report_cold = build_report(&SuiteResults {
            queries_file: PathBuf::from("q.sql"),
            generated_from: PathBuf::from("r.toml"),
            runs: runs_cold_only,
        });
        assert!(!report_cold.contains("## Preload Effectiveness"));

        let runs_with_hot = vec![
            make_execution(
                "sirius-hot-delta-qbeast",
                1,
                1,
                &[
                    (1, ProfileTemperature::Warm, 1.5),
                    (2, ProfileTemperature::Warm, 1.0),
                    (3, ProfileTemperature::Warm, 1.0),
                ],
            ),
        ];
        let report_hot = build_report(&SuiteResults {
            queries_file: PathBuf::from("q.sql"),
            generated_from: PathBuf::from("r.toml"),
            runs: runs_with_hot,
        });
        assert!(report_hot.contains("## Preload Effectiveness"));
        // First-iter 1.5 / steady-state 1.0 = 1.50x — preload "kinda working".
        assert!(report_hot.contains("1.50x"));
        // n_first=1, n_steady=2
        assert!(report_hot.contains("1 / 2"));
    }

    #[test]
    fn cold_hot_summary_omitted_when_no_pair_exists() {
        let runs = vec![make_execution(
            "duckdb-delta-qbeast",
            1,
            1,
            &[(1, ProfileTemperature::Cold, 5.0)],
        )];
        let report = build_report(&SuiteResults {
            queries_file: PathBuf::from("q.sql"),
            generated_from: PathBuf::from("r.toml"),
            runs,
        });
        assert!(!report.contains("## Cold vs Hot Summary"));
    }
}
