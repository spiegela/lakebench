use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::ValueEnum;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::symbols::border;
use ratatui::text::Line;
use ratatui::widgets::{
    Bar, BarChart, BarGroup, Block, Borders, Cell, Paragraph, Row, Table, Tabs,
};
use ratatui::{Frame, Terminal};

use crate::config::{ProfileTemperature, SuiteResults};
use crate::profile::{parse_profile_summary, ProfileSummary};

#[derive(ValueEnum, Clone, Debug)]
pub enum Temperature {
    Cold,
    Warm,
    Best,
}

#[derive(Debug, Clone)]
struct ComparisonInput {
    name: String,
    query_runs: BTreeMap<usize, Vec<ProfileRun>>,
}

#[derive(Debug, Clone)]
struct ProfileRun {
    temperature: ProfileTemperature,
    summary: ProfileSummary,
}

#[derive(Debug, Clone, Default)]
struct QueryAggregate {
    latency_values: Vec<f64>,
    cpu_values: Vec<f64>,
    blocked_values: Vec<f64>,
    bytes_read_values: Vec<f64>,
    peak_buffer_values: Vec<f64>,
    peak_temp_values: Vec<f64>,
    rows_scanned_values: Vec<f64>,
    files_read_values: Vec<f64>,
    scan_timing_values: Vec<f64>,
}

#[derive(Debug, Clone, Default)]
struct AggregateStats {
    total_latency: f64,
    avg_latency: Option<f64>,
    avg_cpu: Option<f64>,
    total_bytes_read: f64,
    avg_peak_buffer: Option<f64>,
    avg_files_read: Option<f64>,
    avg_rows_scanned: Option<f64>,
    avg_scan_timing: Option<f64>,
    avg_blocked: Option<f64>,
}

#[derive(Clone)]
enum MetricFormatter {
    Seconds,
    Bytes,
    Count,
}

#[derive(Clone)]
struct ComparisonMetric {
    name: &'static str,
    formatter: MetricFormatter,
    left_value: Option<f64>,
    right_value: Option<f64>,
    left_geomean: Option<f64>,
    right_geomean: Option<f64>,
    left_sum: Option<f64>,
    right_sum: Option<f64>,
}

#[derive(Clone)]
struct QueryComparisonRow {
    query_label: String,
    left_latency: Option<f64>,
    right_latency: Option<f64>,
    left_cpu_time: Option<f64>,
    right_cpu_time: Option<f64>,
    left_blocked_thread: Option<f64>,
    right_blocked_thread: Option<f64>,
    left_bytes_read: Option<f64>,
    right_bytes_read: Option<f64>,
    left_peak_buffer_mem: Option<f64>,
    right_peak_buffer_mem: Option<f64>,
    left_rows_scanned: Option<f64>,
    right_rows_scanned: Option<f64>,
    left_files_read: Option<f64>,
    right_files_read: Option<f64>,
    left_scan_timing: Option<f64>,
    right_scan_timing: Option<f64>,
    delta: Option<f64>,
    delta_pct: Option<f64>,
}

#[derive(Clone)]
struct ComparisonViewModel {
    title: String,
    left_name: String,
    right_name: String,
    temperature: Temperature,
    summary_rows: Vec<(String, String, String, String)>,
    metric_rows: Vec<ComparisonMetric>,
    query_rows: Vec<QueryComparisonRow>,
}

#[derive(Default)]
struct TuiState {
    active_tab: usize,
    metric_scroll: usize,
    query_scroll: usize,
}

pub fn compare_command(
    left_path: &Path,
    left_scenario: Option<&str>,
    right_path: &Path,
    right_scenario: Option<&str>,
    temperature: Temperature,
    warning_pct: f64,
    tui: bool,
) -> Result<()> {
    let left = load_comparison_input(left_path, left_scenario)?;
    let right = load_comparison_input(right_path, right_scenario)?;
    let view = build_comparison_view(&left, &right, temperature, warning_pct);
    if tui {
        run_compare_tui(view)
    } else {
        println!("{}", render_comparison(&view));
        Ok(())
    }
}

fn load_comparison_input(path: &Path, scenario: Option<&str>) -> Result<ComparisonInput> {
    if path.is_file() {
        if path.file_name().and_then(|name| name.to_str()) == Some("results.json") {
            return load_comparison_from_results_json(path, scenario);
        }
        bail!(
            "unsupported comparison input file {}; expected a scenario directory or results.json",
            path.display()
        );
    }

    // If the directory contains a results.json, use it directly (per-scenario layout).
    let results_json = path.join("results.json");
    if results_json.exists() {
        return load_comparison_from_results_json(&results_json, scenario);
    }

    if scenario.is_some() {
        bail!(
            "scenario selectors are only valid with results.json inputs, not directories like {}",
            path.display()
        );
    }

    load_comparison_from_legacy_dir(path)
}

fn load_comparison_from_legacy_dir(path: &Path) -> Result<ComparisonInput> {
    let mut query_runs = BTreeMap::<usize, Vec<(String, ProfileSummary)>>::new();
    for entry in fs::read_dir(path).with_context(|| format!("failed to read {}", path.display()))? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if !file_type.is_file() {
            continue;
        }
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if !file_name.ends_with(".json") {
            continue;
        }
        if let Some(query_index) = parse_legacy_query_index(&file_name) {
            let profile = parse_profile_summary(&entry.path())?;
            query_runs
                .entry(query_index)
                .or_default()
                .push((file_name.into_owned(), profile));
        }
    }

    if query_runs.is_empty() {
        bail!("no query profile json files found in {}", path.display());
    }

    let query_runs = query_runs
        .into_iter()
        .map(|(query_index, mut runs)| {
            runs.sort_by(|left, right| left.0.cmp(&right.0));
            let profiled_runs = runs
                .into_iter()
                .enumerate()
                .map(|(index, (_, summary))| ProfileRun {
                    temperature: if index == 0 {
                        ProfileTemperature::Cold
                    } else {
                        ProfileTemperature::Warm
                    },
                    summary,
                })
                .collect();
            (query_index, profiled_runs)
        })
        .collect();

    Ok(ComparisonInput {
        name: path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("left")
            .to_string(),
        query_runs,
    })
}

fn load_comparison_from_results_json(
    path: &Path,
    scenario: Option<&str>,
) -> Result<ComparisonInput> {
    let results_text =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let suite_results: SuiteResults = serde_json::from_str(&results_text)
        .with_context(|| format!("failed to parse {}", path.display()))?;

    let mut scenario_names = suite_results
        .runs
        .iter()
        .map(|run| run.scenario.clone())
        .collect::<Vec<_>>();
    scenario_names.sort();
    scenario_names.dedup();

    let selected_scenario = match scenario {
        Some(name) => {
            if scenario_names.iter().any(|candidate| candidate == name) {
                name.to_string()
            } else {
                bail!(
                    "scenario {} not found in {}. Available: {}",
                    name,
                    path.display(),
                    scenario_names.join(", ")
                );
            }
        }
        None => {
            if scenario_names.len() == 1 {
                scenario_names[0].clone()
            } else {
                bail!(
                    "{} contains multiple scenarios ({}). Pass --left-scenario/--right-scenario.",
                    path.display(),
                    scenario_names.join(", ")
                );
            }
        }
    };

    let mut query_runs = BTreeMap::<usize, Vec<ProfileRun>>::new();
    for run in suite_results.runs {
        if run.scenario != selected_scenario {
            continue;
        }
        let bucket = query_runs.entry(run.query_index).or_default();
        for warm in run.warm_runs {
            let temperature = warm.effective_temperature();
            if let Some(summary) = warm.profile_summary {
                bucket.push(ProfileRun {
                    temperature,
                    summary,
                });
            }
        }
    }

    if query_runs.is_empty() {
        bail!("no profile summaries found in {}", path.display());
    }

    Ok(ComparisonInput {
        name: selected_scenario,
        query_runs,
    })
}

fn parse_legacy_query_index(file_name: &str) -> Option<usize> {
    let stripped = file_name.strip_prefix('q')?.strip_suffix(".json")?;
    let query_part = stripped.split('-').next()?;
    query_part.parse::<usize>().ok()
}

fn build_comparison_view(
    left: &ComparisonInput,
    right: &ComparisonInput,
    temperature: Temperature,
    warning_pct: f64,
) -> ComparisonViewModel {
    let left_stats = aggregate_stats(left, &temperature);
    let right_stats = aggregate_stats(right, &temperature);
    let mut summary_rows = Vec::new();

    for temp in [Temperature::Cold, Temperature::Warm, Temperature::Best] {
        let left_temp = aggregate_stats(left, &temp);
        let right_temp = aggregate_stats(right, &temp);
        summary_rows.push((
            temperature_label(&temp).to_string(),
            format!(
                "total={} avg={}",
                format_seconds(left_temp.total_latency),
                format_optional_seconds(left_temp.avg_latency)
            ),
            format!(
                "total={} avg={}",
                format_seconds(right_temp.total_latency),
                format_optional_seconds(right_temp.avg_latency)
            ),
            format_pct_delta(left_temp.total_latency, right_temp.total_latency),
        ));
    }

    let mut query_rows = Vec::new();
    let query_ids = union_query_ids(left, right);
    for query_id in query_ids {
        let left_query = aggregate_query(left.query_runs.get(&query_id), &temperature);
        let right_query = aggregate_query(right.query_runs.get(&query_id), &temperature);
        let left_latency = left_query.latency_values.first().copied();
        let right_latency = right_query.latency_values.first().copied();
        let delta = match (left_latency, right_latency) {
            (Some(left_value), Some(right_value)) => Some(right_value - left_value),
            _ => None,
        };
        let delta_pct = match (left_latency, right_latency) {
            (Some(left_value), Some(right_value)) => percent_delta(left_value, right_value),
            _ => None,
        };
        let warning = delta_pct
            .filter(|value| *value >= warning_pct)
            .map(|_| "  <-- WARNING")
            .unwrap_or("");
        let query_label = if warning.is_empty() {
            format!("q{query_id}")
        } else {
            format!("q{query_id} !")
        };
        query_rows.push(QueryComparisonRow {
            query_label,
            left_latency,
            right_latency,
            left_cpu_time: left_query.cpu_values.first().copied(),
            right_cpu_time: right_query.cpu_values.first().copied(),
            left_blocked_thread: left_query.blocked_values.first().copied(),
            right_blocked_thread: right_query.blocked_values.first().copied(),
            left_bytes_read: left_query.bytes_read_values.first().copied(),
            right_bytes_read: right_query.bytes_read_values.first().copied(),
            left_peak_buffer_mem: left_query.peak_buffer_values.first().copied(),
            right_peak_buffer_mem: right_query.peak_buffer_values.first().copied(),
            left_rows_scanned: left_query.rows_scanned_values.first().copied(),
            right_rows_scanned: right_query.rows_scanned_values.first().copied(),
            left_files_read: left_query.files_read_values.first().copied(),
            right_files_read: right_query.files_read_values.first().copied(),
            left_scan_timing: left_query.scan_timing_values.first().copied(),
            right_scan_timing: right_query.scan_timing_values.first().copied(),
            delta,
            delta_pct,
        });
    }

    let metric_rows = [
        (
            "latency",
            left_stats.avg_latency,
            right_stats.avg_latency,
            MetricFormatter::Seconds,
        ),
        (
            "cpu time",
            left_stats.avg_cpu,
            right_stats.avg_cpu,
            MetricFormatter::Seconds,
        ),
        (
            "blocked thread",
            left_stats.avg_blocked,
            right_stats.avg_blocked,
            MetricFormatter::Seconds,
        ),
        (
            "bytes read",
            Some(left_stats.total_bytes_read),
            Some(right_stats.total_bytes_read),
            MetricFormatter::Bytes,
        ),
        (
            "peak buffer mem",
            left_stats.avg_peak_buffer,
            right_stats.avg_peak_buffer,
            MetricFormatter::Bytes,
        ),
        (
            "rows scanned",
            left_stats.avg_rows_scanned,
            right_stats.avg_rows_scanned,
            MetricFormatter::Count,
        ),
        (
            "files read",
            left_stats.avg_files_read,
            right_stats.avg_files_read,
            MetricFormatter::Count,
        ),
        (
            "scan timing",
            left_stats.avg_scan_timing,
            right_stats.avg_scan_timing,
            MetricFormatter::Seconds,
        ),
    ]
    .into_iter()
    .map(|(name, left_value, right_value, formatter)| {
        let (left_series, right_series) = suite_metric_series(name, &query_rows);
        ComparisonMetric {
            name,
            formatter,
            left_value,
            right_value,
            left_geomean: geometric_mean(&left_series),
            right_geomean: geometric_mean(&right_series),
            left_sum: sum_values(&left_series),
            right_sum: sum_values(&right_series),
        }
    })
    .collect::<Vec<_>>();

    ComparisonViewModel {
        title: format!("Compare: {} vs {}", left.name, right.name),
        left_name: left.name.clone(),
        right_name: right.name.clone(),
        temperature,
        summary_rows,
        metric_rows,
        query_rows,
    }
}

fn render_comparison(view: &ComparisonViewModel) -> String {
    let mut lines = Vec::new();

    lines.push(view.title.clone());
    lines.push(String::new());
    lines.push("Execution times:".to_string());
    lines.push(format!(
        "  {:<13} {:<30} {:<30} {:>10}",
        "Temperature", view.left_name, view.right_name, "delta"
    ));
    lines.push(format!("  {}", "-".repeat(13 + 1 + 30 + 1 + 30 + 1 + 10)));
    for (temperature, left_text, right_text, delta_text) in &view.summary_rows {
        lines.push(format!(
            "  {:<13} {:<30} {:<30} {:>10}",
            temperature, left_text, right_text, delta_text
        ));
    }

    lines.push(String::new());
    lines.push(format!(
        "Profile metrics (temperature: {}):",
        temperature_label(&view.temperature).to_lowercase()
    ));
    lines.push(format!(
        "  {:<27} {:<24} {:<24} {:>10}",
        "Metric", view.left_name, view.right_name, "delta%"
    ));
    lines.push(format!("  {}", "-".repeat(27 + 1 + 24 + 1 + 24 + 1 + 10)));
    for metric in &view.metric_rows {
        lines.push(format!(
            "  {:<27} {:<24} {:<24} {:>10}",
            metric.name,
            metric.formatter.format(metric.left_value),
            metric.formatter.format(metric.right_value),
            format_optional_pct_delta(metric.left_value, metric.right_value),
        ));
    }

    lines.push(String::new());
    lines.push(format!(
        "Suite summary (temperature: {}):",
        temperature_label(&view.temperature).to_lowercase()
    ));
    lines.push(format!(
        "  {:<18} {:<12} {:<18} {:<18} {:>10}",
        "Metric", "Stat", view.left_name, view.right_name, "delta%"
    ));
    lines.push(format!("  {}", "-".repeat(18 + 1 + 12 + 1 + 18 + 1 + 18 + 1 + 10)));
    for metric in &view.metric_rows {
        lines.push(format!(
            "  {:<18} {:<12} {:<18} {:<18} {:>10}",
            metric.name,
            "GeoMean",
            metric.formatter.format(metric.left_geomean),
            metric.formatter.format(metric.right_geomean),
            format_optional_pct_delta(metric.left_geomean, metric.right_geomean),
        ));
        lines.push(format!(
            "  {:<18} {:<12} {:<18} {:<18} {:>10}",
            "",
            "Sum",
            metric.formatter.format(metric.left_sum),
            metric.formatter.format(metric.right_sum),
            format_optional_pct_delta(metric.left_sum, metric.right_sum),
        ));
    }

    lines.push(String::new());
    lines.push(format!(
        "Per-query comparison (temperature: {}):",
        temperature_label(&view.temperature).to_lowercase()
    ));
    lines.push(format!(
        "  {:<8} {:>12} {:>12} {:>10} {:>10}",
        "Query", view.left_name, view.right_name, "delta", "delta%"
    ));
    lines.push(format!(
        "  {}",
        "-".repeat(8 + 1 + 12 + 1 + 12 + 1 + 10 + 1 + 10)
    ));
    for row in &view.query_rows {
        lines.push(format!(
            "  {:<8} {:>12} {:>12} {:>10} {:>10}",
            row.query_label,
            format_optional_seconds(row.left_latency),
            format_optional_seconds(row.right_latency),
            format_optional_seconds_signed(row.delta),
            format_optional_pct(row.delta_pct),
        ));
    }

    lines.join("\n")
}

fn run_compare_tui(view: ComparisonViewModel) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    let mut state = TuiState::default();

    let result = loop {
        terminal.draw(|frame| render_compare_tui(frame, &view, &state))?;

        if !event::poll(Duration::from_millis(100))? {
            continue;
        }

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                KeyCode::Tab => state.active_tab = (state.active_tab + 1) % 2,
                KeyCode::BackTab => state.active_tab = (state.active_tab + 1) % 2,
                KeyCode::Up => {
                    if state.active_tab == 1 {
                        state.metric_scroll = state.metric_scroll.saturating_sub(1);
                    }
                }
                KeyCode::Down => {
                    if state.active_tab == 1 {
                        state.metric_scroll =
                            (state.metric_scroll + 1).min(view.metric_rows.len().saturating_sub(1));
                    }
                }
                KeyCode::Left => state.query_scroll = state.query_scroll.saturating_sub(1),
                KeyCode::Right => {
                    if state.query_scroll + 1 < view.query_rows.len() {
                        state.query_scroll += 1;
                    }
                }
                _ => {}
            }
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

fn render_compare_tui(frame: &mut Frame<'_>, view: &ComparisonViewModel, state: &TuiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(10),
            Constraint::Min(10),
            Constraint::Length(2),
        ])
        .split(frame.area());

    let titles = ["Table", "Charts"]
        .into_iter()
        .map(Line::from)
        .collect::<Vec<_>>();
    let tabs = Tabs::new(titles)
        .select(state.active_tab)
        .block(
            Block::default()
                .title(view.title.as_str())
                .borders(Borders::ALL)
                .border_set(border::ROUNDED),
        )
        .highlight_style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, chunks[0]);

    render_summary(frame, chunks[1], view);
    if state.active_tab == 0 {
        render_query_table(frame, chunks[2], view, state);
    } else {
        render_metric_charts(frame, chunks[2], view, state);
    }

    let help = Paragraph::new(
        "Tab: switch view  Left/Right: query scroll  Up/Down: metric scroll  q/Esc: quit",
    )
    .block(Block::default().borders(Borders::ALL));
    frame.render_widget(help, chunks[3]);
}

fn render_summary(frame: &mut Frame<'_>, area: Rect, view: &ComparisonViewModel) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);

    let rows = view.summary_rows.iter().map(|(temp, left, right, delta)| {
        Row::new(vec![
            Cell::from(temp.clone()),
            Cell::from(left.clone()),
            Cell::from(right.clone()),
            Cell::from(delta.clone()),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(12),
            Constraint::Percentage(35),
            Constraint::Percentage(35),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec![
            "Temp",
            view.left_name.as_str(),
            view.right_name.as_str(),
            "Delta",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title("Execution Times")
            .borders(Borders::ALL)
            .border_set(border::ROUNDED),
    );
    frame.render_widget(table, chunks[0]);

    let latency_metric = view.metric_rows.iter().find(|metric| metric.name == "latency");
    let rows = latency_metric.into_iter().flat_map(|metric| {
        [
            Row::new(vec![
                Cell::from("GeoMean"),
                Cell::from(metric.formatter.format(metric.left_geomean)),
                Cell::from(metric.formatter.format(metric.right_geomean)),
                Cell::from(format_optional_pct_delta(
                    metric.left_geomean,
                    metric.right_geomean,
                )),
            ]),
            Row::new(vec![
                Cell::from("Sum"),
                Cell::from(metric.formatter.format(metric.left_sum)),
                Cell::from(metric.formatter.format(metric.right_sum)),
                Cell::from(format_optional_pct_delta(metric.left_sum, metric.right_sum)),
            ]),
        ]
    });
    let suite_summary = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Percentage(32),
            Constraint::Percentage(32),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec![
            "Stat",
            view.left_name.as_str(),
            view.right_name.as_str(),
            "Delta",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(format!(
                "Suite Summary ({})",
                temperature_label(&view.temperature)
            ))
            .borders(Borders::ALL)
            .border_set(border::ROUNDED),
    );
    frame.render_widget(suite_summary, chunks[1]);
}

fn render_query_table(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ComparisonViewModel,
    state: &TuiState,
) {
    let visible_rows = area.height.saturating_sub(3) as usize;
    let start = state
        .query_scroll
        .min(view.query_rows.len().saturating_sub(1));
    let end = (start + visible_rows).min(view.query_rows.len());
    let rows = view.query_rows[start..end].iter().map(|row| {
        Row::new(vec![
            Cell::from(row.query_label.clone()),
            Cell::from(format_optional_seconds(row.left_latency)),
            Cell::from(format_optional_seconds(row.right_latency)),
            Cell::from(format_optional_seconds_signed(row.delta)),
            Cell::from(format_optional_pct(row.delta_pct)),
        ])
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(10),
            Constraint::Length(14),
            Constraint::Length(14),
            Constraint::Length(12),
            Constraint::Length(10),
        ],
    )
    .header(
        Row::new(vec![
            "Query",
            view.left_name.as_str(),
            view.right_name.as_str(),
            "Delta",
            "Delta%",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title(format!(
                "Per-Query Table ({}, showing {}-{} of {})",
                temperature_label(&view.temperature),
                start + 1,
                end,
                view.query_rows.len()
            ))
            .borders(Borders::ALL)
            .border_set(border::ROUNDED),
    );
    frame.render_widget(table, area);
}

fn render_metric_charts(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ComparisonViewModel,
    state: &TuiState,
) {
    let chart_height = 9usize;
    let visible_metrics = ((area.height as usize) / chart_height).max(1);
    let metric_start = state
        .metric_scroll
        .min(view.metric_rows.len().saturating_sub(1));
    let metric_end = (metric_start + visible_metrics).min(view.metric_rows.len());
    let constraints = (metric_start..metric_end)
        .map(|_| Constraint::Length(chart_height as u16))
        .collect::<Vec<_>>();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    for (chunk, metric) in chunks
        .into_iter()
        .zip(&view.metric_rows[metric_start..metric_end])
    {
        render_metric_chart(frame, *chunk, view, metric, state.query_scroll);
    }
}

fn render_metric_chart(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ComparisonViewModel,
    metric: &ComparisonMetric,
    query_scroll: usize,
) {
    if area.width < 40 {
        render_query_metric_chart(frame, area, view, metric, query_scroll);
        return;
    }

    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(28), Constraint::Min(20)])
        .split(area);
    render_suite_summary_chart(frame, split[0], metric);
    render_query_metric_chart(frame, split[1], view, metric, query_scroll);
}

fn render_query_metric_chart(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &ComparisonViewModel,
    metric: &ComparisonMetric,
    query_scroll: usize,
) {
    let query_capacity = (((area.width as usize).saturating_sub(6)) / 8).max(1);
    let start = query_scroll.min(view.query_rows.len().saturating_sub(1));
    let end = (start + query_capacity).min(view.query_rows.len());

    let mut max_value = 1u64;
    let groups = view.query_rows[start..end]
        .iter()
        .map(|row| {
            let left_value = metric_value_for_row(metric.name, row, true);
            let right_value = metric_value_for_row(metric.name, row, false);
            let left_scaled = scale_metric_value(left_value);
            let right_scaled = scale_metric_value(right_value);
            max_value = max_value.max(left_scaled).max(right_scaled);
            BarGroup::default()
                .label(Line::from(row.query_label.clone()))
                .bars(&[
                    Bar::default()
                        .value(left_scaled)
                        .style(Style::default().fg(Color::Cyan)),
                    Bar::default()
                        .value(right_scaled)
                        .style(Style::default().fg(Color::Magenta)),
                ])
        })
        .collect::<Vec<_>>();
    let title = format!(
        "{} [{}-{} of {}]",
        metric.name,
        start + 1,
        end,
        view.query_rows.len()
    );
    let chart = BarChart::grouped(groups)
        .max(max_value.max(1))
        .bar_width(3)
        .bar_gap(0)
        .group_gap(1)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_set(border::ROUNDED),
        );
    frame.render_widget(chart, area);
}

fn render_suite_summary_chart(
    frame: &mut Frame<'_>,
    area: Rect,
    metric: &ComparisonMetric,
) {
    let mut max_value = 1u64;
    let summary_groups = [
        (
            "GeoMean",
            scale_metric_value(metric.left_geomean),
            scale_metric_value(metric.right_geomean),
        ),
        (
            "Sum",
            scale_metric_value(metric.left_sum),
            scale_metric_value(metric.right_sum),
        ),
    ]
    .into_iter()
    .map(|(label, left_value, right_value)| {
        max_value = max_value.max(left_value).max(right_value);
        BarGroup::default().label(Line::from(label)).bars(&[
            Bar::default()
                .value(left_value)
                .style(Style::default().fg(Color::Cyan)),
            Bar::default()
                .value(right_value)
                .style(Style::default().fg(Color::Magenta)),
        ])
    })
    .collect::<Vec<_>>();

    let chart = BarChart::grouped(summary_groups)
        .max(max_value.max(1))
        .bar_width(4)
        .bar_gap(0)
        .group_gap(1)
        .block(
            Block::default()
                .title(format!("Summary | {}", metric.name))
                .borders(Borders::ALL)
                .border_set(border::ROUNDED),
        );
    frame.render_widget(chart, area);
}

fn metric_value_for_row(
    metric_name: &str,
    row: &QueryComparisonRow,
    left_side: bool,
) -> Option<f64> {
    match metric_name {
        "latency" => pick_side(left_side, row.left_latency, row.right_latency),
        "cpu time" => pick_side(left_side, row.left_cpu_time, row.right_cpu_time),
        "blocked thread" => pick_side(left_side, row.left_blocked_thread, row.right_blocked_thread),
        "bytes read" => pick_side(left_side, row.left_bytes_read, row.right_bytes_read),
        "peak buffer mem" => pick_side(
            left_side,
            row.left_peak_buffer_mem,
            row.right_peak_buffer_mem,
        ),
        "rows scanned" => pick_side(left_side, row.left_rows_scanned, row.right_rows_scanned),
        "files read" => pick_side(left_side, row.left_files_read, row.right_files_read),
        "scan timing" => pick_side(left_side, row.left_scan_timing, row.right_scan_timing),
        _ => pick_side(left_side, row.left_latency, row.right_latency),
    }
}

fn pick_side(left_side: bool, left: Option<f64>, right: Option<f64>) -> Option<f64> {
    if left_side {
        left
    } else {
        right
    }
}

fn scale_metric_value(value: Option<f64>) -> u64 {
    match value {
        Some(value) if value.is_finite() && value > 0.0 => value.ceil().max(1.0) as u64,
        _ => 0,
    }
}

fn suite_metric_series(metric_name: &str, rows: &[QueryComparisonRow]) -> (Vec<f64>, Vec<f64>) {
    let mut left = Vec::new();
    let mut right = Vec::new();
    for row in rows {
        if let Some(value) = metric_value_for_row(metric_name, row, true) {
            left.push(value);
        }
        if let Some(value) = metric_value_for_row(metric_name, row, false) {
            right.push(value);
        }
    }
    (left, right)
}

fn union_query_ids(left: &ComparisonInput, right: &ComparisonInput) -> Vec<usize> {
    let mut query_ids = left
        .query_runs
        .keys()
        .chain(right.query_runs.keys())
        .copied()
        .collect::<Vec<_>>();
    query_ids.sort();
    query_ids.dedup();
    query_ids
}

fn aggregate_stats(input: &ComparisonInput, temperature: &Temperature) -> AggregateStats {
    let mut total_latency = 0.0;
    let mut total_cpu = 0.0;
    let mut total_bytes_read = 0.0;
    let mut peak_buffers = Vec::new();
    let mut files_read = Vec::new();
    let mut rows_scanned = Vec::new();
    let mut scan_timing = Vec::new();
    let mut blocked = Vec::new();
    let mut latency_count = 0usize;
    let mut cpu_count = 0usize;

    for runs in input.query_runs.values() {
        let aggregate = aggregate_query(Some(runs), temperature);
        if let Some(value) = aggregate.latency_values.first().copied() {
            total_latency += value;
            latency_count += 1;
        }
        if let Some(value) = aggregate.cpu_values.first().copied() {
            total_cpu += value;
            cpu_count += 1;
        }
        if let Some(value) = aggregate.bytes_read_values.first().copied() {
            total_bytes_read += value;
        }
        if let Some(value) = aggregate.peak_buffer_values.first().copied() {
            peak_buffers.push(value);
        }
        if let Some(value) = aggregate.files_read_values.first().copied() {
            files_read.push(value);
        }
        if let Some(value) = aggregate.rows_scanned_values.first().copied() {
            rows_scanned.push(value);
        }
        if let Some(value) = aggregate.scan_timing_values.first().copied() {
            scan_timing.push(value);
        }
        if let Some(value) = aggregate.blocked_values.first().copied() {
            blocked.push(value);
        }
    }

    AggregateStats {
        total_latency,
        avg_latency: if latency_count > 0 {
            Some(total_latency / latency_count as f64)
        } else {
            None
        },
        avg_cpu: if cpu_count > 0 {
            Some(total_cpu / cpu_count as f64)
        } else {
            None
        },
        total_bytes_read,
        avg_peak_buffer: mean(&peak_buffers),
        avg_files_read: mean(&files_read),
        avg_rows_scanned: mean(&rows_scanned),
        avg_scan_timing: mean(&scan_timing),
        avg_blocked: mean(&blocked),
    }
}

fn aggregate_query(
    runs: Option<&Vec<ProfileRun>>,
    temperature: &Temperature,
) -> QueryAggregate {
    let mut aggregate = QueryAggregate::default();
    let Some(runs) = runs else {
        return aggregate;
    };

    if runs.is_empty() {
        return aggregate;
    }

    let chosen = match temperature {
        Temperature::Cold => runs
            .iter()
            .find(|run| run.temperature == ProfileTemperature::Cold)
            .or_else(|| runs.first()),
        Temperature::Warm => runs
            .iter()
            .find(|run| run.temperature == ProfileTemperature::Warm)
            .or_else(|| runs.first()),
        Temperature::Best => runs.iter().min_by(|a, b| {
            a.summary
                .latency_seconds
                .unwrap_or(f64::INFINITY)
                .total_cmp(&b.summary.latency_seconds.unwrap_or(f64::INFINITY))
        }),
    };

    if let Some(run) = chosen {
        let summary = &run.summary;
        push_optional(&mut aggregate.latency_values, summary.latency_seconds);
        push_optional(&mut aggregate.cpu_values, summary.cpu_time_seconds);
        push_optional(
            &mut aggregate.blocked_values,
            summary.blocked_thread_time_seconds,
        );
        push_optional(&mut aggregate.bytes_read_values, summary.total_bytes_read);
        push_optional(
            &mut aggregate.peak_buffer_values,
            summary.peak_buffer_memory_bytes,
        );
        push_optional(
            &mut aggregate.peak_temp_values,
            summary.peak_temp_dir_size_bytes,
        );
        push_optional(
            &mut aggregate.rows_scanned_values,
            summary.cumulative_rows_scanned,
        );
        push_optional(&mut aggregate.files_read_values, summary.total_files_read);
        push_optional(
            &mut aggregate.scan_timing_values,
            summary.scan_operator_timing_seconds,
        );
    }

    aggregate
}

fn push_optional(values: &mut Vec<f64>, value: Option<f64>) {
    if let Some(value) = value {
        values.push(value);
    }
}

fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn geometric_mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }

    if values.iter().any(|value| *value < 0.0 || !value.is_finite()) {
        return None;
    }

    if values.iter().any(|value| *value == 0.0) {
        return Some(0.0);
    }

    let log_sum = values.iter().map(|value| value.ln()).sum::<f64>();
    Some((log_sum / values.len() as f64).exp())
}

fn sum_values(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum())
    }
}

fn percent_delta(left: f64, right: f64) -> Option<f64> {
    if left.abs() < f64::EPSILON {
        None
    } else {
        Some(((right - left) / left) * 100.0)
    }
}

fn temperature_label(value: &Temperature) -> &'static str {
    match value {
        Temperature::Cold => "cold",
        Temperature::Warm => "warm",
        Temperature::Best => "best",
    }
}

fn format_seconds(value: f64) -> String {
    format!("{value:.1}s")
}

fn format_optional_seconds(value: Option<f64>) -> String {
    value
        .map(format_seconds)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_optional_seconds_signed(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:+.1}s"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_pct_delta(left: f64, right: f64) -> String {
    format_optional_pct(percent_delta(left, right))
}

fn format_optional_pct_delta(left: Option<f64>, right: Option<f64>) -> String {
    match (left, right) {
        (Some(left), Some(right)) => format_optional_pct(percent_delta(left, right)),
        _ => "n/a".to_string(),
    }
}

fn format_optional_pct(value: Option<f64>) -> String {
    value
        .map(|v| format!("{v:+.1}%"))
        .unwrap_or_else(|| "n/a".to_string())
}

impl MetricFormatter {
    fn format(&self, value: Option<f64>) -> String {
        match self {
            MetricFormatter::Seconds => format_optional_seconds(value),
            MetricFormatter::Bytes => value.map(format_bytes).unwrap_or_else(|| "n/a".to_string()),
            MetricFormatter::Count => value
                .map(|v| format!("{v:.1}"))
                .unwrap_or_else(|| "n/a".to_string()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_legacy_query_index() {
        assert_eq!(parse_legacy_query_index("q14-3.json"), Some(14));
        assert_eq!(parse_legacy_query_index("q1-1.json"), Some(1));
        assert_eq!(parse_legacy_query_index("report.md"), None);
    }

    #[test]
    fn computes_suite_summary_for_metric() {
        let rows = vec![
            QueryComparisonRow {
                query_label: "q1".to_string(),
                left_latency: Some(4.0),
                right_latency: Some(1.0),
                left_cpu_time: None,
                right_cpu_time: None,
                left_blocked_thread: None,
                right_blocked_thread: None,
                left_bytes_read: None,
                right_bytes_read: None,
                left_peak_buffer_mem: None,
                right_peak_buffer_mem: None,
                left_rows_scanned: None,
                right_rows_scanned: None,
                left_files_read: None,
                right_files_read: None,
                left_scan_timing: None,
                right_scan_timing: None,
                delta: None,
                delta_pct: None,
            },
            QueryComparisonRow {
                query_label: "q2".to_string(),
                left_latency: Some(9.0),
                right_latency: Some(4.0),
                left_cpu_time: None,
                right_cpu_time: None,
                left_blocked_thread: None,
                right_blocked_thread: None,
                left_bytes_read: None,
                right_bytes_read: None,
                left_peak_buffer_mem: None,
                right_peak_buffer_mem: None,
                left_rows_scanned: None,
                right_rows_scanned: None,
                left_files_read: None,
                right_files_read: None,
                left_scan_timing: None,
                right_scan_timing: None,
                delta: None,
                delta_pct: None,
            },
        ];

        let (left, right) = suite_metric_series("latency", &rows);
        assert_eq!(sum_values(&left), Some(13.0));
        assert_eq!(sum_values(&right), Some(5.0));
        assert_eq!(geometric_mean(&right), Some(2.0));
        assert_eq!(geometric_mean(&left), Some(6.0));
    }
}
