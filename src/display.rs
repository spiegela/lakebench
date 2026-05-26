use std::path::Path;

#[derive(Clone, Copy)]
pub(crate) enum AnsiColor {
    Green,
    Red,
    Yellow,
    Blue,
    BlueBold,
    CyanBold,
    MagentaBold,
    Cyan,
}

impl AnsiColor {
    fn code(self) -> &'static str {
        match self {
            AnsiColor::Green => "32",
            AnsiColor::Red => "31",
            AnsiColor::Yellow => "33",
            AnsiColor::Blue => "34",
            AnsiColor::BlueBold => "1;34",
            AnsiColor::CyanBold => "1;36",
            AnsiColor::MagentaBold => "1;35",
            AnsiColor::Cyan => "36",
        }
    }
}

pub(crate) fn colorize(text: &str, color: AnsiColor) -> String {
    format!("\x1b[{}m{}\x1b[0m", color.code(), text)
}

pub(crate) fn print_run_status(
    success: bool,
    scenario_name: &str,
    query_index: usize,
    cold_run: usize,
    phase: Option<&str>,
    elapsed_seconds: f64,
    stderr_path: &Path,
    stderr: &[u8],
) {
    let status = if success {
        colorize("[SUCCESS]", AnsiColor::Green)
    } else {
        colorize("[FAILURE]", AnsiColor::Red)
    };
    let phase = phase.map(|value| format!(" phase={value}")).unwrap_or_default();
    let elapsed = if elapsed_seconds > 0.0 {
        format!(" elapsed={elapsed_seconds:.2}s")
    } else {
        String::new()
    };
    println!(
        "  {status} scenario={scenario_name} query=q{query_index} cold_run={cold_run}{phase}{elapsed}"
    );

    if !success {
        print_failure_log(stderr);
        println!(
            "{} {}",
            colorize("         log:", AnsiColor::Yellow),
            stderr_path.display()
        );
    }
}

pub(crate) fn print_warm_run_status(
    success: bool,
    scenario_name: &str,
    query_index: usize,
    cold_run: usize,
    warm_run: usize,
    temperature: &str,
    latency_seconds: f64,
) {
    let status = if success {
        colorize("[SUCCESS]", AnsiColor::Green)
    } else {
        colorize("[FAILURE]", AnsiColor::Red)
    };
    let latency = if latency_seconds > 0.0 {
        format!(" latency={latency_seconds:.3}s")
    } else {
        String::new()
    };
    println!(
        "  {status} scenario={scenario_name} query=q{query_index} cold_run={cold_run} warm_run={warm_run} temp={temperature}{latency}"
    );
}

pub(crate) fn print_scenario_banner(
    scenario_name: &str,
    engine_name: &str,
    dataset_name: &str,
    warm_runs: usize,
    cold_runs: usize,
    sample_rows: Option<usize>,
) {
    let line = colorize(&"=".repeat(80), AnsiColor::Blue);
    println!();
    println!("{line}");
    println!(
        "{}",
        colorize(
            &format!(
                "| Scenario: {} | engine={} | dataset={} | cold_runs={} | warm_runs={}{}",
                scenario_name,
                engine_name,
                dataset_name,
                cold_runs,
                warm_runs,
                sample_rows
                    .map(|rows| format!(" | sample_rows={rows}"))
                    .unwrap_or_default()
            ),
            AnsiColor::BlueBold,
        )
    );
    println!("{line}");
}

pub(crate) fn print_query_banner(
    engine_name: &str,
    query_index: usize,
    cold_run: usize,
    warm_runs: usize,
) {
    let title = format!(
        " q{query_index:02} | cold run {cold_run:02} | {engine_name} | {warm_runs} warm execution(s) "
    );
    let border_len = 88usize.saturating_sub(title.len()).max(6);
    let left = "-".repeat(border_len / 2);
    let right = "-".repeat(border_len - left.len());
    println!(
        "{}",
        colorize(
            &format!("{left}{title}{right}"),
            AnsiColor::CyanBold,
        )
    );
}

pub(crate) fn print_failure_log(stderr: &[u8]) {
    let text = String::from_utf8_lossy(stderr);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        println!(
            "{} {}",
            colorize("         stderr:", AnsiColor::Yellow),
            "<empty>"
        );
        return;
    }

    println!("{}", colorize("         stderr:", AnsiColor::Yellow));
    for line in trimmed.lines() {
        println!("           {line}");
    }
}

pub(crate) fn print_debug_query_sql(query_sql: &str) {
    println!("{}", colorize("  sql:", AnsiColor::Yellow));
    for line in highlight_sql(query_sql).lines() {
        println!("    {line}");
    }
}

pub(crate) fn highlight_sql(sql: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "select", "from", "where", "group", "by", "order", "limit", "having", "as", "and",
        "or", "not", "case", "when", "then", "else", "end", "join", "left", "right", "inner",
        "outer", "cross", "on", "with", "union", "all", "distinct", "count", "sum", "avg",
        "min", "max", "extract", "interval", "create", "replace", "temp", "view",
    ];

    let mut output = String::new();
    let mut token = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for ch in sql.chars() {
        match ch {
            '\'' if !in_double_quote => {
                flush_sql_token(&mut output, &mut token);
                in_single_quote = !in_single_quote;
                output.push_str(&colorize("'", AnsiColor::Yellow));
            }
            '"' if !in_single_quote => {
                flush_sql_token(&mut output, &mut token);
                in_double_quote = !in_double_quote;
                output.push_str(&colorize("\"", AnsiColor::Cyan));
            }
            _ if in_single_quote => output.push_str(&colorize(&ch.to_string(), AnsiColor::Yellow)),
            _ if in_double_quote => output.push_str(&colorize(&ch.to_string(), AnsiColor::Cyan)),
            _ if ch.is_ascii_alphanumeric() || ch == '_' => token.push(ch),
            _ => {
                flush_sql_token_with_keywords(&mut output, &mut token, KEYWORDS);
                output.push(ch);
            }
        }
    }
    flush_sql_token_with_keywords(&mut output, &mut token, KEYWORDS);
    output
}

fn flush_sql_token(output: &mut String, token: &mut String) {
    if !token.is_empty() {
        output.push_str(token);
        token.clear();
    }
}

fn flush_sql_token_with_keywords(output: &mut String, token: &mut String, keywords: &[&str]) {
    if token.is_empty() {
        return;
    }

    let lower = token.to_ascii_lowercase();
    if keywords.contains(&lower.as_str()) {
        output.push_str(&colorize(token, AnsiColor::BlueBold));
    } else if token.chars().all(|ch| ch.is_ascii_digit()) {
        output.push_str(&colorize(token, AnsiColor::Green));
    } else {
        output.push_str(token);
    }
    token.clear();
}
