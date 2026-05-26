use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProfileSummary {
    #[serde(default)]
    pub query_name: Option<String>,
    #[serde(default)]
    pub latency_seconds: Option<f64>,
    #[serde(default)]
    pub cpu_time_seconds: Option<f64>,
    #[serde(default)]
    pub blocked_thread_time_seconds: Option<f64>,
    #[serde(default)]
    pub rows_returned: Option<f64>,
    #[serde(default)]
    pub cumulative_rows_scanned: Option<f64>,
    #[serde(default)]
    pub cumulative_cardinality: Option<f64>,
    #[serde(default)]
    pub peak_buffer_memory_bytes: Option<f64>,
    #[serde(default)]
    pub peak_temp_dir_size_bytes: Option<f64>,
    #[serde(default)]
    pub total_bytes_read: Option<f64>,
    #[serde(default)]
    pub total_bytes_written: Option<f64>,
    #[serde(default)]
    pub result_set_size_bytes: Option<f64>,
    #[serde(default)]
    pub total_files_read: Option<f64>,
    #[serde(default)]
    pub scan_operator_timing_seconds: Option<f64>,
    #[serde(default)]
    pub scan_operator_rows_scanned: Option<f64>,
    #[serde(default)]
    pub scan_operator_cardinality: Option<f64>,
    #[serde(default)]
    pub extra_metrics: BTreeMap<String, f64>,
}

#[derive(Debug, Clone)]
struct ScanOperatorSummary {
    operator_timing: Option<f64>,
    operator_rows_scanned: Option<f64>,
    operator_cardinality: Option<f64>,
}

pub fn parse_profile_summary(path: &Path) -> Result<ProfileSummary> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read profile {}", path.display()))?;
    let json: Value = serde_json::from_str(&text)
        .with_context(|| format!("failed to parse profile {}", path.display()))?;

    let mut extra_metrics = BTreeMap::new();
    let interesting = [
        "latency",
        "cpu_time",
        "blocked_thread_time",
        "rows_returned",
        "cumulative_rows_scanned",
        "cumulative_cardinality",
        "system_peak_buffer_memory",
        "system_peak_temp_dir_size",
        "total_bytes_read",
        "total_bytes_written",
        "result_set_size",
    ];

    for key in interesting {
        if let Some(value) = find_numeric_key(&json, key) {
            extra_metrics.insert(key.to_string(), value);
        }
    }

    let total_files_read = find_numeric_extra_info_value(&json, "Total Files Read");
    let scan_info = find_primary_scan_operator(&json);

    Ok(ProfileSummary {
        query_name: find_string_key(&json, "query_name"),
        latency_seconds: extra_metrics.get("latency").copied(),
        cpu_time_seconds: extra_metrics.get("cpu_time").copied(),
        blocked_thread_time_seconds: extra_metrics.get("blocked_thread_time").copied(),
        rows_returned: extra_metrics.get("rows_returned").copied(),
        cumulative_rows_scanned: extra_metrics.get("cumulative_rows_scanned").copied(),
        cumulative_cardinality: extra_metrics.get("cumulative_cardinality").copied(),
        peak_buffer_memory_bytes: extra_metrics.get("system_peak_buffer_memory").copied(),
        peak_temp_dir_size_bytes: extra_metrics.get("system_peak_temp_dir_size").copied(),
        total_bytes_read: extra_metrics.get("total_bytes_read").copied(),
        total_bytes_written: extra_metrics.get("total_bytes_written").copied(),
        result_set_size_bytes: extra_metrics.get("result_set_size").copied(),
        total_files_read,
        scan_operator_timing_seconds: scan_info.as_ref().and_then(|s| s.operator_timing),
        scan_operator_rows_scanned: scan_info.as_ref().and_then(|s| s.operator_rows_scanned),
        scan_operator_cardinality: scan_info.as_ref().and_then(|s| s.operator_cardinality),
        extra_metrics,
    })
}

fn find_numeric_key(value: &Value, key: &str) -> Option<f64> {
    match value {
        Value::Object(map) => find_numeric_key_in_map(map, key),
        Value::Array(values) => values.iter().find_map(|child| find_numeric_key(child, key)),
        _ => None,
    }
}

fn find_string_key(value: &Value, key: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            if let Some(text) = map.get(key).and_then(Value::as_str) {
                return Some(text.to_string());
            }
            map.values().find_map(|child| find_string_key(child, key))
        }
        Value::Array(values) => values.iter().find_map(|child| find_string_key(child, key)),
        _ => None,
    }
}

fn find_numeric_key_in_map(map: &Map<String, Value>, key: &str) -> Option<f64> {
    if let Some(value) = map.get(key) {
        if let Some(number) = value.as_f64() {
            return Some(number);
        }
        if let Some(text) = value.as_str() {
            if let Ok(number) = text.parse::<f64>() {
                return Some(number);
            }
        }
    }

    map.values().find_map(|child| find_numeric_key(child, key))
}

fn find_numeric_extra_info_value(value: &Value, key: &str) -> Option<f64> {
    match value {
        Value::Object(map) => {
            if let Some(extra_info) = map.get("extra_info").and_then(Value::as_object) {
                if let Some(raw) = extra_info.get(key) {
                    if let Some(number) = raw.as_f64() {
                        return Some(number);
                    }
                    if let Some(text) = raw.as_str() {
                        if let Ok(number) = text.parse::<f64>() {
                            return Some(number);
                        }
                    }
                }
            }
            map.values()
                .find_map(|child| find_numeric_extra_info_value(child, key))
        }
        Value::Array(values) => values
            .iter()
            .find_map(|child| find_numeric_extra_info_value(child, key)),
        _ => None,
    }
}

fn find_primary_scan_operator(value: &Value) -> Option<ScanOperatorSummary> {
    match value {
        Value::Object(map) => {
            let is_scan = map
                .get("operator_type")
                .and_then(Value::as_str)
                .map(|kind| kind == "TABLE_SCAN")
                .unwrap_or(false);
            if is_scan {
                return Some(ScanOperatorSummary {
                    operator_timing: map.get("operator_timing").and_then(as_f64_like),
                    operator_rows_scanned: map.get("operator_rows_scanned").and_then(as_f64_like),
                    operator_cardinality: map.get("operator_cardinality").and_then(as_f64_like),
                });
            }

            if let Some(children) = map.get("children").and_then(Value::as_array) {
                for child in children {
                    if let Some(scan) = find_primary_scan_operator(child) {
                        return Some(scan);
                    }
                }
            }
            None
        }
        Value::Array(values) => values.iter().find_map(find_primary_scan_operator),
        _ => None,
    }
}

fn as_f64_like(value: &Value) -> Option<f64> {
    if let Some(number) = value.as_f64() {
        return Some(number);
    }
    value.as_str().and_then(|text| text.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_polars_style_profile_summary() {
        let json = serde_json::json!({
            "query_name": "select 1",
            "latency": 0.42,
            "rows_returned": 10,
            "cpu_time": 0.31,
            "blocked_thread_time": 0.0,
            "result_set_size": 128,
            "cumulative_cardinality": 10,
            "system_peak_buffer_memory": 2048,
            "scan_operator_timing_seconds": 0.12,
            "extra_info": {
                "collect_engine": "gpu",
                "profile_rows": [
                    {"node": "scan parquet", "start": 0, "end": 120000}
                ]
            }
        });

        let path = std::env::temp_dir().join("polars-profile-summary-test.json");
        fs::write(&path, serde_json::to_vec_pretty(&json).unwrap()).unwrap();
        let summary = parse_profile_summary(&path).unwrap();
        let _ = fs::remove_file(&path);

        assert_eq!(summary.query_name.as_deref(), Some("select 1"));
        assert_eq!(summary.latency_seconds, Some(0.42));
        assert_eq!(summary.cpu_time_seconds, Some(0.31));
        assert_eq!(summary.rows_returned, Some(10.0));
        assert_eq!(summary.result_set_size_bytes, Some(128.0));
        assert_eq!(summary.peak_buffer_memory_bytes, Some(2048.0));
    }
}
