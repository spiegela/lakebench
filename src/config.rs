use std::collections::BTreeMap;
use std::path::PathBuf;

use bytesize::ByteSize;
use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub suite: SuiteConfig,
    #[serde(default)]
    pub engines: Vec<EngineConfig>,
    #[serde(default)]
    pub datasets: Vec<DatasetConfig>,
    #[serde(default)]
    pub scenarios: Vec<ScenarioConfig>,
}

#[derive(Debug, Deserialize)]
pub struct SuiteConfig {
    pub queries_file: PathBuf,
    pub output_dir: PathBuf,
    #[serde(default = "default_profiling_mode")]
    pub profiling_mode: String,
    #[serde(default = "default_warm_runs")]
    pub warm_runs: usize,
    #[serde(default = "default_cold_runs")]
    pub cold_runs: usize,
    #[serde(default)]
    pub setup_sql: Vec<String>,
    #[serde(default)]
    pub selected_queries: Vec<usize>,
    #[serde(default)]
    pub s3: Option<S3Config>,
    #[serde(default)]
    pub warehouse: Option<WarehouseConfig>,
}

#[derive(Debug, Deserialize)]
pub struct EngineConfig {
    pub name: String,
    #[serde(default = "default_engine_backend")]
    pub backend: EngineBackend,
    #[serde(default)]
    pub query_execution_mode: QueryExecutionMode,
    pub binary: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub setup_sql: Vec<String>,
    /// SQL statements run once after setup and table-view creation, before the
    /// measured warm-run loop.  Used to warm the GPU buffer manager / cache.
    /// DuckDB backend only; ignored for the Polars backend.
    #[serde(default)]
    pub preload_sql: Vec<String>,
    /// Whether `preload_sql` actually warms the GPU cache.  Default `true` —
    /// preload includes a `CALL gpu_execution(...)` warming query, so every
    /// measured iteration sees a warm GPU and is labelled `Warm`.
    ///
    /// Set to `false` when preload only materialises data into a CPU table
    /// (no GPU access).  Then warm_run=1 is still GPU-cold (CPU→GPU transfer
    /// happens inside the first measured query) and labelled `Cold`.
    #[serde(default)]
    pub preload_warms_gpu: Option<bool>,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_table_name")]
    pub table_name: String,
    #[serde(default)]
    pub polars_collect_engine: Option<String>,
    /// How to preload data inside the long-lived Polars subprocess before
    /// measured queries begin.  Polars backend only.
    #[serde(default)]
    pub polars_preload: Option<PolarsPreload>,
    /// When true (and collect_engine is "gpu"), passes `raise_on_fail=True` to
    /// GPUEngine so silent CPU fallback is a hard error.
    #[serde(default)]
    pub polars_raise_on_fail: bool,
}

#[derive(Debug, Deserialize)]
pub struct DatasetConfig {
    pub name: String,
    pub table_sql: String,
    #[serde(default)]
    pub polars_lazyframe_py: Option<String>,
    #[serde(default)]
    pub polars_source: Option<String>,
    #[serde(default)]
    pub polars_format: Option<PolarsFormat>,
    #[serde(default)]
    pub sample: Option<SampleConfig>,
    #[serde(default)]
    pub setup_sql: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ScenarioConfig {
    pub name: String,
    pub engine: String,
    pub dataset: String,
    #[serde(default)]
    pub warm_runs: Option<usize>,
    #[serde(default)]
    pub cold_runs: Option<usize>,
    #[serde(default)]
    pub setup_sql: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SuiteResults {
    pub queries_file: PathBuf,
    pub generated_from: PathBuf,
    pub runs: Vec<QueryExecution>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryExecution {
    pub scenario: String,
    pub engine: String,
    pub dataset: String,
    pub query_index: usize,
    pub query_sql: String,
    pub cold_run: usize,
    pub tags: Vec<String>,
    pub process: ProcessResult,
    pub warm_runs: Vec<WarmRunResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProcessResult {
    pub status_code: Option<i32>,
    pub success: bool,
    pub elapsed_seconds: f64,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
    pub command: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WarmRunResult {
    pub warm_run: usize,
    #[serde(default)]
    pub temperature: Option<ProfileTemperature>,
    pub profile_path: PathBuf,
    pub profile_summary: Option<crate::profile::ProfileSummary>,
}

impl WarmRunResult {
    pub fn effective_temperature(&self) -> ProfileTemperature {
        self.temperature.unwrap_or_else(|| {
            if self.warm_run == 1 {
                ProfileTemperature::Cold
            } else {
                ProfileTemperature::Warm
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum EngineBackend {
    Duckdb,
    Polars,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum QueryExecutionMode {
    #[default]
    Direct,
    GpuExecution,
}

/// Controls how the Polars backend pre-warms GPU/CPU state before the
/// measured warm-run loop begins, within the same long-lived subprocess.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "kebab-case")]
pub enum PolarsPreload {
    /// No preload — first warm_run is cold (default).
    #[default]
    None,
    /// Eagerly collect the full LazyFrame into a CPU DataFrame once, then run
    /// all measured queries against `df.lazy()`.  Removes scan/IO overhead
    /// from the measured runs; useful as an upper-bound data-loaded scenario.
    Collect,
    /// Run one extra `.collect(engine=…)` of the benchmark query before the
    /// measured loop.  Warms GPU buffer manager / page cache without fully
    /// materialising the frame.  Best equivalent to the Sirius hot mode.
    Warmup,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum PolarsFormat {
    Parquet,
    Delta,
    Iceberg,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProfileTemperature {
    Cold,
    Warm,
}

#[derive(Debug, Deserialize)]
pub struct SampleConfig {
    #[serde(default)]
    pub rows: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_opt_bytes")]
    pub target_bytes: Option<u64>,
    #[serde(default = "default_probe_rows")]
    pub probe_rows: usize,
    #[serde(default = "default_overhead_multiplier")]
    pub overhead_multiplier: f64,
}

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub access_key_id: String,
    pub secret_access_key: String,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub url_style: Option<S3UrlStyle>,
    #[serde(default)]
    pub use_ssl: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum S3UrlStyle {
    Path,
    Vhost,
}

#[derive(Debug, Deserialize)]
pub struct WarehouseConfig {
    #[serde(default = "default_warehouse_enabled")]
    pub enabled: bool,
    #[serde(default = "default_warehouse_table_path")]
    pub qlens_runs_table_path: String,
    #[serde(default = "default_warehouse_python")]
    pub python: PathBuf,
    #[serde(default = "default_warehouse_experiment_template")]
    pub experiment_id_template: String,
    #[serde(default)]
    pub s3: Option<S3Config>,
}

fn default_profiling_mode() -> String {
    "json".to_string()
}

fn default_warm_runs() -> usize {
    3
}

fn default_cold_runs() -> usize {
    1
}

fn default_database() -> String {
    ":memory:".to_string()
}

fn default_engine_backend() -> EngineBackend {
    EngineBackend::Duckdb
}

fn default_table_name() -> String {
    "hits".to_string()
}

fn default_probe_rows() -> usize {
    100_000
}

fn default_overhead_multiplier() -> f64 {
    3.0
}

fn default_warehouse_enabled() -> bool {
    true
}

fn default_warehouse_table_path() -> String {
    "s3://qb-performance/bronze/qlens_runs/".to_string()
}

fn default_warehouse_python() -> PathBuf {
    PathBuf::from("python3")
}

fn default_warehouse_experiment_template() -> String {
    "{output}-{scenario}".to_string()
}

fn deserialize_opt_bytes<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    struct OptBytesVisitor;

    impl<'de> Visitor<'de> for OptBytesVisitor {
        type Value = Option<u64>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an integer byte count or a human-readable size string")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserialize_bytes_value(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(OptBytesVisitor)
}

fn deserialize_bytes_value<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    struct BytesVisitor;

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("an integer byte count or a human-readable size string")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            u64::try_from(value).map_err(|_| E::custom("byte size must be non-negative"))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<ByteSize>()
                .map(|size| size.as_u64())
                .map_err(|err| E::custom(format!("invalid byte size `{value}`: {err}")))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(BytesVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_example_config() {
        let config_text = include_str!("../runner.toml");
        let config: Config = toml::from_str(config_text).expect("example config should parse");
        assert!(!config.engines.is_empty());
        assert!(!config.datasets.is_empty());
        assert!(!config.scenarios.is_empty());
        assert_eq!(config.suite.warm_runs, 3);
        assert_eq!(config.suite.cold_runs, 2);
        assert_eq!(
            config
                .suite
                .s3
                .as_ref()
                .map(|s3| !s3.access_key_id.is_empty()),
            Some(true)
        );
        if let Some(sample) = config.datasets.iter().find_map(|dataset| dataset.sample.as_ref()) {
            assert!(sample.probe_rows > 0);
        }
    }

    #[test]
    fn parses_human_readable_target_bytes() {
        let config: Config = toml::from_str(
            r#"
            [suite]
            queries_file = "queries.sql"
            output_dir = "results/local"

            [suite.s3]
            access_key_id = "AKIA_TEST"
            secret_access_key = "secret"
            region = "us-east-1"
            endpoint = "http://minio:9000"
            url_style = "path"
            use_ssl = false

            [[engines]]
            name = "duckdb"
            binary = "duckdb"

            [[datasets]]
            name = "sampled"
            table_sql = "SELECT 1"
            sample = { target_bytes = "2GiB" }

            [[scenarios]]
            name = "scenario"
            engine = "duckdb"
            dataset = "sampled"
            "#,
        )
        .expect("config with human bytes should parse");

        assert_eq!(
            config.datasets[0]
                .sample
                .as_ref()
                .and_then(|sample| sample.target_bytes),
            Some(2_147_483_648)
        );
        assert_eq!(
            config
                .suite
                .s3
                .as_ref()
                .and_then(|s3| s3.region.as_deref()),
            Some("us-east-1")
        );
        assert_eq!(
            config.suite.s3.as_ref().and_then(|s3| s3.use_ssl),
            Some(false)
        );
    }

    #[test]
    fn parses_query_execution_mode() {
        let config: Config = toml::from_str(
            r#"
            [suite]
            queries_file = "queries.sql"
            output_dir = "results/local"

            [[engines]]
            name = "duckdb"
            binary = "duckdb"

            [[engines]]
            name = "siriusdb"
            backend = "duckdb"
            query_execution_mode = "gpu-execution"
            binary = "siriusdb"

            [[datasets]]
            name = "sampled"
            table_sql = "SELECT 1"

            [[scenarios]]
            name = "scenario"
            engine = "siriusdb"
            dataset = "sampled"
            "#,
        )
        .expect("config with query execution mode should parse");

        assert_eq!(
            config.engines[0].query_execution_mode,
            QueryExecutionMode::Direct
        );
        assert_eq!(
            config.engines[1].query_execution_mode,
            QueryExecutionMode::GpuExecution
        );
    }

    #[test]
    fn infers_profile_temperature_for_legacy_results() {
        let cold = WarmRunResult {
            warm_run: 1,
            temperature: None,
            profile_path: PathBuf::from("warm01.json"),
            profile_summary: None,
        };
        let warm = WarmRunResult {
            warm_run: 2,
            temperature: None,
            profile_path: PathBuf::from("warm02.json"),
            profile_summary: None,
        };

        assert_eq!(cold.effective_temperature(), ProfileTemperature::Cold);
        assert_eq!(warm.effective_temperature(), ProfileTemperature::Warm);
    }
}
