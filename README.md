# LakeBench

This repository now includes a Rust-based benchmark harness for executing ClickBench-style SQL suites against multiple Lakehouse engines and table formats.

## What it does

- Executes ClickBench-style SQL suites against:
  - DuckDB
  - SiriusDB
- Polars (Python)
- Polars (Rust)
- Separates cold runs from warm runs by restarting the engine process between cold executions.
- Captures profiling output for every profiled execution, including the cold execution immediately after each restart and the subsequent warm executions.
- Writes structured result artifacts plus a Markdown comparison report.
- Can export benchmark runs into the shared `qlens_runs` warehouse table used by the performance workbench.
- Keeps engine-specific setup, dataset setup, and scenario setup separate so standard DuckDB, SiriusDB, and future backends can share the same suite definition.

## Config

Start from [`runner.example.toml`](runner.toml).

The model is:

- `suite`: global queries file, output location, default warm/cold run counts, and shared SQL setup.
- `engines`: executable path, env vars, engine-specific SQL setup, and default database target.
- `datasets`: logical table source plus dataset-specific SQL setup. Datasets can also declare `sample.rows` for reduced-size dry runs, and either `polars_lazyframe_py` or `polars_format` + `polars_source` for Polars-backed scenarios.
- `scenarios`: engine + dataset pairing, optional run-count overrides, and freeform tags.

If you want lakebench to publish into the same performance warehouse schema as `qlens`, add an optional `[suite.warehouse]` block:

```toml
[suite.warehouse]
enabled = true
qlens_runs_table_path = "s3://qb-performance/bronze/qlens_runs/"
python = "python3"
experiment_id_template = "{output}-{scenario}"

# Optional override if warehouse writes should use different S3 credentials
#[suite.warehouse.s3]
#access_key_id = "replace-me"
#secret_access_key = "replace-me"
#region = "us-east-1"
```

`experiment_id_template` supports `{output}`, `{scenario}`, `{engine}`, and `{dataset}`. Each LakeBench profiled execution is uploaded as a `qlens_runs` row, so the existing workbench can group and explore DuckDB, SiriusDB, and Polars runs in the same workspace.

If you want to benchmark against S3-backed datasets, you can add a shared `[suite.s3]` block:

```toml
[suite.s3]
access_key_id = "replace-me"
secret_access_key = "replace-me"
region = "us-east-1"
# session_token = "replace-me"
# endpoint = "http://localhost:9000"
# url_style = "path"
# use_ssl = false
```

Those credentials are applied to both backends before execution:

- DuckDB/Sirius get AWS-style environment variables and DuckDB `SET s3_*` statements.
- Polars gets AWS-style environment variables so `scan_parquet("s3://...")` and similar readers can authenticate.

The same S3 config is also used by the internal DuckDB probe path, so `probe-capacity` and `sample.target_bytes` work against S3 datasets too.

Each scenario builds a temporary `hits` view from the configured dataset, then runs every query in `queries.sql`.

For SiriusDB GPU execution, set `query_execution_mode = "gpu-execution"` on the DuckDB-compatible engine. LakeBench will execute each query as `CALL gpu_execution('<query text>');` while keeping reports keyed to the original query SQL.

`sample.rows` is intended for smoke-testing on smaller machines. In DuckDB it wraps the dataset SQL with a `LIMIT`; in Polars it applies `.limit(n)` to the configured LazyFrame expression before query execution.

You can also set `sample.target_bytes` instead of `sample.rows`. In that mode the runner internally uses the capacity-probe logic to estimate bytes per row for the selected scenario and queries, then derives one safe row cap from the most demanding selected query. If both are present, `sample.rows` wins.

This works for both DuckDB/Sirius and Polars scenarios. For Polars-backed scenarios, the harness uses an available DuckDB-compatible engine internally for the byte-estimation probe, then applies the derived row cap to the Polars `LazyFrame` via `.limit(n)`.

`sample.target_bytes` accepts either a raw integer byte count or a human-readable size such as `"2GiB"`, `"8 GB"`, or `"1536MiB"`.

## Usage

```bash
cargo run -- run --config runner.toml
```

This writes:

- `results/.../results.json`: structured execution metadata and extracted profile metrics.
- `results/.../report.md`: scenario and per-query comparison tables.
- `results/.../<scenario>/qXX/coldYY/`: per-process stdout/stderr logs and per-profiled-execution JSON profiles. `warm01.json` is the cold execution for that process, and later files are warm executions.

If `[suite.warehouse]` is configured and enabled, `run` also uploads those executions into the shared `qlens_runs` Delta table after writing `results.json`.

To upload an existing `results.json` later:

```bash
cargo run -- upload-warehouse --config runner.toml
```

or point it at a specific results file:

```bash
cargo run -- upload-warehouse \
  --config runner.toml \
  --results results/local/results.json
```

To rebuild the report from an existing `results.json`:

```bash
cargo run -- report --results results/local/results.json
```

To compare two legacy DuckDB profile directories directly:

```bash
cargo run -- compare \
  --left results/duckdb-local-iceberg-vanilla \
  --right results/duckdb-local-qbeast \
  --temperature best
```

Add `--tui` to launch an interactive terminal UI with a table tab and a chart tab:

```bash
cargo run -- compare \
  --left results/duckdb-local-iceberg-vanilla \
  --right results/duckdb-local-qbeast \
  --temperature best \
  --tui
```

Controls:

- `Tab`: switch between table and chart views
- `Left` / `Right`: scroll queries horizontally
- `Up` / `Down`: scroll vertically across metrics in chart view
- `q` or `Esc`: quit

To compare scenarios inside structured `results.json` output:

```bash
cargo run -- compare \
  --left results/local/results.json \
  --left-scenario duckdb-iceberg \
  --right results/local/results.json \
  --right-scenario siriusdb-iceberg-gpu \
  --temperature warm
```

The comparison output includes total and average latency, then DuckDB-specific metrics such as CPU time, bytes read, peak buffer memory, rows scanned, files read, and scan operator timing.

Cold and warm reporting is preserved in both `report` and `compare`. New `results.json` files also store an explicit `cold` or `warm` tag for each profiled execution so the reporting path no longer has to infer it from position alone.

## Capacity Probe

Use `probe-capacity` before benchmarking on smaller GPUs. It runs a schema-aware sample over the dataset, estimates average bytes per referenced row, projects that to the full table, and then suggests safe sample sizes for several GPU budgets.

```bash
cargo run -- probe-capacity \
  --config runner.toml \
  --scenario siriusdb-iceberg-gpu \
  --query 23 \
  --probe-rows 100000 \
  --overhead-multiplier 3.0
```

The standalone probe command executes through a DuckDB-compatible engine. It estimates:

- referenced columns for each query
- average bytes per sampled row
- estimated raw dataset bytes for the full query footprint
- estimated working-set bytes after applying an overhead multiplier
- recommended sample row counts for 8 GB, 16 GB, 24 GB, and 48 GB GPUs

That same probe path is also used internally when a benchmark scenario sets `sample.target_bytes`, including Polars scenarios.

## Debug Shell

When a specific query is failing and you want to inspect setup interactively, launch a debug shell for that query:

```bash
cargo run -- debug --config runner.toml --query 19 --scenario duckdb-iceberg-eventdate-part-samp
```

You can also target an engine and dataset directly:

```bash
cargo run -- debug --config runner.toml --query 19 --engine duckdb --dataset iceberg-eventdate-part
```

Behavior:

- DuckDB/Sirius first runs a preflight execution of the selected query with the generated setup. If that succeeds, the shell opens with the query already executed. If it fails, the error is shown and then the shell opens without auto-running the query so you can keep debugging in the same prepared environment.
- Polars launches an interactive Python session with `dataset`, `ctx`, `query_text`, `run()`, `run_sql(...)`, and `profile()` preloaded.
- Generated debug assets are written under `results/.../debug/...` so you can inspect or rerun the setup/query files directly.

If you use the example ClickBench compatibility macro, prefer a timestamp-safe definition:

```sql
CREATE OR REPLACE MACRO toDateTime(t) AS
  CASE
    WHEN starts_with(typeof(t), 'TIMESTAMP') THEN CAST(t AS TIMESTAMP)
    ELSE epoch_ms(CAST(t AS BIGINT) * 1000)
  END
```

That handles both epoch-style integer inputs and columns already typed as `TIMESTAMP`/`TIMESTAMP WITH TIME ZONE`.

## Polars

Polars scenarios use a Python interpreter as the engine binary and execute the ClickBench SQL through `polars.SQLContext`. The current config surface supports:

- CPU runs with `polars_collect_engine = "auto"`.
- GPU/CuDF-oriented runs with `polars_collect_engine = "gpu"`.
- Native format-aware dataset loading via `polars_format = "iceberg" | "delta" | "parquet"` plus `polars_source`.
- Arbitrary dataset loading via `polars_lazyframe_py`, which is still available as an escape hatch and should evaluate to a `pl.LazyFrame`.
- Shared row-target or byte-target sampling through the dataset `sample` config.

For common lakehouse layouts, prefer the format-aware fields so Polars uses the native scanner for the table format:

```toml
[[datasets]]
name = "iceberg-eventdate-part"
table_sql = "SELECT * FROM iceberg_scan('s3://bucket/path/metadata/00001.metadata.json')"
polars_format = "iceberg"
polars_source = "s3://bucket/path/metadata/00001.metadata.json"
```

That lets the harness select `pl.scan_iceberg`, `pl.scan_delta`, or `pl.scan_parquet` automatically and apply the right storage/auth options for each format.

The harness writes a JSON profile artifact for Polars too, but the available metrics are different from DuckDB. Latency is always present; scan timing and aggregate node timing are captured when Polars profiling returns them.

## MUSL build

For a portable static binary:

```bash
cargo build --release --target x86_64-unknown-linux-musl
```

If you want to ship to multiple Linux environments, that target is the right default. For SiriusDB you still need the target runtime to satisfy whatever the engine binary itself requires.

## Next extension points

- Add engine adapters for Polars and Polars + CuDF behind the same scenario model.
- Expand the profile parser with engine-specific metrics once you lock down the exact JSON schema you want to compare.
- Add chart/report export beyond Markdown once you decide on the preferred consumption path.
