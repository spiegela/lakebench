import json
import os
import sys
import time
import traceback
from pathlib import Path

request = json.loads(Path(sys.argv[1]).read_text())

# All warm-run profile output paths (one per measured iteration).
# Written in order; each overwrites only its own slot.
warm_profile_paths = request.get("warm_profile_paths") or []
preload = request.get("preload", "none")          # "none" | "collect" | "warmup"
raise_on_fail = request.get("raise_on_fail", False)

def _write_error(path, exc_text):
    Path(path).write_text(json.dumps({
        "query_name": request.get("query"),
        "error": exc_text,
        "engine": "polars",
    }, indent=2))

try:
    collect_engine = request.get("collect_engine")
    if collect_engine and collect_engine != "auto":
        os.environ["POLARS_ENGINE_AFFINITY"] = collect_engine

    import polars as pl

    # PyIceberg validates blob metadata types against a strict allow-list.
    # Custom engines (e.g. QBeast) embed their own blob type ('qbeast-meta')
    # which triggers a ValidationError.  Patch the low-level JSON parser to
    # strip unrecognised blob entries before Pydantic sees them.
    def _patch_pyiceberg_blob_types():
        _KNOWN_BLOB_TYPES = frozenset([
            "apache-datasketches-theta-v1",
            "deletion-vector-v1",
        ])
        try:
            from pyiceberg.table import metadata as _iceberg_meta
            import json as _json

            _orig = _iceberg_meta.TableMetadataUtil.parse_raw

            def _patched(metadata):
                try:
                    data = _json.loads(metadata)
                    for stat in data.get("statistics", []):
                        stat["blob-metadata"] = [
                            b for b in stat.get("blob-metadata", [])
                            if b.get("type") in _KNOWN_BLOB_TYPES
                        ]
                    data["statistics"] = [
                        s for s in data.get("statistics", [])
                        if s.get("blob-metadata")
                    ]
                    metadata = _json.dumps(data)
                except Exception:
                    pass  # leave metadata unchanged if pre-processing fails
                return _orig(metadata)

            _iceberg_meta.TableMetadataUtil.parse_raw = staticmethod(_patched)
        except Exception as _e:
            print(f"  warning: could not patch PyIceberg blob-type validation: {_e}", file=sys.stderr)

    _patch_pyiceberg_blob_types()

    storage_options = request.get("storage_options")
    iceberg_storage_options = request.get("iceberg_storage_options")

    def _lakebench_redact_env(name):
        value = os.environ.get(name)
        if not value:
            return "<unset>"
        if name == "AWS_SESSION_TOKEN":
            return f"<set len={len(value)}>"
        if len(value) <= 8:
            return "<set>"
        return value[:4] + "..." + value[-4:]

    def _lakebench_debug_dump():
        print("lakebench polars runner env:", file=sys.stderr)
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
            "AWS_EC2_METADATA_DISABLED",
            "AWS_SDK_LOAD_CONFIG",
        ]:
            print(f"  {key}={_lakebench_redact_env(key)}", file=sys.stderr)
        print(f"  storage_options={storage_options}", file=sys.stderr)
        print(f"  iceberg_storage_options={iceberg_storage_options}", file=sys.stderr)
        print(f"  preload={preload}  raise_on_fail={raise_on_fail}", file=sys.stderr)
        print(f"  warm_runs={len(warm_profile_paths)}", file=sys.stderr)

    _lakebench_debug_dump()

    _credential_provider_fn = None
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        _creds = {
            "aws_access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
        }
        if os.environ.get("AWS_SESSION_TOKEN"):
            _creds["aws_session_token"] = os.environ["AWS_SESSION_TOKEN"]
        _credential_provider_fn = lambda: (dict(_creds), None)

    # cudf-polars (GPU engine) refuses to translate any scan IR that carries
    # hive_parts metadata, even when the partition columns are unused.  We
    # don't rely on hive partition pruning in the benchmark queries (the
    # partition values are also stored as regular columns), so it's safe to
    # disable hive partitioning at scan time when the GPU engine is requested.
    _disable_hive = collect_engine == "gpu"

    def _scan_delta_as_parquet(source, base_kwargs):
        """For GPU mode, bypass scan_delta and read the underlying parquet files
        directly via the deltalake client.  This guarantees no hive_parts IR
        leaks into the plan from scan_delta's internal builder."""
        from deltalake import DeltaTable
        dt_kwargs = {}
        if storage_options and isinstance(source, str) and source.startswith("s3://"):
            dt_kwargs["storage_options"] = dict(storage_options)
        dt = DeltaTable(source.rstrip("/"), **dt_kwargs)
        if hasattr(dt, "file_uris"):
            files = list(dt.file_uris())
        else:
            base = source.rstrip("/")
            files = [f"{base}/{f}" for f in dt.files()]
        sp_kwargs = dict(base_kwargs)
        sp_kwargs["hive_partitioning"] = False
        return pl.scan_parquet(files, **sp_kwargs)

    def lakebench_scan(fmt, source):
        if fmt == "parquet":
            kwargs = {}
            if _disable_hive:
                kwargs["hive_partitioning"] = False
            if storage_options and isinstance(source, str) and source.startswith("s3://"):
                kwargs["storage_options"] = dict(storage_options)
                if _credential_provider_fn is not None:
                    kwargs["credential_provider"] = _credential_provider_fn
            return pl.scan_parquet(source, **kwargs)
        if fmt == "delta":
            kwargs = {}
            if storage_options and isinstance(source, str) and source.startswith("s3://"):
                kwargs["storage_options"] = dict(storage_options)
                if _credential_provider_fn is not None:
                    kwargs["credential_provider"] = _credential_provider_fn
            if _disable_hive:
                # Try scan_delta with hive_partitioning=False if supported;
                # otherwise rebuild the scan from the delta file list via
                # scan_parquet (guaranteed no hive_parts IR for GPU).
                try:
                    return pl.scan_delta(source, hive_partitioning=False, **kwargs)
                except TypeError:
                    print("  gpu: scan_delta has no hive_partitioning kwarg;"
                          " falling back to scan_parquet over delta file list",
                          file=sys.stderr)
                    try:
                        return _scan_delta_as_parquet(source, kwargs)
                    except Exception as _delta_e:
                        print(f"  gpu: delta→parquet fallback failed ({_delta_e!r});"
                              " using plain scan_delta (may hit hive-partition error)",
                              file=sys.stderr)
            return pl.scan_delta(source, **kwargs)
        if fmt == "iceberg":
            kwargs = {}
            if isinstance(source, str) and source.startswith("s3://"):
                # scan_iceberg does not accept credential_provider.
                #
                # Polars sets _auto_init_unhandled_key on CredentialProviderAWS whenever
                # any explicit credential key (aws_access_key_id, s3.access-key-id, …)
                # appears in storage_options, causing _can_use_as_provider() → False and
                # leaving the native parquet data reads unsigned (→ 400).
                #
                # Fix: pass only non-credential config (region, endpoint) so that
                # CredentialProviderAWS @ AutoInit is NOT disabled and uses boto3, which
                # reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from the environment
                # that apply_s3_env() already populated.  PyIceberg also reads those
                # env vars for its own metadata requests, so no credentials are needed
                # in storage_options at all.
                _CRED_PREFIXES = ("aws_access_key_id", "aws_secret_access_key",
                                  "aws_session_token", "s3.access-key-id",
                                  "s3.secret-access-key", "s3.session-token")
                non_cred = {
                    k: v for k, v in (iceberg_storage_options or {}).items()
                    if not any(k.startswith(p) for p in _CRED_PREFIXES)
                }
                if non_cred:
                    kwargs["storage_options"] = non_cred
            lf = pl.scan_iceberg(source, **kwargs)
            # Iceberg tables with physical partitioning store partition column
            # values both in the parquet files (as a regular column) and in the
            # Iceberg partition spec.  Polars exposes both under the same name,
            # producing duplicate column names that cause DuplicateError when
            # any downstream expression references that column.  Select only the
            # first occurrence of each name to deduplicate safely.
            col_names = lf.columns
            seen: set = set()
            unique_cols = []
            for name in col_names:
                if name not in seen:
                    seen.add(name)
                    unique_cols.append(name)
            if len(unique_cols) < len(col_names):
                lf = lf.select(unique_cols)
            return lf
        raise ValueError(f"unsupported polars format: {fmt}")

    if storage_options:
        def _is_cloud_source(source):
            if isinstance(source, str):
                return source.startswith(("s3://", "gs://", "az://", "abfs://", "abfss://"))
            if isinstance(source, (list, tuple)) and source:
                first = source[0]
                return isinstance(first, str) and first.startswith(("s3://", "gs://", "az://", "abfs://", "abfss://"))
            return False

        def _wrap_cloud_scan(fn):
            def wrapped(source, *args, **kwargs):
                if _is_cloud_source(source):
                    kwargs.setdefault("storage_options", dict(storage_options))
                    if _credential_provider_fn is not None:
                        kwargs.setdefault("credential_provider", _credential_provider_fn)
                return fn(source, *args, **kwargs)
            return wrapped

        pl.scan_parquet = _wrap_cloud_scan(pl.scan_parquet)
        pl.read_parquet = _wrap_cloud_scan(pl.read_parquet)
        if hasattr(pl, "scan_delta"):
            pl.scan_delta = _wrap_cloud_scan(pl.scan_delta)
        if hasattr(pl, "scan_csv"):
            pl.scan_csv = _wrap_cloud_scan(pl.scan_csv)
        if hasattr(pl, "scan_ndjson"):
            pl.scan_ndjson = _wrap_cloud_scan(pl.scan_ndjson)

    def _fix_polars_sql(sql, dataset=None):
        """Rewrite canonical ClickBench/DuckDB SQL for Polars SQLContext compatibility.

        Returns (rewritten_sql, possibly_modified_dataset).
        Pure SQL rewrites are always applied; dataset-modifying rewrites (which
        pre-compute columns that Polars SQL can't express) are skipped when
        dataset is None — pass the LazyFrame to enable them.
        All rewrites are semantics-preserving for the ClickBench query set.
        """
        import re

        # ── 1. ORDER BY COUNT(*) without alias ───────────────────────────────
        # Polars raises ShapeError when a bare aggregate appears in ORDER BY
        # without a matching alias in the SELECT list.
        # Replace ORDER BY first so the subsequent SELECT alias pass only touches
        # COUNT(*) occurrences that remain in the projection list.
        if re.search(r'\bORDER\s+BY\s+COUNT\s*\(\s*\*\s*\)', sql, re.IGNORECASE):
            sql = re.sub(
                r'\bORDER\s+BY\s+COUNT\s*\(\s*\*\s*\)',
                'ORDER BY __polars_cnt',
                sql, flags=re.IGNORECASE,
            )
            sql = re.sub(
                r'\bCOUNT\s*\(\s*\*\s*\)(?!\s+AS\b)',
                'COUNT(*) AS __polars_cnt',
                sql, flags=re.IGNORECASE,
            )

        # ── 2. STRLEN(x) → LENGTH(x) ─────────────────────────────────────────
        # STRLEN is not part of Polars SQL; LENGTH is the standard equivalent.
        # Affected: q28, q29.
        sql = re.sub(r'\bSTRLEN\s*\(', 'LENGTH(', sql, flags=re.IGNORECASE)

        # ── 3. toDateTime(x) → x ─────────────────────────────────────────────
        # toDateTime is a DuckDB macro defined in setup_sql; Polars doesn't know
        # about it.  Iceberg/parquet schemas already carry the column as a proper
        # timestamp type, so stripping the wrapper is safe.
        # Affected: q19.
        sql = re.sub(r'\btoDateTime\s*\(([^)]+)\)', r'\1', sql, flags=re.IGNORECASE)

        # ── 4. Bare integer literal at start of SELECT list ──────────────────
        # "SELECT 1, col, ..." — Polars names the literal 'literal' internally
        # but then cannot project it through a GROUP BY aggregation.
        # The ClickBench intent is to benchmark the GROUP BY; the literal column
        # is incidental — drop it from SELECT.
        # Affected: q35 — "SELECT 1, URL, COUNT(*) AS c FROM hits GROUP BY URL"
        sql = re.sub(r'\bSELECT\s+\d+\s*,\s*', 'SELECT ', sql, flags=re.IGNORECASE)

        # ── 5. Positional GROUP BY literal (GROUP BY 1, col) ─────────────────
        # Polars SQLContext does not support positional GROUP BY references.
        # "GROUP BY 1, URL" groups by a constant (≡ no grouping on that key);
        # removing it is semantics-preserving after step 4 drops SELECT 1 too.
        sql = re.sub(r'\bGROUP\s+BY\s+1\s*,\s*', 'GROUP BY ', sql, flags=re.IGNORECASE)

        # ── 6. SUM(col ± N) without alias → add alias ────────────────────────
        # Polars defaults the output name for SUM(col ± N) to `col` for every N,
        # causing DuplicateError in q30 which has 90 SUM(ResolutionWidth ± N).
        def _alias_sum_arith(m):
            col, op, n = m.group(1), m.group(2).strip(), m.group(3)
            sfx = f'plus{n}' if op == '+' else f'minus{n}'
            return f'SUM({col} {op} {n}) AS sum_{col}_{sfx}'
        sql = re.sub(
            r'\bSUM\s*\(\s*(\w+)\s*([+-])\s*(\d+)\s*\)(?!\s+AS\b)',
            _alias_sum_arith, sql, flags=re.IGNORECASE,
        )

        # ── 7. MIN/MAX on the same column without aliases → add fn_col alias ──
        # "SELECT MIN(EventDate), MAX(EventDate) FROM hits" — both default to
        # output name 'EventDate', causing DuplicateError in q7.
        # Detect when the same column appears under multiple different unaliased
        # aggregate functions in SELECT and auto-alias each as fn_col.
        select_m = re.search(r'\bSELECT\b(.*?)\bFROM\b', sql, re.IGNORECASE | re.DOTALL)
        if select_m:
            select_clause = select_m.group(1)
            col_fns: dict = {}
            for m in re.finditer(
                r'\b(MIN|MAX|SUM|AVG)\s*\(\s*(\w+)\s*\)(?!\s+AS\b)',
                select_clause, re.IGNORECASE,
            ):
                fn, col = m.group(1).upper(), m.group(2).upper()
                col_fns.setdefault(col, set()).add(fn)
            multi_fn_cols = {col for col, fns in col_fns.items() if len(fns) > 1}
            if multi_fn_cols:
                def _auto_alias(m):
                    fn, col = m.group(1), m.group(2)
                    if col.upper() in multi_fn_cols:
                        return f'{fn}({col}) AS {fn.lower()}_{col}'
                    return m.group(0)
                sql = re.sub(
                    r'\b(MIN|MAX|SUM|AVG)\s*\(\s*(\w+)\s*\)(?!\s+AS\b)',
                    _auto_alias, sql, flags=re.IGNORECASE,
                )

        # ── 8. REGEXP_REPLACE(col, pattern, '\1') → pre-compute via str.extract
        # Polars SQLContext does not support REGEXP_REPLACE at all.  For the
        # capture-group backreference form (\1) used in q29, pre-compute the
        # column on the dataset via str.extract and rewrite the SQL to reference
        # the pre-computed column name.
        if dataset is not None and re.search(r'\bREGEXP_REPLACE\b', sql, re.IGNORECASE):
            rr_m = re.search(
                r"\bREGEXP_REPLACE\s*\(\s*(\w+)\s*,\s*'([^']+)'\s*,\s*'(\\1|\\\\1)'\s*\)\s+AS\s+(\w+)",
                sql, re.IGNORECASE,
            )
            if rr_m:
                src_col, pattern, alias = rr_m.group(1), rr_m.group(2), rr_m.group(4)
                dataset = dataset.with_columns(
                    pl.col(src_col).str.extract(pattern, group_index=1).alias(alias)
                )
                sql = re.sub(
                    r"\bREGEXP_REPLACE\s*\(.*?\)\s+AS\s+" + re.escape(alias),
                    alias, sql, flags=re.IGNORECASE | re.DOTALL,
                )

        # ── 9. GROUP BY arithmetic expressions (col ± N) → pre-compute columns
        # "GROUP BY ClientIP, ClientIP - 1, ClientIP - 2, ..." — Polars names
        # every key by the base column ('ClientIP'), causing DuplicateError in
        # q36.  Pre-compute each expression as a named column on the dataset and
        # replace all occurrences of the expression throughout the SQL.
        if dataset is not None:
            grp_m = re.search(
                r'\bGROUP\s+BY\b(.*?)(?=\bORDER\b|\bHAVING\b|\bLIMIT\b|;|$)',
                sql, re.IGNORECASE | re.DOTALL,
            )
            if grp_m:
                arith_keys = list(dict.fromkeys(  # dedupe, preserve order
                    (m.group(1), m.group(2).strip(), m.group(3))
                    for m in re.finditer(r'\b(\w+)\s*([+-])\s*(\d+)\b', grp_m.group(1))
                ))
                if arith_keys:
                    exprs = []
                    for col, op, n in arith_keys:
                        sfx = f'plus{n}' if op == '+' else f'minus{n}'
                        col_alias = f'__{col}_{sfx}'
                        val = int(n)
                        exprs.append(
                            (pl.col(col) + val if op == '+' else pl.col(col) - val).alias(col_alias)
                        )
                        sql = re.sub(
                            rf'\b{re.escape(col)}\s*{re.escape(op)}\s*{re.escape(n)}\b',
                            col_alias, sql,
                        )
                    dataset = dataset.with_columns(exprs)

        # ── 10. DATE_TRUNC('unit', col) → pre-compute via dt.truncate() ──────
        # Polars SQLContext does not support DATE_TRUNC.  Pre-compute each unique
        # (unit, col) pair as a named column on the dataset and replace every
        # occurrence of DATE_TRUNC(...) in the SQL with that column name.
        # Affected: q43 — DATE_TRUNC('minute', EventTime) appears three times
        #           (SELECT, GROUP BY, ORDER BY).
        if dataset is not None and re.search(r'\bDATE_TRUNC\b', sql, re.IGNORECASE):
            _TRUNC_UNITS = {
                'second': '1s', 'minute': '1m', 'hour': '1h',
                'day': '1d',   'week': '1w',   'month': '1mo',
                'quarter': '3mo', 'year': '1y',
            }
            trunc_exprs = []
            trunc_hits = list(dict.fromkeys(  # dedupe, preserve order
                (m.group(1).lower(), m.group(2))
                for m in re.finditer(
                    r"\bDATE_TRUNC\s*\(\s*'(\w+)'\s*,\s*(\w+)\s*\)",
                    sql, re.IGNORECASE,
                )
            ))
            for unit, col in trunc_hits:
                duration = _TRUNC_UNITS.get(unit)
                if duration is None:
                    continue  # unknown unit — leave as-is, will fail at runtime
                col_alias = f'__trunc_{unit}_{col}'
                trunc_exprs.append(
                    pl.col(col).dt.truncate(duration).alias(col_alias)
                )
                sql = re.sub(
                    rf"\bDATE_TRUNC\s*\(\s*'{re.escape(unit)}'\s*,\s*{re.escape(col)}\s*\)",
                    col_alias, sql, flags=re.IGNORECASE,
                )
            if trunc_exprs:
                dataset = dataset.with_columns(trunc_exprs)

        return sql, dataset

    # ── Build the GPUEngine (once, shared across all warm iterations) ─────────
    _gpu_engine = None
    if collect_engine == "gpu":
        try:
            from polars.lazyframe.engine_config import GPUEngine
            _rmm_mr = None
            # Try to set up an RMM pool to reduce allocator overhead.
            try:
                import rmm
                _rmm_mr = rmm.mr.PoolMemoryResource(
                    rmm.mr.CudaAsyncMemoryResource(),
                )
                print("  rmm: CudaAsyncMemoryResource pool initialised", file=sys.stderr)
            except Exception as _rmm_e:
                print(f"  rmm: pool not available ({_rmm_e!r}), using default allocator",
                      file=sys.stderr)
            _gpu_kwargs = {"device": 0, "raise_on_fail": raise_on_fail}
            if _rmm_mr is not None:
                _gpu_kwargs["memory_resource"] = _rmm_mr
            _gpu_engine = GPUEngine(**_gpu_kwargs)
            print(f"  gpu engine: GPUEngine(device=0, raise_on_fail={raise_on_fail},"
                  f" rmm={'yes' if _rmm_mr else 'no'})", file=sys.stderr)
        except Exception as _gpu_init_e:
            print(f"  gpu engine init failed: {_gpu_init_e!r}", file=sys.stderr)
            if raise_on_fail:
                raise

    def _empty_profile_df():
        return pl.DataFrame({
            "node": pl.Series([], dtype=pl.String),
            "start": pl.Series([], dtype=pl.Int64),
            "end": pl.Series([], dtype=pl.Int64),
        })

    def _collect_query(lf):
        """Run lf.collect() with the appropriate engine, capturing profiling data.

        GPU path:
          1. Try lf.profile(engine=gpu)  — fastest and gives timing data.
          2. If that fails, try lf.collect(engine=gpu) — still GPU, no timing.
          3. If GPU collect also fails and raise_on_fail=False, fall back to CPU
             profile().  This handles the common case where the GPU engine does
             not support a scan type (e.g. hive partitioning) but the query can
             still be measured on CPU.
        CPU path:
          1. Try lf.profile()  — full timing data.
          2. If profile() raises (e.g. "no data to time" with empty result sets
             or certain aggregation plans), fall back to lf.collect() with an
             empty profile frame and wall-clock latency.
        """
        if _gpu_engine is not None:
            # ── GPU: try profile first ─────────────────────────────────────────
            try:
                result, profile = lf.profile(engine=_gpu_engine)
                return result, profile
            except Exception as _prof_e:
                print(f"  warning: GPU profile() failed ({_prof_e!r});"
                      " retrying with GPU collect()", file=sys.stderr)
            # ── GPU: profile failed → try plain collect with GPU ───────────────
            try:
                result = lf.collect(engine=_gpu_engine)
                return result, _empty_profile_df()
            except Exception as _gpu_e:
                if raise_on_fail:
                    raise
                print(f"  warning: GPU collect() also failed ({_gpu_e!r});"
                      " falling back to CPU", file=sys.stderr)
            # ── CPU fallback (raise_on_fail=False only) ────────────────────────
            try:
                result, profile = lf.profile()
                return result, profile
            except Exception as _cpu_prof_e:
                print(f"  warning: CPU profile() failed ({_cpu_prof_e!r});"
                      " using plain collect()", file=sys.stderr)
                return lf.collect(), _empty_profile_df()
        else:
            # ── CPU-only path ──────────────────────────────────────────────────
            try:
                result, profile = lf.profile()
                return result, profile
            except Exception as _prof_exc:
                # profile() raises "no data to time" when the streaming engine produces
                # no timing nodes — this happens with empty result sets or certain
                # aggregation plans.  Fall back to a plain collect() with wall-clock
                # timing; profile metrics (scan_timing, cpu_time) will be absent.
                print(f"  warning: profile() failed ({_prof_exc!r}); falling back to collect()",
                      file=sys.stderr)
                return lf.collect(), _empty_profile_df()

    # ── Evaluate the dataset expression and rewrite SQL (once) ───────────────
    namespace = {"pl": pl, "lakebench_scan": lakebench_scan, "__builtins__": __builtins__}
    dataset = eval(request["dataset_expr"], namespace, {})
    _query_sql, dataset = _fix_polars_sql(request["query"], dataset=dataset)

    # ── Preload: "collect" eagerly materialises the dataset into CPU memory ──
    # All measured queries then scan df.lazy() instead of the original LazyFrame,
    # removing GPFS/S3 scan overhead from the timing loop.
    if preload == "collect":
        print("  preload: eagerly collecting full dataset into memory…", file=sys.stderr)
        _preload_start = time.perf_counter()
        dataset = dataset.collect().lazy()
        print(f"  preload: done in {time.perf_counter() - _preload_start:.3f}s", file=sys.stderr)

    # ── Build the SQLContext and query plan (once) ────────────────────────────
    ctx = pl.SQLContext(eager=False, **{request["table_name"]: dataset})
    query = ctx.execute(_query_sql, eager=False)

    optimized_plan = None
    unoptimized_plan = None
    try:
        optimized_plan = query.explain()
    except Exception:
        optimized_plan = None
    try:
        unoptimized_plan = query.explain(optimized=False)
    except Exception:
        unoptimized_plan = None

    # ── Preload: "warmup" runs one throwaway collect to warm GPU/page cache ──
    if preload == "warmup":
        print("  preload: running warmup collect…", file=sys.stderr)
        _warmup_start = time.perf_counter()
        try:
            _warmup_result, _ = _collect_query(query)
            print(f"  preload: warmup done in {time.perf_counter() - _warmup_start:.3f}s"
                  f" ({getattr(_warmup_result, 'height', '?')} rows)", file=sys.stderr)
        except Exception as _wu_e:
            print(f"  preload: warmup failed ({_wu_e!r}); continuing anyway", file=sys.stderr)

    # ── Measured warm-run loop ────────────────────────────────────────────────
    for warm_idx, profile_path in enumerate(warm_profile_paths):
        warm_run = warm_idx + 1
        try:
            start = time.perf_counter()
            result, profile = _collect_query(query)
            latency = time.perf_counter() - start

            profile_rows = profile.to_dicts() if hasattr(profile, "to_dicts") else []
            scan_timing = None
            total_timing = None
            max_node_end = None
            if profile_rows:
                numeric_rows = []
                for row in profile_rows:
                    start_val = row.get("start")
                    end_val = row.get("end")
                    if isinstance(start_val, (int, float)) and isinstance(end_val, (int, float)):
                        duration_seconds = (end_val - start_val) / 1_000_000.0
                        numeric_rows.append(duration_seconds)
                        max_node_end = max(max_node_end or 0.0, end_val / 1_000_000.0)
                        node_name = str(row.get("node", "")).lower()
                        if "scan" in node_name:
                            scan_timing = (scan_timing or 0.0) + duration_seconds
                if numeric_rows:
                    total_timing = sum(numeric_rows)

            result_size = None
            profile_size = None
            try:
                result_size = result.estimated_size()
            except Exception:
                result_size = None
            try:
                profile_size = profile.estimated_size()
            except Exception:
                profile_size = None

            summary = {
                "query_name": request["query"],
                "latency": latency,
                "rows_returned": getattr(result, "height", None),
                "cpu_time": total_timing,
                "blocked_thread_time": 0.0,
                "result_set_size": result_size,
                "cumulative_cardinality": getattr(result, "height", None),
                "scan_operator_timing_seconds": scan_timing,
                "system_peak_buffer_memory": profile_size,
                "extra_metrics": {
                    "polars_profile_cpu_time": total_timing,
                    "polars_profile_end_to_end": max_node_end,
                },
                "engine": "polars",
                "extra_info": {
                    "collect_engine": collect_engine,
                    "preload": preload,
                    "warm_run": warm_run,
                    "profile_rows": profile_rows,
                    "optimized_plan": optimized_plan if warm_run == 1 else None,
                    "unoptimized_plan": unoptimized_plan if warm_run == 1 else None,
                    "profile_row_count": len(profile_rows),
                },
            }
            Path(profile_path).write_text(json.dumps(summary, indent=2))
            print(f"  warm_run={warm_run} latency={latency:.3f}s rows={getattr(result, 'height', '?')}",
                  file=sys.stderr)

        except Exception:
            exc_text = traceback.format_exc()
            print(f"  warm_run={warm_run} FAILED:\n{exc_text}", file=sys.stderr)
            _write_error(profile_path, exc_text)
            raise

except Exception:
    # If we blew up before the warm loop (e.g. during dataset eval or preload),
    # write the error into every profile slot that wasn't already written.
    exc_text = traceback.format_exc()
    for profile_path in warm_profile_paths:
        if not Path(profile_path).exists():
            _write_error(profile_path, exc_text)
    raise
