import code
import json
import os
import readline
import sys
import traceback
from pathlib import Path

request = json.loads(Path(sys.argv[1]).read_text())

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
        print(f"warning: could not patch PyIceberg blob-type validation: {_e}", file=sys.stderr)

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
    print("lakebench polars debug env:", file=sys.stderr)
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

def lakebench_scan(fmt, source):
    if fmt == "parquet":
        kwargs = {}
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
        return pl.scan_delta(source, **kwargs)
    if fmt == "iceberg":
        kwargs = {}
        if isinstance(source, str) and source.startswith("s3://"):
            # scan_iceberg does not accept credential_provider.
            # Passing any explicit credential key disables CredentialProviderAWS AutoInit
            # (sets _auto_init_unhandled_key → _can_use_as_provider() = False), leaving
            # native parquet data reads unsigned.  Pass only non-credential config so
            # AutoInit can use boto3 → env vars (set by apply_s3_env).
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
    Pure SQL rewrites are always applied; dataset-modifying rewrites are skipped
    when dataset is None — pass the LazyFrame to enable them.
    All rewrites are semantics-preserving for the ClickBench query set.
    """
    import re

    # ── 1. ORDER BY COUNT(*) without alias ───────────────────────────────────
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

    # ── 2. STRLEN(x) → LENGTH(x) ─────────────────────────────────────────────
    sql = re.sub(r'\bSTRLEN\s*\(', 'LENGTH(', sql, flags=re.IGNORECASE)

    # ── 3. toDateTime(x) → x ─────────────────────────────────────────────────
    sql = re.sub(r'\btoDateTime\s*\(([^)]+)\)', r'\1', sql, flags=re.IGNORECASE)

    # ── 4. Bare integer literal at start of SELECT list ──────────────────────
    # "SELECT 1, col, ..." — Polars names the literal 'literal' internally but
    # cannot project it through a GROUP BY aggregation.  Drop it from SELECT.
    sql = re.sub(r'\bSELECT\s+\d+\s*,\s*', 'SELECT ', sql, flags=re.IGNORECASE)

    # ── 5. Positional GROUP BY literal (GROUP BY 1, col) ─────────────────────
    sql = re.sub(r'\bGROUP\s+BY\s+1\s*,\s*', 'GROUP BY ', sql, flags=re.IGNORECASE)

    # ── 6. SUM(col ± N) without alias → add alias ────────────────────────────
    def _alias_sum_arith(m):
        col, op, n = m.group(1), m.group(2).strip(), m.group(3)
        sfx = f'plus{n}' if op == '+' else f'minus{n}'
        return f'SUM({col} {op} {n}) AS sum_{col}_{sfx}'
    sql = re.sub(
        r'\bSUM\s*\(\s*(\w+)\s*([+-])\s*(\d+)\s*\)(?!\s+AS\b)',
        _alias_sum_arith, sql, flags=re.IGNORECASE,
    )

    # ── 7. MIN/MAX on same column without aliases → add fn_col alias ─────────
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

    # ── 8. REGEXP_REPLACE(col, pattern, '\1') → pre-compute via str.extract ──
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

    # ── 9. GROUP BY arithmetic expressions (col ± N) → pre-compute columns ───
    # "GROUP BY ClientIP, ClientIP - 1, ..." — Polars names every key by the
    # base column, causing DuplicateError.  Pre-compute named columns.
    if dataset is not None:
        grp_m = re.search(
            r'\bGROUP\s+BY\b(.*?)(?=\bORDER\b|\bHAVING\b|\bLIMIT\b|;|$)',
            sql, re.IGNORECASE | re.DOTALL,
        )
        if grp_m:
            arith_keys = list(dict.fromkeys(
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
                    continue
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

query_text = request["query"]
dataset_expr = request["dataset_expr"]
table_name = request["table_name"]
namespace = {"pl": pl, "lakebench_scan": lakebench_scan, "__builtins__": __builtins__}
dataset = eval(dataset_expr, namespace, {})
# Apply SQL rewrites; dataset-modifying transforms (REGEXP_REPLACE, arithmetic
# GROUP BY keys) are applied once here so ctx sees the pre-computed columns.
query_text, dataset = _fix_polars_sql(query_text, dataset=dataset)
ctx = pl.SQLContext(eager=False, **{table_name: dataset})

def run_sql(query):
    # Apply pure SQL rewrites only (no dataset modification in REPL helpers).
    query, _ = _fix_polars_sql(query, dataset=None)
    lf = ctx.execute(query, eager=False)
    if collect_engine and collect_engine != "auto":
        return lf.collect(engine=collect_engine)
    return lf.collect()

def run():
    return run_sql(query_text)

def profile(query=query_text):
    query, _ = _fix_polars_sql(query, dataset=None)
    lf = ctx.execute(query, eager=False)
    return lf.profile()

try:
    result = run()
    print(result)
except Exception:
    traceback.print_exc()

readline.add_history("run()")
banner = (
    "Polars debug shell\n"
    "Available helpers: run(), run_sql(...), profile(), dataset, ctx, query_text\n"
    "Use up-arrow to rerun run() after adjusting setup."
)
code.interact(banner=banner, local=globals())
