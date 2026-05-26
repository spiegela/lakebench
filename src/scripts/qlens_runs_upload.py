import json
import sys
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: qlens_runs_upload.py <table_path> <rows.json>", file=sys.stderr)
        return 2

    table_path = sys.argv[1]
    rows_path = Path(sys.argv[2])
    rows = json.loads(rows_path.read_text())
    if not rows:
        print("warehouse uploader: no rows to upload")
        return 0

    schema = pa.schema(
        [
            pa.field("experiment_id", pa.string(), nullable=False),
            pa.field("run_id", pa.string(), nullable=False),
            pa.field("table_details", pa.string(), nullable=False),
            pa.field("query", pa.string(), nullable=False),
            pa.field("ts_start", pa.int64(), nullable=False),
            pa.field("ts_end", pa.int64(), nullable=False),
            pa.field("execution_time_ms", pa.int64(), nullable=False),
            pa.field("success", pa.bool_(), nullable=True),
            pa.field("error_message", pa.string(), nullable=True),
            pa.field("spark_conf", pa.string(), nullable=False),
            pa.field("runtime_info", pa.string(), nullable=False),
            pa.field("query_index", pa.int32(), nullable=True),
            pa.field("temperature", pa.string(), nullable=True),
            pa.field("query_name", pa.string(), nullable=True),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    write_deltalake(table_path, table, mode="append", schema_mode="merge")
    print(f"warehouse uploader: appended {len(rows)} row(s) to {table_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
