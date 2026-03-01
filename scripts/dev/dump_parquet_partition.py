"""Dump a parquet partition directory to a viewer-friendly CSV.

Example:
  python scripts/dev/dump_parquet_partition.py ^
    "E:\\git_projects\\procurement-watchdog-lakehouse\\data\\silver\\notice_type_tables\\noticeType=ContractNotice\\publicationDateDay=2025-10-02"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow.dataset as ds


DEFAULT_OUT_ROOT = Path(r"E:\git_projects\procurement-watchdog-lakehouse\data\sample_parquets")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dump one parquet partition directory to CSV.")
    parser.add_argument("source_path", help="Source parquet partition directory path")
    parser.add_argument(
        "--name",
        default="",
        help="Optional output CSV basename (without extension). Default: source folder name",
    )
    parser.add_argument(
        "--out-root",
        default=str(DEFAULT_OUT_ROOT),
        help=f"Target directory for CSV output (default: {DEFAULT_OUT_ROOT})",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=0,
        help="Optional max number of rows to dump (0 = all rows)",
    )
    return parser.parse_args()


def _to_jsonable(value):
    if isinstance(value, np.ndarray):
        return [_to_jsonable(v) for v in value.tolist()]
    if isinstance(value, list):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, tuple):
        return [_to_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_jsonable(v) for k, v in value.items()}
    return value


def main() -> None:
    args = _parse_args()
    source = Path(args.source_path).expanduser().resolve()
    if not source.exists() or not source.is_dir():
        raise SystemExit(f"Source path is not a directory: {source}")

    out_root = Path(args.out_root).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    output_name = args.name.strip() or source.name
    output_csv = out_root / f"{output_name}.csv"

    table = ds.dataset(source, format="parquet").to_table()
    df = table.to_pandas()
    if args.rows and args.rows > 0:
        df = df.head(args.rows)

    for col_name in df.columns:
        if df[col_name].dtype == "object":
            df[col_name] = df[col_name].map(
                lambda v: json.dumps(_to_jsonable(v), ensure_ascii=False) if isinstance(v, (list, tuple, dict, np.ndarray)) else v
            )

    df.to_csv(output_csv, index=False, encoding="utf-8")
    print(f"Dumped rows: {len(df)}")
    print(f"source: {source}")
    print(f"csv: {output_csv}")


if __name__ == "__main__":
    main()
