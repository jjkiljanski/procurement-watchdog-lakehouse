"""Dump unique `cpn_contractor_countries_437` values from Silver to JSON.

Example:
  python scripts/dev/dump_unique_cpn_contractor_countries.py ^
    --silver-dir "E:\\git_projects\\procurement-watchdog-api-exploration\\data\\silver" ^
    --out-json "E:\\git_projects\\procurement-watchdog-lakehouse\\data\\sample_parquets\\cpn_contractor_countries_437_unique.json"
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable

import pyarrow.dataset as ds

DEFAULT_SILVER_DIR = Path(r"E:\git_projects\procurement-watchdog-api-exploration\data\silver")
DEFAULT_OUT_JSON = Path(
    r"E:\git_projects\procurement-watchdog-lakehouse\data\sample_parquets\cpn_contractor_countries_437_unique.json"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect unique cpn_contractor_countries_437 values from Silver ContractPerformingNotice data."
    )
    parser.add_argument(
        "--silver-dir",
        default=str(DEFAULT_SILVER_DIR),
        help=f"Silver root directory (default: {DEFAULT_SILVER_DIR})",
    )
    parser.add_argument(
        "--out-json",
        default=str(DEFAULT_OUT_JSON),
        help=f"Output JSON path (default: {DEFAULT_OUT_JSON})",
    )
    parser.add_argument(
        "--date-from",
        default="",
        help="Optional lower bound publicationDateDay (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--date-to",
        default="",
        help="Optional upper bound publicationDateDay (YYYY-MM-DD)",
    )
    return parser.parse_args()


def _iter_partition_days(cpn_root: Path, date_from: str, date_to: str) -> Iterable[tuple[str, Path]]:
    for p in sorted(cpn_root.glob("publicationDateDay=*")):
        if not p.is_dir():
            continue
        day = p.name.replace("publicationDateDay=", "")
        if date_from and day < date_from:
            continue
        if date_to and day > date_to:
            continue
        yield day, p


def _as_values(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        value = raw.strip()
        return [value] if value else []
    if isinstance(raw, (list, tuple)):
        out: list[str] = []
        for item in raw:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                out.append(text)
        return out
    # numpy arrays / pyarrow scalars end here too
    text = str(raw).strip()
    return [text] if text else []


def main() -> None:
    args = _parse_args()
    silver_dir = Path(args.silver_dir).expanduser().resolve()
    out_json = Path(args.out_json).expanduser().resolve()

    cpn_root = silver_dir / "notice_type_tables" / "noticeType=ContractPerformingNotice"
    if not cpn_root.exists() or not cpn_root.is_dir():
        raise SystemExit(f"Missing CPN silver root: {cpn_root}")

    counts: Counter[str] = Counter()
    rows_scanned = 0
    partitions_scanned = 0

    source_column = ""

    for day, partition_dir in _iter_partition_days(cpn_root, args.date_from, args.date_to):
        dataset = ds.dataset(partition_dir, format="parquet")
        cols = set(dataset.schema.names)
        if "cpn_contractor_countries_437" in cols:
            source_column = "cpn_contractor_countries_437"
        elif "cpn_contractor_country_437" in cols:
            source_column = "cpn_contractor_country_437"
        elif "cpn_contractor_country" in cols:
            source_column = "cpn_contractor_country"
        else:
            continue

        table = dataset.to_table(columns=[source_column])
        rows_scanned += table.num_rows
        partitions_scanned += 1
        if source_column not in table.column_names:
            continue
        for raw in table[source_column].to_pylist():
            for country in _as_values(raw):
                counts[country] += 1

    out_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "field": "cpn_contractor_countries_437",
        "source_column_detected": source_column or None,
        "silver_dir": str(silver_dir),
        "partitions_scanned": partitions_scanned,
        "rows_scanned": rows_scanned,
        "unique_values": len(counts),
        "values": [
            {"value": value, "count": count}
            for value, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        ],
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"CPN partitions scanned: {partitions_scanned}")
    print(f"Rows scanned: {rows_scanned}")
    print(f"Unique values: {len(counts)}")
    print(f"JSON: {out_json}")


if __name__ == "__main__":
    main()
