"""Dump all Silver rows for a tenderId into data/raw/sample_tenders.

Usage:
  python scripts/dev/dump_tender_from_silver.py <tenderId>
  python scripts/dev/dump_tender_from_silver.py <tenderId> --silver-dir E:\\...\\data\\silver
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.dataset as ds


DEFAULT_SILVER_DIR = Path(r"E:\git_projects\procurement-watchdog-api-exploration\data\silver")
DEFAULT_OUTPUT_DIR = Path(r"E:\git_projects\procurement-watchdog-lakehouse\data\raw\sample_tenders")


def _safe_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def _iter_silver_days(silver_dir: Path) -> list[Path]:
    return sorted(p for p in silver_dir.glob("bzp_*.parquet") if p.is_dir())


def _read_matches(day_path: Path, tender_id: str) -> list[dict[str, Any]]:
    dataset = ds.dataset(str(day_path), format="parquet", partitioning="hive")
    table = dataset.to_table(filter=(ds.field("tenderId") == tender_id))
    rows = table.to_pylist()
    day = day_path.name.replace("bzp_", "").replace(".parquet", "")
    for row in rows:
        row["_source_day"] = day
        row["_source_dataset"] = day_path.name
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Dump all Silver rows for a tenderId")
    parser.add_argument("tender_id", help="Target tenderId (exact match)")
    parser.add_argument(
        "--silver-dir",
        default=str(DEFAULT_SILVER_DIR),
        help=f"Silver directory (default: {DEFAULT_SILVER_DIR})",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional exact output JSON path",
    )
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)
    if not silver_dir.exists():
        raise FileNotFoundError(f"Silver directory not found: {silver_dir}")

    day_paths = _iter_silver_days(silver_dir)
    if not day_paths:
        raise FileNotFoundError(f"No bzp_*.parquet directories found in: {silver_dir}")

    all_rows: list[dict[str, Any]] = []
    matched_days: list[str] = []
    for day_path in day_paths:
        rows = _read_matches(day_path, args.tender_id)
        if rows:
            all_rows.extend(rows)
            matched_days.append(day_path.name.replace("bzp_", "").replace(".parquet", ""))

    output_path = (
        Path(args.output)
        if args.output
        else Path(args.output_dir) / f"{_safe_filename(args.tender_id)}.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "tenderId": args.tender_id,
        "silver_dir": str(silver_dir),
        "searched_days": len(day_paths),
        "matched_rows_total": len(all_rows),
        "matched_days": matched_days,
        "rows": all_rows,
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"tenderId={args.tender_id}")
    print(f"silver_dir={silver_dir}")
    print(f"searched_days={len(day_paths)}")
    print(f"matched_rows_total={len(all_rows)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
