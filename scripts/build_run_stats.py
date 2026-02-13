"""Build lightweight run statistics for Silver and Gold outputs.

Reads:
  data/silver/bzp_YYYY-MM-DD.parquet
  data/gold/*/date=YYYY-MM-DD/

Writes:
  data/reports/run_stats/run_stats_YYYY-MM-DD.json
  data/reports/run_stats/run_stats_YYYY-MM-DD.md
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pyarrow.dataset as ds

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def _safe_ratio(num: int, den: int) -> float | None:
    if den == 0:
        return None
    return num / den


def _value_counts(rows: list[dict], field: str, top_n: int = 10) -> list[dict]:
    counts: dict[str, int] = {}
    for row in rows:
        value = row.get(field)
        key = str(value) if value is not None else "null"
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [{"value": value, "count": count} for value, count in ordered]


def _nonnull_rate(rows: list[dict], field: str) -> float | None:
    if not rows:
        return None
    nn = sum(1 for row in rows if row.get(field) is not None)
    return nn / len(rows)


def _load_parquet_rows(path: Path) -> list[dict]:
    return ds.dataset(str(path), format="parquet", partitioning="hive").to_table().to_pylist()


def _silver_stats(target_date: str) -> dict:
    silver_path = Path("data/silver") / f"bzp_{target_date}.parquet"
    if not silver_path.exists():
        return {"path": str(silver_path), "exists": False}

    rows = _load_parquet_rows(silver_path)
    total = len(rows)
    return {
        "path": str(silver_path),
        "exists": True,
        "rows": total,
        "field_non_null_rate": {
            "caseId": _nonnull_rate(rows, "caseId"),
            "organizationId": _nonnull_rate(rows, "organizationId"),
            "noticeType": _nonnull_rate(rows, "noticeType"),
            "cpvCodes": _nonnull_rate(rows, "cpvCodes"),
            "procedureResultParsed": _nonnull_rate(rows, "procedureResultParsed"),
            "htmlExtracted": _nonnull_rate(rows, "htmlExtracted"),
        },
        "notice_type_top10": _value_counts(rows, "noticeType", top_n=10),
        "notice_stage_top10": _value_counts(rows, "noticeStage", top_n=10),
        "case_distinct": len({row.get("caseId") for row in rows if row.get("caseId") is not None}),
        "buyer_distinct": len(
            {row.get("organizationId") for row in rows if row.get("organizationId") is not None}
        ),
    }


def _gold_dataset_stats(target_date: str, dataset_name: str, key_fields: list[str]) -> dict:
    dataset_path = Path("data/gold") / dataset_name / f"date={target_date}"
    if not dataset_path.exists():
        return {"path": str(dataset_path), "exists": False}

    rows = _load_parquet_rows(dataset_path)
    out: dict = {
        "path": str(dataset_path),
        "exists": True,
        "rows": len(rows),
        "field_non_null_rate": {},
    }
    for field in key_fields:
        out["field_non_null_rate"][field] = _nonnull_rate(rows, field)
    return out


def _build_markdown(stats: dict) -> str:
    lines: list[str] = []
    lines.append(f"# Run Stats: {stats['target_date']}")
    lines.append("")
    lines.append("## Silver")
    silver = stats["silver"]
    if not silver.get("exists"):
        lines.append(f"- Missing input: `{silver['path']}`")
    else:
        lines.append(f"- Path: `{silver['path']}`")
        lines.append(f"- Rows: `{silver['rows']}`")
        lines.append(f"- Distinct cases: `{silver['case_distinct']}`")
        lines.append(f"- Distinct buyers: `{silver['buyer_distinct']}`")
        lines.append("- Non-null rates:")
        for k, v in silver["field_non_null_rate"].items():
            lines.append(f"  - `{k}`: `{v}`")
        lines.append("- Top notice types:")
        for item in silver["notice_type_top10"]:
            lines.append(f"  - `{item['value']}`: `{item['count']}`")
        lines.append("- Top notice stages:")
        for item in silver["notice_stage_top10"]:
            lines.append(f"  - `{item['value']}`: `{item['count']}`")

    lines.append("")
    lines.append("## Gold")
    for name, details in stats["gold"].items():
        lines.append(f"### {name}")
        if not details.get("exists"):
            lines.append(f"- Missing output: `{details['path']}`")
            continue
        lines.append(f"- Path: `{details['path']}`")
        lines.append(f"- Rows: `{details['rows']}`")
        lines.append("- Non-null rates:")
        for k, v in details["field_non_null_rate"].items():
            lines.append(f"  - `{k}`: `{v}`")
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    stats = {
        "target_date": target_date,
        "silver": _silver_stats(target_date),
        "gold": {
            "case_mart": _gold_dataset_stats(
                target_date,
                "case_mart",
                ["caseId", "buyer_id", "time_to_award_days", "award_to_completion_days"],
            ),
            "buyer_mart": _gold_dataset_stats(
                target_date,
                "buyer_mart",
                ["organizationId", "single_bid_rate", "hhi", "concentration_top1_share"],
            ),
            "market_mart": _gold_dataset_stats(
                target_date,
                "market_mart",
                ["cpv_2digit", "hhi", "top1_share", "value_total"],
            ),
            "signals_buyer_daily": _gold_dataset_stats(
                target_date,
                "signals_buyer_daily",
                ["buyer_id", "single_bid_rate_today", "hhi_today", "update_intensity_today"],
            ),
        },
    }

    out_dir = Path("data/reports/run_stats")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_json = out_dir / f"run_stats_{target_date}.json"
    out_md = out_dir / f"run_stats_{target_date}.md"

    out_json.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    out_md.write_text(_build_markdown(stats), encoding="utf-8")

    log.info("Wrote run stats JSON: %s", out_json)
    log.info("Wrote run stats Markdown: %s", out_md)


if __name__ == "__main__":
    main()
