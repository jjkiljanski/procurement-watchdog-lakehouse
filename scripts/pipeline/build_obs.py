"""Build observability snapshot: compute silver/gold stats, write to obs tables, print dashboard.

Reads:
  data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/
  data/silver/notice_type_tables/noticeType=*/publicationDateDay=YYYY-MM-DD/
  data/gold/*/date=YYYY-MM-DD/
  data/obs/**  (previous runs — for trending)

Writes:
  data/obs/dq_metrics/dt=YYYY-MM-DD/       (silver + gold field-level quality)
  data/obs/pipeline_runs/dt=YYYY-MM-DD/    (if gold run_id not already written)

Prints:
  Markdown dashboard to stdout (captured in pipeline logs)

Exits non-zero if any configured threshold is violated.
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.logging import setup_logging
from procurement.obs import write_dq_metrics, write_pipeline_run, now_utc_iso

setup_logging()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Threshold definitions — (metric_layer, notice_type, metric_name): (op, value)
# ---------------------------------------------------------------------------
THRESHOLDS: dict[tuple[str, str, str], tuple[str, float]] = {
    ("bronze", "__all__", "valid_rate"): (">=", 0.95),
    ("silver", "__all__", "street_non_null_rows"): (">=", 1),  # at least some non-null streets
}


# ---------------------------------------------------------------------------
# Data loading helpers (pandas / pyarrow, no Spark needed)
# ---------------------------------------------------------------------------

def _load_parquet_rows(path: Path) -> list[dict]:
    import pyarrow.dataset as ds
    return ds.dataset(str(path), format="parquet", partitioning="hive").to_table().to_pylist()


def _load_obs_table(table_dir: Path, last_n_days: int) -> list[dict]:
    """Load up to last_n_days partitions from an obs table directory."""
    if not table_dir.exists():
        return []
    rows: list[dict] = []
    cutoff = (date.today() - timedelta(days=last_n_days)).isoformat()
    for part_dir in sorted(table_dir.iterdir()):
        if not part_dir.is_dir():
            continue
        # part_dir name is dt=YYYY-MM-DD
        dt = part_dir.name.replace("dt=", "")
        if dt < cutoff:
            continue
        try:
            rows.extend(_load_parquet_rows(part_dir))
        except Exception as exc:
            log.warning("Failed to read obs partition %s: %s", part_dir, exc)
    return rows


def _nonnull_rate(rows: list[dict], field: str) -> float | None:
    if not rows:
        return None
    nn = sum(1 for r in rows if r.get(field) is not None)
    return nn / len(rows)


def _value_counts(rows: list[dict], field: str, top_n: int = 10) -> list[dict]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get(field)) if row.get(field) is not None else "null"
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    return [{"value": v, "count": c} for v, c in ordered]


# ---------------------------------------------------------------------------
# Per-day silver stats (reads silver output → writes to obs/dq_metrics)
# ---------------------------------------------------------------------------

def _compute_and_write_silver_stats(target_date: str, silver_dir: Path) -> dict:
    """Read silver output for target_date, compute stats, write to obs, return summary."""

    def _include(p: Path) -> bool:
        return p.parent.name.replace("noticeType=", "") != "ContractNotice_parts"

    specific_paths = sorted(
        p
        for p in silver_dir.glob(f"notice_type_tables/noticeType=*/publicationDateDay={target_date}")
        if p.is_dir() and _include(p)
    )
    envelope_path = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
    if not specific_paths and not envelope_path.is_dir():
        return {"exists": False}

    env_rows: list[dict] = []
    if envelope_path.is_dir():
        try:
            env_rows = _load_parquet_rows(envelope_path)
        except Exception as exc:
            log.warning("Failed to read silver envelope: %s", exc)

    spec_rows: list[dict] = []
    for p in specific_paths:
        try:
            spec_rows.extend(_load_parquet_rows(p))
        except Exception as exc:
            log.warning("Failed to read silver specific table %s: %s", p, exc)

    # Merge envelope into spec rows by objectId for full picture
    if env_rows and spec_rows:
        env_by_id = {r.get("objectId"): r for r in env_rows if r.get("objectId")}
        rows = []
        for r in spec_rows:
            base = env_by_id.get(r.get("objectId"), {})
            merged = dict(base)
            merged.update(r)
            rows.append(merged)
    elif env_rows:
        rows = env_rows
    else:
        rows = spec_rows

    total = len(rows)
    non_null_rates = {
        f"nonnull_{f}": (_nonnull_rate(rows, f) or 0.0)
        for f in ["caseId", "organizationId", "noticeType", "cpvCodes", "procedureResultParsed"]
    }
    write_dq_metrics(
        layer="silver",
        target_date=target_date,
        notice_type="__obs_snapshot__",
        metrics={
            "total_rows": total,
            "case_distinct": len({r.get("caseId") for r in rows if r.get("caseId")}),
            "buyer_distinct": len({r.get("organizationId") for r in rows if r.get("organizationId")}),
            **non_null_rates,
        },
    )
    return {
        "exists": True,
        "rows": total,
        "non_null_rates": non_null_rates,
        "notice_type_top5": _value_counts(rows, "noticeType", 5),
        "notice_stage_top5": _value_counts(rows, "noticeStage", 5),
        "case_distinct": len({r.get("caseId") for r in rows if r.get("caseId")}),
        "buyer_distinct": len({r.get("organizationId") for r in rows if r.get("organizationId")}),
    }


def _compute_and_write_gold_stats(target_date: str, gold_dir: Path) -> dict:
    """Read gold output for target_date, compute stats, write to obs, return summary."""
    GOLD_DATASETS = {
        "case_mart": ["caseId", "buyer_id", "time_to_award_days"],
        "buyer_mart": ["organizationId", "single_bid_rate", "hhi"],
        "market_mart": ["cpv_2digit", "hhi", "value_total"],
        "signals_buyer_daily": ["buyer_id", "single_bid_rate_today", "hhi_today"],
    }
    result: dict = {}
    for dataset, key_fields in GOLD_DATASETS.items():
        dataset_path = gold_dir / dataset / f"date={target_date}"
        if not dataset_path.exists():
            result[dataset] = {"exists": False}
            continue
        try:
            rows = _load_parquet_rows(dataset_path)
        except Exception as exc:
            log.warning("Failed to read gold dataset %s: %s", dataset, exc)
            result[dataset] = {"exists": False, "error": str(exc)}
            continue
        metrics = {"total_rows": len(rows)}
        for f in key_fields:
            rate = _nonnull_rate(rows, f)
            if rate is not None:
                metrics[f"nonnull_{f}"] = rate
        write_dq_metrics(
            layer="gold",
            target_date=target_date,
            notice_type=dataset,
            metrics=metrics,
        )
        result[dataset] = {"exists": True, "rows": len(rows)}
    return result


# ---------------------------------------------------------------------------
# Dashboard rendering
# ---------------------------------------------------------------------------

def _render_dashboard(
    target_date: str,
    silver_stats: dict,
    gold_stats: dict,
    pipeline_runs: list[dict],
    dq_metrics: list[dict],
    quarantine_summary: list[dict],
) -> str:
    lines: list[str] = []
    lines.append(f"# Observability Dashboard — {target_date}")
    lines.append("")

    # --- Today's silver snapshot ---
    lines.append("## Silver (today)")
    if not silver_stats.get("exists"):
        lines.append("- No silver output found for this date.")
    else:
        lines.append(f"- Rows: {silver_stats['rows']}")
        lines.append(f"- Distinct cases: {silver_stats['case_distinct']}")
        lines.append(f"- Distinct buyers: {silver_stats['buyer_distinct']}")
        lines.append("- Top notice types:")
        for item in silver_stats.get("notice_type_top5", []):
            lines.append(f"  - {item['value']}: {item['count']}")
    lines.append("")

    # --- Today's gold snapshot ---
    lines.append("## Gold (today)")
    for dataset, info in gold_stats.items():
        if info.get("exists"):
            lines.append(f"- `{dataset}`: {info['rows']} rows")
        else:
            lines.append(f"- `{dataset}`: missing")
    lines.append("")

    # --- Pipeline runs (last 14 days) ---
    lines.append("## Pipeline runs (last 14 days)")
    if not pipeline_runs:
        lines.append("- No run history yet.")
    else:
        # Group by target_date + layer, show most recent status
        by_day: dict[str, dict[str, str]] = {}
        for r in pipeline_runs:
            day = r.get("target_date", "?")
            layer = r.get("layer", "?")
            status = r.get("status", "?")
            by_day.setdefault(day, {})[layer] = status
        lines.append("| date | fetch | bronze | silver | gold |")
        lines.append("|------|-------|--------|--------|------|")
        for day in sorted(by_day.keys(), reverse=True)[:14]:
            row = by_day[day]
            lines.append(
                f"| {day} "
                f"| {row.get('fetch', '-')} "
                f"| {row.get('bronze', '-')} "
                f"| {row.get('silver', '-')} "
                f"| {row.get('gold', '-')} |"
            )
    lines.append("")

    # --- DQ metrics trending (bronze valid_rate) ---
    lines.append("## Bronze valid_rate (last 14 days)")
    bronze_vr = [
        r for r in dq_metrics
        if r.get("layer") == "bronze"
        and r.get("notice_type") == "__all__"
        and r.get("metric_name") == "valid_rate"
    ]
    if not bronze_vr:
        lines.append("- No data yet.")
    else:
        for r in sorted(bronze_vr, key=lambda x: x.get("target_date", ""), reverse=True)[:14]:
            lines.append(f"- {r['target_date']}: {r['metric_value']:.4f}")
    lines.append("")

    # --- Quarantine summary (last 14 days) ---
    lines.append("## Quarantine summary (last 14 days)")
    if not quarantine_summary:
        lines.append("- No quarantine data.")
    else:
        by_day_nt: dict[str, dict[str, int]] = {}
        for r in quarantine_summary:
            day = r.get("target_date", "?")
            nt = r.get("notice_type", "?")
            cnt = int(r.get("row_count", 0))
            by_day_nt.setdefault(day, {})[nt] = by_day_nt.get(day, {}).get(nt, 0) + cnt
        for day in sorted(by_day_nt.keys(), reverse=True)[:14]:
            total_q = sum(by_day_nt[day].values())
            detail = ", ".join(f"{nt}={cnt}" for nt, cnt in sorted(by_day_nt[day].items()))
            lines.append(f"- {day}: {total_q} total  ({detail})")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Threshold checking
# ---------------------------------------------------------------------------

def _check_thresholds(dq_metrics: list[dict], target_date: str) -> list[str]:
    """Return list of violation messages for today's metrics."""
    today_metrics: dict[tuple[str, str, str], float] = {}
    for r in dq_metrics:
        if r.get("target_date") != target_date:
            continue
        key = (r.get("layer", ""), r.get("notice_type", ""), r.get("metric_name", ""))
        today_metrics[key] = float(r.get("metric_value", 0))

    violations: list[str] = []
    for (layer, nt, metric), (op, threshold) in THRESHOLDS.items():
        value = today_metrics.get((layer, nt, metric))
        if value is None:
            continue
        violated = (op == ">=" and value < threshold) or (op == "<=" and value > threshold)
        if violated:
            violations.append(
                f"THRESHOLD VIOLATION: {layer}.{nt}.{metric} = {value:.4f} (expected {op} {threshold})"
            )
    return violations


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_dir = Path("data/silver")
    gold_dir = Path("data/gold")
    obs_dir = Path("data/obs")

    log.info("Computing silver stats for %s", target_date)
    silver_stats = _compute_and_write_silver_stats(target_date, silver_dir)

    log.info("Computing gold stats for %s", target_date)
    gold_stats = _compute_and_write_gold_stats(target_date, gold_dir)

    # Load obs history for trending (last 14 days)
    pipeline_runs = _load_obs_table(obs_dir / "pipeline_runs", last_n_days=14)
    dq_metrics = _load_obs_table(obs_dir / "dq_metrics", last_n_days=14)
    quarantine_summary = _load_obs_table(obs_dir / "quarantine_summary", last_n_days=14)

    dashboard = _render_dashboard(
        target_date=target_date,
        silver_stats=silver_stats,
        gold_stats=gold_stats,
        pipeline_runs=pipeline_runs,
        dq_metrics=dq_metrics,
        quarantine_summary=quarantine_summary,
    )

    print(dashboard)

    violations = _check_thresholds(dq_metrics, target_date)
    for v in violations:
        log.error(v)
    if violations:
        sys.exit(1)


if __name__ == "__main__":
    main()
