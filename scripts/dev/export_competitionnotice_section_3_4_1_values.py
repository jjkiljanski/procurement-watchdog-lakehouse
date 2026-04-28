from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pyarrow.dataset as ds
from bs4 import BeautifulSoup, NavigableString, Tag

DEFAULT_BRONZE_NOTICES_DIR = Path(r"E:\git_projects\procurement-watchdog-api-exploration\data\bronze\notices")
DEFAULT_OUT_JSON = Path("data/sample_parquets/competition_notice_section_3_4_1_values_2025.json")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Extract all section 3.4.1 values from CompetitionNotice HTML in 2025, "
            "including source objectId."
        )
    )
    parser.add_argument(
        "--bronze-notices-dir",
        default=str(DEFAULT_BRONZE_NOTICES_DIR),
        help=f"Bronze notices root (default: {DEFAULT_BRONZE_NOTICES_DIR})",
    )
    parser.add_argument(
        "--out-json",
        default=str(DEFAULT_OUT_JSON),
        help=f"Output JSON path (default: {DEFAULT_OUT_JSON})",
    )
    return parser.parse_args()


def _extract_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def _span_value(h3: Tag | None) -> str | None:
    if h3 is None:
        return None
    span = h3.find("span", class_="normal")
    if span is None:
        return None
    text = span.get_text().strip()
    return text or None


def _text_after_h3(h3: Tag | None) -> str | None:
    if h3 is None:
        return None
    sibling = h3.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            text = str(sibling).strip()
            if text:
                return text
        elif hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "br":
                sibling = sibling.next_sibling
                continue
            break
        sibling = sibling.next_sibling
    return None


def _collect_p_values(h3: Tag | None) -> list[str]:
    if h3 is None:
        return []
    parts: list[str] = []
    sibling = h3.next_sibling
    while sibling is not None:
        if hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "p":
                text = sibling.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        sibling = sibling.next_sibling
    return parts


def _extract_section_value(h3: Tag) -> str | None:
    value = _span_value(h3)
    if value:
        return value
    value = _text_after_h3(h3)
    if value:
        return value
    p_values = _collect_p_values(h3)
    if p_values:
        return " ".join(p_values)
    return None


def _extract_3_4_1_values(html_body: str) -> list[str]:
    values: list[str] = []
    soup = BeautifulSoup(html_body, "lxml")
    for h3 in soup.find_all("h3"):
        section_number = _extract_section_number(h3.get_text(separator=" ", strip=True))
        if section_number != "3.4.1":
            continue
        value = _extract_section_value(h3)
        if value:
            values.append(value)
    return values


def main() -> None:
    args = parse_args()
    bronze_notices_dir = Path(args.bronze_notices_dir).expanduser().resolve()
    out_json = Path(args.out_json).expanduser().resolve()

    cpn_root = bronze_notices_dir / "noticeType=CompetitionNotice"
    if not cpn_root.exists() or not cpn_root.is_dir():
        raise SystemExit(f"Missing CompetitionNotice root: {cpn_root}")

    partition_dirs = sorted(
        p for p in cpn_root.glob("publicationDateDay=2025-*") if p.is_dir()
    )
    if not partition_dirs:
        raise SystemExit(f"No 2025 partitions found under: {cpn_root}")

    rows_out: list[dict[str, object]] = []
    by_object_id: dict[str, list[dict[str, object]]] = {}
    notices_scanned = 0

    for partition_dir in partition_dirs:
        day_token = partition_dir.name.replace("publicationDateDay=", "")
        dataset = ds.dataset(partition_dir, format="parquet")
        cols = set(dataset.schema.names)
        required_cols = ["htmlBody"]
        if "objectId" in cols:
            required_cols.append("objectId")
        table = dataset.to_table(columns=required_cols)

        for row in table.to_pylist():
            html_body = row.get("htmlBody")
            if not isinstance(html_body, str) or not html_body.strip():
                continue
            notices_scanned += 1
            object_id_raw = row.get("objectId")
            object_id = str(object_id_raw) if object_id_raw is not None else "<missing_object_id>"
            values = _extract_3_4_1_values(html_body)
            for idx, value in enumerate(values, start=1):
                rec = {
                    "publicationDateDay": day_token,
                    "section": "3.4.1",
                    "occurrence": idx,
                    "value": value,
                }
                rows_out.append({"objectId": object_id, **rec})
                by_object_id.setdefault(object_id, []).append(rec)

    out_json.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "noticeType": "CompetitionNotice",
        "year": 2025,
        "partitions_count": len(partition_dirs),
        "notices_scanned": notices_scanned,
        "rows_count": len(rows_out),
        "object_ids_count": len(by_object_id),
        "by_objectId": by_object_id,
        "rows": rows_out,
    }
    out_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"partitions_count={len(partition_dirs)}")
    print(f"notices_scanned={notices_scanned}")
    print(f"rows_count={len(rows_out)}")
    print(f"object_ids_count={len(by_object_id)}")
    print(f"out_json={out_json}")


if __name__ == "__main__":
    main()
