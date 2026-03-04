from __future__ import annotations

import argparse
import json
import re
from datetime import date, timedelta
from pathlib import Path

import pyarrow.dataset as ds
from bs4 import BeautifulSoup, NavigableString, Tag

DEFAULT_BRONZE_DIR = Path(r"E:\git_projects\procurement-watchdog-api-exploration\data\bronze")
DEFAULT_NOTICE_TYPES_DIR = Path("src/procurement/silver/notice_types")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build section profile JSON for a noticeType from first N days of 2025. "
            "Output is a dict: section_name -> {section_header, example_values}."
        )
    )
    parser.add_argument("--notice-type", required=True, help="noticeType token, e.g. TenderResultNotice")
    parser.add_argument("--n", type=int, required=True, help="Number of first calendar days in 2025 to scan.")
    parser.add_argument(
        "--bronze-dir",
        default=str(DEFAULT_BRONZE_DIR),
        help=f"Bronze root directory (default: {DEFAULT_BRONZE_DIR})",
    )
    parser.add_argument(
        "--out-json",
        default="",
        help=(
            "Optional output JSON path. If empty, writes to "
            "src/procurement/silver/notice_types/<notice_type>_sections_profile_automatic.json."
        ),
    )
    return parser.parse_args()


def _days_2025(n: int) -> list[str]:
    if n <= 0:
        raise ValueError("--n must be > 0")
    start = date(2025, 1, 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(n)]


def _extract_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def _section_to_field_name(section_number: str) -> str:
    return f"cn_section_{section_number.replace('.', '_')}"


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


def _extract_section_header(h3_text: str, section_number: str, value: str | None) -> str | None:
    without_prefix = re.sub(
        rf"^\s*{re.escape(section_number)}\.?\)\s*",
        "",
        h3_text.strip(),
        flags=re.IGNORECASE,
    )
    if not without_prefix:
        return None

    header = without_prefix
    if ":" in header:
        header = header.split(":", 1)[0]
    elif value:
        pos = header.find(value)
        if pos > 0:
            header = header[:pos]

    header = header.strip(" :\t\r\n")
    return header or None


def _notice_type_to_snake(notice_type: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", notice_type).lower()


def _default_out_path(notice_type: str) -> Path:
    return DEFAULT_NOTICE_TYPES_DIR / f"{_notice_type_to_snake(notice_type)}_sections_profile_automatic.json"


def main() -> None:
    args = parse_args()
    notice_type = args.notice_type.strip()
    bronze_dir = Path(args.bronze_dir).expanduser().resolve()
    out_json = Path(args.out_json).expanduser().resolve() if args.out_json else _default_out_path(notice_type).resolve()

    days = _days_2025(args.n)
    notices_root = bronze_dir / "notices" / f"noticeType={notice_type}"
    if not notices_root.exists() or not notices_root.is_dir():
        raise SystemExit(f"Missing noticeType root: {notices_root}")

    partition_dirs: list[Path] = []
    for day in days:
        p = notices_root / f"publicationDateDay={day}"
        if p.exists() and p.is_dir():
            partition_dirs.append(p)

    if not partition_dirs:
        raise SystemExit(
            f"No partitions found for noticeType={notice_type} in first {args.n} days of 2025 under {notices_root}"
        )

    notices_scanned = 0
    sections: dict[str, dict[str, object]] = {}

    for partition_dir in partition_dirs:
        dataset = ds.dataset(partition_dir, format="parquet")
        if "htmlBody" not in dataset.schema.names:
            continue
        table = dataset.to_table(columns=["htmlBody"])

        for row in table.to_pylist():
            html_body = row.get("htmlBody")
            if not isinstance(html_body, str) or not html_body.strip():
                continue
            notices_scanned += 1

            soup = BeautifulSoup(html_body, "lxml")
            for h3 in soup.find_all("h3"):
                h3_text = h3.get_text(separator=" ", strip=True)
                section_number = _extract_section_number(h3_text)
                if not section_number:
                    continue
                field_name = _section_to_field_name(section_number)
                value = _extract_section_value(h3)
                if not value:
                    continue
                header = _extract_section_header(h3_text, section_number, value)

                entry = sections.setdefault(
                    section_number,
                    {
                        "col_name": field_name,
                        "section_header": header,
                        "data_model": "core",
                        "example_values": [],
                    },
                )

                # Keep first non-empty header as canonical label for the section.
                if not entry.get("section_header") and header:
                    entry["section_header"] = header

                examples = entry["example_values"]
                if isinstance(examples, list) and value not in examples and len(examples) < 3:
                    examples.append(value)

    # Stable section ordering (numeric by section token).
    def _sort_key(section_name: str) -> tuple[int, ...]:
        parts = section_name.split(".")
        return tuple(int(x) if x.isdigit() else 10**9 for x in parts)

    sections_sorted = {k: sections[k] for k in sorted(sections.keys(), key=_sort_key)}

    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(sections_sorted, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"partitions_count={len(partition_dirs)}")
    print(f"notices_scanned={notices_scanned}")
    print(f"sections_count={len(sections_sorted)}")
    print(f"out_json={out_json}")


if __name__ == "__main__":
    main()
