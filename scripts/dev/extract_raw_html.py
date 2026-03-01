"""Extract HTML for a single notice from a raw BZP JSON file.

Usage:
  python scripts/dev/extract_raw_html.py bzp_2026-02-10.json 08de... --output out.html
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _resolve_raw_path(raw_file: str) -> Path:
    candidate = Path(raw_file)
    if candidate.exists():
        return candidate
    default_path = Path("data/raw") / raw_file
    return default_path


def _load_records(raw_path: Path) -> list[dict[str, Any]]:
    text = raw_path.read_text(encoding="utf-8")
    parsed = json.loads(text)
    if isinstance(parsed, list):
        return [r for r in parsed if isinstance(r, dict)]
    if isinstance(parsed, dict):
        return [parsed]
    raise ValueError("Unsupported JSON structure: expected list[object] or object")


def _extract_html(record: dict[str, Any]) -> str | None:
    direct_keys = [
        "htmlBody",
        "html",
        "htmlContent",
        "noticeHtml",
        "noticeContent",
        "content",
        "body",
    ]
    for key in direct_keys:
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value

    nested_candidates = [
        ("html", "body"),
        ("content", "html"),
        ("notice", "html"),
        ("payload", "html"),
    ]
    for outer, inner in nested_candidates:
        outer_value = record.get(outer)
        if isinstance(outer_value, dict):
            inner_value = outer_value.get(inner)
            if isinstance(inner_value, str) and inner_value.strip():
                return inner_value
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract raw HTML by objectId")
    parser.add_argument("raw_file", help="Raw JSON path or filename in data/raw/")
    parser.add_argument("object_id", help="Target objectId")
    parser.add_argument(
        "--output",
        dest="output",
        default=None,
        help="Output HTML path (default: data/raw/sample_html/<objectId>.html)",
    )
    args = parser.parse_args()

    raw_path = _resolve_raw_path(args.raw_file)
    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    records = _load_records(raw_path)
    matches = [r for r in records if str(r.get("objectId", "")).strip() == args.object_id]
    if not matches:
        raise ValueError(f"objectId not found: {args.object_id}")

    html = _extract_html(matches[0])
    if not html:
        raise ValueError(f"HTML payload missing for objectId: {args.object_id}")

    output_path = (
        Path(args.output)
        if args.output
        else Path("data/raw/sample_html") / f"{args.object_id}.html"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print(f"raw_file={raw_path}")
    print(f"object_id={args.object_id}")
    print(f"matches={len(matches)}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
