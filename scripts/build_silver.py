"""Build silver layer by parsing HTML from validated bronze records.

Reads   data/raw/bzp_YYYY-MM-DD.json       (raw, via bronze validate_raw)
Writes  data/silver/bzp_YYYY-MM-DD.json     (all fields + parsed HTML, no raw HTML)
"""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.bronze.models import BzpNoticeBronze
from procurement.logging import setup_logging
from procurement.silver.html_parser import parse_cpv_codes, parse_html
from procurement.silver.models import BzpNoticeSilver

setup_logging()
log = logging.getLogger(__name__)


def validate_raw(raw_records: list[dict]) -> list[BzpNoticeBronze]:
    """Validate raw dicts, skip invalid records with a warning."""
    from pydantic import ValidationError

    valid: list[BzpNoticeBronze] = []
    for idx, record in enumerate(raw_records):
        try:
            valid.append(BzpNoticeBronze.model_validate(record))
        except ValidationError:
            log.warning(
                "Record %d (objectId=%s) failed bronze validation, skipping",
                idx,
                record.get("objectId", "?"),
            )
    return valid


def to_silver(notice: BzpNoticeBronze) -> BzpNoticeSilver:
    """Convert a bronze notice to a silver record."""
    html_extracted = parse_html(notice.htmlBody)
    data = notice.model_dump()
    del data["htmlBody"]
    data["cpvCodes"] = parse_cpv_codes(data.pop("cpvCode"))
    data["htmlExtracted"] = html_extracted
    return BzpNoticeSilver(**data)


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    raw_path = Path("data/raw") / f"bzp_{target_date}.json"
    if not raw_path.exists():
        log.error("Raw file not found: %s", raw_path)
        sys.exit(1)

    log.info("Reading %s", raw_path)
    raw_records: list[dict] = json.loads(raw_path.read_text(encoding="utf-8"))
    log.info("Loaded %d raw records", len(raw_records))

    # Step 1: bronze validation (get models with full HTML)
    notices = validate_raw(raw_records)
    log.info("Validated %d records", len(notices))

    # Step 2: bronze → silver (parse HTML, split CPV codes)
    silver_records = []
    parse_errors = 0
    for notice in notices:
        try:
            silver_records.append(to_silver(notice))
        except Exception:
            log.warning(
                "Failed to parse objectId=%s", notice.objectId, exc_info=True
            )
            parse_errors += 1

    # Write output
    out_dir = Path("data/silver")
    out_dir.mkdir(parents=True, exist_ok=True)

    silver_path = out_dir / f"bzp_{target_date}.json"
    silver_path.write_text(
        json.dumps(
            [r.model_dump() for r in silver_records],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("Wrote %d silver records to %s", len(silver_records), silver_path)

    log.info(
        "Summary: validated=%d  parsed=%d  parse_errors=%d",
        len(notices),
        len(silver_records),
        parse_errors,
    )


if __name__ == "__main__":
    main()
