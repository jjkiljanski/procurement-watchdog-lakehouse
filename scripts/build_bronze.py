"""Build bronze layer from raw BZP JSON.

Reads   data/raw/bzp_YYYY-MM-DD.json
Writes  data/bronze/bzp_YYYY-MM-DD.json          (valid, HTML replaced with SHA-256)
        data/bronze/bzp_YYYY-MM-DD_errors.json    (records that failed validation)
"""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.bronze.models import (
    BzpNoticeBronze,
    BzpNoticeBronzeOut,
    to_bronze_output,
)
from procurement.logging import setup_logging
from pydantic import ValidationError

setup_logging()
log = logging.getLogger(__name__)


def validate_raw(
    raw_records: list[dict],
) -> tuple[list[BzpNoticeBronze], list[dict]]:
    """Validate raw dicts and split into valid models + error dicts.

    Returns (valid, errors) where *valid* keeps the full htmlBody so that
    downstream silver/Spark processing can consume it before it is hashed.
    """
    valid: list[BzpNoticeBronze] = []
    errors: list[dict] = []

    for idx, record in enumerate(raw_records):
        try:
            valid.append(BzpNoticeBronze.model_validate(record))
        except ValidationError as exc:
            log.warning(
                "Record %d (objectId=%s) failed validation: %s",
                idx,
                record.get("objectId", "?"),
                exc.error_count(),
            )
            errors.append(
                {
                    "index": idx,
                    "objectId": record.get("objectId"),
                    "errors": exc.errors(),
                    "raw": record,
                }
            )

    return valid, errors


def main() -> None:
    # Determine target date
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

    # Step 1: validate (returns models WITH full htmlBody)
    valid, errors = validate_raw(raw_records)

    # Step 2: produce bronze output (htmlBody → sha256)
    bronze_out: list[BzpNoticeBronzeOut] = [to_bronze_output(n) for n in valid]

    # Write outputs
    out_dir = Path("data/bronze")
    out_dir.mkdir(parents=True, exist_ok=True)

    bronze_path = out_dir / f"bzp_{target_date}.json"
    bronze_path.write_text(
        json.dumps(
            [r.model_dump() for r in bronze_out],
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log.info("Wrote %d bronze records to %s", len(bronze_out), bronze_path)

    if errors:
        errors_path = out_dir / f"bzp_{target_date}_errors.json"
        errors_path.write_text(
            json.dumps(errors, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        log.info("Wrote %d error records to %s", len(errors), errors_path)

    log.info(
        "Summary: total=%d  valid=%d  invalid=%d",
        len(raw_records),
        len(valid),
        len(errors),
    )


if __name__ == "__main__":
    main()
