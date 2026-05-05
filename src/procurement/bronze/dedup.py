"""Cross-day deduplication filter for the bronze layer.

This module contains only pure-Python logic so it can be tested without a
Spark environment and imported from both the single-date and range scripts.
"""

from __future__ import annotations


def apply_dedup_filter(
    records: list[dict],
    ids_seen_other_day: set[str],
) -> tuple[list[dict], dict]:
    """Filter *records* against *ids_seen_other_day* and deduplicate within file.

    Rules applied in order:
    1. Records with a missing or empty ``objectId`` always pass through.
    2. The second (and later) occurrence of the same ``objectId`` within
       *records* is dropped (within-file dedup).
    3. Records whose ``objectId`` is already in *ids_seen_other_day* are
       dropped (cross-day dedup).

    ``objectId`` values are coerced to ``str`` and stripped of surrounding
    whitespace before comparison.

    Returns ``(filtered_records, stats)`` where *stats* contains:

    - ``input_rows`` — total records received
    - ``output_rows`` — records returned
    - ``dropped_duplicates_in_input`` — dropped by within-file dedup
    - ``dropped_duplicates_seen_index_other_day`` — dropped by cross-day dedup
    """
    in_file_seen: set[str] = set()
    filtered: list[dict] = []
    dropped_in_file = 0
    dropped_seen_other_day = 0

    for rec in records:
        object_id_raw = rec.get("objectId")
        object_id = str(object_id_raw).strip() if object_id_raw is not None else ""

        if not object_id:
            filtered.append(rec)
            continue

        if object_id in in_file_seen:
            dropped_in_file += 1
            continue
        in_file_seen.add(object_id)

        if object_id in ids_seen_other_day:
            dropped_seen_other_day += 1
            continue

        filtered.append(rec)

    return filtered, {
        "input_rows": len(records),
        "output_rows": len(filtered),
        "dropped_duplicates_in_input": dropped_in_file,
        "dropped_duplicates_seen_index_other_day": dropped_seen_other_day,
    }
