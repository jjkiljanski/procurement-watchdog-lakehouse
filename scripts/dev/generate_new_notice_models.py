"""Generate Pydantic section models for four new notice types.

Reads the *_profile_automatic.json files, groups sections by data_model,
and writes a corresponding *_models.py file for each notice type.

Run from the repo root:
    python scripts/dev/generate_new_notice_models.py
"""

from __future__ import annotations

import json
import re
from pathlib import Path

SCHEMAS_DIR = Path("src/procurement/silver/notice_schemas")

# Parser fn name → Python type annotation
PARSER_TYPE_MAP: dict[str, str] = {
    "parse_tak_nie":                "bool | None",
    "parse_pln_value":              "float | None",
    "parse_criterion_weight":       "int | None",
    "parse_int_from_text":          "int | None",
    "parse_duration_days_from_range": "int | None",
    "parse_cpv_codes":              "list[str] | None",
    "parse_list_from_newlines":     "list[str] | None",
}

# Parser fn name → inline comment (omitted when None)
PARSER_COMMENT_MAP: dict[str, str] = {
    "parse_tak_nie":                "Tak/Nie → bool",
    "parse_pln_value":              "monetary value",
    "parse_criterion_weight":       "weight 0–100",
    "parse_int_from_text":          "integer",
    "parse_duration_days_from_range": "duration in days",
    "parse_duration_end_date":      "YYYY-MM-DD end date",
    "parse_duration_iso":           "ISO 8601 duration (e.g. P24M, P30D)",
    "parse_duration_start_date":    "YYYY-MM-DD start date",
    "parse_cpv_codes":              "list of CPV codes",
    "parse_list_from_newlines":     "list of items",
    "parse_currency_code":          "currency code",
    "parse_date_from_text":         "YYYY-MM-DD",
    "parse_datetime_from_text":     "YYYY-MM-DDTHH:MM",
    "parse_nuts3_code":             "NUTS-3 code",
    "parse_nuts3_name":             "NUTS-3 region name",
    "parse_national_id_value":      "parsed national ID value",
    "parse_national_id_type":       "NIP / REGON / PESEL / foreign",
}

# (profile filename, CamelCase notice class prefix)
TARGETS: list[tuple[str, str]] = [
    (
        "competition_result_notice_profile_automatic.json",
        "CompetitionResultNotice",
    ),
    (
        "concession_agreement_notice_profile_automatic.json",
        "ConcessionAgreementNotice",
    ),
    (
        "concession_update_agreement_notice_profile_automatic.json",
        "ConcessionUpdateAgreementNotice",
    ),
    (
        "notice_update_concession_profile_automatic.json",
        "NoticeUpdateConcession",
    ),
]


def _data_model_to_class_suffix(data_model: str) -> str:
    """'part.core' → 'PartCore', 'criterion_procedure' → 'CriterionProcedure'."""
    return "".join(p.capitalize() for p in re.split(r"[._]", data_model))


def _fn_to_type(fn: str) -> str:
    return PARSER_TYPE_MAP.get(fn, "str | None")


def _build_field(col_name: str, fn: str | None) -> str:
    ann = _fn_to_type(fn) if fn else "str | None"
    comment = f"  # {PARSER_COMMENT_MAP[fn]}" if fn and fn in PARSER_COMMENT_MAP else ""
    return f"    {col_name}: {ann} = None{comment}"


def _collect_fields(
    profile: dict,
) -> dict[str, list[tuple[str, str | None]]]:
    """Return data_model → [(field_name, fn_or_None)] preserving section order."""
    groups: dict[str, list[tuple[str, str | None]]] = {}
    seen_per_group: dict[str, set[str]] = {}

    for section in profile.values():
        dm = section.get("data_model", "core")
        groups.setdefault(dm, [])
        seen_per_group.setdefault(dm, set())

        if "derived_cols" in section:
            for derived_col_name, derived in section["derived_cols"].items():
                fn = derived.get("fn")
                if derived_col_name not in seen_per_group[dm]:
                    groups[dm].append((derived_col_name, fn))
                    seen_per_group[dm].add(derived_col_name)
        else:
            col_name = section["col_name"]
            fn = section.get("parser", {}).get("fn") if "parser" in section else None
            if col_name not in seen_per_group[dm]:
                groups[dm].append((col_name, fn))
                seen_per_group[dm].add(col_name)

    return groups


def generate_models_for_profile(profile_path: Path, notice_class_name: str) -> str:
    with open(profile_path, encoding="utf-8") as f:
        profile = json.load(f)

    groups = _collect_fields(profile)

    lines: list[str] = [
        f'"""Pydantic section models for {notice_class_name}.',
        "",
        f"Auto-generated from {profile_path.name}.",
        "All str fields are raw Silver values; richer types come from registered parsers.",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "from pydantic import BaseModel",
        "",
        "",
    ]

    for dm, fields in groups.items():
        suffix = _data_model_to_class_suffix(dm)
        class_name = f"{notice_class_name}{suffix}Model"
        lines.append(f"class {class_name}(BaseModel):")
        for col_name, fn in fields:
            lines.append(_build_field(col_name, fn))
        lines.append("")

    return "\n".join(lines) + "\n"


def main() -> None:
    for profile_filename, notice_class_name in TARGETS:
        profile_path = SCHEMAS_DIR / profile_filename
        if not profile_path.exists():
            print(f"SKIP (not found): {profile_path}")
            continue

        stem = profile_filename.replace("_profile_automatic.json", "")
        out_path = SCHEMAS_DIR / f"{stem}_models.py"

        content = generate_models_for_profile(profile_path, notice_class_name)
        out_path.write_text(content, encoding="utf-8")
        print(f"Written: {out_path}")


if __name__ == "__main__":
    main()
