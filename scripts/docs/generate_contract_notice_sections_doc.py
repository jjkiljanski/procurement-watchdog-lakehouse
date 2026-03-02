from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path


MODEL_PATH = Path("src/procurement/silver/notice_types/contract_notice_split_models.py")
OUTPUT_PATH = Path("docs/data_model/contractnotice_sections_catalog.md")
SINGLE_PROFILE_PATH = Path(
    "examples/contractnotice_sections/contractnotice_2025-10-01_to_2025-10-30_single_part_sections_unique.json"
)
MULTI_PROFILE_PATH = Path(
    "examples/contractnotice_sections/contractnotice_2025-10-01_to_2025-10-30_multi_part_sections_unique.json"
)


@dataclass
class SectionField:
    class_name: str
    field_name: str
    section: str
    section_name: str
    mandatory: str
    examples: list[str]


FIELD_RE = re.compile(
    r"^\s+(cn_section_[0-9_]+):\s*([^=]+)=\s*None\s*#\s*(.+?)\s*\|\s*examples=(.*)$"
)
SECTION_RE = re.compile(r"^([0-9]+(?:\.[0-9]+)*)\)\s*type=(.*)$")


def parse_model_sections(text: str) -> tuple[list[SectionField], set[str], set[str]]:
    current_class: str | None = None
    fields: list[SectionField] = []
    validator_block = _extract_between(text, "multi_only_fields = {", "}")

    multi_only_fields = set(re.findall(r'"(cn_section_[0-9_]+)"', validator_block))
    if "if self._has_text(self.cn_section_4_1_9):" in text:
        multi_only_fields.add("cn_section_4_1_9")

    for line in text.splitlines():
        class_match = re.match(r"^class\s+([A-Za-z0-9_]+)\(", line)
        if class_match:
            current_class = class_match.group(1)
            continue

        match = FIELD_RE.match(line)
        if not match or current_class is None:
            continue

        field_name = match.group(1)
        annotation = match.group(2).strip()
        raw_meta = match.group(3).strip()
        raw_examples = match.group(4).strip()

        meta_match = SECTION_RE.match(raw_meta)
        if not meta_match:
            continue
        section_number = meta_match.group(1)
        section_name = meta_match.group(2).strip()

        mandatory = "YES" if "| None" not in annotation else "NO"
        examples = _split_examples(raw_examples)

        fields.append(
            SectionField(
                class_name=current_class,
                field_name=field_name,
                section=section_number,
                section_name=section_name,
                mandatory=mandatory,
                examples=examples,
            )
        )

    part_dependent = {
        f.field_name
        for f in fields
        if f.class_name == "ContractNoticeCoreRawV1"
        and _looks_part_dependent_label(f.section_name)
    }
    return fields, multi_only_fields, part_dependent


def load_profile_maps() -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    section_to_names: dict[str, list[str]] = {}
    section_to_examples: dict[str, list[str]] = {}
    for profile_path in (SINGLE_PROFILE_PATH, MULTI_PROFILE_PATH):
        if not profile_path.exists():
            continue
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
        for sec in payload.get("sections", []):
            section = sec.get("section_number")
            if not section:
                continue

            names = [x.strip() for x in sec.get("section_type_unique_values", []) if x and x.strip()]
            examples = [x.strip() for x in sec.get("value_unique_values", []) if x and x.strip()]

            if names:
                section_to_names.setdefault(section, [])
                for name in names:
                    if name not in section_to_names[section]:
                        section_to_names[section].append(name)
            if examples:
                section_to_examples.setdefault(section, [])
                for value in examples:
                    if value not in section_to_examples[section]:
                        section_to_examples[section].append(value)
    return section_to_names, section_to_examples


def _extract_between(text: str, start_marker: str, end_marker: str) -> str:
    start = text.find(start_marker)
    if start < 0:
        return ""
    end = text.find(end_marker, start)
    if end < 0:
        return text[start:]
    return text[start : end + 1]


def _split_examples(raw_examples: str) -> list[str]:
    values = [x.strip() for x in raw_examples.split(";") if x.strip()]
    return values[:3]


def _normalize_ascii(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text.casefold())
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _looks_part_dependent_label(section_name: str) -> bool:
    norm = _normalize_ascii(section_name)
    return ";" in section_name and ("czesci" in norm or "czesc" in norm)


def _looks_mojibake(text: str) -> bool:
    return any(marker in text for marker in ("Ă", "Ä", "Ĺ", "â€", "â€“"))


def _section_sort_key(section_number: str) -> tuple[int, ...]:
    return tuple(int(x) for x in section_number.split("."))


def _md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()


def _examples_text(values: list[str]) -> str:
    if not values:
        return ""
    return "<br>".join(_md_escape(v) for v in values[:3])


def enrich_from_profiles(
    fields: list[SectionField],
    section_to_names: dict[str, list[str]],
    section_to_examples: dict[str, list[str]],
) -> list[SectionField]:
    enriched: list[SectionField] = []
    for f in fields:
        names = section_to_names.get(f.section, [])
        examples = section_to_examples.get(f.section, [])

        section_name = f.section_name
        if names and (_looks_mojibake(section_name) or section_name == "n/a"):
            section_name = "; ".join(names)

        final_examples = f.examples
        if examples and (not final_examples or any(_looks_mojibake(x) for x in final_examples)):
            final_examples = examples[:3]

        enriched.append(
            SectionField(
                class_name=f.class_name,
                field_name=f.field_name,
                section=f.section,
                section_name=section_name,
                mandatory=f.mandatory,
                examples=final_examples,
            )
        )
    return enriched


def render_table_core(fields: list[SectionField]) -> str:
    header = (
        "| Section | Section Name | Is Mandatory | Example Values |\n"
        "|---|---|---|---|\n"
    )
    rows: list[str] = []
    for f in sorted(fields, key=lambda x: _section_sort_key(x.section)):
        rows.append(
            f"| `{f.section}` | {_md_escape(f.section_name)} | {f.mandatory} | {_examples_text(f.examples)} |"
        )
    return header + "\n".join(rows) + "\n"


def render_table_special(fields: list[SectionField], multi_only: set[str]) -> str:
    header = (
        "| Section | Section Name | Mandatory for Singlepart | Mandatory for MultiPart | Examples |\n"
        "|---|---|---|---|---|\n"
    )
    rows: list[str] = []
    for f in sorted(fields, key=lambda x: _section_sort_key(x.section)):
        if f.field_name in multi_only:
            mandatory_single = "NO"
            mandatory_multi = "YES"
        else:
            mandatory_single = "YES"
            mandatory_multi = "YES"
        rows.append(
            f"| `{f.section}` | {_md_escape(f.section_name)} | {mandatory_single} | {mandatory_multi} | {_examples_text(f.examples)} |"
        )
    return header + "\n".join(rows) + "\n"


def main() -> None:
    text = MODEL_PATH.read_text(encoding="utf-8")
    fields, multi_only, part_dependent = parse_model_sections(text)
    section_to_names, section_to_examples = load_profile_maps()
    fields = enrich_from_profiles(fields, section_to_names, section_to_examples)

    core_fields = [f for f in fields if f.class_name == "ContractNoticeCoreRawV1"]
    part_fields = [f for f in fields if f.class_name == "ContractNoticePartRawV1"]

    special_set = multi_only | part_dependent
    core_regular = [f for f in core_fields if f.field_name not in special_set]
    core_special = [f for f in core_fields if f.field_name in special_set]

    output: list[str] = []
    output.append("# ContractNotice Sections Catalog")
    output.append("")
    output.append("Auto-generated from:")
    output.append(f"- `{MODEL_PATH.as_posix()}`")
    output.append(f"- `{SINGLE_PROFILE_PATH.as_posix()}`")
    output.append(f"- `{MULTI_PROFILE_PATH.as_posix()}`")
    output.append("")
    output.append("Notes:")
    output.append("- `Is Mandatory` is derived from model typing (`T` vs `T | None`).")
    output.append("- `Mandatory for Singlepart/MultiPart` in table 2 describes applicability by contract structure.")
    output.append("- Example values are truncated to max 3 entries.")
    output.append("")
    output.append("## 1) ContractNotice Core Sections")
    output.append("")
    output.append(render_table_core(core_regular))
    output.append("## 2) Multi-part-specific or Part-dependent Core Sections")
    output.append("")
    output.append(render_table_special(core_special, multi_only))
    output.append("## 3) ContractNotice Part Sections")
    output.append("")
    output.append(render_table_core(part_fields))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("\n".join(output), encoding="utf-8")
    print(f"Generated: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()

