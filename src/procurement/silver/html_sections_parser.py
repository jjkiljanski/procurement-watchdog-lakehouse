"""Section-model builder entrypoint.

Thin wrapper over the current profile-driven section parser.
"""

from __future__ import annotations

from procurement.silver.raw_html_sections_parser import (
    build_notice_sections_model,
    extract_contract_notice_section_number,
    extract_contract_notice_section_value,
    section_number_key,
    section_to_field_name,
)

__all__ = [
    "build_notice_sections_model",
    "extract_contract_notice_section_number",
    "extract_contract_notice_section_value",
    "section_number_key",
    "section_to_field_name",
]

