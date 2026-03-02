"""Pydantic models for ContractNotice section profile JSON outputs.

Target file shape:
examples/contractnotice_sections/contractnotice_2025-10-01_single_part_sections_unique.json
"""

from __future__ import annotations

from pydantic import BaseModel


class ContractNoticeSectionUniqueValues(BaseModel):
    """Unique values observed for one section number."""

    section_number: str
    section_type_unique_values: list[str]
    value_unique_values: list[str]


class ContractNoticeSectionProfile(BaseModel):
    """Aggregated section profile for ContractNotice cohort (single/multi-part)."""

    date: str
    noticeType: str
    contract_class: str
    contract_count: int
    sections: list[ContractNoticeSectionUniqueValues]

