"""Orchestrator for modular Silver HTML parsing pipeline."""

from __future__ import annotations

from typing import Any

from bs4 import BeautifulSoup

from procurement.silver.html_parsing.sections_parser import build_notice_sections_model
from procurement.silver.field_parsers import parse_notice_values


def parse_notice_html(
    *,
    html: str,
    notice_type: str | None,
    procedure_result: str | None = None,
    notice_dicts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Parse one notice HTML into a section model + parsed values.

    This function is the new orchestration entrypoint. For now it provides
    a stable integration surface while notice-specific parser logic is being
    migrated out of `html_parser.py`.
    """
    soup = BeautifulSoup(html, "lxml")
    sections_model = build_notice_sections_model(
        soup,
        notice_type=notice_type,
        notice_dicts=notice_dicts,
    )
    parsed_values = parse_notice_values(
        notice_type=notice_type,
        sections_model=sections_model,
        soup=soup,
        procedure_result=procedure_result,
    )
    return {
        "notice_type": notice_type,
        "sections_model": sections_model,
        "parsed_values": parsed_values,
    }

