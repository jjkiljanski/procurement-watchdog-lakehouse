"""Common HTML parsing helpers shared across notice parsers.

This file is a scaffold for the modular parser split.
Incremental migration from `html_parser.py` will move reusable helpers here.
"""

from __future__ import annotations

import re
from bs4 import Tag


def section_to_field_name(section_number: str) -> str:
    """Map `1.5.10` -> `cn_section_1_5_10`."""
    return f"cn_section_{section_number.replace('.', '_')}"


def extract_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def extract_section_value(h3: Tag) -> str | None:
    """Placeholder: keeps migration surface explicit.

    Current production value extraction still lives in `html_parser.py`.
    """
    _ = h3
    return None

