"""Notice-type section profiles and utilities."""

from __future__ import annotations

import re


def normalized_notice_type_token(notice_type: str | None) -> str:
    if notice_type is None:
        return "__NULL__"
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(notice_type)).strip("_")
    return normalized or "__EMPTY__"


__all__ = ["normalized_notice_type_token"]
