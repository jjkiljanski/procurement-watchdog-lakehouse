"""Gold-layer extraction helpers for NoticeUpdateNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.parser_utils import (
    _collect_p_text,
    _find_h3,
    _span_value,
    _text_after_h3,
)
from procurement.silver.models import ChangeEntry, NoticeChange


def _extract_details_notice_update(soup: BeautifulSoup) -> NoticeChange | None:
    """NoticeUpdateNotice: fields 3.2, 3.3, 3.4/3.4.1 (repeating).

    Section mapping knowledge:
    - 3.2: changed notice number
    - 3.3: changed notice version
    - 3.4: changed section name (repeating)
    - 3.4.1: change description (paired with 3.4)
    """
    changed_notice_number = _span_value(_find_h3(soup, "3.2."))
    changed_notice_version = _span_value(_find_h3(soup, "3.3."))

    # Collect 3.4/3.4.1 pairs by walking h3 tags in document order
    # (mirrors the _extract_criteria pattern for 4.3.5/4.3.6 pairs)
    changes: list[ChangeEntry] = []
    all_h3s = soup.find_all("h3")
    i = 0
    while i < len(all_h3s):
        text = all_h3s[i].get_text()
        if "3.4.)" in text and "3.4.1.)" not in text:
            changed_section = _span_value(all_h3s[i]) or _text_after_h3(all_h3s[i])
            change_desc = None
            if i + 1 < len(all_h3s) and "3.4.1.)" in all_h3s[i + 1].get_text():
                change_desc = _collect_p_text(all_h3s[i + 1])
                i += 1  # skip the 3.4.1 we just consumed
            if changed_section or change_desc:
                changes.append(
                    ChangeEntry(
                        changed_section=changed_section,
                        change_description=change_desc,
                    )
                )
        i += 1

    if changed_notice_number is None and changed_notice_version is None and not changes:
        return None
    return NoticeChange(
        changed_notice_number=changed_notice_number,
        changed_notice_version=changed_notice_version,
        changes=changes or None,
    )
