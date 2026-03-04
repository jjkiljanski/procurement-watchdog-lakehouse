"""Common HTML parsing helpers shared across notice parsers."""

from __future__ import annotations

import re
import unicodedata
from html import unescape

from bs4 import BeautifulSoup, NavigableString, Tag

_SPAN_NORMAL_RE = re.compile(
    r"<span[^>]*class=['\"]normal['\"][^>]*>(.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def section_to_field_name(section_number: str) -> str:
    """Map `1.5.10` -> `cn_section_1_5_10`."""
    return f"cn_section_{section_number.replace('.', '_')}"


def extract_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def _field_marker_re(field_num: str) -> re.Pattern[str]:
    """Regex for exact field markers like '4.4.)' (not matching '1.4.4.)')."""
    return re.compile(rf"(?<![\d.]){re.escape(field_num)}\)")


def _find_h3(soup: BeautifulSoup, field_num: str) -> Tag | None:
    """Find the first <h3> whose text starts with a given field number."""
    marker_re = _field_marker_re(field_num)
    for h3 in soup.find_all("h3"):
        if marker_re.search(h3.get_text()):
            return h3
    return None


def _find_all_h3(soup: BeautifulSoup, field_num: str) -> list[Tag]:
    """Find all <h3> tags matching a given field number."""
    results = []
    marker_re = _field_marker_re(field_num)
    for h3 in soup.find_all("h3"):
        if marker_re.search(h3.get_text()):
            results.append(h3)
    return results


def _span_value(h3: Tag | None) -> str | None:
    """Extract text from <span class='normal'> inside an h3."""
    if h3 is None:
        return None
    span = h3.find("span", class_="normal")
    if span is None:
        return None
    text = span.get_text().strip()
    return text or None


def _normalize_label_text(text: str) -> str:
    """Normalize text for robust label matching across encoding variants."""
    lowered = text.casefold()
    replacements = {
        "Ä…": "a",
        "Ä‡": "c",
        "Ä™": "e",
        "Ĺ‚": "l",
        "Ĺ„": "n",
        "Ăł": "o",
        "Ĺ›": "s",
        "Ĺş": "z",
        "ĹĽ": "z",
        "Ă„â€¦": "a",
        "Ă„â€ˇ": "c",
        "Ă„â„˘": "e",
        "Äąâ€š": "l",
        "Äąâ€ž": "n",
        "Ä‚Ĺ‚": "o",
        "Äąâ€ş": "s",
        "ÄąÂş": "z",
        "ÄąÂĽ": "z",
    }
    for src, dst in replacements.items():
        lowered = lowered.replace(src, dst)
    lowered = unicodedata.normalize("NFKD", lowered)
    lowered = "".join(ch for ch in lowered if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", lowered).strip()


def _find_h3_by_label(soup: BeautifulSoup, patterns: list[str]) -> Tag | None:
    """Find h3 by case-insensitive label fragments in text content."""
    lowered_patterns = [_normalize_label_text(p) for p in patterns]
    for h3 in soup.find_all("h3"):
        text = _normalize_label_text(h3.get_text(separator=" ", strip=True))
        if all(pattern in text for pattern in lowered_patterns):
            return h3
    return None


def _field_num(h3: Tag | None) -> str | None:
    """Extract a field number prefix from an h3 (e.g. 6.2.)."""
    if h3 is None:
        return None
    text = h3.get_text(separator=" ", strip=True)
    match = re.search(r"(\d+\.\d+(?:\.\d+)?\.)\)", text)
    if match is None:
        return None
    return match.group(1)


def _text_after_h3(h3: Tag | None) -> str | None:
    """Extract plain text that follows an <h3> as a sibling text node."""
    if h3 is None:
        return None
    sibling = h3.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            text = str(sibling).strip()
            if text:
                return text
        elif hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "br":
                sibling = sibling.next_sibling
                continue
            break
        sibling = sibling.next_sibling
    return None


def _collect_p_text(h3: Tag) -> str | None:
    """Collect text from sibling <p> tags after an h3 until the next h3/h2."""
    parts: list[str] = []
    sibling = h3.next_sibling
    while sibling is not None:
        if hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "p":
                text = sibling.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        sibling = sibling.next_sibling
    return "\n".join(parts) if parts else None


def _collect_p_values(h3: Tag | None) -> list[str]:
    """Collect plain values from sibling <p> tags after an h3 until next h3/h2."""
    if h3 is None:
        return []
    parts: list[str] = []
    sibling = h3.next_sibling
    while sibling is not None:
        if hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "p":
                text = sibling.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        sibling = sibling.next_sibling
    return parts


def _extract_h3_field_fast(html: str, field_num: str) -> str | None:
    marker_re = _field_marker_re(field_num)
    match = marker_re.search(html)
    if match is None:
        return None
    idx = match.start()
    start = html.rfind("<h3", 0, idx)
    end = html.find("</h3>", idx)
    if start < 0 or end < 0:
        return None
    snippet = html[start : end + 5]

    span = _SPAN_NORMAL_RE.search(snippet)
    if span:
        text = unescape(_TAG_RE.sub(" ", span.group(1)))
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

    rel_idx = max(0, idx - start)
    tail = snippet[rel_idx + len(field_num) + 1 :]
    text = unescape(_TAG_RE.sub(" ", tail))
    text = re.sub(r"\s+", " ", text).strip(" :\u00a0\t\r\n")
    return text or None
