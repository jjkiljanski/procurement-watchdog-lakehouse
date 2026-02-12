"""Load official BZP dictionary files from refs/bzp_api/.

Each dictionary JSON has a nested tree structure with {key, identifier, Items}.
This module flattens them into simple identifier → key mappings.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

REFS_DIR = Path(__file__).resolve().parent.parent.parent / "refs" / "bzp_api"


def _flatten_items(items: list[dict]) -> dict[str, str]:
    """Recursively flatten a BZP dictionary tree into {identifier: key}."""
    result: dict[str, str] = {}
    for item in items:
        identifier = item.get("identifier")
        key = item.get("key")
        if identifier and key:
            result[identifier] = key
        children = item.get("Items")
        if children:
            result.update(_flatten_items(children))
    return result


def _load_dict(filename: str) -> dict[str, str]:
    path = REFS_DIR / filename
    data = json.loads(path.read_text(encoding="utf-8"))
    return _flatten_items(data["items"])


@lru_cache(maxsize=1)
def province_names() -> dict[str, str]:
    """Map province code → name (e.g. 'PL14' → 'mazowieckie')."""
    return _load_dict("SL.MT.007.json")


@lru_cache(maxsize=1)
def client_type_names() -> dict[str, str]:
    """Map clientType code → description (e.g. '1.1.2' → 'jednostka samorządu terytorialnego')."""
    return _load_dict("SL.MO.013.json")


@lru_cache(maxsize=1)
def order_type_names() -> dict[str, str]:
    """Map orderType code → Polish name (e.g. 'Delivery' → 'Dostawy')."""
    return _load_dict("ENUM.002.json")
