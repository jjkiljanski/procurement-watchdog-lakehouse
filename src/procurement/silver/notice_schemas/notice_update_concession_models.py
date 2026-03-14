"""Pydantic section models for NoticeUpdateConcession.

Auto-generated from notice_update_concession_profile_automatic.json.
All str fields are raw Silver values; richer types come from registered parsers.
"""

from __future__ import annotations

from pydantic import BaseModel


class NoticeUpdateConcessionCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_3_value: str | None = None  # parsed national ID value
    section_1_3_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_1_4_1: str | None = None
    section_1_4_2: str | None = None
    section_1_4_3: str | None = None
    section_1_4_4: str | None = None
    section_1_4_5: str | None = None
    section_1_4_6_code: str | None = None  # NUTS-3 code
    section_1_4_6_name: str | None = None  # NUTS-3 region name
    section_1_4_7: str | None = None
    section_1_4_8: str | None = None
    section_1_4_9: str | None = None
    section_1_4_10: str | None = None
    section_1_5: str | None = None
    section_1_6: str | None = None
    section_1_7: bool | None = None  # Tak/Nie → bool
    section_2_1: str | None = None
    section_2_2: str | None = None  # YYYY-MM-DD
    section_3_2: str | None = None
    section_3_3: str | None = None

class NoticeUpdateConcessionPartPartModel(BaseModel):
    section_3_4_1: str | None = None

class NoticeUpdateConcessionPartCoreModel(BaseModel):
    section_4_1: str | None = None

