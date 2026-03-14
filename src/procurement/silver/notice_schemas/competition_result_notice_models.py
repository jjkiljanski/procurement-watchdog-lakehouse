"""Pydantic section models for CompetitionResultNotice.

Auto-generated from competition_result_notice_profile_automatic.json.
All str fields are raw Silver values; richer types come from registered parsers.
"""

from __future__ import annotations

from pydantic import BaseModel


class CompetitionResultNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_2: str | None = None
    section_1_3: str | None = None
    section_1_4_value: str | None = None  # parsed national ID value
    section_1_4_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_1_5_1: str | None = None
    section_1_5_2: str | None = None
    section_1_5_3: str | None = None
    section_1_5_4: str | None = None
    section_1_5_5: str | None = None
    section_1_5_6_code: str | None = None  # NUTS-3 code
    section_1_5_6_name: str | None = None  # NUTS-3 region name
    section_1_5_7: str | None = None
    section_1_5_8: str | None = None
    section_1_5_9: str | None = None
    section_1_5_10: str | None = None
    section_1_6: str | None = None
    section_1_7: str | None = None
    section_1_8: str | None = None
    section_1_10_1: str | None = None
    section_1_10_3_value: str | None = None  # parsed national ID value
    section_1_10_3_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_1_10_4: str | None = None
    section_1_10_5: str | None = None
    section_1_10_6: str | None = None
    section_1_10_7: str | None = None
    section_1_10_8: str | None = None
    section_1_10_9_code: str | None = None  # NUTS-3 code
    section_1_10_9_name: str | None = None  # NUTS-3 region name
    section_1_10_10: str | None = None
    section_1_10_11: str | None = None
    section_1_10_12: str | None = None
    section_1_10_13: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None  # YYYY-MM-DD
    section_2_6: str | None = None
    section_2_8: bool | None = None  # Tak/Nie → bool
    section_2_9: str | None = None
    section_3_1: str | None = None
    section_3_2: str | None = None
    section_4_1: str | None = None
    section_4_2: str | None = None
    section_4_3: list[str] | None = None  # list of CPV codes
    section_4_4: list[str] | None = None  # list of CPV codes
    section_5_1: str | None = None
    section_5_2: str | None = None
    section_5_3: str | None = None  # YYYY-MM-DD
    section_5_4: int | None = None  # integer
    section_5_5: int | None = None  # integer
    section_5_6: int | None = None  # integer
    section_5_11: str | None = None

class CompetitionResultNoticeAuthorModel(BaseModel):
    section_5_7_1: str | None = None
    section_5_7_2: str | None = None
    section_5_7_3: str | None = None
    section_5_7_4: str | None = None
    section_5_7_5: str | None = None
    section_5_7_6: str | None = None
    section_5_7_7_value: str | None = None  # parsed national ID value
    section_5_7_7_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_5_8: str | None = None
    section_5_9: str | None = None
    section_5_10_value: float | None = None  # monetary value
    section_5_10_currency: str | None = None  # currency code

