"""Pydantic section models for ConcessionUpdateAgreementNotice.

Auto-generated from concession_update_agreement_notice_profile_automatic.json.
All str fields are raw Silver values; richer types come from registered parsers.
"""

from __future__ import annotations

from pydantic import BaseModel


class ConcessionUpdateAgreementNoticeCoreModel(BaseModel):
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
    section_1_4_9: str | None = None
    section_1_4_10: str | None = None
    section_1_5: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None  # YYYY-MM-DD
    section_2_5: str | None = None
    section_2_6: str | None = None
    section_3_1: str | None = None
    section_3_2: bool | None = None  # Tak/Nie → bool
    section_3_3: bool | None = None  # Tak/Nie → bool
    section_3_4: bool | None = None  # Tak/Nie → bool
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: list[str] | None = None  # list of CPV codes
    section_3_8: list[str] | None = None  # list of CPV codes
    section_4_1: str | None = None  # YYYY-MM-DD
    section_4_2_start_date: str | None = None  # YYYY-MM-DD start date
    section_4_2_end_date: str | None = None  # YYYY-MM-DD end date
    section_4_2_duration: str | None = None  # ISO 8601 duration (e.g. P24M, P30D)
    section_4_3_value: float | None = None  # monetary value
    section_4_3_currency: str | None = None  # currency code
    section_4_4: bool | None = None  # Tak/Nie → bool
    section_4_4_1: str | None = None
    section_4_4_2: str | None = None
    section_4_4_3: str | None = None
    section_4_4_4: str | None = None
    section_4_4_5: str | None = None
    section_4_4_6: str | None = None
    section_4_4_7_value: str | None = None  # parsed national ID value
    section_4_4_7_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_5_1: str | None = None  # YYYY-MM-DD
    section_5_2: str | None = None
    section_5_3: str | None = None
    section_5_4: str | None = None
    section_5_5_1: str | None = None
    section_5_5_2: str | None = None

