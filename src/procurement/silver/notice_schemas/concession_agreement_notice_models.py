"""Pydantic section models for ConcessionAgreementNotice.

Auto-generated from concession_agreement_notice_profile_automatic.json.
All str fields are raw Silver values; richer types come from registered parsers.
"""

from __future__ import annotations

from pydantic import BaseModel


class ConcessionAgreementNoticeCoreModel(BaseModel):
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
    section_1_4_9: str | None = None
    section_1_4_10: str | None = None
    section_1_5: str | None = None
    section_1_6: str | None = None
    section_1_6_1: str | None = None
    section_1_7: bool | None = None  # Tak/Nie → bool
    section_2_1: bool | None = None  # Tak/Nie → bool
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None
    section_2_6: str | None = None
    section_2_7: str | None = None  # YYYY-MM-DD
    section_2_8: bool | None = None  # Tak/Nie → bool
    section_3_1: str | None = None
    section_3_2: bool | None = None  # Tak/Nie → bool
    section_3_3_1_value: float | None = None  # monetary value
    section_3_3_1_currency: str | None = None  # currency code
    section_3_4: bool | None = None  # Tak/Nie → bool
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: list[str] | None = None  # list of CPV codes
    section_3_8: list[str] | None = None  # list of CPV codes
    section_4_1: str | None = None
    section_4_2: bool | None = None  # Tak/Nie → bool
    section_4_2_1: str | None = None
    section_4_2_2: str | None = None
    section_4_3_1: str | None = None
    section_4_4_1: bool | None = None  # Tak/Nie → bool
    section_4_4_2: int | None = None  # integer
    section_5_1: int | None = None  # integer
    section_5_2: int | None = None  # integer
    section_5_3: int | None = None  # integer
    section_5_4: int | None = None  # integer
    section_5_5: int | None = None  # integer
    section_5_6: int | None = None  # integer
    section_5_7: bool | None = None  # Tak/Nie → bool
    section_6_1: bool | None = None  # Tak/Nie → bool
    section_6_1_1: str | None = None
    section_6_1_2: str | None = None
    section_6_1_3: str | None = None
    section_6_1_4: str | None = None
    section_6_1_5: str | None = None
    section_6_1_6: str | None = None
    section_6_1_7_value: str | None = None  # parsed national ID value
    section_6_1_7_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_6_1_8: str | None = None
    section_6_1_9: bool | None = None  # Tak/Nie → bool
    section_6_1_10: bool | None = None  # Tak/Nie → bool
    section_7_1: str | None = None  # YYYY-MM-DD
    section_7_2_start_date: str | None = None  # YYYY-MM-DD start date
    section_7_2_end_date: str | None = None  # YYYY-MM-DD end date
    section_7_2_days: int | None = None  # duration in days
    section_7_3_value: float | None = None  # monetary value
    section_7_3_currency: str | None = None  # currency code
    section_7_4: str | None = None
    section_7_5_value: float | None = None  # monetary value
    section_7_5_currency: str | None = None  # currency code
    section_7_6: bool | None = None  # Tak/Nie → bool
    section_7_8: bool | None = None  # Tak/Nie → bool
    section_7_9: str | None = None
    section_7_10: bool | None = None  # Tak/Nie → bool
    section_7_11: bool | None = None  # Tak/Nie → bool
    section_7_12: bool | None = None  # Tak/Nie → bool
    section_7_13: bool | None = None  # Tak/Nie → bool
    section_8_1: str | None = None

class ConcessionAgreementNoticeCriterionProcedureModel(BaseModel):
    section_4_3_2: str | None = None
    section_4_3_3: str | None = None
    section_4_3_4: int | None = None  # weight 0–100
    section_4_3_7: str | None = None

