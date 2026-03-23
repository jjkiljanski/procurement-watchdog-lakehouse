"""Pydantic section models for TenderResultNotice.

Generated from tender_result_notice_profile.json.
Types reflect Silver-layer parsing output; richer Gold types may be added later.

Column types after parser application:
- parse_tak_nie                  → bool | None
- parse_cpv_codes                → list[str] | None
- parse_list_from_newlines       → list[str] | None
- parse_int_from_text            → int | None
- parse_pln_value                → float | None
- parse_duration_days_from_range → int | None
- parse_date_from_text / parse_datetime_from_text / parse_duration_end_date
  / parse_currency_code / parse_national_id_* / parse_nuts3_* → str | None
"""

from __future__ import annotations

from pydantic import BaseModel


class TenderResultNoticeClientModel(BaseModel):
    section_1_2: str | None = None
    section_1_3: str | None = None
    section_1_4: str | None = None
    section_1_4_type: str | None = None
    section_1_5_1: str | None = None
    section_1_5_2: str | None = None
    section_1_5_3: str | None = None
    section_1_5_4: str | None = None
    section_1_5_5: str | None = None
    section_1_5_6_code: str | None = None
    section_1_5_6_name: str | None = None
    section_1_5_7: str | None = None
    section_1_5_8: str | None = None
    section_1_5_9: str | None = None
    section_1_5_10: str | None = None


class TenderResultNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_6: str | None = None
    section_1_7: str | None = None
    section_1_8: str | None = None
    section_2_1: str | None = None
    section_2_2: bool | None = None          # parse_tak_nie
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None
    section_2_6: str | None = None
    section_2_7: str | None = None           # parse_date_from_text → date string
    section_2_8: bool | None = None          # parse_tak_nie
    section_2_9: str | None = None
    section_2_10: str | None = None
    section_2_11: bool | None = None         # parse_tak_nie
    section_2_12: str | None = None
    section_2_13: bool | None = None         # parse_tak_nie
    section_2_14: str | None = None
    section_3_1: str | None = None
    section_3_1_1: str | None = None
    section_3_1_2: str | None = None
    section_4_1: str | None = None
    section_4_2: bool | None = None          # parse_tak_nie
    section_4_3: float | None = None         # parse_pln_value
    section_4_3_currency: str | None = None  # parse_currency_code
    section_4_3_1: float | None = None       # parse_pln_value
    section_4_3_1_currency: str | None = None  # parse_currency_code
    section_4_3_2: float | None = None       # parse_pln_value
    section_4_4: str | None = None


class TenderResultNoticePartModel(BaseModel):
    section_4_5_1: str | None = None
    section_4_5_2: str | None = None
    section_4_5_3: list[str] | None = None   # parse_cpv_codes
    section_4_5_4: list[str] | None = None   # parse_cpv_codes
    section_4_5_5: float | None = None       # parse_pln_value
    section_4_5_5_currency: str | None = None  # parse_currency_code
    section_5_1: str | None = None
    section_5_2: list[str] | None = None     # parse_list_from_newlines
    section_5_2_1: str | None = None
    section_6_1: int | None = None           # parse_int_from_text
    section_6_1_1: int | None = None         # parse_int_from_text
    section_6_1_2: int | None = None         # parse_int_from_text
    section_6_1_3: int | None = None         # parse_int_from_text
    section_6_1_4: int | None = None         # parse_int_from_text
    section_6_1_5: int | None = None         # parse_int_from_text
    section_6_1_6: int | None = None         # parse_int_from_text
    section_6_1_7: int | None = None         # parse_int_from_text
    section_6_2: float | None = None         # parse_pln_value
    section_6_2_currency: str | None = None  # parse_currency_code
    section_6_3: float | None = None         # parse_pln_value
    section_6_3_currency: str | None = None  # parse_currency_code
    section_6_4: float | None = None         # parse_pln_value
    section_6_4_currency: str | None = None  # parse_currency_code
    section_6_5: bool | None = None          # parse_tak_nie
    section_6_6: bool | None = None          # parse_tak_nie
    section_6_7: str | None = None
    section_7_1: bool | None = None          # parse_tak_nie
    section_7_2: str | None = None
    section_7_3_1: str | None = None
    section_7_3_2: str | None = None
    section_7_3_2_type: str | None = None
    section_7_3_3: str | None = None
    section_7_3_4: str | None = None
    section_7_3_5: str | None = None
    section_7_3_6: str | None = None
    section_7_3_7: str | None = None
    section_7_3_8: bool | None = None        # parse_tak_nie
    section_7_3_9: str | None = None
    section_7_4: bool | None = None          # parse_tak_nie
    section_7_4_1: str | None = None
    section_8_1: str | None = None           # parse_date_from_text → date string
    section_8_2: float | None = None         # parse_pln_value
    section_8_2_currency: str | None = None  # parse_currency_code
    section_8_3_days: int | None = None      # parse_duration_days_from_range
    section_8_3_end_date: str | None = None  # parse_duration_end_date → date string
    section_8_4: str | None = None
