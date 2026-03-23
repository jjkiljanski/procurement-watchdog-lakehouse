"""Pydantic section models for ContractNotice.

Generated from contract_notice_profile.json.
Types reflect Silver-layer parsing output; richer Gold types may be added later.

Column types after parser application:
- parse_tak_nie                  → bool | None
- parse_cpv_codes                → list[str] | None
- parse_list_from_newlines       → list[str] | None
- parse_criterion_weight         → int | None
- parse_int_from_text            → int | None
- parse_pln_value                → float | None
- parse_duration_days_from_range → int | None
- parse_date_from_text / parse_datetime_from_text / parse_duration_end_date
  / parse_currency_code / parse_national_id_* / parse_nuts3_* → str | None
"""

from __future__ import annotations

from pydantic import BaseModel


class ContractNoticeClientModel(BaseModel):
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


class ContractNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_6: str | None = None
    section_1_7: str | None = None
    section_1_9: str | None = None
    section_1_10: str | None = None
    section_1_11_1: str | None = None
    section_1_11_2: str | None = None
    section_1_11_2_type: str | None = None
    section_1_11_3: str | None = None
    section_1_11_4: str | None = None
    section_1_11_5: str | None = None
    section_1_11_6: str | None = None
    section_1_11_7: str | None = None
    section_1_11_8_code: str | None = None
    section_1_11_8_name: str | None = None
    section_1_11_9: str | None = None
    section_1_11_10: str | None = None
    section_1_11_11: str | None = None
    section_1_11_12: str | None = None
    section_2_1: str | None = None
    section_2_2: bool | None = None         # parse_tak_nie
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None
    section_2_6: str | None = None
    section_2_7: str | None = None          # parse_date_from_text → date string
    section_2_8: bool | None = None         # parse_tak_nie
    section_2_9: str | None = None
    section_2_10: str | None = None
    section_2_11: bool | None = None        # parse_tak_nie
    section_2_12: list[str] | None = None   # parse_list_from_newlines
    section_2_13: bool | None = None        # parse_tak_nie
    section_2_14: bool | None = None        # parse_tak_nie
    section_2_15: str | None = None
    section_2_16: str | None = None
    section_2_17: str | None = None
    section_3_1: str | None = None
    section_3_2: bool | None = None         # parse_tak_nie
    section_3_3: str | None = None
    section_3_4: bool | None = None         # parse_tak_nie
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: str | None = None
    section_3_8: bool | None = None         # parse_tak_nie
    section_3_9: str | None = None
    section_3_10: str | None = None
    section_3_11: str | None = None
    section_3_12: str | None = None
    section_3_14: str | None = None
    section_3_15: str | None = None
    section_3_16: str | None = None
    section_4_1_1: bool | None = None       # parse_tak_nie
    section_4_1_2: str | None = None
    section_4_1_3: str | None = None
    section_4_1_4: bool | None = None       # parse_tak_nie
    section_4_1_5: float | None = None      # parse_pln_value
    section_4_1_5_currency: str | None = None
    section_4_1_6: float | None = None      # parse_pln_value
    section_4_1_6_currency: str | None = None
    section_4_1_7: float | None = None      # parse_pln_value
    section_4_1_7_currency: str | None = None
    section_4_1_8: bool | None = None       # parse_tak_nie
    section_4_1_9: int | None = None        # parse_int_from_text
    section_4_1_11: bool | None = None      # parse_tak_nie
    section_4_1_12: int | None = None       # parse_int_from_text
    section_4_1_13: bool | None = None      # parse_tak_nie
    section_4_1_14: str | None = None
    section_5_1: bool | None = None         # parse_tak_nie
    section_5_2: list[str] | None = None    # parse_list_from_newlines
    section_5_3: bool | None = None         # parse_tak_nie
    section_5_4: str | None = None
    section_5_5: bool | None = None         # parse_tak_nie
    section_5_6: str | None = None
    section_5_7: str | None = None
    section_5_8: str | None = None
    section_5_9: bool | None = None         # parse_tak_nie
    section_5_10: str | None = None
    section_5_11: str | None = None
    section_6_1: bool | None = None         # parse_tak_nie
    section_6_2: str | None = None
    section_6_3: bool | None = None         # parse_tak_nie
    section_6_3_1: str | None = None
    section_6_3_2: str | None = None
    section_6_4: bool | None = None         # parse_tak_nie
    section_6_4_1: str | None = None
    section_6_5: bool | None = None         # parse_tak_nie
    section_6_6: str | None = None
    section_6_7: bool | None = None         # parse_tak_nie
    section_7_1: bool | None = None         # parse_tak_nie
    section_7_2: str | None = None
    section_7_3: bool | None = None         # parse_tak_nie
    section_7_4: str | None = None
    section_7_5: bool | None = None         # parse_tak_nie
    section_7_6: str | None = None
    section_8_1: str | None = None          # parse_datetime_from_text → datetime string
    section_8_2: str | None = None
    section_8_3: str | None = None          # parse_datetime_from_text → datetime string
    section_8_4: str | None = None          # parse_duration_end_date → date string
    section_8_5: str | None = None
    section_8_6: int | None = None          # parse_int_from_text
    section_8_7: str | None = None
    section_8_9: str | None = None          # parse_datetime_from_text → datetime string
    section_8_10: str | None = None
    section_8_11: str | None = None         # parse_datetime_from_text → datetime string
    section_8_18: bool | None = None        # parse_tak_nie
    section_8_19: str | None = None
    section_8_20: bool | None = None        # parse_tak_nie
    section_8_21: int | None = None         # parse_int_from_text
    section_8_22: str | None = None
    section_8_23: str | None = None
    section_8_8: str | None = None


class ContractNoticePartModel(BaseModel):
    section_4_2_2: str | None = None
    section_4_2_3: str | None = None
    section_4_2_4: bool | None = None       # parse_tak_nie
    section_4_2_5: float | None = None      # parse_pln_value
    section_4_2_5_currency: str | None = None
    section_4_2_6: list[str] | None = None  # parse_cpv_codes
    section_4_2_7: list[str] | None = None  # parse_cpv_codes
    section_4_2_8: bool | None = None       # parse_tak_nie
    section_4_2_9: str | None = None
    section_4_2_10_days: int | None = None  # parse_duration_days_from_range
    section_4_2_10_end_date: str | None = None  # parse_duration_end_date → date string
    section_4_2_11: bool | None = None      # parse_tak_nie
    section_4_2_12: str | None = None
    section_4_2_13: bool | None = None      # parse_tak_nie
    section_4_2_14: str | None = None
    section_4_3_1: str | None = None
    section_4_3_2: str | None = None
    section_4_3_3: str | None = None
    section_4_3_10: bool | None = None      # parse_tak_nie
    section_4_3_11: str | None = None


class ContractNoticePartCriterionModel(BaseModel):
    section_4_3_4: str | None = None
    section_4_3_5: str | None = None
    section_4_3_6: int | None = None        # parse_criterion_weight
    section_4_3_7: int | None = None        # parse_int_from_text
