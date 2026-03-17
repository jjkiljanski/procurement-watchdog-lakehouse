"""Pydantic section models for ContractPerformingNotice.

Generated from contract_performing_notice_profile.json.
Types reflect Silver-layer parsing output; richer Gold types may be added later.

Column types after parser application:
- parse_tak_nie                  → bool | None
- parse_cpv_codes                → list[str] | None
- parse_list_from_newlines       → list[str] | None
- parse_int_from_text            → int | None
- parse_pln_value                → float | None
- parse_duration_end_date / parse_date_from_text / parse_currency_code → str | None
"""

from __future__ import annotations

from pydantic import BaseModel


class ContractPerformingNoticeChangeMatterModel(BaseModel):
    section_5_4_2: str | None = None
    section_5_4_3: list[str] | None = None   # parse_list_from_newlines
    section_5_4_4: str | None = None
    section_5_4_5: str | None = None
    section_5_4_6: float | None = None       # parse_pln_value
    section_5_4_7: str | None = None         # parse_currency_code
    section_5_4_8: bool | None = None        # parse_tak_nie


class ContractPerformingNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_2: str | None = None
    section_1_3: str | None = None
    section_1_3_type: str | None = None
    section_1_4_1: str | None = None
    section_1_4_2: str | None = None
    section_1_4_3: str | None = None
    section_1_4_4: str | None = None
    section_1_4_5: str | None = None
    section_1_4_6_code: str | None = None
    section_1_4_6_name: str | None = None
    section_1_4_7: str | None = None
    section_1_4_8: str | None = None
    section_1_4_9: str | None = None
    section_1_4_10: str | None = None
    section_1_5: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_3_1: str | None = None
    section_3_2: bool | None = None          # parse_tak_nie
    section_3_2_1: str | None = None
    section_3_3: bool | None = None          # parse_tak_nie
    section_3_4: str | None = None
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: str | None = None
    section_4_1: str | None = None
    section_4_2: str | None = None           # parse_duration_end_date → date string
    section_4_3: str | None = None
    section_4_3_1: str | None = None
    section_4_3_2: str | None = None
    section_4_3_2_type: str | None = None
    section_4_3_3: str | None = None
    section_4_3_4: str | None = None
    section_4_3_5: str | None = None
    section_4_3_6: str | None = None
    section_4_3_7: str | None = None
    section_4_4: float | None = None         # parse_pln_value
    section_4_4_currency: str | None = None  # parse_currency_code
    section_4_5: str | None = None
    section_5_1: bool | None = None          # parse_tak_nie
    section_5_2: str | None = None
    section_5_3: bool | None = None          # parse_tak_nie
    section_5_4_1: int | None = None         # parse_int_from_text
    section_5_5: float | None = None         # parse_pln_value
    section_5_5_currency: str | None = None  # parse_currency_code
    section_5_6: bool | None = None          # parse_tak_nie
    section_5_7: str | None = None


class ContractPerformingNoticePartModel(BaseModel):
    section_3_8: str | None = None
    section_3_9: list[str] | None = None     # parse_cpv_codes
    section_3_10: list[str] | None = None    # parse_cpv_codes
