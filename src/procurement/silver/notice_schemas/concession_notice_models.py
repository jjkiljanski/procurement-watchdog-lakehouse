"""Pydantic section models for ConcessionNotice.

Generated from concession_notice_profile.json.
Types reflect Silver-layer parsing output; richer Gold types may be added later.

Column types after parser application:
- parse_tak_nie       → bool | None
- parse_cpv_codes     → list[str] | None
- parse_list_from_newlines → list[str] | None
- parse_criterion_weight   → int | None
- parse_duration_days_from_range → int | None
- parse_date_from_text / parse_datetime_from_text / parse_duration_end_date → str | None
"""

from __future__ import annotations

from pydantic import BaseModel


class ConcessionNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
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
    section_1_6: str | None = None
    section_1_7: bool | None = None         # parse_tak_nie
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None          # parse_date_from_text → date string
    section_2_6_1: bool | None = None       # parse_tak_nie
    section_2_7: bool | None = None         # parse_tak_nie
    section_3_1: str | None = None
    section_3_2: bool | None = None         # parse_tak_nie
    section_3_3: str | None = None
    section_3_4: str | None = None
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: str | None = None
    section_3_8: bool | None = None         # parse_tak_nie
    section_3_10: str | None = None
    section_3_11: str | None = None
    section_3_12: str | None = None
    section_4_1: str | None = None
    section_4_2: str | None = None
    section_4_3: bool | None = None         # parse_tak_nie
    section_4_4: str | None = None
    section_4_5: bool | None = None         # parse_tak_nie
    section_4_6: str | None = None
    section_4_7: list[str] | None = None    # parse_cpv_codes
    section_4_8: list[str] | None = None    # parse_cpv_codes
    section_4_9: str | None = None
    section_4_10: bool | None = None        # parse_tak_nie
    section_4_11: bool | None = None        # parse_tak_nie
    section_4_11_1: str | None = None
    section_4_12_days: int | None = None    # parse_duration_days_from_range
    section_4_12_end_date: str | None = None  # parse_duration_end_date → date string
    section_4_13: bool | None = None        # parse_tak_nie
    section_5_1: str | None = None
    section_5_2: str | None = None
    section_5_3: str | None = None
    section_5_4: bool | None = None         # parse_tak_nie
    section_5_6: bool | None = None         # parse_tak_nie
    section_5_8: bool | None = None         # parse_tak_nie
    section_5_9_2: str | None = None        # parse_datetime_from_text → datetime string
    section_5_9_3: str | None = None
    section_5_12: bool | None = None        # parse_tak_nie
    section_5_12_1: str | None = None
    section_5_13_1: str | None = None
    section_5_14: bool | None = None        # parse_tak_nie
    section_5_15: bool | None = None        # parse_tak_nie
    section_6_2: str | None = None
    section_6_3: list[str] | None = None    # parse_list_from_newlines
    section_6_3_1: str | None = None
    section_6_4: bool | None = None         # parse_tak_nie
    section_6_4_1: list[str] | None = None  # parse_list_from_newlines
    section_6_4_2: str | None = None
    section_6_5: str | None = None
    section_7_1: str | None = None
    section_7_2: bool | None = None         # parse_tak_nie
    section_7_3: str | None = None
    section_7_4: bool | None = None         # parse_tak_nie
    section_7_5: str | None = None
    section_7_6: str | None = None


class ConcessionNoticeCriterionProcedureModel(BaseModel):
    section_5_13_2: str | None = None
    section_5_13_3: str | None = None
    section_5_13_4: int | None = None       # parse_criterion_weight
    section_5_13_6: str | None = None
    section_5_13_7: str | None = None


class ConcessionNoticeCriterionQualificationModel(BaseModel):
    section_6_1_1: str | None = None
    section_6_1_2: str | None = None
    section_6_1_3: str | None = None
    section_6_1_4: str | None = None
    section_6_1_5: bool | None = None       # parse_tak_nie
    section_6_1_6: str | None = None
