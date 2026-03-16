"""Pydantic section models for ContractPerformingNotice.

Auto-generated from contract_performing_notice_profile.json.
All field types are str | None; richer types will be added in Gold.
"""

from __future__ import annotations

from pydantic import BaseModel


class ContractPerformingNoticeChangeMatterModel(BaseModel):
    section_5_4_2: str | None = None
    section_5_4_3: str | None = None
    section_5_4_4: str | None = None
    section_5_4_5: str | None = None
    section_5_4_6: str | None = None
    section_5_4_7: str | None = None
    section_5_4_8: str | None = None

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
    section_3_2: str | None = None
    section_3_2_1: str | None = None
    section_3_3: str | None = None
    section_3_4: str | None = None
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: str | None = None
    section_4_1: str | None = None
    section_4_2: str | None = None
    section_4_3: str | None = None
    section_4_3_1: str | None = None
    section_4_3_2: str | None = None
    section_4_3_2_type: str | None = None
    section_4_3_3: str | None = None
    section_4_3_4: str | None = None
    section_4_3_5: str | None = None
    section_4_3_6: str | None = None
    section_4_3_7: str | None = None
    section_4_4: str | None = None
    section_4_4_currency: str | None = None
    section_4_5: str | None = None
    section_5_1: str | None = None
    section_5_2: str | None = None
    section_5_3: str | None = None
    section_5_4_1: str | None = None
    section_5_5: str | None = None
    section_5_5_currency: str | None = None
    section_5_6: str | None = None
    section_5_7: str | None = None

class ContractPerformingNoticePartModel(BaseModel):
    section_3_8: str | None = None
    section_3_9: str | None = None
    section_3_10: str | None = None
