"""Pydantic section models for CircumstancesFulfillmentNotice.

Generated from circumstances_fulfillment_notice_profile.json.
Types reflect Silver-layer parsing; richer Gold types may be added later.
"""

from __future__ import annotations

from pydantic import BaseModel


class CircumstancesFulfillmentNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_2: str | None = None
    # 1.3: national ID split into value + type
    section_1_3: str | None = None
    section_1_3_type: str | None = None
    section_1_4_1: str | None = None
    section_1_4_2: str | None = None
    section_1_4_3: str | None = None
    section_1_4_4: str | None = None
    section_1_4_5: str | None = None
    # 1.4.6: NUTS-3 split into code + name
    section_1_4_6_code: str | None = None
    section_1_4_6_name: str | None = None
    section_1_4_7: str | None = None
    section_1_4_8: str | None = None
    section_1_4_9: str | None = None
    section_1_4_10: str | None = None
    section_1_5: str | None = None
    section_1_6: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None          # date string YYYY-MM-DD
    section_2_6_1: bool | None = None       # Tak/Nie
    section_2_6_2: str | None = None
    section_3_1: str | None = None
    section_3_2: str | None = None
    section_3_3: bool | None = None         # Tak/Nie
    section_3_4: str | None = None
    section_3_5: str | None = None
    section_3_6: str | None = None
    section_3_7: str | None = None
    section_3_8: list[str] | None = None    # CPV codes
    section_3_9: list[str] | None = None    # CPV codes
    section_4_1: str | None = None          # contract date (raw)
    # 4.2: duration → calendar days (computed from 4.1 + raw 4.2)
    section_4_2: int | None = None
    # computed end date from 4.1 + 4.2
    section_4_1_and_2_contract_end: str | None = None
    section_4_3_1: str | None = None
    # 4.3.2: national ID split into value + type
    section_4_3_2: str | None = None
    section_4_3_2_type: str | None = None
    section_4_3_3: str | None = None
    section_4_3_4: str | None = None
    section_4_3_5: str | None = None
    section_4_3_6: str | None = None
    section_4_3_7: str | None = None
    section_4_4: bool | None = None         # Tak/Nie
