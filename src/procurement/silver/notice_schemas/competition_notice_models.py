"""Pydantic section models for CompetitionNotice.

Auto-generated from competition_notice_profile.json.
Types reflect column parsers and derived_cols defined in the profile.
"""

from __future__ import annotations

from pydantic import BaseModel


class CompetitionNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_2: str | None = None
    section_1_3: str | None = None
    section_1_4: str | None = None        # parsed national ID value
    section_1_4_type: str | None = None   # NIP / REGON / PESEL / foreign
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
    section_1_9_1: str | None = None
    section_1_9_2: str | None = None       # parsed national ID value
    section_1_9_2_type: str | None = None  # NIP / REGON / PESEL / foreign
    section_1_9_3: str | None = None
    section_1_9_4: str | None = None
    section_1_9_5: str | None = None
    section_1_9_6: str | None = None
    section_1_9_7: str | None = None
    section_1_9_8: str | None = None
    section_1_9_9: str | None = None
    section_1_9_10: str | None = None
    section_1_9_11: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None         # YYYY-MM-DD
    section_2_6: bool | None = None        # reserved competition flag
    section_2_7: bool | None = None        # EU cofunding flag
    section_2_8: str | None = None
    section_3_1: str | None = None
    section_3_2: str | None = None
    section_3_3: str | None = None         # YYYY-MM-DDTHH:MM
    section_3_4: bool | None = None        # participant limit flag
    section_3_4_1: str | None = None
    section_3_5: str | None = None         # YYYY-MM-DDTHH:MM
    section_3_6: str | None = None         # YYYY-MM-DDTHH:MM
    section_3_7: str | None = None
    section_4_1: str | None = None
    section_4_2: str | None = None
    section_4_3: bool | None = None        # electronic-only communication
    section_4_4: str | None = None
    section_4_5: str | None = None
    section_4_6: str | None = None
    section_4_7: bool | None = None        # BIM tools required
    section_4_9: str | None = None
    section_4_10: str | None = None
    section_4_11: str | None = None
    section_4_12: str | None = None
    section_4_13: str | None = None
    section_5_1: str | None = None
    section_5_2: bool | None = None        # precedes architectural contract
    section_5_3: str | None = None
    section_5_4: list[str] | None = None   # main CPV codes
    section_5_5: list[str] | None = None   # additional CPV codes
    section_5_6: str | None = None
    section_6_1: str | None = None
    section_6_2: int | None = None         # number of prizes
    section_6_3: int | None = None         # number of awarded entries
    section_6_4: float | None = None       # prize value
    section_6_4_currency: str | None = None
    section_6_5_1: float | None = None     # follow-on contract value
    section_6_5_2: str | None = None
    section_7_1: str | None = None
    section_7_2: bool | None = None        # environmental/social requirements
    section_7_3: bool | None = None        # professional qualifications required
    section_7_4: str | None = None
    section_7_5: str | None = None
    section_8_1: str | None = None
    section_8_2: str | None = None
    section_8_3: str | None = None
