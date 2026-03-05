"""Pydantic section models for AgreementIntentionNotice.

Field types reflect the Silver-layer parsed values:
  - bool | None   : Tak/Nie boolean fields
  - float | None  : PLN monetary values
  - int | None    : integer counts
  - list[str] | None : CPV code lists
  - str | None    : all other fields (free text, identifiers, dates as ISO strings)
"""

from __future__ import annotations

from pydantic import BaseModel


class AgreementIntentionNoticeClientModel(BaseModel):
    section_1_2: str | None = None
    section_1_3: str | None = None
    section_1_4: str | None = None        # national ID digits (NIP/REGON/PESEL)
    section_1_4_type: str | None = None   # "NIP" / "REGON" / "PESEL" / "foreign"
    section_1_5_1: str | None = None
    section_1_5_2: str | None = None
    section_1_5_3: str | None = None
    section_1_5_4: str | None = None
    section_1_5_5: str | None = None
    section_1_5_6_code: str | None = None  # e.g. "PL21A"
    section_1_5_6_name: str | None = None  # e.g. "Oświęcimski"
    section_1_5_7: str | None = None
    section_1_5_8: str | None = None
    section_1_5_9: str | None = None
    section_1_5_10: str | None = None


class AgreementIntentionNoticeCoreModel(BaseModel):
    section_1_1: str | None = None
    section_1_6: str | None = None
    section_1_7: str | None = None
    section_1_9_1: str | None = None
    section_1_9_2: str | None = None        # national ID digits
    section_1_9_2_type: str | None = None   # "NIP" / "REGON" / "PESEL" / "foreign"
    section_1_9_3: str | None = None
    section_1_9_4: str | None = None
    section_1_9_5: str | None = None
    section_1_9_6: str | None = None
    section_1_9_7: str | None = None
    section_1_9_8_code: str | None = None   # e.g. "PL22B"
    section_1_9_8_name: str | None = None   # e.g. "Sosnowiecki"
    section_1_9_9: str | None = None
    section_1_9_10: str | None = None
    section_1_9_11: str | None = None
    section_1_9_12: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_2_3: str | None = None
    section_2_4: str | None = None
    section_2_5: str | None = None          # ISO date string "YYYY-MM-DD"
    section_2_6: bool | None = None         # in procurement plan?
    section_2_7: str | None = None
    section_2_8: str | None = None
    section_2_9: bool | None = None         # social / special services?
    section_2_10: bool | None = None        # EU co-financing?
    section_2_11: str | None = None
    section_3_1: bool | None = None         # market consultations carried out?
    section_3_2: str | None = None
    section_3_3: str | None = None
    section_3_4: bool | None = None         # divided into separate lots?
    section_3_5: float | None = None        # total contract value (PLN)
    section_3_5_1: float | None = None      # this procedure's value excl. VAT (PLN)
    section_3_6: bool | None = None         # partial offers allowed?
    section_3_7: int | None = None          # number of lots
    section_4_1: str | None = None
    section_4_2: str | None = None
    section_4_3: str | None = None


class AgreementIntentionNoticePartModel(BaseModel):
    section_3_8: str | None = None
    section_3_9: float | None = None        # part value (PLN)
    section_3_10: list[str] | None = None   # main CPV code(s)
    section_3_11: list[str] | None = None   # additional CPV codes
    section_5_1_1: str | None = None
    section_5_1_2: str | None = None
    section_5_1_3: str | None = None
    section_5_1_4: str | None = None
    section_5_1_5: str | None = None
    section_5_1_6: str | None = None
