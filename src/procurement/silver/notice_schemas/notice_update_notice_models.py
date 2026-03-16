"""Pydantic section models for NoticeUpdateNotice.

Auto-generated from notice_update_notice_profile.json.
All field types are str | None; richer types will be added in Gold.
"""

from __future__ import annotations

from pydantic import BaseModel


class NoticeUpdateNoticeCoreModel(BaseModel):
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
    section_1_6: str | None = None
    section_2_1: str | None = None
    section_2_2: str | None = None
    section_3_1: str | None = None
    section_3_2: str | None = None
    section_3_3: str | None = None

class NoticeUpdateNoticePartPartModel(BaseModel):
    section_3_4_1: str | None = None

class NoticeUpdateNoticePartModel(BaseModel):
    section_3_4: str | None = None
    part_items: list[NoticeUpdateNoticePartPartModel] | None = None
