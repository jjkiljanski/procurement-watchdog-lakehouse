"""Pydantic models for the BZP bronze layer."""

from __future__ import annotations

import hashlib
import json
from typing import Literal

from pydantic import BaseModel, field_validator

NOTICE_TYPES = (
    "ContractNotice",
    "AgreementIntentionNotice",
    "TenderResultNotice",
    "CompetitionNotice",
    "CompetitionResultNotice",
    "NoticeUpdateNotice",
    "AgreementUpdateNotice",
    "ContractPerformingNotice",
    "CircumstancesFulfillmentNotice",
    "SmallContractNotice",
    "ConcessionNotice",
    "ConcessionIntentionAgreementNotice",
    "NoticeUpdateConcession",
    "ConcessionAgreementNotice",
    "ConcessionUpdateAgreementNotice",
)


class ContractorDto(BaseModel):
    contractorName: str | None = None
    contractorCity: str | None = None
    contractorProvince: str | None = None
    contractorCountry: str | None = None
    contractorNationalId: str | None = None


class BzpNoticeBronze(BaseModel):
    """Validated BZP notice — keeps the full htmlBody for downstream use."""

    objectId: str
    noticeType: str
    noticeNumber: str
    bzpNumber: str
    publicationDate: str
    isTenderAmountBelowEU: bool
    orderObject: str | None = None
    cpvCode: str
    htmlBody: str
    clientType: str | None = None
    orderType: str | None = None
    tenderType: str | None = None
    submittingOffersDate: str | None = None
    procedureResult: str | None = None
    organizationName: str
    organizationCity: str
    organizationProvince: str | None = None
    organizationCountry: str
    organizationNationalId: str
    organizationId: str
    tenderId: str | None = None
    contractors: list[ContractorDto] | None = None

    @field_validator("htmlBody")
    @classmethod
    def html_not_truncated(cls, v: str) -> str:
        if not v.rstrip().endswith("</html>"):
            raise ValueError("htmlBody appears truncated (missing </html>)")
        return v

    @field_validator("noticeType")
    @classmethod
    def known_notice_type(cls, v: str) -> str:
        if v not in NOTICE_TYPES:
            raise ValueError(f"Unknown noticeType: {v}")
        return v


class BzpNoticeBronzeOut(BaseModel):
    """Output model — htmlBody replaced with its SHA-256 hash."""

    objectId: str
    noticeType: str
    noticeNumber: str
    bzpNumber: str
    publicationDate: str
    isTenderAmountBelowEU: bool
    orderObject: str | None = None
    cpvCode: str
    htmlBodySha256: str
    clientType: str | None = None
    orderType: str | None = None
    tenderType: str | None = None
    submittingOffersDate: str | None = None
    procedureResult: str | None = None
    organizationName: str
    organizationCity: str
    organizationProvince: str | None = None
    organizationCountry: str
    organizationNationalId: str
    organizationId: str
    tenderId: str | None = None
    contractors: list[ContractorDto] | None = None


def to_bronze_output(notice: BzpNoticeBronze) -> BzpNoticeBronzeOut:
    """Replace htmlBody with its SHA-256 hash."""
    data = notice.model_dump()
    html = data.pop("htmlBody")
    data["htmlBodySha256"] = hashlib.sha256(html.encode("utf-8")).hexdigest()
    return BzpNoticeBronzeOut(**data)


def notice_record_hash(notice: BzpNoticeBronze) -> str:
    """Stable hash of the validated notice payload used for change detection."""
    payload = notice.model_dump(mode="json", exclude_none=False)
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
