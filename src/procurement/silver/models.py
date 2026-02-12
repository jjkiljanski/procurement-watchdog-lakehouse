"""Pydantic models for the BZP silver layer."""

from __future__ import annotations

from pydantic import BaseModel

from procurement.bronze.models import ContractorDto


class EvalCriterion(BaseModel):
    """Single bid evaluation criterion with its weight."""

    name: str
    weight: int


class HtmlExtracted(BaseModel):
    """Structured fields extracted from the notice HTML."""

    ulica: str | None = None
    kod_pocztowy: str | None = None
    nuts3_code: str | None = None
    nuts3_name: str | None = None
    opis: str | None = None
    kryteria_oceny: list[EvalCriterion] | None = None
    wartosc_umowy_pln: float | None = None


class BzpNoticeSilver(BaseModel):
    """Full notice with htmlBody replaced by parsed structured data."""

    objectId: str
    noticeType: str
    noticeNumber: str
    bzpNumber: str
    publicationDate: str
    isTenderAmountBelowEU: bool
    orderObject: str | None = None
    cpvCodes: list[str]
    clientType: str | None = None
    clientTypeName: str | None = None
    orderType: str | None = None
    tenderType: str | None = None
    submittingOffersDate: str | None = None
    procedureResult: str | None = None
    procedureResultParsed: list[str] | None = None
    organizationName: str
    organizationCity: str
    organizationProvince: str | None = None
    provinceName: str | None = None
    organizationCountry: str
    organizationNationalId: str
    organizationId: str
    tenderId: str | None = None
    contractors: list[ContractorDto] | None = None

    # Replaces htmlBody
    htmlExtracted: HtmlExtracted
