"""Pydantic models for the BZP silver layer."""

from __future__ import annotations

from pydantic import BaseModel

from procurement.bronze.models import ContractorDto


class EvalCriterion(BaseModel):
    """Single bid evaluation criterion with its weight."""

    name: str
    weight: int


class ExtractedValues(BaseModel):
    """Monetary values extracted from the notice HTML.

    Different fields are populated depending on the notice type:
    - ContractPerformingNotice: contract_value, total_paid
    - TenderResultNotice: contract_value, estimated_value, lowest_bid, highest_bid, winning_bid
    - ContractNotice: estimated_value
    - AgreementUpdateNotice: contract_value
    - AgreementIntentionNotice: estimated_value
    - SmallContractNotice: contract_value
    """

    contract_value: float | None = None
    total_paid: float | None = None
    estimated_value: float | None = None
    lowest_bid: float | None = None
    highest_bid: float | None = None
    winning_bid: float | None = None
    currency: str = "PLN"


class TenderResultLot(BaseModel):
    """Per-lot values from TenderResultNotice."""

    lot_id: str | None = None
    contract_value: float | None = None
    lowest_bid: float | None = None
    highest_bid: float | None = None
    winning_bid: float | None = None
    estimated_value: float | None = None
    winner: str | None = None


class TenderResultEnrichment(BaseModel):
    """Contractor enrichment fields from TenderResultNotice SEKCJA VII.

    These fields are only available in HTML, not in the JSON contractors field.
    """

    joint_bidders: bool | None = None  # 7.1 Tak/Nie
    contractor_size: str | None = None  # 7.2 enterprise size string


class ContractExecution(BaseModel):
    """Contract execution details from ContractPerformingNotice."""

    contract_date: str | None = None  # 4.1 YYYY-MM-DD
    execution_period: str | None = None  # 4.2 free text (e.g. "56 dni")
    contract_executed: bool | None = None  # 5.1 Tak/Nie
    execution_end_date: str | None = None  # 5.2 YYYY-MM-DD
    executed_on_time: bool | None = None  # 5.3 Tak/Nie
    num_changes: int | None = None  # 5.4.1 integer
    executed_properly: bool | None = None  # 5.6 Tak/Nie


class ChangeEntry(BaseModel):
    """Single section change within a NoticeUpdateNotice."""

    changed_section: str | None = None  # 3.4 section identifier
    change_description: str | None = None  # 3.4.1 before/after text


class NoticeChange(BaseModel):
    """Notice amendment details from NoticeUpdateNotice SEKCJA III."""

    changed_notice_number: str | None = None  # 3.2 BZP notice number
    changed_notice_version: str | None = None  # 3.3 version identifier
    changes: list[ChangeEntry] | None = None  # repeating 3.4 + 3.4.1 pairs


class HtmlExtracted(BaseModel):
    """Structured fields extracted from the notice HTML."""

    ulica: str | None = None
    kod_pocztowy: str | None = None
    nuts3_code: str | None = None
    nuts3_name: str | None = None
    opis: str | None = None
    kryteria_oceny: list[EvalCriterion] | None = None
    values: ExtractedValues | None = None
    lots: list[TenderResultLot] | None = None
    tender_result_enrichment: TenderResultEnrichment | None = None
    contract_execution: ContractExecution | None = None
    notice_change: NoticeChange | None = None


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
    caseId: str | None = None
    noticeStage: str | None = None
    hasTenderResult: bool | None = None
    hasContractExecution: bool | None = None

    biddingWindowDays: int | None = None
    numCriteria: int | None = None
    priceWeight: int | None = None
    nonPriceWeightSum: int | None = None

    deadlineChanged: bool | None = None
    criteriaChanged: bool | None = None
    scopeChanged: bool | None = None

    executionDurationDays: int | None = None
    paidRatio: float | None = None
    executionDelayed: bool | None = None
    executionRiskFlag: bool | None = None

    organizationNameNormalized: str | None = None
    contractorNameNormalized: list[str] | None = None

    # Replaces htmlBody
    htmlExtracted: HtmlExtracted
