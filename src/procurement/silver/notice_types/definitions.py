"""NoticeType-specific Silver processing definitions."""

from __future__ import annotations

import re
from dataclasses import dataclass


BASE_SPECIFIC_COLUMNS = [
    "objectId",
    "noticeType",
    "noticeNumber",
    "bzpNumber",
    "publicationDate",
    "publicationDateDay",
    "tenderId",
    "caseId",
    "cpvCode",
    "cpvCodes",
    "contractors",
    "numCriteria",
    "priceWeight",
    "nonPriceWeightSum",
    "contractorNameNormalized",
    "htmlExtracted",
]

AGREEMENT_INTENTION_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "ai_street_512",
    "value_estimated_procurement_ai_35",
    "ai_prior_market_consultation_31",
]

AGREEMENT_UPDATE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
]

CIRCUMSTANCES_FULFILLMENT_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
]

SMALL_CONTRACT_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "contractors",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
]

COMPETITION_NOTICE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "contractors",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "comp_num_awarded_63",
    "value_competition_prizes_64",
    "value_competition_followon_order_651",
    "comp_requirements_72",
    "comp_submission_deadline",
]

COMPETITION_RESULT_NOTICE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "contractors",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "comp_result_approval_date_53",
]

CONCESSION_NOTICE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "contractors",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "submittingOffersDate",
]

CONTRACT_NOTICE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c not in {"contractors", "contractorNameNormalized", "htmlExtracted"}
] + [
    "cn_notice_concerns",
    "contract_planned_execution_date",
    "contract_planned_execution_date_parsed",
    "criteria",
    "cn_award_criteria_by_part",
    "cn_criteria_aspects_4310",
    "cn_criteria_aspects_4310_flag",
    "cn_description_by_part",
    "cpvMainCode",
    "cpvSecondaryCode",
    "submittingOffersDate",
]

CONTRACT_PERFORMING_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "cpn_contractor_names_431",
    "contractor_id_raw",
    "contractor_id_parsed",
    "contractor_id_type",
    "cpn_contractor_cities_434",
    "cpn_contractor_provinces_436",
    "cpn_contractor_countries_437",
    "cpn_contract_date_41",
    "cpn_contract_planned_execution_date_raw",
    "cpn_contract_planned_execution_date_parsed",
    "cpn_execution_end_date_52",
    "executed_in_time",
    "proper_execution",
    "value_contract_reported_execution_44",
    "value_paid_total_55",
]

TENDER_RESULT_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "procedureResult",
    "procedureResultParsed",
    "trn_notice_concerns",
    "trn_value_bid_lowest",
    "trn_value_bid_highest",
    "trn_value_winning_offer",
    "trn_parts",
]

NOTICE_UPDATE_SPECIFIC_COLUMNS = [
    c
    for c in BASE_SPECIFIC_COLUMNS
    if c
    not in {
        "cpvCodes",
        "procedureResult",
        "procedureResultParsed",
        "contractors",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
        "contractorNameNormalized",
        "htmlExtracted",
    }
] + [
    "changed_notice_number",
    "changed_notice_version",
    "changes",
]


@dataclass(frozen=True)
class NoticeTypeDefinition:
    notice_type: str | None
    specific_columns: tuple[str, ...]
    html_extracted_fields: tuple[str, ...] = ()


TRN_ONLY_COLUMNS = (
    "procedureResult",
    "procedureResultParsed",
)


HTML_FIELDS_COMMON_ADDRESS = (
    "nuts3_code",
    "nuts3_name",
)


HTML_FIELDS_CONTRACT_NOTICE = (
    "ogloszenie_dotyczy",
    "opis",
    "kryteria_oceny",
    "criteria_aspects_4310",
    "criteria_aspects_4310_flag",
    "contract_notice_parts",
    "values",
    *HTML_FIELDS_COMMON_ADDRESS,
)


HTML_FIELDS_TENDER_RESULT = (
    "ogloszenie_dotyczy",
    "values",
    "lots",
    "tender_result_enrichment",
    *HTML_FIELDS_COMMON_ADDRESS,
)


HTML_FIELDS_EXECUTION = (
    "values",
    "contract_execution",
    *HTML_FIELDS_COMMON_ADDRESS,
)


HTML_FIELDS_UPDATE = (
    "notice_change",
    *HTML_FIELDS_COMMON_ADDRESS,
)


HTML_FIELDS_AGREEMENT = (
    "values",
    *HTML_FIELDS_COMMON_ADDRESS,
)


_NOTICE_TYPE_DEFINITIONS: dict[str | None, NoticeTypeDefinition] = {
    None: NoticeTypeDefinition(
        notice_type=None,
        specific_columns=tuple(BASE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "ContractNotice": NoticeTypeDefinition(
        notice_type="ContractNotice",
        specific_columns=tuple(CONTRACT_NOTICE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "TenderResultNotice": NoticeTypeDefinition(
        notice_type="TenderResultNotice",
        specific_columns=tuple(TENDER_RESULT_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "ContractPerformingNotice": NoticeTypeDefinition(
        notice_type="ContractPerformingNotice",
        specific_columns=tuple(CONTRACT_PERFORMING_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "NoticeUpdateNotice": NoticeTypeDefinition(
        notice_type="NoticeUpdateNotice",
        specific_columns=tuple(NOTICE_UPDATE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "AgreementIntentionNotice": NoticeTypeDefinition(
        notice_type="AgreementIntentionNotice",
        specific_columns=tuple(AGREEMENT_INTENTION_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "AgreementNotice": NoticeTypeDefinition(
        notice_type="AgreementNotice",
        specific_columns=tuple(BASE_SPECIFIC_COLUMNS),
        html_extracted_fields=HTML_FIELDS_AGREEMENT,
    ),
    "AgreementUpdateNotice": NoticeTypeDefinition(
        notice_type="AgreementUpdateNotice",
        specific_columns=tuple(AGREEMENT_UPDATE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "CircumstancesFulfillmentNotice": NoticeTypeDefinition(
        notice_type="CircumstancesFulfillmentNotice",
        specific_columns=tuple(CIRCUMSTANCES_FULFILLMENT_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "CompetitionNotice": NoticeTypeDefinition(
        notice_type="CompetitionNotice",
        specific_columns=tuple(COMPETITION_NOTICE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "CompetitionResultNotice": NoticeTypeDefinition(
        notice_type="CompetitionResultNotice",
        specific_columns=tuple(COMPETITION_RESULT_NOTICE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "ConcessionNotice": NoticeTypeDefinition(
        notice_type="ConcessionNotice",
        specific_columns=tuple(CONCESSION_NOTICE_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
    "SmallContractNotice": NoticeTypeDefinition(
        notice_type="SmallContractNotice",
        specific_columns=tuple(SMALL_CONTRACT_SPECIFIC_COLUMNS),
        html_extracted_fields=tuple(),
    ),
}

NOTICE_TYPE_SPECIFIC_COLUMNS = {
    k: list(v.specific_columns) for k, v in _NOTICE_TYPE_DEFINITIONS.items()
}

NOTICE_TYPE_HTML_EXTRACTED_FIELDS = {
    k: list(v.html_extracted_fields) for k, v in _NOTICE_TYPE_DEFINITIONS.items()
}


def normalized_notice_type_token(notice_type: str | None) -> str:
    if notice_type is None:
        return "__NULL__"
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(notice_type)).strip("_")
    return normalized or "__EMPTY__"


def specific_columns_for_notice_type(notice_type: str | None) -> list[str]:
    definition = _NOTICE_TYPE_DEFINITIONS.get(notice_type)
    if definition is None:
        return list(BASE_SPECIFIC_COLUMNS)
    return list(definition.specific_columns)


def html_extracted_fields_for_notice_type(notice_type: str | None) -> list[str]:
    definition = _NOTICE_TYPE_DEFINITIONS.get(notice_type)
    if definition is None:
        return []
    return list(definition.html_extracted_fields)
