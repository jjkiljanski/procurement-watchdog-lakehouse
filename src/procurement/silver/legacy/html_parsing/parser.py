"""Extract structured fields from BZP notice HTML.

NOTE: This file is being decomposed. All extraction functions have been moved to:
  - silver/html_value_parsers/common_values.py  (contractor ID utilities)
  - gold/notice_types/common.py                 (_extract_address, _extract_ogloszenie_dotyczy)
  - gold/notice_types/contract_notice.py        (ContractNotice-specific)
  - gold/notice_types/tender_result_notice.py   (TenderResultNotice-specific)
  - gold/notice_types/contract_performing_notice.py
  - gold/notice_types/agreement_intention_notice.py
  - gold/notice_types/agreement_update_notice.py
  - gold/notice_types/small_contract_notice.py
  - gold/notice_types/competition_notice.py
  - gold/notice_types/notice_update_notice.py

Only _extract_address (used by spark_transforms) and parse_html (legacy entry point)
remain here temporarily. parse_html will be removed once all callers are migrated.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.html_parsing.utils import (
    _find_h3,
    _find_h3_by_label,
    _span_value,
)
# Re-exported from their new homes for backward compatibility with existing callers.
# Remove these once all callers have been updated to import directly.
from procurement.silver.field_parsers.common import (
    _parse_pln_value,
    _parse_tak_nie,
    classify_contractor_id_for_notice,
    normalize_tender_result_contractors,
    parse_cpv_codes,
)
from procurement.gold.notice_types.agreement_intention_notice import (
    parse_html_agreement_intention_light,
)
from procurement.gold.notice_types.competition_notice import (
    parse_html_competition_light,
)
from procurement.gold.notice_types.contract_performing_notice import (
    parse_html_contract_performing_light,
)
from procurement.silver.legacy.models import (
    ExtractedValues,
    HtmlExtracted,
)
from procurement.silver.section_pipeline.html_extractor import (
    build_notice_sections_model as _build_notice_sections_model,
)


_ADDRESS_FIELD_NUMS_BY_TYPE: dict[str | None, dict[str, tuple[str, ...]]] = {
    None: {
        # Some templates use section 1.5.x, others 1.4.x.
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ContractNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "TenderResultNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "NoticeUpdateNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementUpdateNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementIntentionNotice": {
        "ulica": ("1.4.1.", "1.5.1.", "5.1.2."),
        "kod_pocztowy": ("1.4.3.", "1.5.3.", "5.1.4."),
        "nuts3": ("1.4.6.", "1.5.6."),
    },
    "ContractPerformingNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CompetitionNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CompetitionResultNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CircumstancesFulfillmentNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "SmallContractNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionIntentionAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "NoticeUpdateConcession": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionUpdateAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
}


def _extract_address(soup: BeautifulSoup, notice_type: str | None = None) -> dict:
    """Extract street, postal code, NUTS3 with noticeType-aware + label fallback logic."""
    mapping = _ADDRESS_FIELD_NUMS_BY_TYPE.get(notice_type) or _ADDRESS_FIELD_NUMS_BY_TYPE[None]

    def _first(field_nums: tuple[str, ...]) -> str | None:
        for fn in field_nums:
            value = _span_value(_find_h3(soup, fn))
            if value:
                return value
        return None

    ulica = _first(mapping["ulica"])
    if not ulica:
        ulica = _span_value(_find_h3_by_label(soup, ["ulica"]))

    kod_pocztowy = _first(mapping["kod_pocztowy"])
    if not kod_pocztowy:
        kod_pocztowy = _span_value(_find_h3_by_label(soup, ["kod", "poczt"]))

    nuts3_code = None
    nuts3_name = None
    nuts3_raw = _first(mapping["nuts3"])
    if not nuts3_raw:
        nuts3_raw = _span_value(_find_h3_by_label(soup, ["nuts", "3"]))
    if nuts3_raw and " - " in nuts3_raw:
        nuts3_code, nuts3_name = nuts3_raw.split(" - ", 1)
        nuts3_code = nuts3_code.strip()
        nuts3_name = nuts3_name.strip()

    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "nuts3_code": nuts3_code,
        "nuts3_name": nuts3_name,
    }


def parse_html(
    html: str,
    notice_type: str | None = None,
    procedure_result: str | None = None,
) -> HtmlExtracted:
    """Parse a single BZP notice HTML into extracted fields.

    TODO: This function is being migrated to gold/notice_types/. It will fail
    at runtime for most notice types. Use the gold-layer functions instead.
    """
    soup = BeautifulSoup(html, "lxml")

    sections_model = _build_notice_sections_model(soup, notice_type=notice_type)
    core_model = sections_model if isinstance(sections_model, ContractNoticeCoreRaw) else None
    ogloszenie_dotyczy = (
        core_model.cn_section_2_1
        if core_model and core_model.cn_section_2_1
        else _extract_ogloszenie_dotyczy(soup)  # noqa: F821 — defined in gold/notice_types/common.py
    )
    address = _extract_address(soup, notice_type=notice_type)
    opis = None
    kryteria = None
    criteria_aspects_4310 = None
    criteria_aspects_4310_flag = None
    if notice_type in (None, "ContractNotice"):
        opis = _extract_description(soup)  # noqa: F821 — defined in gold/notice_types/contract_notice.py
        kryteria = _extract_criteria(soup)  # noqa: F821
        criteria_aspects_4310, criteria_aspects_4310_flag = _extract_criteria_aspects_4310(soup)  # noqa: F821
    contract_notice_parts = None
    contract_notice_parts_sections = None
    lots = _extract_tender_result_lots(soup) if notice_type == "TenderResultNotice" else None  # noqa: F821
    tender_result_parts = (
        _extract_tender_result_parts(soup) if notice_type == "TenderResultNotice" else None  # noqa: F821
    )
    if notice_type == "TenderResultNotice" and not lots:
        lots = _extract_status_lots_from_procedure_result(procedure_result)  # noqa: F821
    ai_street_512 = None
    value_estimated_procurement_ai_35 = None
    ai_prior_market_consultation_31 = None
    cpn_contractor_names_431 = None
    contractor_id_raw = None
    contractor_id_parsed = None
    contractor_id_type = None
    cpn_contractor_cities_434 = None
    cpn_contractor_provinces_436 = None
    cpn_contractor_countries_437 = None
    value_contract_reported_execution_44 = None
    value_paid_total_55 = None
    comp_num_awarded_63 = None
    value_competition_prizes_64 = None
    value_competition_followon_order_651 = None
    comp_requirements_72 = None
    comp_submission_deadline = None
    comp_result_approval_date_53 = None
    cn_partial_offers_allowed_418 = None
    cn_offers_scope_4110 = None
    if notice_type == "AgreementIntentionNotice":
        (
            ai_street_512,
            value_estimated_procurement_ai_35,
            ai_prior_market_consultation_31,
        ) = _extract_agreement_intention_fields(soup)  # noqa: F821
    if notice_type == "ContractPerformingNotice":
        (
            cpn_contractor_names_431,
            contractor_id_raw,
            contractor_id_parsed,
            contractor_id_type,
            cpn_contractor_cities_434,
            cpn_contractor_provinces_436,
            cpn_contractor_countries_437,
            value_contract_reported_execution_44,
        ) = _extract_contract_performing_party_fields(soup)  # noqa: F821
        value_paid_total_55 = _parse_pln_value(_span_value(_find_h3(soup, "5.5.")))
    if notice_type == "CompetitionNotice":
        (
            comp_num_awarded_63,
            value_competition_prizes_64,
            value_competition_followon_order_651,
            comp_requirements_72,
            comp_submission_deadline,
        ) = _extract_competition_notice_fields(soup)  # noqa: F821
    if notice_type == "CompetitionResultNotice":
        comp_result_approval_date_53 = _extract_competition_result_fields(soup)  # noqa: F821
    if notice_type == "ContractNotice":
        raw_418 = core_model.cn_section_4_1_8 if core_model else None
        raw_4110 = core_model.cn_section_4_1_10 if core_model else None
        cn_partial_offers_allowed_418 = (
            _parse_tak_nie(raw_418) if raw_418 is not None else _extract_cn_partial_offers_allowed_418(soup)  # noqa: F821
        )
        cn_offers_scope_4110 = _map_cn_offers_scope(raw_4110) if raw_4110 is not None else _extract_cn_offers_scope_4110(soup)  # noqa: F821

    contract_notice_core_sections = None
    if notice_type == "ContractNotice" and core_model is not None:
        _raw = core_model.model_dump(exclude_none=True)
        contract_notice_core_sections = {str(k): str(v) for k, v in _raw.items()}

    # Type-aware value extraction — moved to gold/notice_types/<type>.py
    values = None
    if notice_type is None:
        # Legacy fallback: extract only field 8.2
        val = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        if val is not None:
            values = ExtractedValues(value_awarded_contract=val)

    details: dict[str, object] = {}

    return HtmlExtracted(
        ogloszenie_dotyczy=ogloszenie_dotyczy,
        **address,
        opis=opis,
        kryteria_oceny=kryteria,
        criteria_aspects_4310=criteria_aspects_4310,
        criteria_aspects_4310_flag=criteria_aspects_4310_flag,
        contract_notice_parts=contract_notice_parts,
        contract_notice_core_sections=contract_notice_core_sections,
        contract_notice_parts_sections=contract_notice_parts_sections,
        values=values,
        lots=lots,
        tender_result_parts=tender_result_parts,
        ai_street_512=ai_street_512,
        value_estimated_procurement_ai_35=value_estimated_procurement_ai_35,
        ai_prior_market_consultation_31=ai_prior_market_consultation_31,
        cpn_contractor_names_431=cpn_contractor_names_431,
        contractor_id_raw=contractor_id_raw,
        contractor_id_parsed=contractor_id_parsed,
        contractor_id_type=contractor_id_type,
        cpn_contractor_cities_434=cpn_contractor_cities_434,
        cpn_contractor_provinces_436=cpn_contractor_provinces_436,
        cpn_contractor_countries_437=cpn_contractor_countries_437,
        value_contract_reported_execution_44=value_contract_reported_execution_44,
        value_paid_total_55=value_paid_total_55,
        comp_num_awarded_63=comp_num_awarded_63,
        value_competition_prizes_64=value_competition_prizes_64,
        value_competition_followon_order_651=value_competition_followon_order_651,
        comp_requirements_72=comp_requirements_72,
        comp_submission_deadline=comp_submission_deadline,
        comp_result_approval_date_53=comp_result_approval_date_53,
        cn_partial_offers_allowed_418=cn_partial_offers_allowed_418,
        cn_offers_scope_4110=cn_offers_scope_4110,
        **details,
    )
