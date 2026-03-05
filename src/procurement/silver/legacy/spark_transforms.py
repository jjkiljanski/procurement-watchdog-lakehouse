"""PySpark transforms for the BZP silver layer."""

from __future__ import annotations

import logging
import re
import unicodedata
from calendar import monthrange
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    datediff,
    expr,
    lit,
    posexplode_outer,
    size,
    split,
    to_date,
    to_timestamp,
    udf,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from procurement.dictionaries import client_type_names, order_type_names, province_names
from procurement.silver.legacy.html_parsing.parser import (
    _extract_address,
    parse_html,
)
from procurement.silver.section_value_parsers.common import (
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

log = logging.getLogger(__name__)

EVAL_CRITERION_SCHEMA = StructType(
    [
        StructField("no", IntegerType()),
        StructField("str", StringType()),
        StructField("weight", IntegerType()),
    ]
)

CONTRACT_NOTICE_PART_SCHEMA = StructType(
    [
        StructField("part_id", StringType()),
        StructField("opis", StringType()),
        StructField("kryteria_oceny", ArrayType(EVAL_CRITERION_SCHEMA)),
        StructField("mainCPV", StringType()),
        StructField("secondaryCPV", ArrayType(StringType())),
        StructField("contract_planned_execution_date", StringType()),
        StructField("criteria_aspects_4310", StringType()),
        StructField("criteria_aspects_4310_flag", BooleanType()),
    ]
)

EXTRACTED_VALUES_SCHEMA = StructType(
    [
        StructField("value_awarded_contract", DoubleType()),
        StructField("value_contract_reported_execution", DoubleType()),
        StructField("value_paid_total", DoubleType()),
        StructField("value_estimated_procurement", DoubleType()),
        StructField("value_bid_lowest", DoubleType()),
        StructField("value_bid_highest", DoubleType()),
        StructField("value_winning_offer", DoubleType()),
        StructField("currency", StringType()),
    ]
)

TENDER_RESULT_ENRICHMENT_SCHEMA = StructType(
    [
        StructField("joint_bidders", BooleanType()),
        StructField("contractor_size", StringType()),
    ]
)

TENDER_RESULT_LOT_SCHEMA = StructType(
    [
        StructField("lot_id", StringType()),
        StructField("value_awarded_contract", DoubleType()),
        StructField("value_bid_lowest", DoubleType()),
        StructField("value_bid_highest", DoubleType()),
        StructField("value_winning_offer", DoubleType()),
        StructField("value_estimated_procurement", DoubleType()),
        StructField("winner", StringType()),
    ]
)

TENDER_RESULT_PART_SCHEMA = StructType(
    [
        StructField("part_id", StringType()),
        StructField("opis", StringType()),
        StructField("mainCPV", StringType()),
        StructField("secondaryCPV", ArrayType(StringType())),
        StructField("value_estimated_procurement", DoubleType()),
    ]
)

CONTRACT_EXECUTION_SCHEMA = StructType(
    [
        StructField("contract_date", StringType()),
        StructField("execution_period", StringType()),
        StructField("contract_executed", BooleanType()),
        StructField("execution_end_date", StringType()),
        StructField("executed_on_time", BooleanType()),
        StructField("num_changes", IntegerType()),
        StructField("executed_properly", BooleanType()),
    ]
)

CHANGE_ENTRY_SCHEMA = StructType(
    [
        StructField("changed_section", StringType()),
        StructField("change_description", StringType()),
    ]
)

NOTICE_CHANGE_SCHEMA = StructType(
    [
        StructField("changed_notice_number", StringType()),
        StructField("changed_notice_version", StringType()),
        StructField("changes", ArrayType(CHANGE_ENTRY_SCHEMA)),
    ]
)

HTML_EXTRACTED_SCHEMA = StructType(
    [
        StructField("ogloszenie_dotyczy", StringType()),
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("nuts3_code", StringType()),
        StructField("nuts3_name", StringType()),
        StructField("opis", StringType()),
        StructField("kryteria_oceny", ArrayType(EVAL_CRITERION_SCHEMA)),
        StructField("criteria_aspects_4310", StringType()),
        StructField("criteria_aspects_4310_flag", BooleanType()),
        StructField("contract_notice_parts", ArrayType(CONTRACT_NOTICE_PART_SCHEMA)),
        StructField("contract_notice_core_sections", MapType(StringType(), StringType())),
        StructField(
            "contract_notice_parts_sections",
            ArrayType(MapType(StringType(), StringType())),
        ),
        StructField("values", EXTRACTED_VALUES_SCHEMA),
        StructField("lots", ArrayType(TENDER_RESULT_LOT_SCHEMA)),
        StructField("tender_result_parts", ArrayType(TENDER_RESULT_PART_SCHEMA)),
        StructField("tender_result_enrichment", TENDER_RESULT_ENRICHMENT_SCHEMA),
        StructField("contract_execution", CONTRACT_EXECUTION_SCHEMA),
        StructField("notice_change", NOTICE_CHANGE_SCHEMA),
        StructField("ai_street_512", StringType()),
        StructField("value_estimated_procurement_ai_35", DoubleType()),
        StructField("ai_prior_market_consultation_31", StringType()),
        StructField("cpn_contractor_names_431", ArrayType(StringType())),
        StructField("contractor_id_raw", ArrayType(StringType())),
        StructField("contractor_id_parsed", ArrayType(StringType())),
        StructField("contractor_id_type", ArrayType(StringType())),
        StructField("cpn_contractor_cities_434", ArrayType(StringType())),
        StructField("cpn_contractor_provinces_436", ArrayType(StringType())),
        StructField("cpn_contractor_countries_437", ArrayType(StringType())),
        StructField("value_contract_reported_execution_44", DoubleType()),
        StructField("value_paid_total_55", DoubleType()),
        StructField("comp_num_awarded_63", IntegerType()),
        StructField("value_competition_prizes_64", DoubleType()),
        StructField("value_competition_followon_order_651", DoubleType()),
        StructField("comp_requirements_72", StringType()),
        StructField("comp_submission_deadline", StringType()),
        StructField("comp_result_approval_date_53", StringType()),
        StructField("cn_partial_offers_allowed_418", BooleanType()),
        StructField("cn_offers_scope_4110", StringType()),
    ]
)

HTML_ADDRESS_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("nuts3_code", StringType()),
        StructField("nuts3_name", StringType()),
    ]
)

HTML_AI_LIGHT_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("ai_street_512", StringType()),
        StructField("value_estimated_procurement_ai_35", DoubleType()),
        StructField("ai_prior_market_consultation_31", StringType()),
    ]
)

HTML_COMP_LIGHT_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("comp_num_awarded_63", IntegerType()),
        StructField("value_competition_prizes_64", DoubleType()),
        StructField("value_competition_followon_order_651", DoubleType()),
        StructField("comp_requirements_72", StringType()),
        StructField("comp_submission_deadline", StringType()),
        StructField("comp_result_approval_date_53", StringType()),
    ]
)

HTML_CPN_LIGHT_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("cpn_contractor_names_431", ArrayType(StringType())),
        StructField("contractor_id_raw", ArrayType(StringType())),
        StructField("contractor_id_parsed", ArrayType(StringType())),
        StructField("contractor_id_type", ArrayType(StringType())),
        StructField("cpn_contractor_cities_434", ArrayType(StringType())),
        StructField("cpn_contractor_provinces_436", ArrayType(StringType())),
        StructField("cpn_contractor_countries_437", ArrayType(StringType())),
        StructField("cpn_contract_date_41", StringType()),
        StructField("cpn_contract_planned_execution_date_raw", StringType()),
        StructField("cpn_execution_end_date_52", StringType()),
        StructField("executed_in_time", BooleanType()),
        StructField("proper_execution", BooleanType()),
        StructField("value_contract_reported_execution_44", DoubleType()),
        StructField("value_paid_total_55", DoubleType()),
    ]
)


def _parse_html_safe(
    html: str | None,
    notice_type: str | None,
    procedure_result: str | None,
) -> dict | None:
    if not html:
        return None
    try:
        return parse_html(
            html,
            notice_type=notice_type,
            procedure_result=procedure_result,
        ).model_dump()
    except Exception:
        log.warning("Failed to parse HTML (len=%d)", len(html), exc_info=True)
        return None


def _parse_cpv_safe(cpv_raw: str | None) -> list[str]:
    if not cpv_raw:
        return []
    return parse_cpv_codes(cpv_raw)


def _parse_html_address_safe(html: str | None, notice_type: str | None) -> dict | None:
    if not html:
        return None
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        return _extract_address(soup, notice_type=notice_type)
    except Exception:
        log.warning("Failed to parse HTML address (len=%d)", len(html), exc_info=True)
        return None


def _parse_html_ai_light_safe(html: str | None) -> dict | None:
    if not html:
        return None
    try:
        return parse_html_agreement_intention_light(html)
    except Exception:
        log.warning("Failed to parse AI light HTML (len=%d)", len(html), exc_info=True)
        return None


def _parse_html_comp_light_safe(html: str | None) -> dict | None:
    if not html:
        return None
    try:
        return parse_html_competition_light(html)
    except Exception:
        log.warning("Failed to parse Competition light HTML (len=%d)", len(html), exc_info=True)
        return None


def _parse_html_cpn_light_safe(html: str | None) -> dict | None:
    if not html:
        return None
    try:
        return parse_html_contract_performing_light(html)
    except Exception:
        log.warning("Failed to parse CPN light HTML (len=%d)", len(html), exc_info=True)
        return None


def _parse_organization_national_id_safe(
    organization_country: str | None,
    organization_national_id: str | None,
) -> str | None:
    try:
        _, parsed, _ = classify_contractor_id_for_notice(organization_country, organization_national_id)
        return parsed
    except Exception:
        return None


CONTRACTOR_TRN_SCHEMA = StructType(
    [
        StructField("contractorCity", StringType()),
        StructField("contractorCountry", StringType()),
        StructField("contractorName", StringType()),
        StructField("contractorProvince", StringType()),
        StructField("contractorNationalId_raw", StringType()),
        StructField("contractorNationalId_parsed", StringType()),
        StructField("contractorNationalId_type", StringType()),
    ]
)


def _normalize_tender_result_contractors_safe(contractors: list[dict] | None) -> list[dict] | None:
    try:
        return normalize_tender_result_contractors(contractors)
    except Exception:
        return contractors


_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_SPACE_RE = re.compile(r"\s+")
_LEGAL_SUFFIX_RE = re.compile(
    r"\b("
    r"sp\s*z\s*o\.?\s*o\.?|spolka\s*z\s*ograniczona\s*odpowiedzialnoscia|"
    r"s\.?\s*a\.?|s\.?\s*p\.?\s*j\.?|sp\.?\s*k\.?|"
    r"spolka\s*jawna|spolka\s*komandytowa|spolka\s*akcyjna|"
    r"sa|spzoo"
    r")\b",
    flags=re.IGNORECASE,
)


def _normalize_entity_name(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = name.lower()
    cleaned = _LEGAL_SUFFIX_RE.sub(" ", cleaned)
    cleaned = _PUNCT_RE.sub(" ", cleaned)
    cleaned = _SPACE_RE.sub(" ", cleaned).strip()
    return cleaned or None


def _normalize_contractor_names(contractors: list[dict] | None) -> list[str] | None:
    if not contractors:
        return None
    out: list[str] = []
    for contractor in contractors:
        if not isinstance(contractor, dict):
            continue
        normalized = _normalize_entity_name(contractor.get("contractorName"))
        if normalized:
            out.append(normalized)
    return out or None


def _extract_execution_duration_days(execution_period: str | None) -> int | None:
    if not execution_period:
        return None
    text = execution_period.casefold()
    replacements = {
        "ą": "a",
        "ć": "c",
        "ę": "e",
        "ł": "l",
        "ń": "n",
        "ó": "o",
        "ś": "s",
        "ź": "z",
        "ż": "z",
        "Ä…": "a",
        "Ä‡": "c",
        "Ä™": "e",
        "Ĺ‚": "l",
        "Ĺ„": "n",
        "Ăł": "o",
        "Ĺ›": "s",
        "Ĺº": "z",
        "Ĺ¼": "z",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    match = re.search(r"(\d+)\s*(?:dn\w*|days?)", text)
    if match is None:
        week_match = re.search(r"(\d+)\s*(?:tyg\w*|weeks?)", text)
        if week_match is not None:
            return int(week_match.group(1)) * 7
        month_match = re.search(r"(\d+)\s*(?:mies\w*|months?)", text)
        if month_match is not None:
            return int(month_match.group(1)) * 30
        return None
    return int(match.group(1))


def _add_months(base: datetime, months: int) -> datetime:
    month_index = (base.month - 1) + months
    year = base.year + (month_index // 12)
    month = (month_index % 12) + 1
    day = min(base.day, monthrange(year, month)[1])
    return base.replace(year=year, month=month, day=day)


def _parse_contract_date(contract_date: str | None) -> datetime | None:
    if not contract_date:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", contract_date)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d")
    except ValueError:
        return None


def _normalize_period_text(text: str) -> str:
    replacements = {
        "Ä…": "a",
        "Ä‡": "c",
        "Ä™": "e",
        "Ĺ‚": "l",
        "Ĺ„": "n",
        "Ăł": "o",
        "Ĺ›": "s",
        "Ĺş": "z",
        "ĹĽ": "z",
    }
    normalized = text.casefold()
    for src, dst in replacements.items():
        normalized = normalized.replace(src, dst)
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _parse_cpn_contract_planned_execution_date(
    execution_period_raw: str | None,
    contract_date_41: str | None,
) -> str | None:
    if not execution_period_raw:
        return None

    raw = execution_period_raw.strip()
    if not raw:
        return None

    # Prefer explicit end date when present: "od ... do ..." or "do YYYY-MM-DD"
    iso_dates = re.findall(r"\b(\d{4}-\d{2}-\d{2})\b", raw)
    if iso_dates:
        return iso_dates[-1]

    # Handle DD.MM.YYYY / DD-MM-YYYY variants.
    dmy_dates = re.findall(r"\b(\d{2})[./-](\d{2})[./-](\d{4})\b", raw)
    if dmy_dates:
        dd, mm, yyyy = dmy_dates[-1]
        try:
            return datetime(int(yyyy), int(mm), int(dd)).date().isoformat()
        except ValueError:
            return None

    base = _parse_contract_date(contract_date_41)
    if base is None:
        return None

    normalized = _normalize_period_text(raw)
    match = re.search(r"\b(\d+)\s*([a-z]+)\b", normalized)
    if match is None:
        return None

    amount = int(match.group(1))
    unit = match.group(2)

    if unit.startswith("dzien") or unit.startswith("dni") or unit.startswith("day"):
        return (base + timedelta(days=amount)).date().isoformat()
    if unit.startswith("tyg") or unit.startswith("week"):
        return (base + timedelta(days=amount * 7)).date().isoformat()
    if unit.startswith("mies") or unit.startswith("month"):
        return _add_months(base, amount).date().isoformat()
    if unit.startswith("rok") or unit.startswith("lat") or unit.startswith("year"):
        return _add_months(base, amount * 12).date().isoformat()
    return None


def _parse_contract_notice_planned_execution_dates(
    planned_execution_dates_raw: list[str] | None,
    publication_date: str | None,
) -> list[str | None] | None:
    if not planned_execution_dates_raw:
        return None
    parsed: list[str | None] = []
    for raw in planned_execution_dates_raw:
        parsed.append(_parse_cpn_contract_planned_execution_date(raw, publication_date))
    return parsed


def _criteria_summary(criteria: list[dict] | None) -> tuple[int | None, int | None, int | None]:
    if not criteria:
        return None, None, None
    num_criteria = len(criteria)
    price_weight = 0
    total_weight = 0
    for criterion in criteria:
        if not isinstance(criterion, dict):
            continue
        name = (criterion.get("str") or criterion.get("name") or "").lower()
        weight = criterion.get("weight")
        if isinstance(weight, int):
            total_weight += weight
            if "cena" in name:
                price_weight += weight
    non_price_weight_sum = total_weight - price_weight
    return num_criteria, price_weight, non_price_weight_sum


def _classify_notice_change(changes: list[dict] | None) -> tuple[bool, bool, bool]:
    if not changes:
        return False, False, False
    parts: list[str] = []
    for change in changes:
        if not isinstance(change, dict):
            continue
        parts.append(change.get("changed_section") or "")
        parts.append(change.get("change_description") or "")
    text = " ".join(parts).lower()
    deadline_changed = bool(re.search(r"termin|deadline|skladania ofert|otwarcia ofert", text))
    criteria_changed = bool(re.search(r"kryter|cena|waga", text))
    scope_changed = bool(re.search(r"zakres|przedmiot|opis", text))
    return deadline_changed, criteria_changed, scope_changed


parse_html_udf = udf(_parse_html_safe, HTML_EXTRACTED_SCHEMA)
parse_html_address_udf = udf(_parse_html_address_safe, HTML_ADDRESS_SCHEMA)
parse_html_ai_light_udf = udf(_parse_html_ai_light_safe, HTML_AI_LIGHT_SCHEMA)
parse_html_comp_light_udf = udf(_parse_html_comp_light_safe, HTML_COMP_LIGHT_SCHEMA)
parse_html_cpn_light_udf = udf(_parse_html_cpn_light_safe, HTML_CPN_LIGHT_SCHEMA)
normalize_tender_result_contractors_udf = udf(
    _normalize_tender_result_contractors_safe,
    ArrayType(CONTRACTOR_TRN_SCHEMA),
)
parse_organization_national_id_udf = udf(_parse_organization_national_id_safe, StringType())
parse_cpn_contract_planned_execution_date_udf = udf(_parse_cpn_contract_planned_execution_date, StringType())
parse_contract_notice_planned_execution_dates_udf = udf(
    _parse_contract_notice_planned_execution_dates,
    ArrayType(StringType()),
)
parse_cpv_udf = udf(_parse_cpv_safe, ArrayType(StringType()))
normalize_name_udf = udf(_normalize_entity_name, StringType())
normalize_contractors_udf = udf(_normalize_contractor_names, ArrayType(StringType()))


def _make_lookup_udf(mapping: dict[str, str]):
    """Create a UDF that maps code â†’ description using a dictionary."""

    def _lookup(code: str | None) -> str | None:
        if code is None:
            return None
        return mapping.get(code)

    return udf(_lookup, StringType())


HTML_FULL_DERIVED_COLUMNS = {
    "htmlExtracted",
    "numCriteria",
    "priceWeight",
    "nonPriceWeightSum",
    "cn_notice_concerns",
    "cn_partial_offers_allowed_418",
    "cn_offers_scope_4110",
    "contract_planned_execution_date",
    "contract_planned_execution_date_parsed",
    "cn_award_criteria_by_part",
    "cn_criteria_aspects_4310",
    "cn_criteria_aspects_4310_flag",
    "cn_description_by_part",
    "criteria",
    "cpvMainCode",
    "cpvSecondaryCode",
    "trn_notice_concerns",
    "trn_value_bid_lowest",
    "trn_value_bid_highest",
    "trn_value_winning_offer",
    "trn_parts",
    "changed_notice_number",
    "changed_notice_version",
    "changes",
}


def build_silver_for_notice_type(
    df: DataFrame,
    notice_type: str | None,
    required_columns: set[str] | None = None,
) -> DataFrame:
    """Build Silver rows for a single noticeType using only required computations."""
    required = set(required_columns or [])
    province_udf = _make_lookup_udf(province_names())
    client_type_udf = _make_lookup_udf(client_type_names())
    order_type_udf = _make_lookup_udf(order_type_names())
    out = df.filter(col("htmlBody").endswith("</html>"))

    need_contractors = "contractors" in required or "contractorNameNormalized" in required
    if need_contractors:
        out = out.withColumn("contractors", expr("filter(contractors, x -> x is not null)"))
        if notice_type == "TenderResultNotice":
            out = out.withColumn(
                "contractors",
                normalize_tender_result_contractors_udf(col("contractors")),
            )

    if "cpvCodes" in required:
        out = out.withColumn("cpvCodes", parse_cpv_udf(col("cpvCode")))
    if "provinceName" in required:
        out = out.withColumn("provinceName", province_udf(col("organizationProvince")))
    if "clientTypeName" in required:
        out = out.withColumn("clientTypeName", client_type_udf(col("clientType")))
    if "orderType" in required:
        # Normalize ENUM.002 identifier to Polish label
        # (e.g. Delivery -> Dostawy), preserving unknown values.
        out = out.withColumn("orderType", coalesce(order_type_udf(col("orderType")), col("orderType")))
    if "organizationNationalId_parsed" in required:
        out = out.withColumn(
            "organizationNationalId_parsed",
            parse_organization_national_id_udf(col("organizationCountry"), col("organizationNationalId")),
        )
    if "procedureResultParsed" in required:
        out = out.withColumn("procedureResultParsed", split(col("procedureResult"), ";"))
    if "caseId" in required:
        out = out.withColumn("caseId", coalesce(col("tenderId"), col("objectId")))
    if "noticeStage" in required:
        out = out.withColumn(
            "noticeStage",
            when(col("noticeType") == lit("TenderResultNotice"), lit("RESULT"))
            .when(col("noticeType") == lit("ContractPerformingNotice"), lit("EXECUTION"))
            .when(col("noticeType").isin("NoticeUpdateNotice", "AgreementUpdateNotice"), lit("UPDATE"))
            .otherwise(lit("INIT")),
        )
    if "organizationNameNormalized" in required:
        out = out.withColumn("organizationNameNormalized", normalize_name_udf(col("organizationName")))
    if "contractorNameNormalized" in required:
        out = out.withColumn("contractorNameNormalized", normalize_contractors_udf(col("contractors")))
    if "biddingWindowDays" in required:
        out = out.withColumn(
            "biddingWindowDays",
            datediff(to_timestamp(col("submittingOffersDate")), to_timestamp(col("publicationDate"))),
        )

    need_html_full = bool(required & HTML_FULL_DERIVED_COLUMNS)
    use_ai_light = notice_type == "AgreementIntentionNotice" and not need_html_full
    use_comp_light = notice_type in ("CompetitionNotice", "CompetitionResultNotice") and not need_html_full
    use_cpn_light = notice_type == "ContractPerformingNotice" and not need_html_full
    need_html_address = (
        ("street" in required or "postal_code" in required)
        and not need_html_full
        and not use_ai_light
        and not use_comp_light
        and not use_cpn_light
    )

    if need_html_full:
        out = out.withColumn(
            "htmlExtracted",
            parse_html_udf(col("htmlBody"), col("noticeType"), col("procedureResult")),
        )
    elif use_ai_light:
        out = out.withColumn("htmlLight", parse_html_ai_light_udf(col("htmlBody")))
    elif use_comp_light:
        out = out.withColumn("htmlLight", parse_html_comp_light_udf(col("htmlBody")))
    elif use_cpn_light:
        out = out.withColumn("htmlLight", parse_html_cpn_light_udf(col("htmlBody")))
    elif need_html_address:
        out = out.withColumn("htmlAddress", parse_html_address_udf(col("htmlBody"), col("noticeType")))

    if "street" in required:
        out = out.withColumn(
            "street",
            col("htmlExtracted.ulica")
            if need_html_full
            else (
                col("htmlLight.ulica")
                if (use_ai_light or use_comp_light or use_cpn_light)
                else col("htmlAddress.ulica")
            ),
        )
    if "postal_code" in required:
        out = out.withColumn(
            "postal_code",
            col("htmlExtracted.kod_pocztowy")
            if need_html_full
            else (
                col("htmlLight.kod_pocztowy")
                if (use_ai_light or use_comp_light or use_cpn_light)
                else col("htmlAddress.kod_pocztowy")
            ),
        )

    if "ai_street_512" in required:
        out = out.withColumn(
            "ai_street_512",
            col("htmlExtracted.ai_street_512") if need_html_full else col("htmlLight.ai_street_512"),
        )
    if "value_estimated_procurement_ai_35" in required:
        out = out.withColumn(
            "value_estimated_procurement_ai_35",
            col("htmlExtracted.value_estimated_procurement_ai_35")
            if need_html_full
            else col("htmlLight.value_estimated_procurement_ai_35"),
        )
    if "ai_prior_market_consultation_31" in required:
        out = out.withColumn(
            "ai_prior_market_consultation_31",
            col("htmlExtracted.ai_prior_market_consultation_31")
            if need_html_full
            else col("htmlLight.ai_prior_market_consultation_31"),
        )
    if "cpn_contractor_names_431" in required:
        out = out.withColumn(
            "cpn_contractor_names_431",
            col("htmlExtracted.cpn_contractor_names_431")
            if need_html_full
            else col("htmlLight.cpn_contractor_names_431"),
        )
    if "contractor_id_raw" in required:
        out = out.withColumn(
            "contractor_id_raw",
            col("htmlExtracted.contractor_id_raw")
            if need_html_full
            else col("htmlLight.contractor_id_raw"),
        )
    if "contractor_id_parsed" in required:
        out = out.withColumn(
            "contractor_id_parsed",
            col("htmlExtracted.contractor_id_parsed")
            if need_html_full
            else col("htmlLight.contractor_id_parsed"),
        )
    if "contractor_id_type" in required:
        out = out.withColumn(
            "contractor_id_type",
            col("htmlExtracted.contractor_id_type")
            if need_html_full
            else col("htmlLight.contractor_id_type"),
        )
    if "cpn_contractor_cities_434" in required:
        out = out.withColumn(
            "cpn_contractor_cities_434",
            col("htmlExtracted.cpn_contractor_cities_434")
            if need_html_full
            else col("htmlLight.cpn_contractor_cities_434"),
        )
    if "cpn_contractor_provinces_436" in required:
        out = out.withColumn(
            "cpn_contractor_provinces_436",
            col("htmlExtracted.cpn_contractor_provinces_436")
            if need_html_full
            else col("htmlLight.cpn_contractor_provinces_436"),
        )
    if "cpn_contractor_countries_437" in required:
        out = out.withColumn(
            "cpn_contractor_countries_437",
            col("htmlExtracted.cpn_contractor_countries_437")
            if need_html_full
            else col("htmlLight.cpn_contractor_countries_437"),
        )
    if "value_contract_reported_execution_44" in required:
        out = out.withColumn(
            "value_contract_reported_execution_44",
            col("htmlExtracted.value_contract_reported_execution_44")
            if need_html_full
            else col("htmlLight.value_contract_reported_execution_44"),
        )
    if "cpn_contract_date_41" in required:
        out = out.withColumn(
            "cpn_contract_date_41",
            to_date(
                col("htmlExtracted.cpn_contract_date_41")
                if need_html_full
                else col("htmlLight.cpn_contract_date_41")
            ),
        )
    if "cpn_contract_planned_execution_date_raw" in required:
        out = out.withColumn(
            "cpn_contract_planned_execution_date_raw",
            col("htmlExtracted.cpn_contract_planned_execution_date_raw")
            if need_html_full
            else col("htmlLight.cpn_contract_planned_execution_date_raw"),
        )
    if "cpn_contract_planned_execution_date_parsed" in required:
        out = out.withColumn(
            "cpn_contract_planned_execution_date_parsed",
            to_date(
                parse_cpn_contract_planned_execution_date_udf(
                    col("cpn_contract_planned_execution_date_raw"),
                    col("cpn_contract_date_41").cast("string"),
                )
            ),
        )
    if "cpn_execution_end_date_52" in required:
        out = out.withColumn(
            "cpn_execution_end_date_52",
            to_date(
                col("htmlExtracted.cpn_execution_end_date_52")
                if need_html_full
                else col("htmlLight.cpn_execution_end_date_52")
            ),
        )
    if "executed_in_time" in required:
        out = out.withColumn(
            "executed_in_time",
            col("htmlExtracted.contract_execution.executed_on_time")
            if need_html_full
            else col("htmlLight.executed_in_time"),
        )
    if "proper_execution" in required:
        out = out.withColumn(
            "proper_execution",
            col("htmlExtracted.contract_execution.executed_properly")
            if need_html_full
            else col("htmlLight.proper_execution"),
        )
    if "value_paid_total_55" in required:
        out = out.withColumn(
            "value_paid_total_55",
            col("htmlExtracted.value_paid_total_55")
            if need_html_full
            else col("htmlLight.value_paid_total_55"),
        )
    if "comp_num_awarded_63" in required:
        out = out.withColumn(
            "comp_num_awarded_63",
            col("htmlExtracted.comp_num_awarded_63")
            if need_html_full
            else col("htmlLight.comp_num_awarded_63"),
        )
    if "value_competition_prizes_64" in required:
        out = out.withColumn(
            "value_competition_prizes_64",
            col("htmlExtracted.value_competition_prizes_64")
            if need_html_full
            else col("htmlLight.value_competition_prizes_64"),
        )
    if "value_competition_followon_order_651" in required:
        out = out.withColumn(
            "value_competition_followon_order_651",
            col("htmlExtracted.value_competition_followon_order_651")
            if need_html_full
            else col("htmlLight.value_competition_followon_order_651"),
        )
    if "comp_requirements_72" in required:
        out = out.withColumn(
            "comp_requirements_72",
            col("htmlExtracted.comp_requirements_72")
            if need_html_full
            else col("htmlLight.comp_requirements_72"),
        )
    if "comp_submission_deadline" in required:
        out = out.withColumn(
            "comp_submission_deadline",
            col("htmlExtracted.comp_submission_deadline")
            if need_html_full
            else col("htmlLight.comp_submission_deadline"),
        )
    if "comp_result_approval_date_53" in required:
        out = out.withColumn(
            "comp_result_approval_date_53",
            col("htmlExtracted.comp_result_approval_date_53")
            if need_html_full
            else col("htmlLight.comp_result_approval_date_53"),
        )
    if "changed_notice_number" in required:
        out = out.withColumn(
            "changed_notice_number",
            col("htmlExtracted.notice_change.changed_notice_number"),
        )
    if "changed_notice_version" in required:
        out = out.withColumn(
            "changed_notice_version",
            col("htmlExtracted.notice_change.changed_notice_version"),
        )
    if "changes" in required:
        out = out.withColumn("changes", col("htmlExtracted.notice_change.changes"))
    if "trn_notice_concerns" in required:
        out = out.withColumn("trn_notice_concerns", col("htmlExtracted.ogloszenie_dotyczy"))
    if "trn_value_bid_lowest" in required:
        out = out.withColumn("trn_value_bid_lowest", col("htmlExtracted.values.value_bid_lowest"))
    if "trn_value_bid_highest" in required:
        out = out.withColumn("trn_value_bid_highest", col("htmlExtracted.values.value_bid_highest"))
    if "trn_value_winning_offer" in required:
        out = out.withColumn(
            "trn_value_winning_offer", col("htmlExtracted.values.value_winning_offer")
        )
    if "trn_parts" in required:
        out = out.withColumn("trn_parts", col("htmlExtracted.tender_result_parts"))
    if required & {
        "cn_notice_concerns",
        "cn_partial_offers_allowed_418",
        "cn_offers_scope_4110",
        "contract_planned_execution_date",
        "contract_planned_execution_date_parsed",
        "cn_award_criteria_by_part",
        "cn_criteria_aspects_4310",
        "cn_criteria_aspects_4310_flag",
        "cn_description_by_part",
        "criteria",
        "cpvMainCode",
        "cpvSecondaryCode",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
    }:
        out = out.withColumn(
            "cn_parts_normalized",
            expr(
                "coalesce(htmlExtracted.contract_notice_parts, cast(array() as array<struct<"
                "part_id:string,"
                "opis:string,"
                "kryteria_oceny:array<struct<no:int,str:string,weight:int>>,"
                "mainCPV:string,"
                "secondaryCPV:array<string>,"
                "contract_planned_execution_date:string,"
                "criteria_aspects_4310:string,"
                "criteria_aspects_4310_flag:boolean"
                ">>))"
            ),
        )
    if "cn_notice_concerns" in required:
        out = out.withColumn("cn_notice_concerns", col("htmlExtracted.ogloszenie_dotyczy"))
    if "cn_partial_offers_allowed_418" in required:
        out = out.withColumn(
            "cn_partial_offers_allowed_418",
            col("htmlExtracted.cn_partial_offers_allowed_418"),
        )
    if "cn_offers_scope_4110" in required:
        out = out.withColumn(
            "cn_offers_scope_4110",
            col("htmlExtracted.cn_offers_scope_4110"),
        )
    if "cn_award_criteria_by_part" in required:
        out = out.withColumn(
            "cn_award_criteria_by_part",
            expr(
                "transform(cn_parts_normalized, p -> "
                "map_from_entries(transform(coalesce(p.kryteria_oceny, array()), "
                "x -> named_struct('key', x.str, 'value', x.weight))))"
            ),
        )
    if "criteria" in required:
        out = out.withColumn(
            "criteria",
            expr("transform(cn_parts_normalized, p -> coalesce(p.kryteria_oceny, array()))"),
        )
    if "cn_criteria_aspects_4310" in required:
        out = out.withColumn(
            "cn_criteria_aspects_4310",
            expr("transform(cn_parts_normalized, p -> p.criteria_aspects_4310)"),
        )
    if "cn_criteria_aspects_4310_flag" in required:
        out = out.withColumn(
            "cn_criteria_aspects_4310_flag",
            expr("transform(cn_parts_normalized, p -> p.criteria_aspects_4310_flag)"),
        )
    if "cn_description_by_part" in required:
        out = out.withColumn("cn_description_by_part", expr("transform(cn_parts_normalized, p -> p.opis)"))
    if "contract_planned_execution_date" in required:
        out = out.withColumn(
            "contract_planned_execution_date",
            expr("transform(cn_parts_normalized, p -> p.contract_planned_execution_date)"),
        )
    if "contract_planned_execution_date_parsed" in required:
        out = out.withColumn(
            "contract_planned_execution_date_parsed",
            parse_contract_notice_planned_execution_dates_udf(
                col("contract_planned_execution_date"),
                col("publicationDate"),
            ),
        )
    if "cpvMainCode" in required:
        out = out.withColumn("cpvMainCode", expr("transform(cn_parts_normalized, p -> p.mainCPV)"))
    if "cpvSecondaryCode" in required:
        out = out.withColumn(
            "cpvSecondaryCode",
            expr("transform(cn_parts_normalized, p -> coalesce(p.secondaryCPV, array()))"),
        )

    if "numCriteria" in required:
        out = out.withColumn(
            "numCriteria",
            expr("transform(cn_parts_normalized, p -> size(coalesce(p.kryteria_oceny, array())))")
            if notice_type == "ContractNotice"
            else lit(None).cast("array<int>"),
        )
    if "priceWeight" in required:
        out = out.withColumn(
            "priceWeight",
            expr(
                "transform(cn_parts_normalized, p -> "
                "aggregate("
                "filter(coalesce(p.kryteria_oceny, array()), x -> lower(coalesce(x.str, '')) like '%cena%'),"
                "0,"
                "(acc, x) -> acc + coalesce(x.weight, 0)"
                "))"
            )
            if notice_type == "ContractNotice"
            else lit(None).cast("array<int>"),
        )
    if "nonPriceWeightSum" in required:
        out = out.withColumn(
            "nonPriceWeightSum",
            expr(
                "transform(cn_parts_normalized, p -> "
                "aggregate(coalesce(p.kryteria_oceny, array()), 0, (acc, x) -> acc + coalesce(x.weight, 0)) "
                "- aggregate("
                "filter(coalesce(p.kryteria_oceny, array()), x -> lower(coalesce(x.str, '')) like '%cena%'),"
                "0, "
                "(acc, x) -> acc + coalesce(x.weight, 0)"
                "))"
            )
            if notice_type == "ContractNotice"
            else lit(None).cast("array<int>"),
        )

    if notice_type is None:
        # Backward-compatible mixed-mode build: only ContractNotice rows should carry list metrics.
        if "numCriteria" in required:
            out = out.withColumn(
                "numCriteria",
                when(col("noticeType") == lit("ContractNotice"), col("numCriteria")).otherwise(lit(None).cast("array<int>")),
            )
        if "priceWeight" in required:
            out = out.withColumn(
                "priceWeight",
                when(col("noticeType") == lit("ContractNotice"), col("priceWeight")).otherwise(
                    lit(None).cast("array<int>")
                ),
            )
        if "nonPriceWeightSum" in required:
            out = out.withColumn(
                "nonPriceWeightSum",
                when(col("noticeType") == lit("ContractNotice"), col("nonPriceWeightSum")).otherwise(
                    lit(None).cast("array<int>")
                ),
            )

    if notice_type == "ContractNotice" and "htmlExtracted" in out.columns:
        # Expose all ContractNotice core sections as first-class columns.
        for section_col in ContractNoticeCoreRaw.model_fields.keys():
            out = out.withColumn(
                section_col,
                col("htmlExtracted.contract_notice_core_sections").getItem(section_col),
            )

    drop_cols = ["htmlBody", "cpvCode"]
    if "htmlAddress" in out.columns:
        drop_cols.append("htmlAddress")
    if "htmlLight" in out.columns:
        drop_cols.append("htmlLight")
    if "cn_parts_normalized" in out.columns:
        drop_cols.append("cn_parts_normalized")
    out = out.drop(*drop_cols)
    if not need_html_full and "htmlExtracted" in out.columns:
        out = out.drop("htmlExtracted")
    return out


def build_silver(df: DataFrame) -> DataFrame:
    """Backward-compatible broad Silver transform."""
    legacy_required = {
        "contractors",
        "htmlExtracted",
        "street",
        "postal_code",
        "ai_street_512",
        "value_estimated_procurement_ai_35",
        "ai_prior_market_consultation_31",
        "cpn_contractor_names_431",
        "contractor_id_raw",
        "contractor_id_parsed",
        "contractor_id_type",
        "cpn_contractor_cities_434",
        "cpn_contractor_provinces_436",
        "cpn_contractor_countries_437",
        "value_contract_reported_execution_44",
        "value_paid_total_55",
        "comp_num_awarded_63",
        "value_competition_prizes_64",
        "value_competition_followon_order_651",
        "comp_requirements_72",
        "comp_submission_deadline",
        "comp_result_approval_date_53",
        "changed_notice_number",
        "changed_notice_version",
        "changes",
        "trn_notice_concerns",
        "trn_value_bid_lowest",
        "trn_value_bid_highest",
        "trn_value_winning_offer",
        "trn_parts",
        "cn_notice_concerns",
        "contract_planned_execution_date",
        "contract_planned_execution_date_parsed",
        "cn_award_criteria_by_part",
        "cn_criteria_aspects_4310",
        "cn_criteria_aspects_4310_flag",
        "cn_description_by_part",
        "cpvCodes",
        "provinceName",
        "clientTypeName",
        "procedureResultParsed",
        "caseId",
        "noticeStage",
        "organizationNameNormalized",
        "contractorNameNormalized",
        "biddingWindowDays",
        "numCriteria",
        "priceWeight",
        "nonPriceWeightSum",
    }
    return build_silver_for_notice_type(df, notice_type=None, required_columns=legacy_required)


def build_contract_notice_parts_table(df: DataFrame) -> DataFrame:
    """Build one-row-per-part ContractNotice table from validated Silver rows.

    Expected input: `valid_batch` from build_silver scripts, with `htmlExtracted`
    present for ContractNotice records.
    """
    parts = (
        df.select(
            col("objectId"),
            col("publicationDateDay"),
            col("publicationDate"),
            col("noticeType"),
            col("caseId"),
            col("caseId_shard"),
            col("tenderId"),
            col("organizationId"),
            col("htmlExtracted.contract_notice_parts_sections").alias("parts_sections_raw_arr"),
            posexplode_outer(col("htmlExtracted.contract_notice_parts")).alias("part_ordinal0", "part"),
        )
        .where(col("part").isNotNull())
        .withColumn("part_ordinal", col("part_ordinal0") + lit(1))
        .withColumn(
            "part_sections_raw",
            expr("element_at(parts_sections_raw_arr, part_ordinal0 + 1)"),
        )
        .withColumn("part_id", coalesce(col("part.part_id"), col("part_ordinal").cast("string")))
        .withColumn("part_description", col("part.opis"))
        .withColumn("part_main_cpv", col("part.mainCPV"))
        .withColumn(
            "part_secondary_cpv",
            coalesce(col("part.secondaryCPV"), expr("cast(array() as array<string>)")),
        )
        .withColumn("part_contract_planned_execution_date_raw", col("part.contract_planned_execution_date"))
        .withColumn(
            "part_contract_planned_execution_date_parsed",
            to_date(
                parse_cpn_contract_planned_execution_date_udf(
                    col("part.contract_planned_execution_date"),
                    col("publicationDate"),
                )
            ),
        )
        .withColumn(
            "part_criteria",
            coalesce(
                col("part.kryteria_oceny"),
                expr("cast(array() as array<struct<no:int,str:string,weight:int>>)"),
            ),
        )
        .withColumn("part_num_criteria", size(col("part_criteria")))
        .withColumn(
            "part_price_weight",
            expr(
                "aggregate("
                "filter(part_criteria, x -> lower(coalesce(x.str, '')) like '%cena%'),"
                "0,"
                "(acc, x) -> acc + coalesce(x.weight, 0)"
                ")"
            ),
        )
        .withColumn(
            "part_non_price_weight_sum",
            expr(
                "aggregate(part_criteria, 0, (acc, x) -> acc + coalesce(x.weight, 0)) "
                "- aggregate(filter(part_criteria, x -> lower(coalesce(x.str, '')) like '%cena%'), 0, (acc, x) -> acc + coalesce(x.weight, 0))"
            ),
        )
        .withColumn("part_criteria_aspects_4310", col("part.criteria_aspects_4310"))
        .withColumn("part_criteria_aspects_4310_flag", col("part.criteria_aspects_4310_flag"))
        .drop("part_ordinal0", "parts_sections_raw_arr", "part")
    )
    for section_col in ContractNoticePartRaw.model_fields.keys():
        parts = parts.withColumn(
            section_col,
            col("part_sections_raw").getItem(section_col),
        )

    return parts

