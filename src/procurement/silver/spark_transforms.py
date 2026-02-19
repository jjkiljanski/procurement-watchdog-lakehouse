"""PySpark transforms for the BZP silver layer."""

from __future__ import annotations

import logging
import re
import unicodedata

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    datediff,
    expr,
    lit,
    size,
    split,
    to_timestamp,
    udf,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from procurement.dictionaries import client_type_names, province_names
from procurement.silver.html_parser import (
    parse_cpv_codes,
    parse_html,
    parse_html_address_light,
    parse_html_agreement_intention_light,
    parse_html_competition_light,
    parse_html_contract_performing_light,
)

log = logging.getLogger(__name__)

EVAL_CRITERION_SCHEMA = StructType(
    [
        StructField("name", StringType()),
        StructField("weight", IntegerType()),
    ]
)

CONTRACT_NOTICE_PART_SCHEMA = StructType(
    [
        StructField("part_id", StringType()),
        StructField("opis", StringType()),
        StructField("kryteria_oceny", ArrayType(EVAL_CRITERION_SCHEMA)),
        StructField("criteria_aspects_4310", StringType()),
        StructField("criteria_aspects_4310_flag", BooleanType()),
    ]
)

EXTRACTED_VALUES_SCHEMA = StructType(
    [
        StructField("contract_value", DoubleType()),
        StructField("total_paid", DoubleType()),
        StructField("estimated_value", DoubleType()),
        StructField("lowest_bid", DoubleType()),
        StructField("highest_bid", DoubleType()),
        StructField("winning_bid", DoubleType()),
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
        StructField("contract_value", DoubleType()),
        StructField("lowest_bid", DoubleType()),
        StructField("highest_bid", DoubleType()),
        StructField("winning_bid", DoubleType()),
        StructField("estimated_value", DoubleType()),
        StructField("winner", StringType()),
    ]
)

TENDER_RESULT_PART_SCHEMA = StructType(
    [
        StructField("part_id", StringType()),
        StructField("opis", StringType()),
        StructField("mainCPV", StringType()),
        StructField("secondaryCPV", ArrayType(StringType())),
        StructField("expected_value", DoubleType()),
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
        StructField("values", EXTRACTED_VALUES_SCHEMA),
        StructField("lots", ArrayType(TENDER_RESULT_LOT_SCHEMA)),
        StructField("tender_result_parts", ArrayType(TENDER_RESULT_PART_SCHEMA)),
        StructField("tender_result_enrichment", TENDER_RESULT_ENRICHMENT_SCHEMA),
        StructField("contract_execution", CONTRACT_EXECUTION_SCHEMA),
        StructField("notice_change", NOTICE_CHANGE_SCHEMA),
        StructField("ai_street_512", StringType()),
        StructField("ai_contract_value_35", DoubleType()),
        StructField("ai_prior_market_consultation_31", StringType()),
        StructField("cpn_contractor_national_ids_432", ArrayType(StringType())),
        StructField("cpn_contractor_cities_434", ArrayType(StringType())),
        StructField("cpn_contractor_provinces_436", ArrayType(StringType())),
        StructField("cpn_contract_value_44", DoubleType()),
        StructField("comp_num_awarded_63", IntegerType()),
        StructField("comp_prizes_value_64", DoubleType()),
        StructField("comp_order_value_651", DoubleType()),
        StructField("comp_requirements_72", StringType()),
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
        StructField("ai_contract_value_35", DoubleType()),
        StructField("ai_prior_market_consultation_31", StringType()),
    ]
)

HTML_COMP_LIGHT_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("comp_num_awarded_63", IntegerType()),
        StructField("comp_prizes_value_64", DoubleType()),
        StructField("comp_order_value_651", DoubleType()),
        StructField("comp_requirements_72", StringType()),
    ]
)

HTML_CPN_LIGHT_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("cpn_contractor_national_ids_432", ArrayType(StringType())),
        StructField("cpn_contractor_cities_434", ArrayType(StringType())),
        StructField("cpn_contractor_provinces_436", ArrayType(StringType())),
        StructField("cpn_contract_value_44", DoubleType()),
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
        return parse_html_address_light(html, notice_type=notice_type)
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


def _criteria_summary(criteria: list[dict] | None) -> tuple[int | None, int | None, int | None]:
    if not criteria:
        return None, None, None
    num_criteria = len(criteria)
    price_weight = 0
    total_weight = 0
    for criterion in criteria:
        if not isinstance(criterion, dict):
            continue
        name = (criterion.get("name") or "").lower()
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
    "cn_ogloszenie_dotyczy",
    "cn_kryteria_oceny_by_part",
    "cn_criteria_aspects_4310",
    "cn_criteria_aspects_4310_flag",
    "cn_opis_by_part",
    "trn_ogloszenie_dotyczy",
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
    out = df.filter(col("htmlBody").endswith("</html>"))

    need_contractors = "contractors" in required or "contractorNameNormalized" in required
    if need_contractors:
        out = out.withColumn("contractors", expr("filter(contractors, x -> x is not null)"))

    if "cpvCodes" in required:
        out = out.withColumn("cpvCodes", parse_cpv_udf(col("cpvCode")))
    if "provinceName" in required:
        out = out.withColumn("provinceName", province_udf(col("organizationProvince")))
    if "clientTypeName" in required:
        out = out.withColumn("clientTypeName", client_type_udf(col("clientType")))
    if "procedureResultParsed" in required:
        out = out.withColumn("procedureResultParsed", split(col("procedureResult"), ";"))
    if "caseId" in required:
        out = out.withColumn("caseId", coalesce(col("tenderId"), col("noticeNumber")))
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
    use_comp_light = notice_type == "CompetitionNotice" and not need_html_full
    use_cpn_light = notice_type == "ContractPerformingNotice" and not need_html_full
    need_html_address = (
        ("ulica" in required or "kod_pocztowy" in required)
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

    if "ulica" in required:
        out = out.withColumn(
            "ulica",
            col("htmlExtracted.ulica")
            if need_html_full
            else (
                col("htmlLight.ulica")
                if (use_ai_light or use_comp_light or use_cpn_light)
                else col("htmlAddress.ulica")
            ),
        )
    if "kod_pocztowy" in required:
        out = out.withColumn(
            "kod_pocztowy",
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
    if "ai_contract_value_35" in required:
        out = out.withColumn(
            "ai_contract_value_35",
            col("htmlExtracted.ai_contract_value_35")
            if need_html_full
            else col("htmlLight.ai_contract_value_35"),
        )
    if "ai_prior_market_consultation_31" in required:
        out = out.withColumn(
            "ai_prior_market_consultation_31",
            col("htmlExtracted.ai_prior_market_consultation_31")
            if need_html_full
            else col("htmlLight.ai_prior_market_consultation_31"),
        )
    if "cpn_contractor_national_ids_432" in required:
        out = out.withColumn(
            "cpn_contractor_national_ids_432",
            col("htmlExtracted.cpn_contractor_national_ids_432")
            if need_html_full
            else col("htmlLight.cpn_contractor_national_ids_432"),
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
    if "cpn_contract_value_44" in required:
        out = out.withColumn(
            "cpn_contract_value_44",
            col("htmlExtracted.cpn_contract_value_44")
            if need_html_full
            else col("htmlLight.cpn_contract_value_44"),
        )
    if "comp_num_awarded_63" in required:
        out = out.withColumn(
            "comp_num_awarded_63",
            col("htmlExtracted.comp_num_awarded_63")
            if need_html_full
            else col("htmlLight.comp_num_awarded_63"),
        )
    if "comp_prizes_value_64" in required:
        out = out.withColumn(
            "comp_prizes_value_64",
            col("htmlExtracted.comp_prizes_value_64")
            if need_html_full
            else col("htmlLight.comp_prizes_value_64"),
        )
    if "comp_order_value_651" in required:
        out = out.withColumn(
            "comp_order_value_651",
            col("htmlExtracted.comp_order_value_651")
            if need_html_full
            else col("htmlLight.comp_order_value_651"),
        )
    if "comp_requirements_72" in required:
        out = out.withColumn(
            "comp_requirements_72",
            col("htmlExtracted.comp_requirements_72")
            if need_html_full
            else col("htmlLight.comp_requirements_72"),
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
    if "trn_ogloszenie_dotyczy" in required:
        out = out.withColumn("trn_ogloszenie_dotyczy", col("htmlExtracted.ogloszenie_dotyczy"))
    if "trn_parts" in required:
        out = out.withColumn("trn_parts", col("htmlExtracted.tender_result_parts"))
    if required & {
        "cn_ogloszenie_dotyczy",
        "cn_kryteria_oceny_by_part",
        "cn_criteria_aspects_4310",
        "cn_criteria_aspects_4310_flag",
        "cn_opis_by_part",
    }:
        out = out.withColumn(
            "cn_parts_normalized",
            expr(
                "CASE WHEN htmlExtracted.contract_notice_parts IS NOT NULL AND size(htmlExtracted.contract_notice_parts) > 0 "
                "THEN htmlExtracted.contract_notice_parts "
                "ELSE array(named_struct("
                "'part_id', cast(null as string),"
                "'opis', htmlExtracted.opis,"
                "'kryteria_oceny', htmlExtracted.kryteria_oceny,"
                "'criteria_aspects_4310', htmlExtracted.criteria_aspects_4310,"
                "'criteria_aspects_4310_flag', htmlExtracted.criteria_aspects_4310_flag"
                ")) END"
            ),
        )
    if "cn_ogloszenie_dotyczy" in required:
        out = out.withColumn("cn_ogloszenie_dotyczy", col("htmlExtracted.ogloszenie_dotyczy"))
    if "cn_kryteria_oceny_by_part" in required:
        out = out.withColumn(
            "cn_kryteria_oceny_by_part",
            expr(
                "transform(cn_parts_normalized, p -> "
                "map_from_entries(transform(coalesce(p.kryteria_oceny, array()), "
                "x -> named_struct('key', x.name, 'value', x.weight))))"
            ),
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
    if "cn_opis_by_part" in required:
        out = out.withColumn("cn_opis_by_part", expr("transform(cn_parts_normalized, p -> p.opis)"))

    if "numCriteria" in required:
        out = out.withColumn(
            "numCriteria",
            size(col("htmlExtracted.kryteria_oceny")) if notice_type == "ContractNotice" else lit(None).cast("int"),
        )
    if "priceWeight" in required:
        out = out.withColumn(
            "priceWeight",
            expr(
                "aggregate(filter(htmlExtracted.kryteria_oceny, x -> lower(x.name) like '%cena%'),"
                "0,(acc, x) -> acc + coalesce(x.weight, 0))"
            )
            if notice_type == "ContractNotice"
            else lit(None).cast("int"),
        )
    if "nonPriceWeightSum" in required:
        out = out.withColumn(
            "nonPriceWeightSum",
            when(
                col("htmlExtracted.kryteria_oceny").isNotNull(),
                expr("aggregate(htmlExtracted.kryteria_oceny, 0, (acc, x) -> acc + coalesce(x.weight, 0))")
                - coalesce(col("priceWeight"), lit(0)),
            )
            if notice_type == "ContractNotice"
            else lit(None).cast("int"),
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
        "ulica",
        "kod_pocztowy",
        "ai_street_512",
        "ai_contract_value_35",
        "ai_prior_market_consultation_31",
        "cpn_contractor_national_ids_432",
        "cpn_contractor_cities_434",
        "cpn_contractor_provinces_436",
        "cpn_contract_value_44",
        "comp_num_awarded_63",
        "comp_prizes_value_64",
        "comp_order_value_651",
        "comp_requirements_72",
        "changed_notice_number",
        "changed_notice_version",
        "changes",
        "trn_ogloszenie_dotyczy",
        "trn_parts",
        "cn_ogloszenie_dotyczy",
        "cn_kryteria_oceny_by_part",
        "cn_criteria_aspects_4310",
        "cn_criteria_aspects_4310_flag",
        "cn_opis_by_part",
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

