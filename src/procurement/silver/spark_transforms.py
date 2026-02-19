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
    lower,
    max as spark_max,
    regexp_extract,
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
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from procurement.dictionaries import client_type_names, province_names
from procurement.silver.html_parser import parse_cpv_codes, parse_html

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


def build_silver(df: DataFrame) -> DataFrame:
    """Transform a raw BZP DataFrame into the silver layer.

    - Filters out records with truncated HTML
    - Parses HTML body via UDF â†’ struct of extracted fields (type-aware)
    - Splits CPV codes string â†’ array
    - Resolves organizationProvince code â†’ provinceName
    - Resolves clientType code â†’ clientTypeName
    - Splits procedureResult semicolon-delimited string â†’ array
    - Drops raw htmlBody and cpvCode columns
    """
    province_udf = _make_lookup_udf(province_names())
    client_type_udf = _make_lookup_udf(client_type_names())

    silver_df = (
        df.filter(col("htmlBody").endswith("</html>"))
        .withColumn(
            "contractors",
            expr(
                "filter(contractors, x -> x is not null)"
            ),
        )
        .withColumn(
            "htmlExtracted",
            parse_html_udf(col("htmlBody"), col("noticeType"), col("procedureResult")),
        )
        .withColumn("ulica", col("htmlExtracted.ulica"))
        .withColumn("kod_pocztowy", col("htmlExtracted.kod_pocztowy"))
        .withColumn("ai_street_512", col("htmlExtracted.ai_street_512"))
        .withColumn("ai_contract_value_35", col("htmlExtracted.ai_contract_value_35"))
        .withColumn(
            "ai_prior_market_consultation_31",
            col("htmlExtracted.ai_prior_market_consultation_31"),
        )
        .withColumn(
            "cpn_contractor_national_ids_432",
            col("htmlExtracted.cpn_contractor_national_ids_432"),
        )
        .withColumn(
            "cpn_contractor_cities_434",
            col("htmlExtracted.cpn_contractor_cities_434"),
        )
        .withColumn(
            "cpn_contractor_provinces_436",
            col("htmlExtracted.cpn_contractor_provinces_436"),
        )
        .withColumn("cpn_contract_value_44", col("htmlExtracted.cpn_contract_value_44"))
        .withColumn("comp_num_awarded_63", col("htmlExtracted.comp_num_awarded_63"))
        .withColumn("comp_prizes_value_64", col("htmlExtracted.comp_prizes_value_64"))
        .withColumn("comp_order_value_651", col("htmlExtracted.comp_order_value_651"))
        .withColumn("comp_requirements_72", col("htmlExtracted.comp_requirements_72"))
        .withColumn(
            "changed_notice_number",
            col("htmlExtracted.notice_change.changed_notice_number"),
        )
        .withColumn(
            "changed_notice_version",
            col("htmlExtracted.notice_change.changed_notice_version"),
        )
        .withColumn("changes", col("htmlExtracted.notice_change.changes"))
        .withColumn(
            "trn_ogloszenie_dotyczy",
            when(col("noticeType") == lit("TenderResultNotice"), col("htmlExtracted.ogloszenie_dotyczy")),
        )
        .withColumn(
            "trn_parts",
            when(col("noticeType") == lit("TenderResultNotice"), col("htmlExtracted.tender_result_parts")),
        )
        .withColumn(
            "cn_parts_normalized",
            expr(
                "CASE WHEN noticeType = 'ContractNotice' THEN "
                "CASE WHEN htmlExtracted.contract_notice_parts IS NOT NULL AND size(htmlExtracted.contract_notice_parts) > 0 "
                "THEN htmlExtracted.contract_notice_parts "
                "ELSE array(named_struct("
                "'part_id', cast(null as string),"
                "'opis', htmlExtracted.opis,"
                "'kryteria_oceny', htmlExtracted.kryteria_oceny,"
                "'criteria_aspects_4310', htmlExtracted.criteria_aspects_4310,"
                "'criteria_aspects_4310_flag', htmlExtracted.criteria_aspects_4310_flag"
                ")) END END"
            ),
        )
        .withColumn(
            "cn_ogloszenie_dotyczy",
            when(col("noticeType") == lit("ContractNotice"), col("htmlExtracted.ogloszenie_dotyczy")),
        )
        .withColumn(
            "cn_kryteria_oceny_by_part",
            when(
                col("noticeType") == lit("ContractNotice"),
                expr(
                    "transform(cn_parts_normalized, p -> "
                    "map_from_entries(transform(coalesce(p.kryteria_oceny, array()), "
                    "x -> named_struct('key', x.name, 'value', x.weight))))"
                ),
            ),
        )
        .withColumn(
            "cn_criteria_aspects_4310",
            when(
                col("noticeType") == lit("ContractNotice"),
                expr("transform(cn_parts_normalized, p -> p.criteria_aspects_4310)"),
            ),
        )
        .withColumn(
            "cn_criteria_aspects_4310_flag",
            when(
                col("noticeType") == lit("ContractNotice"),
                expr("transform(cn_parts_normalized, p -> p.criteria_aspects_4310_flag)"),
            ),
        )
        .withColumn(
            "cn_opis_by_part",
            when(
                col("noticeType") == lit("ContractNotice"),
                expr("transform(cn_parts_normalized, p -> p.opis)"),
            ),
        )
        .withColumn("cpvCodes", parse_cpv_udf(col("cpvCode")))
        .withColumn("provinceName", province_udf(col("organizationProvince")))
        .withColumn("clientTypeName", client_type_udf(col("clientType")))
        .withColumn(
            "procedureResultParsed",
            split(col("procedureResult"), ";"),
        )
        .withColumn("caseId", coalesce(col("tenderId"), col("noticeNumber")))
        .withColumn(
            "noticeStage",
            when(col("noticeType") == lit("TenderResultNotice"), lit("RESULT"))
            .when(col("noticeType") == lit("ContractPerformingNotice"), lit("EXECUTION"))
            .when(col("noticeType").isin("NoticeUpdateNotice", "AgreementUpdateNotice"), lit("UPDATE"))
            .otherwise(lit("INIT")),
        )
        .withColumn("organizationNameNormalized", normalize_name_udf(col("organizationName")))
        .withColumn("contractorNameNormalized", normalize_contractors_udf(col("contractors")))
        .withColumn(
            "biddingWindowDays",
            datediff(
                to_timestamp(col("submittingOffersDate")),
                to_timestamp(col("publicationDate")),
            ),
        )
        .withColumn("numCriteria", size(col("htmlExtracted.kryteria_oceny")))
        .withColumn(
            "priceWeight",
            expr(
                "aggregate("
                "filter(htmlExtracted.kryteria_oceny, x -> lower(x.name) like '%cena%'),"
                "0,"
                "(acc, x) -> acc + coalesce(x.weight, 0)"
                ")"
            ),
        )
        .withColumn(
            "nonPriceWeightSum",
            when(
                col("htmlExtracted.kryteria_oceny").isNotNull(),
                expr(
                    "aggregate(htmlExtracted.kryteria_oceny, 0, (acc, x) -> acc + coalesce(x.weight, 0))"
                )
                - coalesce(col("priceWeight"), lit(0)),
            ),
        )
        .withColumn(
            "updateDeltaText",
            lower(
                expr(
                    "concat_ws(' ', transform(coalesce(htmlExtracted.notice_change.changes, array()),"
                    "x -> concat_ws(' ', coalesce(x.changed_section, ''), coalesce(x.change_description, ''))))"
                )
            ),
        )
        .withColumn(
            "deadlineChanged",
            col("updateDeltaText").rlike("termin|deadline|skladania ofert|otwarcia ofert"),
        )
        .withColumn(
            "criteriaChanged",
            col("updateDeltaText").rlike("kryter|cena|waga"),
        )
        .withColumn(
            "scopeChanged",
            col("updateDeltaText").rlike("zakres|przedmiot|opis"),
        )
        .withColumn(
            "executionDurationDays",
            coalesce(
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:dni|dzien|days?)', 1) as int)"
                    ),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:tygod\\w*|weeks?)', 1) as int)"
                    )
                    * lit(7),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:miesi\\w*|months?)', 1) as int)"
                    )
                    * lit(30),
                ),
                when(
                    col("htmlExtracted.contract_execution.contract_date").isNotNull()
                    & col("htmlExtracted.contract_execution.execution_end_date").isNotNull(),
                    datediff(
                        to_date(col("htmlExtracted.contract_execution.execution_end_date")),
                        to_date(col("htmlExtracted.contract_execution.contract_date")),
                    ),
                ),
            ),
        )
        .withColumn(
            "paidRatio",
            when(
                col("htmlExtracted.values.contract_value").isNotNull()
                & (col("htmlExtracted.values.contract_value") != 0)
                & col("htmlExtracted.values.total_paid").isNotNull(),
                col("htmlExtracted.values.total_paid") / col("htmlExtracted.values.contract_value"),
            ),
        )
        .withColumn(
            "executionDelayed",
            when(
                col("htmlExtracted.contract_execution.executed_on_time").isNotNull(),
                ~col("htmlExtracted.contract_execution.executed_on_time"),
            ),
        )
        .withColumn(
            "executionRiskFlag",
            when(
                col("noticeType") == lit("ContractPerformingNotice"),
                coalesce(col("executionDelayed"), lit(False))
                | (coalesce(col("paidRatio"), lit(0.0)) > lit(1.05))
                | (coalesce(col("htmlExtracted.contract_execution.num_changes"), lit(0)) > lit(0))
                | (col("htmlExtracted.contract_execution.executed_properly") == lit(False)),
            ),
        )
        .drop("htmlBody", "cpvCode", "cn_parts_normalized")
    )

    case_window = Window.partitionBy("caseId")
    silver_df = (
        silver_df.withColumn(
            "hasTenderResult",
            spark_max(when(col("noticeType") == lit("TenderResultNotice"), lit(1)).otherwise(lit(0))).over(case_window)
            == lit(1),
        )
        .withColumn(
            "hasContractExecution",
            spark_max(
                when(col("noticeType") == lit("ContractPerformingNotice"), lit(1)).otherwise(lit(0))
            ).over(case_window)
            == lit(1),
        )
        .drop("updateDeltaText")
    )

    return silver_df

