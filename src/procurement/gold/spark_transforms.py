"""PySpark transforms for Gold analytical marts."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    datediff,
    explode_outer,
    expr,
    first,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    regexp_extract,
    size,
    substring,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)

from procurement.gold.utils import has_field, safe_col


def _with_cpv_groups(df: DataFrame) -> DataFrame:
    cpv_primary = when(size(safe_col(df, "cpvCodes", "array<string>")) > 0, col("cpvCodes")[0])
    cpv_digits = regexp_extract(cpv_primary, r"(\d{8})", 1)
    return (
        df.withColumn("cpv_primary", cpv_primary)
        .withColumn("cpv_2digit", when(cpv_digits != "", substring(cpv_digits, 1, 2)))
        .withColumn("cpv_3digit", when(cpv_digits != "", substring(cpv_digits, 1, 3)))
    )


def _base_notice_facts(df: DataFrame, target_date: str) -> DataFrame:
    cpv_df = _with_cpv_groups(df)
    contract_value = safe_col(cpv_df, "htmlExtracted.values.contract_value", "double")
    total_paid = safe_col(cpv_df, "htmlExtracted.values.total_paid", "double")
    lowest_bid = safe_col(cpv_df, "htmlExtracted.values.lowest_bid", "double")
    highest_bid = safe_col(cpv_df, "htmlExtracted.values.highest_bid", "double")
    price_weight = safe_col(cpv_df, "priceWeight", "double")
    non_price_weight = safe_col(cpv_df, "nonPriceWeightSum", "double")
    country_expr = (
        "filter(transform(contractors_struct, x -> lower(trim(x.contractorCountry))), "
        "x -> x is not null and x <> '')"
    )
    domestic_country_expr = (
        "size(filter(contractor_countries_norm, x -> x not in ('pl','polska','poland'))) = 0"
    )
    single_bid_text_expr = (
        "jedn.{0,20}ofert|one\\s+offer|1\\s*ofert|pojedyncz.{0,12}ofert"
    )
    has_notice_change_struct = has_field(cpv_df, "htmlExtracted.notice_change.changes")
    has_notice_change_flat = has_field(cpv_df, "changes")
    if has_notice_change_struct:
        update_delta_text_col = lower(
            expr(
                "concat_ws(' ', transform(coalesce(htmlExtracted.notice_change.changes, array()), "
                "x -> concat_ws(' ', coalesce(x.changed_section, ''), coalesce(x.change_description, ''))))"
            )
        )
    elif has_notice_change_flat:
        update_delta_text_col = lower(
            expr(
                "concat_ws(' ', transform(coalesce(changes, array()), "
                "x -> concat_ws(' ', coalesce(x.changed_section, ''), coalesce(x.change_description, ''))))"
            )
        )
    else:
        update_delta_text_col = lit(None).cast("string")

    return (
        cpv_df.withColumn("target_date", lit(target_date))
        .withColumn(
            "contractors_struct",
            safe_col(cpv_df, "contractors", "array<struct<contractorCountry:string>>"),
        )
        .withColumn("contractor_countries_norm", expr(country_expr))
        .withColumn("publication_date", to_date(safe_col(cpv_df, "publicationDate", "string")))
        .withColumn(
            "biddingWindowDays",
            coalesce(
                safe_col(cpv_df, "biddingWindowDays", "long"),
                datediff(
                    to_timestamp(safe_col(cpv_df, "submittingOffersDate", "string")),
                    to_timestamp(safe_col(cpv_df, "publicationDate", "string")),
                ),
            ),
        )
        .withColumn("updateDeltaText", update_delta_text_col)
        .withColumn(
            "deadlineChanged",
            coalesce(
                safe_col(cpv_df, "deadlineChanged", "boolean"),
                col("updateDeltaText").rlike("termin|deadline|skladania ofert|otwarcia ofert"),
            ),
        )
        .withColumn(
            "criteriaChanged",
            coalesce(
                safe_col(cpv_df, "criteriaChanged", "boolean"),
                col("updateDeltaText").rlike("kryter|cena|waga"),
            ),
        )
        .withColumn(
            "scopeChanged",
            coalesce(
                safe_col(cpv_df, "scopeChanged", "boolean"),
                col("updateDeltaText").rlike("zakres|przedmiot|opis"),
            ),
        )
        .withColumn("contract_value", contract_value)
        .withColumn("winning_bid_value", safe_col(cpv_df, "htmlExtracted.values.winning_bid", "double"))
        .withColumn("estimated_value", safe_col(cpv_df, "htmlExtracted.values.estimated_value", "double"))
        .withColumn("total_paid_value", total_paid)
        .withColumn(
            "paid_ratio_effective",
            coalesce(
                safe_col(cpv_df, "paidRatio", "double"),
                when(contract_value.isNotNull() & (contract_value != 0) & total_paid.isNotNull(), total_paid / contract_value),
            ),
        )
        .withColumn(
            "price_weight_ratio",
            when(
                (price_weight + coalesce(non_price_weight, lit(0.0))) > 0,
                price_weight * lit(100.0) / (price_weight + coalesce(non_price_weight, lit(0.0))),
            ),
        )
        .withColumn(
            "result_date",
            when(
                safe_col(cpv_df, "noticeType", "string") == lit("TenderResultNotice"),
                to_date(safe_col(cpv_df, "publicationDate", "string")),
            ),
        )
        .withColumn(
            "init_date",
            when(
                safe_col(cpv_df, "noticeType", "string").isin(
                    "ContractNotice",
                    "ContractOrOrderNotice",
                    "SmallContractNotice",
                ),
                to_date(safe_col(cpv_df, "publicationDate", "string")),
            ),
        )
        .withColumn(
            "execution_completion_date",
            when(
                safe_col(cpv_df, "noticeType", "string") == lit("ContractPerformingNotice"),
                coalesce(
                    to_date(safe_col(cpv_df, "htmlExtracted.contract_execution.execution_end_date", "string")),
                    to_date(safe_col(cpv_df, "publicationDate", "string")),
                ),
            ),
        )
        .withColumn(
            "executionDelayed",
            coalesce(
                safe_col(cpv_df, "executionDelayed", "boolean"),
                when(
                    safe_col(cpv_df, "htmlExtracted.contract_execution.executed_on_time", "boolean").isNotNull(),
                    ~safe_col(cpv_df, "htmlExtracted.contract_execution.executed_on_time", "boolean"),
                ),
            ),
        )
        .withColumn(
            "single_bid_proxy",
            when(
                safe_col(cpv_df, "noticeType", "string") == lit("TenderResultNotice"),
                (
                    lower(concat_ws(" ", safe_col(cpv_df, "procedureResultParsed", "array<string>"))).rlike(
                        single_bid_text_expr
                    )
                    | (
                        lowest_bid.isNotNull()
                        & highest_bid.isNotNull()
                        & (lowest_bid == highest_bid)
                    )
                ),
            ),
        )
        .withColumn("result_country_known", size(col("contractor_countries_norm")) > 0)
        .withColumn(
            "result_domestic_flag",
            when(col("result_country_known"), expr(domestic_country_expr)).otherwise(lit(None).cast("boolean")),
        )
        .withColumn("result_flag", safe_col(cpv_df, "noticeType", "string") == lit("TenderResultNotice"))
        .withColumn("execution_flag", safe_col(cpv_df, "noticeType", "string") == lit("ContractPerformingNotice"))
        .withColumn("update_flag", safe_col(cpv_df, "noticeType", "string") == lit("NoticeUpdateNotice"))
        .withColumn(
            "executionRiskFlag",
            coalesce(
                safe_col(cpv_df, "executionRiskFlag", "boolean"),
                when(
                    safe_col(cpv_df, "noticeType", "string") == lit("ContractPerformingNotice"),
                    coalesce(col("executionDelayed"), lit(False))
                    | (coalesce(col("paid_ratio_effective"), lit(0.0)) > lit(1.05))
                    | (
                        coalesce(
                            safe_col(cpv_df, "htmlExtracted.contract_execution.num_changes", "long"),
                            lit(0),
                        )
                        > lit(0)
                    )
                    | (
                        safe_col(cpv_df, "htmlExtracted.contract_execution.executed_properly", "boolean")
                        == lit(False)
                    ),
                ),
            ),
        )
        .drop("contractors_struct")
    )


def _lots_facts(df: DataFrame, target_date: str) -> DataFrame:
    cpv_df = _with_cpv_groups(df).withColumn("target_date", lit(target_date))
    has_lots = has_field(cpv_df, "htmlExtracted.lots")

    base_cols = [
        col("target_date"),
        safe_col(cpv_df, "caseId", "string").alias("caseId"),
        safe_col(cpv_df, "objectId", "string").alias("objectId"),
        safe_col(cpv_df, "organizationId", "string").alias("organizationId"),
        safe_col(cpv_df, "noticeType", "string").alias("noticeType"),
        safe_col(cpv_df, "publicationDate", "string").alias("publicationDate"),
        col("cpv_primary"),
        col("cpv_2digit"),
        col("cpv_3digit"),
        safe_col(cpv_df, "contractorNameNormalized", "array<string>").alias("contractorNameNormalized"),
    ]

    if has_lots:
        lots_df = (
            cpv_df.withColumn("lot", explode_outer(safe_col(cpv_df, "htmlExtracted.lots", "array<struct<lot_id:string>>")))
            .select(
                *base_cols,
                col("lot.lot_id").alias("lot_id"),
                col("lot.contract_value").cast("double").alias("lot_contract_value"),
                col("lot.estimated_value").cast("double").alias("lot_estimated_value"),
                col("lot.lowest_bid").cast("double").alias("lot_lowest_bid"),
                col("lot.highest_bid").cast("double").alias("lot_highest_bid"),
                col("lot.winning_bid").cast("double").alias("lot_winning_bid"),
                col("lot.winner").alias("lot_winner"),
                lit(False).alias("lot_fallback_from_notice"),
            )
            .withColumn(
                "is_status_lot",
                col("lot_contract_value").isNull()
                & col("lot_estimated_value").isNull()
                & col("lot_lowest_bid").isNull()
                & col("lot_highest_bid").isNull()
                & col("lot_winning_bid").isNull(),
            )
        )
    else:
        lots_df = cpv_df.select(
            *base_cols,
            lit(None).cast("string").alias("lot_id"),
            safe_col(cpv_df, "htmlExtracted.values.contract_value", "double").alias("lot_contract_value"),
            safe_col(cpv_df, "htmlExtracted.values.estimated_value", "double").alias("lot_estimated_value"),
            safe_col(cpv_df, "htmlExtracted.values.lowest_bid", "double").alias("lot_lowest_bid"),
            safe_col(cpv_df, "htmlExtracted.values.highest_bid", "double").alias("lot_highest_bid"),
            safe_col(cpv_df, "htmlExtracted.values.winning_bid", "double").alias("lot_winning_bid"),
            lit(None).cast("string").alias("lot_winner"),
            lit(True).alias("lot_fallback_from_notice"),
            lit(False).alias("is_status_lot"),
        )

    return lots_df.withColumn(
        "contractor_key",
        coalesce(
            col("lot_winner"),
            when(size(col("contractorNameNormalized")) > 0, col("contractorNameNormalized")[0]),
        ),
    ).withColumn("lot_value_for_concentration", coalesce(col("lot_contract_value"), col("lot_winning_bid")))


def build_gold_case_mart(df_silver: DataFrame, target_date: str) -> DataFrame:
    facts = _base_notice_facts(df_silver, target_date)
    grouped = (
        facts.groupBy("caseId")
        .agg(
            first(safe_col(facts, "organizationId", "string"), ignorenulls=True).alias("buyer_id"),
            spark_min("publication_date").alias("first_publicationDate"),
            spark_max("publication_date").alias("last_publicationDate"),
            count(lit(1)).alias("num_notices"),
            spark_sum(when(col("update_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("num_updates"),
            spark_max(when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_init"),
            spark_max(when(col("noticeType") == lit("TenderResultNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_result"),
            spark_max(when(col("noticeType") == lit("ContractPerformingNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_execution"),
            spark_min("init_date").alias("first_init_date"),
            spark_min("result_date").alias("first_result_date"),
            spark_min("execution_completion_date").alias("first_execution_completion_date"),
            spark_sum(when(safe_col(facts, "deadlineChanged", "boolean"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("deadline_changed_count"),
            spark_sum(when(safe_col(facts, "criteriaChanged", "boolean"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("criteria_changed_count"),
            spark_sum(when(safe_col(facts, "scopeChanged", "boolean"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("scope_changed_count"),
            spark_max(when(safe_col(facts, "executionDelayed", "boolean"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_delayed_any"),
            spark_max(when(safe_col(facts, "executionRiskFlag", "boolean"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_risk_any"),
            spark_max(col("paid_ratio_effective")).alias("paid_ratio_max"),
            percentile_approx(col("paid_ratio_effective"), 0.5, 1000).alias("paid_ratio_median"),
            spark_sum(col("contract_value")).alias("contract_value_sum"),
            spark_sum(col("winning_bid_value")).alias("winning_bid_sum"),
            spark_sum(col("estimated_value")).alias("estimated_value_sum"),
            spark_sum(col("total_paid_value")).alias("total_paid_sum"),
        )
        .withColumn(
            "time_to_award_days",
            when(
                col("first_init_date").isNotNull() & col("first_result_date").isNotNull(),
                datediff(col("first_result_date"), col("first_init_date")),
            ),
        )
        .withColumn(
            "award_to_completion_days",
            when(
                col("first_result_date").isNotNull() & col("first_execution_completion_date").isNotNull(),
                datediff(col("first_execution_completion_date"), col("first_result_date")),
            ),
        )
        .withColumn("target_date", lit(target_date))
        .drop("first_init_date", "first_result_date", "first_execution_completion_date")
    )
    return grouped


def build_gold_buyer_mart(df_silver: DataFrame, target_date: str) -> DataFrame:
    facts = _base_notice_facts(df_silver, target_date).withColumn(
        "nuts3_code", safe_col(df_silver, "htmlExtracted.nuts3_code", "string")
    )
    lots = _lots_facts(df_silver, target_date).filter(col("noticeType") == lit("TenderResultNotice"))

    contractor_values = (
        lots.filter(col("contractor_key").isNotNull() & col("lot_value_for_concentration").isNotNull())
        .groupBy("organizationId", "contractor_key")
        .agg(spark_sum("lot_value_for_concentration").alias("contractor_value"))
    )
    buyer_totals = contractor_values.groupBy("organizationId").agg(
        spark_sum("contractor_value").alias("total_award_value")
    )
    concentration = (
        contractor_values.join(buyer_totals, on="organizationId", how="left")
        .withColumn(
            "value_share",
            when(col("total_award_value") > 0, col("contractor_value") / col("total_award_value")),
        )
        .groupBy("organizationId")
        .agg(
            spark_max("value_share").alias("concentration_top1_share"),
            spark_sum(expr("pow(value_share, 2)")).alias("hhi"),
            first("total_award_value", ignorenulls=True).alias("value_total"),
        )
    )

    buyer = (
        facts.groupBy("organizationId")
        .agg(
            first("provinceName", ignorenulls=True).alias("provinceName"),
            first("nuts3_code", ignorenulls=True).alias("nuts3_code"),
            first(safe_col(facts, "clientTypeName", "string"), ignorenulls=True).alias("clientTypeName"),
            first(safe_col(facts, "organizationNationalId", "string"), ignorenulls=True).alias("organizationNationalId"),
            count(lit(1)).alias("notices_total"),
            countDistinct("caseId").alias("cases_total"),
            spark_sum(when(col("result_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("results_total"),
            spark_sum(when(col("execution_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("executions_total"),
            avg(when(col("result_flag"), col("single_bid_proxy").cast("double"))).alias("single_bid_rate"),
            percentile_approx(
                when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), col("biddingWindowDays")),
                0.5,
                1000,
            ).alias("biddingWindowDays_median"),
            percentile_approx(
                when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), col("priceWeight")),
                0.5,
                1000,
            ).alias("priceWeight_raw_median"),
            percentile_approx(
                when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), col("price_weight_ratio")),
                0.5,
                1000,
            ).alias("priceWeight_median"),
            avg(
                when(
                    col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice")
                    & col("priceWeight").isNotNull(),
                    lit(1.0),
                ).otherwise(lit(0.0))
            ).alias("priceWeight_detected_pct"),
            avg(when(col("execution_flag"), col("executionDelayed").cast("double"))).alias("delayed_pct"),
            percentile_approx(when(col("execution_flag"), col("paid_ratio_effective")), 0.5, 1000).alias("paidRatio_median"),
            spark_sum(when(col("update_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("updates_total"),
            spark_sum(when(col("result_flag") & col("result_country_known"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("results_country_known_total"),
            spark_sum(when(col("result_flag") & col("result_domestic_flag"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("results_domestic_total"),
            spark_sum(when(col("result_flag") & col("result_country_known"), col("contract_value"))).alias(
                "result_value_country_known"
            ),
            spark_sum(when(col("result_flag") & col("result_domestic_flag"), col("contract_value"))).alias(
                "result_value_domestic"
            ),
        )
        .join(concentration, on="organizationId", how="left")
        .withColumn(
            "contracts_domestic_share",
            when(col("results_country_known_total") > 0, col("results_domestic_total") / col("results_country_known_total")),
        )
        .withColumn(
            "value_domestic_share",
            when(col("result_value_country_known") > 0, col("result_value_domestic") / col("result_value_country_known")),
        )
        .withColumn("target_date", lit(target_date))
    )
    return buyer


def build_gold_market_mart(df_silver: DataFrame, target_date: str) -> DataFrame:
    facts = _base_notice_facts(df_silver, target_date)
    lots = _lots_facts(df_silver, target_date)
    market_base = facts.groupBy("cpv_2digit").agg(
        countDistinct("caseId").alias("cases_total"),
        spark_sum(when(col("result_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("results_total"),
    )
    market_values = lots.groupBy("cpv_2digit").agg(
        spark_sum("lot_contract_value").alias("value_total"),
        spark_sum("lot_winning_bid").alias("winning_bid_total"),
        spark_sum("lot_estimated_value").alias("estimated_value_total"),
        percentile_approx("lot_winning_bid", 0.5, 1000).alias("winning_bid_median"),
        percentile_approx("lot_contract_value", 0.5, 1000).alias("contract_value_median"),
    )
    contractor_values = (
        lots.filter(col("contractor_key").isNotNull() & col("lot_value_for_concentration").isNotNull())
        .groupBy("cpv_2digit", "contractor_key")
        .agg(spark_sum("lot_value_for_concentration").alias("contractor_value"))
    )
    totals = contractor_values.groupBy("cpv_2digit").agg(spark_sum("contractor_value").alias("total_value"))
    concentration = (
        contractor_values.join(totals, on="cpv_2digit", how="left")
        .withColumn("share", when(col("total_value") > 0, col("contractor_value") / col("total_value")))
        .groupBy("cpv_2digit")
        .agg(
            spark_max("share").alias("top1_share"),
            spark_sum(expr("pow(share, 2)")).alias("hhi"),
        )
    )
    return (
        market_base.join(market_values, on="cpv_2digit", how="left")
        .join(concentration, on="cpv_2digit", how="left")
        .withColumn("target_date", lit(target_date))
    )


def build_gold_signals_buyer_daily(df_silver: DataFrame, target_date: str) -> DataFrame:
    buyer = build_gold_buyer_mart(df_silver, target_date)
    return buyer.select(
        "target_date",
        col("organizationId").alias("buyer_id"),
        col("notices_total"),
        col("cases_total"),
        col("results_total"),
        col("updates_total"),
        col("single_bid_rate").alias("single_bid_rate_today"),
        col("value_total").alias("value_today"),
        col("hhi").alias("hhi_today"),
        when(col("cases_total") > 0, col("updates_total") / col("cases_total")).alias("update_intensity_today"),
    )
