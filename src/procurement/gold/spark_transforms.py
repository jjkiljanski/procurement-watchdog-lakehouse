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
    return (
        cpv_df.withColumn("target_date", lit(target_date))
        .withColumn("publication_date", to_date(safe_col(cpv_df, "publicationDate", "string")))
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
            "single_bid_proxy",
            when(
                safe_col(cpv_df, "noticeType", "string") == lit("TenderResultNotice"),
                lower(concat_ws(" ", safe_col(cpv_df, "procedureResultParsed", "array<string>"))).rlike(
                    "jedn[ąa]\\s+ofert|one\\s+offer|1\\s+ofert"
                ),
            ),
        )
        .withColumn("result_flag", safe_col(cpv_df, "noticeType", "string") == lit("TenderResultNotice"))
        .withColumn("execution_flag", safe_col(cpv_df, "noticeType", "string") == lit("ContractPerformingNotice"))
        .withColumn("update_flag", safe_col(cpv_df, "noticeType", "string") == lit("NoticeUpdateNotice"))
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
            spark_max(safe_col(facts, "paidRatio", "double")).alias("paid_ratio_max"),
            percentile_approx(safe_col(facts, "paidRatio", "double"), 0.5, 1000).alias("paid_ratio_median"),
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
        facts.groupBy("organizationId", "provinceName", "nuts3_code")
        .agg(
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
            ).alias("priceWeight_median"),
            avg(
                when(
                    col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice")
                    & col("priceWeight").isNotNull(),
                    lit(1.0),
                ).otherwise(lit(0.0))
            ).alias("priceWeight_detected_pct"),
            avg(when(col("execution_flag"), col("executionDelayed").cast("double"))).alias("delayed_pct"),
            percentile_approx(when(col("execution_flag"), col("paidRatio")), 0.5, 1000).alias("paidRatio_median"),
            spark_sum(when(col("update_flag"), lit(1)).otherwise(lit(0))).cast("long").alias("updates_total"),
        )
        .join(concentration, on="organizationId", how="left")
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
