"""Common envelope table definition for the Silver layer.

The common_envelope table stores all non-HTML Bronze columns (except the
internal ``recordHash`` field) plus a small set of derived columns:
``clientTypeName``, ``provinceName`` (dictionary lookups), ``caseId``
(coalesce of tenderId/objectId), and ``noticeStage`` (derived from noticeType).
No BeautifulSoup or HTML parsing takes place here.

Public API
----------
ENVELOPE_COLUMNS      Ordered list of all columns written to common_envelope/.
build_envelope_df     Spark transform: Bronze batch → envelope DataFrame.
CommonEnvelopeRow     Pydantic model (one row); documents the expected schema.
validate_envelope_schema  Driver-side column-presence check; warns on drift.
"""

from __future__ import annotations

import logging
from typing import Optional

from pydantic import BaseModel

log = logging.getLogger(__name__)


# ── Column list ───────────────────────────────────────────────────────────────
# All Bronze structured fields except htmlBody (HTML blob) and recordHash
# (internal pipeline hash, not useful downstream), plus noticeStage.

ENVELOPE_COLUMNS: list[str] = [
    # Direct Bronze fields
    "objectId",
    "noticeType",
    "noticeNumber",
    "bzpNumber",
    "publicationDate",
    "publicationDateDay",          # computed from publicationDate in _process_batch
    "cpvCode",
    "isTenderAmountBelowEU",
    "orderObject",
    "clientType",
    "orderType",
    "tenderType",
    "organizationName",
    "organizationCity",
    "organizationCountry",
    "organizationNationalId",
    "organizationId",
    "organizationProvince",
    "tenderId",
    "submittingOffersDate",
    "procedureResult",
    "contractors",
    # Derived: dictionary lookups
    "clientTypeName",              # clientType code → Polish label
    "provinceName",                # organizationProvince code → Polish name
    # Derived: pure Spark expressions
    "caseId",                      # coalesce(tenderId, objectId)
    "noticeStage",                 # INIT / RESULT / EXECUTION / UPDATE
]


# ── Spark transform ───────────────────────────────────────────────────────────

def build_envelope_df(df: "DataFrame") -> "DataFrame":
    """Build the common_envelope DataFrame from a Bronze batch.

    Adds derived columns (dictionary lookups, pure Spark expressions), then
    selects only ENVELOPE_COLUMNS.  The input is expected to already have
    ``publicationDateDay`` (added by ``_process_batch`` before this call).
    No BeautifulSoup or HTML parsing is performed.
    """
    from itertools import chain

    from pyspark.sql.functions import coalesce, col, create_map, lit, when

    from procurement.dictionaries import client_type_names, province_names

    # Dictionary lookups (functions are @lru_cache-decorated, call them to get the dict)
    client_type_map = create_map([lit(x) for x in chain(*client_type_names().items())])
    province_map = create_map([lit(x) for x in chain(*province_names().items())])
    df = df.withColumn("clientTypeName", client_type_map[col("clientType")])
    df = df.withColumn("provinceName", province_map[col("organizationProvince")])

    # Pure Spark expressions
    df = df.withColumn("caseId", coalesce(col("tenderId"), col("objectId")))
    df = df.withColumn(
        "noticeStage",
        when(col("noticeType") == lit("TenderResultNotice"), lit("RESULT"))
        .when(col("noticeType") == lit("ContractPerformingNotice"), lit("EXECUTION"))
        .when(col("noticeType").isin("NoticeUpdateNotice", "AgreementUpdateNotice"), lit("UPDATE"))
        .otherwise(lit("INIT")),
    )

    return df.select(*[c for c in ENVELOPE_COLUMNS if c in df.columns])


# ── Pydantic model ────────────────────────────────────────────────────────────

class ContractorItem(BaseModel):
    contractorName: Optional[str] = None
    contractorCity: Optional[str] = None
    contractorProvince: Optional[str] = None
    contractorCountry: Optional[str] = None
    contractorNationalId: Optional[str] = None


class CommonEnvelopeRow(BaseModel):
    # Direct Bronze fields
    objectId: str
    noticeType: str
    noticeNumber: str
    bzpNumber: str
    publicationDate: str
    publicationDateDay: Optional[str] = None
    cpvCode: str
    isTenderAmountBelowEU: bool
    orderObject: Optional[str] = None
    clientType: Optional[str] = None
    orderType: Optional[str] = None
    tenderType: Optional[str] = None
    organizationName: str
    organizationCity: str
    organizationCountry: str
    organizationNationalId: str
    organizationId: str
    organizationProvince: Optional[str] = None
    tenderId: Optional[str] = None
    submittingOffersDate: Optional[str] = None
    procedureResult: Optional[str] = None
    contractors: Optional[list[ContractorItem]] = None
    # Derived
    clientTypeName: Optional[str] = None
    provinceName: Optional[str] = None
    caseId: Optional[str] = None
    noticeStage: str


# ── Driver-side schema check ──────────────────────────────────────────────────

def validate_envelope_schema(df: "DataFrame") -> dict:
    """Warn about columns missing from or added to the envelope DataFrame.

    This is a driver-side check (no Spark action) analogous to
    :func:`section_pipeline.final_schema_validator.validate_section_models`.
    Returns a metrics dict suitable for embedding in the run lineage.
    """
    actual = set(df.columns)
    expected = set(ENVELOPE_COLUMNS)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)

    if missing:
        log.warning("common_envelope missing expected columns: %s", missing)

    return {
        "expected_columns": len(expected),
        "actual_columns": len(actual),
        "missing_columns": missing,
        "extra_columns": extra,
    }
