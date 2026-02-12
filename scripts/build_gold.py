"""Build gold layer from silver Parquet using PySpark.

Reads   data/silver/bzp_YYYY-MM-DD.parquet
Writes  data/gold/...

TODO: Define gold-level aggregations and business transforms.
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_path = Path("data/silver") / f"bzp_{target_date}.parquet"
    if not silver_path.exists():
        log.error("Silver data not found: %s", silver_path)
        sys.exit(1)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bzp-gold")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df_silver = spark.read.parquet(str(silver_path))
        log.info("Loaded %d silver records", df_silver.count())

        # TODO: gold-level transforms (aggregations, denormalization, etc.)
        out_dir = Path("data/gold")
        out_dir.mkdir(parents=True, exist_ok=True)

        log.info("Gold layer placeholder — no transforms defined yet")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
