"""Build silver layer from raw BZP JSON using PySpark.

Reads   data/raw/bzp_YYYY-MM-DD.json
Writes  data/silver/bzp_YYYY-MM-DD.parquet
"""

import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
# Also propagate to Spark worker processes via PYTHONPATH
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    raw_path = Path("data/raw") / f"bzp_{target_date}.json"
    if not raw_path.exists():
        log.error("Raw file not found: %s", raw_path)
        sys.exit(1)

    from pyspark.sql import SparkSession

    from procurement.silver.spark_transforms import build_silver

    spark = (
        SparkSession.builder.appName("bzp-silver")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df_raw = spark.read.json(str(raw_path), multiLine=True)
        log.info("Loaded %d raw records", df_raw.count())

        df_silver = build_silver(df_raw)

        out_path = str(Path("data/silver") / f"bzp_{target_date}.parquet")
        df_silver.coalesce(1).write.mode("overwrite").parquet(out_path)

        log.info("Wrote %d silver records to %s", df_silver.count(), out_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
