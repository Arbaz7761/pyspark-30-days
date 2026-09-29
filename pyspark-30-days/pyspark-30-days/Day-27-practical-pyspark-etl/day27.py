"""
Day 27 - Practical PySpark ETL

Topics:
  - Extract -> Transform -> Load structure
  - Cleaning messy real-world CSV
  - Standardising text, types, dates
  - Validating with regex
  - Reusable functions & logging
  - Writing curated output
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day27")

import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("etl")

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

# ---------------- EXTRACT ----------------
def extract(path):
    df = spark.read.csv(path, header=True)          # everything as string first (safe)
    log.info("Extracted %d rows", df.count())
    return df

# ---------------- TRANSFORM ----------------
def transform(df):
    cleaned = (df
        .withColumn("customer_email", F.lower(F.trim("customer_email")))
        .withColumn("customer_email", F.when(F.col("customer_email") == "", None).otherwise(F.col("customer_email")))
        .withColumn("country", F.initcap(F.trim("country")))
        .withColumn("amount", F.expr("try_cast(amount AS double)"))                 # 'abc' -> NULL (try_cast never throws)
        .withColumn("order_date", F.expr("try_to_date(order_date, 'yyyy-MM-dd')"))  # invalid -> NULL (try_to_date never throws)
        .withColumn("order_id", F.col("order_id").cast("int"))
        .dropDuplicates())
    flagged = (cleaned
        .withColumn("valid_email", F.col("customer_email").rlike(EMAIL_RE))
        .withColumn("valid_amount", F.col("amount") > 0)
        .withColumn("valid_date", F.col("order_date").isNotNull())
        .withColumn("is_valid", F.col("valid_email") & F.col("valid_amount") & F.col("valid_date")))
    return flagged

# ---------------- LOAD ----------------
def load(df, path):
    good = df.filter("is_valid").drop("valid_email", "valid_amount", "valid_date", "is_valid")
    bad = df.filter(~F.col("is_valid") | F.col("is_valid").isNull())
    good.write.mode("overwrite").parquet(f"{path}/clean")
    bad.write.mode("overwrite").parquet(f"{path}/rejected")
    log.info("Loaded clean=%d rejected=%d", good.count(), bad.count())
    return good, bad

raw = extract(f"{DATA}/raw_orders_dirty.csv")
flagged = transform(raw)
good, bad = load(flagged, f"{OUT}/day27_etl")

print("---- CLEAN ----");    good.show(truncate=False)
print("---- REJECTED ----"); bad.show(truncate=False)

spark.stop()
