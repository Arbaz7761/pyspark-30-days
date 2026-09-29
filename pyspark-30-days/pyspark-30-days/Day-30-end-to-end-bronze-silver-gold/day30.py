"""
Day 30 - End-to-End Pipeline: Bronze → Silver → Gold

Topics:
  - Medallion architecture
  - Bronze: raw + metadata, append only
  - Silver: cleaned, typed, deduped, validated
  - Gold: business-level aggregates
  - Reusable layer functions
  - Ready to port to Databricks / Delta
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day30")

# Layout: output/day30_medallion/{bronze,silver,gold}
BASE = f"{OUT}/day30_medallion"

# ============ BRONZE : raw data as-is + ingestion metadata ============
def build_bronze():
    raw = spark.read.csv(f"{DATA}/sales.csv", header=True)          # all strings, nothing dropped
    bronze = (raw.withColumn("_ingest_ts", F.current_timestamp())
                 .withColumn("_source_file", F.input_file_name() if hasattr(F, "input_file_name") else F.lit("sales.csv")))
    bronze.write.mode("overwrite").parquet(f"{BASE}/bronze/sales")
    print("BRONZE rows:", bronze.count())

# ============ SILVER : clean, typed, deduped, validated ============
def build_silver():
    bronze = spark.read.parquet(f"{BASE}/bronze/sales")
    cust = spark.read.csv(f"{DATA}/customers.csv", header=True, inferSchema=True)
    silver = (bronze
        .withColumn("order_id", F.col("order_id").cast("int"))
        .withColumn("customer_id", F.col("customer_id").cast("int"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("region", F.initcap(F.trim("region")))
        .withColumn("revenue", F.col("qty") * F.col("price"))
        .filter("order_id IS NOT NULL AND qty > 0 AND price > 0 AND order_date IS NOT NULL")
        .dropDuplicates(["order_id"])
        .join(F.broadcast(cust.select("customer_id", "customer_name", "segment")), "customer_id", "left")
        .drop("_source_file"))
    (silver.write.mode("overwrite").partitionBy("region").parquet(f"{BASE}/silver/sales"))
    print("SILVER rows:", silver.count())

# ============ GOLD : business aggregates ============
def build_gold():
    s = spark.read.parquet(f"{BASE}/silver/sales").filter("status = 'Completed'")

    monthly = (s.groupBy(F.trunc("order_date", "month").alias("month"), "region")
                .agg(F.sum("revenue").alias("revenue"), F.count("*").alias("orders"),
                     F.countDistinct("customer_id").alias("customers")))
    monthly.write.mode("overwrite").parquet(f"{BASE}/gold/monthly_revenue")

    w = Window.partitionBy("category").orderBy(F.col("revenue").desc())
    top_products = (s.groupBy("category", "product").agg(F.sum("revenue").alias("revenue"))
                     .withColumn("rank", F.dense_rank().over(w)).filter("rank <= 2"))
    top_products.write.mode("overwrite").parquet(f"{BASE}/gold/top_products")

    seg = s.groupBy("segment").agg(F.sum("revenue").alias("revenue"), F.avg("revenue").alias("avg_order_value"))
    seg.write.mode("overwrite").parquet(f"{BASE}/gold/segment_summary")
    print("GOLD tables written")

# ============ RUN PIPELINE ============
build_bronze()
build_silver()
build_gold()

print("---- Monthly revenue (gold) ----")
spark.read.parquet(f"{BASE}/gold/monthly_revenue").orderBy("month", "region").show(8)
print("---- Top products per category (gold) ----")
spark.read.parquet(f"{BASE}/gold/top_products").orderBy("category", "rank").show()
print("---- Segment summary (gold) ----")
spark.read.parquet(f"{BASE}/gold/segment_summary").show()

# On Databricks: replace .parquet(...) with .format("delta").save(...) / saveAsTable("bronze.sales") etc.

spark.stop()
