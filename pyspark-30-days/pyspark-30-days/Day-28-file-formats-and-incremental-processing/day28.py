"""
Day 28 - File Formats & Incremental Processing

Topics:
  - CSV vs JSON vs Parquet vs ORC (size, schema, speed)
  - Choosing the right format
  - Incremental load with high-water mark
  - Append new data + dedupe on key (latest wins)
  - Idempotent pipelines
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day28")

import os

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)

# ---------- Part 1: format comparison ----------
formats = {"csv": lambda w, p: w.option("header", True).csv(p),
           "json": lambda w, p: w.json(p),
           "parquet": lambda w, p: w.parquet(p),
           "orc": lambda w, p: w.orc(p)}

def dir_size(p):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(p) for f in fs)

for name, writer in formats.items():
    path = f"{OUT}/day28_{name}"
    writer(sales.coalesce(1).write.mode("overwrite"), path)
    print(f"{name:<8} {dir_size(path)/1024:8.1f} KB")

# CSV/JSON : human readable, row based, no/weak schema, slow
# Parquet  : columnar, compressed, schema + stats, best for analytics (default choice)
# ORC      : columnar, great with Hive
# Delta    : Parquet + ACID transactions + time travel (Databricks)

# ---------- Part 2: incremental processing ----------
target = f"{OUT}/day28_incremental"
sales = sales.withColumn("order_date", F.to_date("order_date"))

# Batch 1 : initial full load (first 6 months)
batch1 = sales.filter("order_date < '2025-07-01'").withColumn("updated_at", F.lit("2025-06-30").cast("date"))
batch1.write.mode("overwrite").parquet(target)
print("after batch 1:", spark.read.parquet(target).count())

# Batch 2 : only records newer than the high-water mark (+ some updated rows)
hwm = spark.read.parquet(target).agg(F.max("updated_at")).first()[0]
print("high-water mark:", hwm)
new_rows = sales.filter("order_date >= '2025-07-01' AND order_date < '2025-08-01'").withColumn("updated_at", F.lit("2025-07-31").cast("date"))
changed = (spark.read.parquet(target).limit(5).withColumn("status", F.lit("Refunded")).withColumn("updated_at", F.lit("2025-07-31").cast("date")))
batch2 = new_rows.unionByName(changed)

# merge: existing + incoming -> keep latest version per order_id
existing = spark.read.parquet(target).cache(); existing.count()
w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
merged = (existing.unionByName(batch2).withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))
merged.write.mode("overwrite").parquet(target + "_v2")      # write to new path (avoid overwriting what we read from)

final = spark.read.parquet(target + "_v2")
print("after batch 2:", final.count(), "| refunded rows:", final.filter("status = 'Refunded'").count())

# Idempotency: running batch 2 again gives the same result because we dedupe on the key.

spark.stop()
