"""
Day 23 - Data Skew, Salting & AQE

Topics:
  - What data skew looks like
  - Detecting skewed keys
  - Salting technique for skewed joins/aggregations
  - Adaptive Query Execution (AQE) & skewJoin
  - Two-stage aggregation
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day23")

# ---- Build a deliberately skewed dataset: 90% of rows have key = 1 ----
big = (spark.range(0, 200_000)
       .withColumn("customer_id", F.when(F.rand(seed=1) < 0.9, F.lit(1)).otherwise((F.rand(seed=2) * 100).cast("int") + 2))
       .withColumn("amount", (F.rand(seed=3) * 1000).cast("int")))
dim = spark.range(1, 102).withColumnRenamed("id", "customer_id").withColumn("name", F.concat(F.lit("cust_"), F.col("customer_id")))

# ---- Detect skew ----
big.groupBy("customer_id").count().orderBy(F.col("count").desc()).show(5)

# ---- Fix 1: Salting for aggregation (two-stage) ----
SALT = 8
salted = big.withColumn("salt", (F.rand(seed=4) * SALT).cast("int"))
stage1 = salted.groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))      # spreads hot key
stage2 = stage1.groupBy("customer_id").agg(F.sum("partial").alias("total"))               # tiny final merge
stage2.orderBy(F.col("total").desc()).show(3)

# ---- Fix 2: Salting for a join ----
salted_big = big.withColumn("salt", (F.rand(seed=5) * SALT).cast("int"))
salt_df = spark.range(SALT).withColumnRenamed("id", "salt")
exploded_dim = dim.crossJoin(salt_df)                                                    # replicate small side
joined = salted_big.join(exploded_dim, ["customer_id", "salt"]).drop("salt")
print("salted join rows:", joined.count())

# ---- Fix 3 (easiest): let AQE handle it ----
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")       # force a shuffle join for the demo
aqe_join = big.join(dim, "customer_id")
print("AQE join rows:", aqe_join.count())
aqe_join.explain()      # look for AdaptiveSparkPlan

# ---- Fix 4: broadcast the small side if it fits ----
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
big.join(F.broadcast(dim), "customer_id").count()

# Summary
# Skew => one task takes far longer than the rest.
# Fixes: AQE skew join -> broadcast -> salting -> isolate hot keys and process separately.

spark.stop()
