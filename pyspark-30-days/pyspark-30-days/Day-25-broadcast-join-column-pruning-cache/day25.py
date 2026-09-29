"""
Day 25 - Broadcast Join, Column Pruning & Cache

Topics:
  - Broadcast join in practice
  - Column pruning & predicate pushdown proof via explain
  - Cache the right thing at the right point
  - Combining the three in one pipeline
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day25")

import time

sales_csv = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
sales_csv.write.mode("overwrite").parquet(f"{OUT}/day25_sales")
cust = spark.read.csv(f"{DATA}/customers.csv", header=True, inferSchema=True)

sales = spark.read.parquet(f"{OUT}/day25_sales")

# ---- 1. Column pruning: select only what you need (parquet reads only those columns) ----
sales.select("order_id", "qty").explain()            # ReadSchema: struct<order_id, qty>
sales.explain()                                      # ReadSchema: ALL columns

# ---- 2. Predicate pushdown ----
sales.filter("region = 'South' AND qty >= 3").explain()   # PushedFilters: [...]

# ---- 3. Broadcast join ----
enriched = (sales.filter("status = 'Completed'")
                 .select("order_id", "customer_id", "qty", "price")
                 .join(F.broadcast(cust.select("customer_id", "customer_name", "segment")), "customer_id")
                 .withColumn("revenue", F.col("qty") * F.col("price")))
enriched.explain()                                   # BroadcastHashJoin

# ---- 4. Cache the enriched result because several reports reuse it ----
enriched.cache()
enriched.count()                                     # materialise once

t = time.time()
enriched.groupBy("segment").agg(F.sum("revenue").alias("revenue")).show()
enriched.groupBy("customer_name").agg(F.sum("revenue").alias("revenue")).orderBy(F.desc("revenue")).show(5)
enriched.agg(F.countDistinct("customer_id")).show()
print(f"3 reports from cache: {time.time() - t:.2f}s")

enriched.unpersist()

# Pipeline order that works well:
# read (prune columns, push filters) -> filter early -> broadcast small dims -> cache reused result -> write

spark.stop()
