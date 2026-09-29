"""
Day 24 - Parquet & Partition Pruning

Topics:
  - Row vs columnar formats
  - Writing & reading Parquet
  - partitionBy on write
  - Partition pruning in explain()
  - Choosing partition columns (low cardinality)
  - Compression
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day24")

import os

sales = (spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
         .withColumn("order_date", F.to_date("order_date"))
         .withColumn("year", F.year("order_date"))
         .withColumn("month", F.month("order_date")))

# ---- write plain parquet ----
sales.write.mode("overwrite").parquet(f"{OUT}/day24_plain")

# ---- write partitioned parquet: folder per value ----
(sales.write.mode("overwrite")
      .partitionBy("region", "month")
      .option("compression", "snappy")
      .parquet(f"{OUT}/day24_partitioned"))
print(sorted(os.listdir(f"{OUT}/day24_partitioned"))[:6])

# ---- compare on-disk size CSV vs Parquet ----
def dir_size(p):
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(p) for f in fs)
csv_size = os.path.getsize(f"{DATA}/sales.csv")
print(f"CSV: {csv_size/1024:.1f} KB | Parquet: {dir_size(OUT + '/day24_plain')/1024:.1f} KB")

# ---- partition pruning: Spark only reads matching folders ----
part = spark.read.parquet(f"{OUT}/day24_partitioned")
q = part.filter((F.col("region") == "North") & (F.col("month") == 3))
q.explain()                        # check: PartitionFilters: [region = North, month = 3]
print("rows:", q.count())

# no pruning when filtering on a non-partition column (still uses column pruning + predicate pushdown)
part.filter("qty > 3").select("order_id").explain()      # check: PushedFilters, ReadSchema

# ---- schema is stored inside parquet ----
part.printSchema()

# ---- overwrite only specific partitions (dynamic mode) ----
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(sales.filter("region = 'North'").write.mode("overwrite")
      .partitionBy("region", "month").parquet(f"{OUT}/day24_partitioned"))

# Guidelines
# - partition by columns used in filters and with LOW cardinality (date, region) - not IDs
# - too many partitions => small-files problem

spark.stop()
