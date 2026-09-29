"""
Day 18 - Repartition & Coalesce

Topics:
  - What partitions are and why they matter
  - getNumPartitions, spark_partition_id
  - repartition (full shuffle, increase/decrease)
  - coalesce (no shuffle, only decrease)
  - repartition by column
  - Rules of thumb for partition sizing
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day18")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
print("default partitions:", sales.rdd.getNumPartitions())

# repartition -> full shuffle, can increase or decrease, evenly balances
big = sales.repartition(8)
print("after repartition(8):", big.rdd.getNumPartitions())

# coalesce -> merges existing partitions, NO shuffle, only decreases
small = big.coalesce(2)
print("after coalesce(2)   :", small.rdd.getNumPartitions())

# see how rows are spread across partitions
big.groupBy(F.spark_partition_id().alias("partition_id")).count().orderBy("partition_id").show()
small.groupBy(F.spark_partition_id().alias("partition_id")).count().orderBy("partition_id").show()

# repartition by column: same key -> same partition (useful before joins / partitioned writes)
by_region = sales.repartition(4, "region")
by_region.groupBy(F.spark_partition_id().alias("pid"), "region").count().orderBy("pid").show()

# effect on output files: number of partitions == number of part-files
big.write.mode("overwrite").parquet(f"{OUT}/day18_8_files")
small.write.mode("overwrite").parquet(f"{OUT}/day18_2_files")
import os
print("files (8 parts):", len([f for f in os.listdir(f"{OUT}/day18_8_files") if f.endswith(".parquet")]))
print("files (2 parts):", len([f for f in os.listdir(f"{OUT}/day18_2_files") if f.endswith(".parquet")]))

# Rules of thumb
# - Aim for ~128 MB per partition, 2-4 partitions per CPU core
# - repartition() to increase parallelism / fix skew ; coalesce() to reduce files before writing
# - spark.sql.shuffle.partitions (default 200) controls partitions after joins / groupBy

spark.stop()
