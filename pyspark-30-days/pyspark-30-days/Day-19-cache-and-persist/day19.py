"""
Day 19 - Cache & Persist

Topics:
  - Why cache: avoid recomputation of lineage
  - cache() vs persist(StorageLevel)
  - Storage levels
  - unpersist
  - When NOT to cache
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day19")

import time
from pyspark import StorageLevel

sales = (spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
         .withColumn("revenue", F.col("qty") * F.col("price"))
         .filter("status = 'Completed'"))

def timed(label, fn):
    t = time.time(); r = fn(); print(f"{label:<28} {time.time() - t:.3f}s  -> {r}")

# WITHOUT cache: file is re-read & re-transformed for every action
timed("count (no cache)", lambda: sales.count())
timed("sum  (no cache)", lambda: sales.agg(F.sum("revenue")).first()[0])

# cache() = persist(MEMORY_AND_DISK). Lazy! Data is stored on the first action.
sales.cache()
timed("count (fills cache)", lambda: sales.count())
timed("sum  (from cache)", lambda: sales.agg(F.sum("revenue")).first()[0])
timed("max  (from cache)", lambda: sales.agg(F.max("revenue")).first()[0])

print("is_cached:", sales.is_cached, "| level:", sales.storageLevel)
sales.explain()          # look for InMemoryTableScan in the plan

sales.unpersist()        # free memory when done
print("is_cached after unpersist:", sales.is_cached)

# persist with explicit levels
s2 = sales.persist(StorageLevel.DISK_ONLY)
s2.count()
print("level:", s2.storageLevel)
s2.unpersist()

# Guidelines
# + cache a DataFrame that is reused by 2+ actions / iterative logic
# - don't cache things used once, or huge data that doesn't fit memory
# - always unpersist when finished

spark.stop()
