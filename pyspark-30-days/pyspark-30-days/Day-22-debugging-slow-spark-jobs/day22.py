"""
Day 22 - Debugging Slow Spark Jobs

Topics:
  - Systematic checklist for slow jobs
  - Reading explain(formatted) and the Spark UI
  - Detecting too many / too few partitions
  - Repeated recomputation & missing cache
  - Skew & shuffle spills
  - Small files problem
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day22")

import time

def timed(label, fn):
    t = time.time(); fn(); print(f"{label:<45} {time.time() - t:.2f}s")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)

# ---------- Step 1: give jobs readable names in the Spark UI ----------
spark.sparkContext.setJobDescription("Day22 - revenue by region")
sales.groupBy("region").agg(F.sum(F.col("qty") * F.col("price"))).collect()

# ---------- Step 2: read the plan ----------
sales.groupBy("region").count().explain(mode="formatted")

# ---------- Step 3: check partition health ----------
n = sales.rdd.getNumPartitions()
sizes = sales.rdd.glom().map(len).collect()
print(f"partitions={n}, rows/partition min={min(sizes)} max={max(sizes)}")   # big gap = skew

# ---------- Symptom A: too many tiny partitions ----------
tiny = sales.repartition(200)
timed("count with 200 tiny partitions", lambda: tiny.count())
timed("count with 4 partitions", lambda: sales.repartition(4).count())

# ---------- Symptom B: recomputing an expensive lineage ----------
heavy = sales.withColumn("rev", F.col("qty") * F.col("price")).groupBy("customer_id").agg(F.sum("rev").alias("rev"))
timed("3 actions WITHOUT cache", lambda: [heavy.count(), heavy.count(), heavy.count()])
heavy.cache(); heavy.count()
timed("3 actions WITH cache", lambda: [heavy.count(), heavy.count(), heavy.count()])
heavy.unpersist()

# ---------- Symptom C: Python UDF where a built-in exists ----------
from pyspark.sql.types import DoubleType
slow_udf = F.udf(lambda q, p: float(q * p), DoubleType())
timed("Python UDF", lambda: sales.select(slow_udf("qty", "price")).count())
timed("Built-in expression", lambda: sales.select(F.col("qty") * F.col("price")).count())

# ---------- Symptom D: collect() / toPandas() on big data => driver OOM ----------
# use .limit(), .take(), or aggregate first.

# ---------- Debugging checklist ----------
# 1. Spark UI -> Stages: which stage is slowest? Any straggler task (skew)?
# 2. Shuffle read/write size & spill (memory/disk) high?  -> more/better partitions, AQE
# 3. explain(): SortMergeJoin on a small table? -> broadcast
# 4. Same DataFrame used in many actions? -> cache
# 5. Many small input files? -> compact them / coalesce
# 6. UDFs? -> built-ins or pandas_udf

spark.stop()
