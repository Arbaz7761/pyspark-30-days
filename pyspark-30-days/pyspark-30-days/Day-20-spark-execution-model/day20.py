"""
Day 20 - Spark Execution Model

Topics:
  - Driver, executors, cluster manager
  - Transformations (lazy) vs Actions
  - Narrow vs Wide transformations
  - DAG -> Jobs -> Stages -> Tasks
  - explain() and reading physical plans
  - Spark UI at http://localhost:4040
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day20")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)

# ---- Lazy evaluation: nothing runs until an ACTION is called ----
step1 = sales.filter("status = 'Completed'")                                  # transformation (narrow)
step2 = step1.withColumn("revenue", F.col("qty") * F.col("price"))            # transformation (narrow)
step3 = step2.groupBy("region").agg(F.sum("revenue").alias("rev"))            # transformation (WIDE -> shuffle)
print("No job has run so far. Spark only built a plan.")

# ---- Three kinds of plans ----
step3.explain(mode="extended")   # parsed -> analyzed -> optimized logical -> physical

# ---- Action triggers execution ----
step3.show()

# ---- Narrow vs wide ----
# Narrow: filter, select, withColumn, map  -> each output partition depends on ONE input partition (no shuffle)
# Wide  : groupBy, join, distinct, orderBy, repartition -> data moves across the network (shuffle = Exchange)
step3.explain()                  # look for "Exchange hashpartitioning" => shuffle boundary => new stage

# ---- Catalyst optimizations you can see in the plan ----
# * Predicate pushdown: filters pushed into the file scan
# * Column pruning    : only needed columns are read
sales.filter("region = 'North'").select("order_id", "qty").explain()

# ---- Job / Stage / Task ----
# 1 action  -> 1 (or more) job
# 1 job     -> N stages (split at every shuffle)
# 1 stage   -> M tasks  (one task per partition)
print("Shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
print("Spark UI:", spark.sparkContext.uiWebUrl)

# Actions: show, count, collect, take, first, write, foreach
# Tip: `collect()` pulls everything to the driver - avoid on big data.

spark.stop()
