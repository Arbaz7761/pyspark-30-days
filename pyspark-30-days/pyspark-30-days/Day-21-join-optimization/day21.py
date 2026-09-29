"""
Day 21 - Join Optimization

Topics:
  - Sort-merge join vs broadcast hash join
  - broadcast() hint & autoBroadcastJoinThreshold
  - Filter and select BEFORE joining
  - Join hints: broadcast, merge, shuffle_hash
  - Reading join strategy in explain()
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day21")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
cust = spark.read.csv(f"{DATA}/customers.csv", header=True, inferSchema=True)

# Force a sort-merge join first (disable auto-broadcast) to see the shuffle
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
smj = sales.join(cust, "customer_id")
smj.explain()                     # look for: SortMergeJoin + 2x Exchange

# Broadcast join: small table is shipped to every executor -> no shuffle of the big table
bj = sales.join(F.broadcast(cust), "customer_id")
bj.explain()                      # look for: BroadcastHashJoin + BroadcastExchange

# Restore default threshold (10 MB) -> Spark auto-broadcasts small tables
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# Optimization 1: reduce data BEFORE the join
slim_sales = sales.filter("status = 'Completed'").select("order_id", "customer_id", "qty", "price")
slim_cust = cust.select("customer_id", "customer_name", "segment")
slim_sales.join(slim_cust, "customer_id").explain()

# Optimization 2: join hints
sales.join(cust.hint("broadcast"), "customer_id").explain()
sales.join(cust.hint("shuffle_hash"), "customer_id").explain()
sales.join(cust.hint("merge"), "customer_id").explain()


# Cheat-sheet
# small (< ~10MB) + big     -> broadcast
# big + big, evenly spread  -> sort-merge (default), consider bucketing
# skewed key                -> AQE skew join / salting (Day 23)

spark.stop()
