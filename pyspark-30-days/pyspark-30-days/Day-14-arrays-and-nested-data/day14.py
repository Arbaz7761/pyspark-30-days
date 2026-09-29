"""
Day 14 - Arrays & Nested Data

Topics:
  - Reading JSON (nested structs + arrays)
  - Struct field access with dot notation
  - explode, explode_outer, posexplode
  - array_contains, size, sort_array, collect_list, collect_set
  - transform / aggregate higher-order functions
  - to_json, from_json
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day14")

orders = spark.read.json(f"{DATA}/orders_nested.json")
orders.printSchema()

# struct access
orders.select("order_id", "customer.name", "customer.address.city").show(3)

# explode array -> one row per item
items = orders.select("order_id", F.explode("items").alias("item"))
items.select("order_id", "item.sku", "item.qty", "item.price").show(5)

# posexplode gives index also
orders.select("order_id", F.posexplode("tags").alias("pos", "tag")).show(5)

# array helpers
orders.select(
    "order_id", "tags",
    F.size("tags").alias("n_tags"),
    F.array_contains("tags", "gift").alias("is_gift"),
    F.sort_array("tags").alias("sorted_tags"),
).show(5, truncate=False)

# order total = sum(qty * price) using higher-order functions (no explode needed)
orders.select(
    "order_id",
    F.expr("aggregate(transform(items, i -> i.qty * i.price), CAST(0 AS BIGINT), (acc, x) -> acc + x)").alias("order_total")
).show(5)

# collect_list / collect_set: rows -> array
(items.groupBy("item.sku").agg(F.collect_set("order_id").alias("in_orders"),
                              F.sum("item.qty").alias("units")).show(5, truncate=False))

# create arrays / structs / maps
spark.createDataFrame([(1, "a", "b")], ["id", "x", "y"]) \
     .select("id", F.array("x", "y").alias("arr"),
             F.struct("x", "y").alias("st"),
             F.create_map(F.lit("k1"), F.col("x")).alias("mp")).show()

# struct <-> JSON string
as_json = orders.select("order_id", F.to_json("customer").alias("customer_json"))
as_json.show(2, truncate=False)
back = as_json.select("order_id", F.from_json("customer_json", orders.schema["customer"].dataType).alias("customer"))
back.select("customer.name").show(2)

spark.stop()
