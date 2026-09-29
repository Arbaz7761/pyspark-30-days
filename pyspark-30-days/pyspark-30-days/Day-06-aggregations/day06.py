"""
Day 06 - Aggregations

Topics:
  - groupBy + agg
  - count, sum, avg, min, max, countDistinct
  - Multiple aggregations with aliases
  - pivot, rollup, cube
  - Filtering after groupBy (HAVING)
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day06")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
sales = sales.withColumn("revenue", F.col("qty") * F.col("price"))

# global aggregation
sales.agg(F.sum("revenue").alias("total_revenue"), F.avg("revenue").alias("avg_order")).show()

# groupBy + multiple aggregations
(sales.groupBy("category")
      .agg(F.count("*").alias("orders"),
           F.sum("revenue").alias("revenue"),
           F.round(F.avg("revenue"), 2).alias("avg_revenue"),
           F.min("revenue").alias("min_rev"),
           F.max("revenue").alias("max_rev"),
           F.countDistinct("customer_id").alias("unique_customers"))
      .orderBy(F.col("revenue").desc())
      .show())

# multiple group keys
sales.groupBy("region", "status").count().orderBy("region", "status").show()

# HAVING equivalent = filter after agg
(sales.groupBy("product").agg(F.sum("revenue").alias("rev"))
      .filter(F.col("rev") > 1_000_000).show())

# pivot: rows -> columns
sales.groupBy("region").pivot("category").agg(F.sum("revenue")).show()

# rollup / cube: subtotals & grand total
sales.rollup("region", "category").agg(F.sum("revenue").alias("rev")).orderBy("region", "category").show(20)

spark.stop()
