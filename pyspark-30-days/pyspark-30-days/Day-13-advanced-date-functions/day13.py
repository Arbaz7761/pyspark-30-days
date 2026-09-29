"""
Day 13 - Advanced Date Functions

Topics:
  - trunc, date_trunc
  - last_day, next_day, weekofyear, quarter
  - unix_timestamp, from_unixtime
  - Generating date ranges with sequence + explode
  - Time-based grouping with window()
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day13")

sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True) \
             .withColumn("order_date", F.to_date("order_date")) \
             .withColumn("revenue", F.col("qty") * F.col("price"))

# truncating to month / quarter / week
sales.select(
    "order_date",
    F.trunc("order_date", "month").alias("month_start"),
    F.date_trunc("quarter", "order_date").alias("quarter_start"),
    F.last_day("order_date").alias("month_end"),
    F.quarter("order_date").alias("qtr"),
    F.weekofyear("order_date").alias("week_no"),
    F.next_day("order_date", "Mon").alias("next_monday"),
).show(5)

# monthly revenue trend
(sales.groupBy(F.trunc("order_date", "month").alias("month"))
      .agg(F.sum("revenue").alias("revenue")).orderBy("month").show(12))

# unix time conversions
spark.range(1).select(
    F.unix_timestamp(F.lit("2025-03-15 10:30:00")).alias("epoch"),
    F.from_unixtime(F.lit(1742034600)).alias("readable")).show(truncate=False)

# calendar / date dimension using sequence + explode
cal = (spark.sql("SELECT sequence(to_date('2025-01-01'), to_date('2025-01-10'), interval 1 day) AS d")
       .select(F.explode("d").alias("date")))
cal.withColumn("is_weekend", F.dayofweek("date").isin(1, 7)).show()

# find missing dates (days with no sales in Jan)
all_days = spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2025-01-31'), interval 1 day)) AS date")
sold = sales.select(F.col("order_date").alias("date")).distinct()
all_days.join(sold, "date", "left_anti").show()

# time window aggregation (tumbling 7 day window)
(sales.groupBy(F.window(F.col("order_date").cast("timestamp"), "7 days"))
      .agg(F.sum("revenue").alias("rev")).orderBy("window").show(5, truncate=False))

spark.stop()
