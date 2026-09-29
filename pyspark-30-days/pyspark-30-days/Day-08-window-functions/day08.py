"""
Day 08 - Window Functions

Topics:
  - Window.partitionBy().orderBy()
  - row_number, rank, dense_rank, ntile
  - lag, lead
  - Running totals with rowsBetween
  - Top-N per group
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day08")

emp = (spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)
       .dropDuplicates(["emp_id"]).dropna(subset=["salary"]))

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc())

ranked = (emp
          .withColumn("row_num", F.row_number().over(w))
          .withColumn("rank", F.rank().over(w))
          .withColumn("dense_rank", F.dense_rank().over(w))
          .withColumn("quartile", F.ntile(4).over(w)))
ranked.select("dept_id", "name", "salary", "row_num", "rank", "dense_rank", "quartile").orderBy("dept_id", "row_num").show(20)

# Top-N per group (top 2 earners per dept)
ranked.filter("row_num <= 2").select("dept_id", "name", "salary").show()

# lag / lead - compare with previous / next row
w2 = Window.partitionBy("dept_id").orderBy("join_date")
(emp.withColumn("prev_salary", F.lag("salary", 1).over(w2))
    .withColumn("next_salary", F.lead("salary", 1).over(w2))
    .withColumn("diff_from_prev", F.col("salary") - F.col("prev_salary"))
    .select("dept_id", "name", "join_date", "salary", "prev_salary", "next_salary", "diff_from_prev").show(10))

# running total & moving average
sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
w3 = Window.partitionBy("region").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("order_date").rowsBetween(-2, 0)
(sales.withColumn("revenue", F.col("qty") * F.col("price"))
      .withColumn("running_total", F.sum("revenue").over(w3))
      .withColumn("moving_avg_3", F.avg("revenue").over(w4))
      .select("region", "order_date", "revenue", "running_total", "moving_avg_3").show(8))

# department average next to every row (no groupBy needed)
emp.withColumn("dept_avg", F.avg("salary").over(Window.partitionBy("dept_id"))).select("name", "dept_id", "salary", "dept_avg").show(5)

spark.stop()
