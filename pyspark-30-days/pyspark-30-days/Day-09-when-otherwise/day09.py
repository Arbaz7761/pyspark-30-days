"""
Day 09 - when() / otherwise()

Topics:
  - Conditional columns
  - Chained when()
  - Combining conditions
  - when inside aggregations (conditional sum/count)
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day09")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])

# simple if / else
emp.withColumn("is_high_earner", F.when(F.col("salary") >= 80000, "Yes").otherwise("No")).select("name", "salary", "is_high_earner").show(5)

# chained when = if / elif / else
banded = emp.withColumn(
    "salary_band",
    F.when(F.col("salary").isNull(), "Unknown")
     .when(F.col("salary") < 40000, "Low")
     .when(F.col("salary") < 70000, "Medium")
     .otherwise("High"))
banded.select("name", "salary", "salary_band").show(10)

# combining conditions
emp.withColumn(
    "grade",
    F.when((F.col("dept_id") == 10) & (F.col("salary") > 60000), "Senior Engineer")
     .when(F.col("dept_id") == 10, "Engineer")
     .otherwise("Non-Engineering")).select("name", "dept_id", "grade").show(5)

# conditional aggregation (very common interview question)
banded.groupBy("dept_id").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("salary_band") == "High", 1).otherwise(0)).alias("high_count"),
    F.sum(F.when(F.col("salary_band") == "Low", 1).otherwise(0)).alias("low_count"),
).orderBy("dept_id").show()

# SQL-style CASE WHEN via expr
emp.withColumn("city_type", F.expr("CASE WHEN city IN ('Delhi','Mumbai','Bangalore') THEN 'Metro' ELSE 'Non-Metro' END")).select("name", "city", "city_type").show(5)

spark.stop()
