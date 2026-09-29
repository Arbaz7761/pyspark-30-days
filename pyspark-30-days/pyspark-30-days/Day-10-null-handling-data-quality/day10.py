"""
Day 10 - Null handling & Data Quality

Topics:
  - isNull / isNotNull
  - coalesce, nullif, ifnull, nvl
  - Null-safe equality (eqNullSafe)
  - Null count per column
  - Null percentage report
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day10")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])

# filter nulls
emp.filter(F.col("salary").isNull()).select("name", "salary").show()
emp.filter(F.col("email").isNotNull()).count()

# coalesce = first non-null value
emp.withColumn("contact", F.coalesce(F.col("email"), F.lit("no-email"))).select("name", "contact").show(5)

# nullif-style: turn empty string into NULL
emp.withColumn("email_clean", F.expr("nullif(trim(email), '')")).select("name", "email_clean").show(5)

# null count per column
null_counts = emp.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in emp.columns])
null_counts.show()

# null percentage report
total = emp.count()
report = [(c, emp.filter(F.col(c).isNull()).count(), round(emp.filter(F.col(c).isNull()).count() * 100 / total, 2))
          for c in emp.columns]
spark.createDataFrame(report, ["column", "null_count", "null_pct"]).orderBy(F.col("null_pct").desc()).show()

# NULL compared with = gives NULL (not true/false). Use eqNullSafe.
emp.select(F.col("manager_id"), (F.col("manager_id") == F.lit(None)).alias("normal_eq"),
           F.col("manager_id").eqNullSafe(F.lit(None)).alias("null_safe_eq")).show(5)

# Aggregations ignore NULLs: count("col") != count("*")
emp.agg(F.count("*").alias("count_star"), F.count("salary").alias("count_salary")).show()

spark.stop()
