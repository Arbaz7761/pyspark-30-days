"""
Day 04 - Filtering & Sorting

Topics:
  - filter() / where()
  - AND (&), OR (|), NOT (~)
  - isin, between, like, startswith, contains
  - orderBy / sort, asc / desc, nulls handling
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day04")

df = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)

# filter == where
df.filter(F.col("salary") > 50000).show()
df.where("city = 'Patna'").show()

# multiple conditions - always wrap each condition in ()
df.filter((F.col("dept_id") == 10) & (F.col("salary") >= 50000)).show()
df.filter((F.col("city") == "Delhi") | (F.col("city") == "Pune")).show()
df.filter(~F.col("city").isin("Delhi", "Pune")).show()

# between / like / string helpers
df.filter(F.col("salary").between(40000, 70000)).show()
df.filter(F.col("name").like("A%")).show()
df.filter(F.col("email").contains("example")).show()
df.filter(F.col("name").startswith("R")).show()

# sorting
df.orderBy("salary").show(5)                                   # asc (nulls first)
df.orderBy(F.col("salary").desc()).show(5)                     # desc
df.orderBy(F.col("salary").desc_nulls_last()).show(5)          # push NULLs to end
df.orderBy(F.col("dept_id").asc(), F.col("salary").desc_nulls_last()).show(10)  # multi-column

# top-N pattern
df.orderBy(F.col("salary").desc_nulls_last()).limit(3).show()

spark.stop()
