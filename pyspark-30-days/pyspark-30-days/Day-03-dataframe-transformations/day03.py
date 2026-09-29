"""
Day 03 - DataFrame transformations

Topics:
  - select, selectExpr, alias
  - withColumn, withColumnRenamed, drop
  - cast, lit
  - Transformations are lazy and immutable
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day03")

df = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)

# select
df.select("name", "salary").show(3)
df.select(F.col("name"), (F.col("salary") * 1.1).alias("salary_after_hike")).show(3)

# withColumn - add / overwrite columns
df = (df
      .withColumn("country", F.lit("India"))
      .withColumn("salary", F.col("salary").cast("double"))
      .withColumn("annual_bonus", F.col("salary") * 0.10))

# rename & drop
df = df.withColumnRenamed("name", "employee_name").drop("manager_id")
df.show(5)

# cast types
df.select(F.col("emp_id").cast("string").alias("emp_id_str"), "join_date").printSchema()

# multiple renames in one go
renamed = df.toDF(*[c.upper() for c in df.columns])
print(renamed.columns)

# DataFrames are immutable - every step returns a NEW DataFrame
print("Original DF unchanged? ->", "annual_bonus" in spark.read.csv(f"{DATA}/employees.csv", header=True).columns)

spark.stop()
