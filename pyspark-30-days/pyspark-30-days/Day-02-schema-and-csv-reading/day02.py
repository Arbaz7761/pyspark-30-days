"""
Day 02 - Schema & CSV reading

Topics:
  - Why inferSchema is slow / risky in production
  - StructType / StructField
  - DDL string schema
  - Read modes: PERMISSIVE, DROPMALFORMED, FAILFAST
  - Writing CSV back
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day02")

from pyspark.sql.types import (StructType, StructField, IntegerType, StringType,
                               DoubleType, DateType)

# 1. Explicit schema with StructType (recommended for production)
emp_schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("salary", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("join_date", DateType(), True),
    StructField("email", StringType(), True),
    StructField("manager_id", IntegerType(), True),
])
df = spark.read.csv(f"{DATA}/employees.csv", header=True, schema=emp_schema)
df.printSchema()

# 2. Same schema as a DDL string (shorter)
ddl = "emp_id INT, name STRING, dept_id INT, salary DOUBLE, city STRING, join_date DATE, email STRING, manager_id INT"
df_ddl = spark.read.csv(f"{DATA}/employees.csv", header=True, schema=ddl)
df_ddl.show(3)

# 3. Read modes for corrupt records
bad_schema = "order_id INT, customer_email STRING, amount DOUBLE, order_date DATE, country STRING"
for mode in ["PERMISSIVE", "DROPMALFORMED"]:
    d = spark.read.csv(f"{DATA}/raw_orders_dirty.csv", header=True, schema=bad_schema, mode=mode)
    print(f"mode={mode} -> rows={d.count()}")
    d.show()

# 4. Capture corrupt rows explicitly
schema_with_corrupt = bad_schema + ", _corrupt STRING"
d = spark.read.csv(f"{DATA}/raw_orders_dirty.csv", header=True, schema=schema_with_corrupt,
                   columnNameOfCorruptRecord="_corrupt")
d.cache()
d.filter("_corrupt IS NOT NULL").show(truncate=False)

# 5. Write back as CSV
df.write.mode("overwrite").option("header", True).csv(f"{OUT}/day02_employees_csv")

spark.stop()
