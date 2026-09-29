"""
Day 01 - File reading in PySpark — CSV, basic read options

Topics:
  - spark.read.csv() basics
  - header, inferSchema, sep options
  - show(), printSchema(), count(), columns
  - Reading multiple files / a folder
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day01")

# 1. Simplest read (no header handling -> columns become _c0, _c1 ...)
df_raw = spark.read.csv(f"{DATA}/employees.csv")
df_raw.show(3)

# 2. With header + schema inference
df = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)
df.printSchema()
df.show(5)

# 3. Chained option() style (same result, more readable for many options)
df2 = (spark.read
       .option("header", True)
       .option("inferSchema", True)
       .option("sep", ",")
       .csv(f"{DATA}/employees.csv"))

# 4. format().load() style - works for every file type
df3 = spark.read.format("csv").option("header", "true").load(f"{DATA}/employees.csv")

# 5. Quick inspection APIs
print("Rows   :", df.count())
print("Columns:", df.columns)
print("Dtypes :", df.dtypes)
df.describe("salary").show()

# 6. Reading several files at once
both = spark.read.csv([f"{DATA}/employees.csv", f"{DATA}/departments.csv"], header=True)
print("Multi-file read columns:", both.columns)

spark.stop()
