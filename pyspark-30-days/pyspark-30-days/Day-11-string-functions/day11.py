"""
Day 11 - String Functions

Topics:
  - upper, lower, initcap, trim, ltrim, rtrim
  - concat, concat_ws, substring, length
  - split, regexp_replace, regexp_extract
  - lpad, rpad, instr, translate
  - rlike for pattern matching
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day11")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])

emp.select(
    "name",
    F.upper("name").alias("upper"),
    F.lower("name").alias("lower"),
    F.initcap(F.lower("name")).alias("initcap"),
    F.length("name").alias("len"),
    F.substring("name", 1, 3).alias("first3"),
).show(5)

# concat / concat_ws (concat_ws skips NULLs, concat returns NULL)
emp.select(F.concat_ws(" | ", "name", "city", "email").alias("profile")).show(5, truncate=False)

# split -> array, then pick element
emp.withColumn("email_parts", F.split("email", "@")) \
   .select("email", F.col("email_parts")[1].alias("domain")).show(5)

# regexp_extract / regexp_replace
emp.select("email",
           F.regexp_extract("email", r"^([a-z]+)@", 1).alias("user_part"),
           F.regexp_replace("email", r"@.*", "@company.com").alias("new_email")).show(5, truncate=False)

# padding & position
emp.select(F.lpad(F.col("emp_id").cast("string"), 5, "0").alias("emp_code"),
           F.instr("email", "@").alias("at_pos")).show(5)

# trim whitespace / translate chars
spark.createDataFrame([("  hello  ",), (" spark ",)], ["txt"]) \
     .select(F.trim("txt").alias("trim"), F.ltrim("txt").alias("ltrim"), F.rtrim("txt").alias("rtrim")).show()

# rlike = regex filter (valid emails)
emp.filter(F.col("email").rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")).count()

spark.stop()
