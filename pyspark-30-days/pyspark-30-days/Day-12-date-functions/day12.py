"""
Day 12 - Date Functions

Topics:
  - to_date, to_timestamp with format
  - current_date, current_timestamp
  - year, month, dayofmonth, dayofweek
  - date_format
  - datediff, months_between, date_add, date_sub, add_months
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day12")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])

# parsing strings into dates
demo = spark.createDataFrame([("2025-03-15",), ("15/03/2025",)], ["d"])
demo.select(F.to_date("d", "yyyy-MM-dd").alias("iso"), F.to_date("d", "dd/MM/yyyy").alias("indian")).show()

# extracting parts
emp.select(
    "name", "join_date",
    F.year("join_date").alias("year"),
    F.month("join_date").alias("month"),
    F.dayofmonth("join_date").alias("day"),
    F.dayofweek("join_date").alias("dow"),        # 1 = Sunday
    F.date_format("join_date", "EEEE").alias("day_name"),
    F.date_format("join_date", "MMM yyyy").alias("mon_year"),
).show(5)

# date arithmetic
emp.select(
    "name", "join_date",
    F.current_date().alias("today"),
    F.datediff(F.current_date(), "join_date").alias("days_in_company"),
    F.round(F.months_between(F.current_date(), "join_date") / 12, 1).alias("years_in_company"),
    F.date_add("join_date", 90).alias("probation_end"),
    F.date_sub("join_date", 7).alias("offer_date"),
    F.add_months("join_date", 12).alias("first_anniversary"),
).show(5)

# filter by date range
emp.filter(F.col("join_date").between("2022-01-01", "2023-12-31")).select("name", "join_date").show()

# timestamps
spark.range(1).select(F.current_timestamp().alias("now"),
                      F.to_timestamp(F.lit("2025-03-15 10:30:00")).alias("ts")).show(truncate=False)

spark.stop()
