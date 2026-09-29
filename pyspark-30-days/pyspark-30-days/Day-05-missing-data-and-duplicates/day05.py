"""
Day 05 - Missing data & Duplicates

Topics:
  - dropna(how, thresh, subset)
  - fillna with dict
  - distinct vs dropDuplicates(subset)
  - Finding duplicates with groupBy
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day05")

df = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)
print("Total rows:", df.count())

# ---- Missing data ----
df.dropna().show()                          # drop if ANY column is null
df.dropna(how="all").count()                # drop only if ALL columns are null
df.dropna(subset=["salary"]).show(3)        # only look at specific columns
df.dropna(thresh=7).count()                 # keep rows having at least 7 non-null values

filled = df.fillna({"salary": 0, "email": "unknown@example.com", "manager_id": -1})
filled.show(5)

# fill numeric with the column mean
mean_sal = df.select(F.mean("salary")).first()[0]
df.fillna({"salary": round(mean_sal, 2)}).select("name", "salary").show(5)

# ---- Duplicates ----
print("distinct rows       :", df.distinct().count())
print("dropDuplicates()    :", df.dropDuplicates().count())
print("dedupe on emp_id    :", df.dropDuplicates(["emp_id"]).count())

# find WHICH rows are duplicated
(df.groupBy(df.columns).count().filter("count > 1").show())

spark.stop()
