"""
Day 26 - Built-in Functions vs UDF

Topics:
  - Why built-ins are faster (JVM + Catalyst)
  - Python UDF cost (serialisation)
  - pandas_udf (vectorised, Arrow)
  - When a UDF is unavoidable
  - Registering UDFs for SQL
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day26")

import time
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

sales = (spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)
         .withColumn("big", F.explode(F.array([F.lit(i) for i in range(20)])))    # ~10k rows to see a timing gap
         .drop("big"))

def timed(label, df):
    t = time.time(); df.write.mode("overwrite").format("noop").save(); print(f"{label:<26} {time.time() - t:.2f}s")

# 1) Built-in - optimised by Catalyst, runs in the JVM
timed("built-in upper()", sales.select(F.upper("product").alias("p")))

# 2) Python UDF - each row is serialised to Python and back
@F.udf(returnType=StringType())
def py_upper(s):
    return s.upper() if s else None
timed("python UDF", sales.select(py_upper("product").alias("p")))

# 3) pandas_udf - vectorised over Arrow batches (much faster than row-by-row UDF)
@F.pandas_udf(StringType())
def pd_upper(s: pd.Series) -> pd.Series:
    return s.str.upper()
timed("pandas_udf", sales.select(pd_upper("product").alias("p")))

# ---- A case where UDF is reasonable: custom logic with no built-in ----
def risk_bucket(amount):
    if amount is None: return "unknown"
    return "high" if amount > 100000 else "medium" if amount > 10000 else "low"

risk_udf = F.udf(risk_bucket, StringType())
sales.withColumn("revenue", F.col("qty") * F.col("price")).withColumn("risk", risk_udf("revenue")).groupBy("risk").count().show()

# ...but the SAME logic with when() is better (Catalyst can optimise it)
sales.withColumn("revenue", F.col("qty") * F.col("price")).withColumn(
    "risk", F.when(F.col("revenue") > 100000, "high").when(F.col("revenue") > 10000, "medium").otherwise("low")
).groupBy("risk").count().show()

# ---- register for SQL usage ----
spark.udf.register("risk_bucket", risk_bucket, StringType())
sales.createOrReplaceTempView("s")
spark.sql("SELECT risk_bucket(qty * price) AS r, COUNT(*) c FROM s GROUP BY 1").show()

# Rule: built-in > SQL expr/when > pandas_udf > python UDF

spark.stop()
