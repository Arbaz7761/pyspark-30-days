"""
Day 17 - Union / UnionByName

Topics:
  - union (position based) vs unionByName (name based)
  - allowMissingColumns
  - union vs SQL UNION ALL vs UNION
  - Combining many DataFrames with reduce
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day17")

from functools import reduce
from pyspark.sql import DataFrame

a = spark.createDataFrame([(1, "Arbaz", "Patna"), (2, "Riya", "Delhi")], ["id", "name", "city"])
b = spark.createDataFrame([(3, "Neha", "Pune"), (2, "Riya", "Delhi")], ["id", "name", "city"])
c = spark.createDataFrame([(4, "Bhopal", "Karan")], ["id", "city", "name"])        # name/city swapped (types still line up positionally)

# union = position based, keeps duplicates (like UNION ALL)
a.union(b).show()
# remove duplicates like SQL UNION
a.union(b).distinct().show()

# WRONG result when column order differs
a.union(c).show()
# unionByName matches by column name
a.unionByName(c).show()

# different set of columns
d = spark.createDataFrame([(5, "Sana", 50000)], ["id", "name", "salary"])
a.unionByName(d, allowMissingColumns=True).show()

# union many DataFrames at once
dfs = [spark.createDataFrame([(i, f"user_{i}")], ["id", "name"]) for i in range(1, 6)]
combined = reduce(DataFrame.unionByName, dfs)
combined.show()

# real-world use: monthly files -> one table
jan = spark.read.csv(f"{DATA}/sales.csv", header=True).limit(3)
feb = spark.read.csv(f"{DATA}/sales.csv", header=True).limit(2)
print("rows after union:", jan.unionByName(feb).count())

spark.stop()
