"""
Day 07 - Joins

Topics:
  - inner, left, right, full outer
  - left_semi & left_anti
  - cross join
  - Joining on different column names
  - Avoiding duplicate columns
  - Self join
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day07")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])
dept = spark.read.csv(f"{DATA}/departments.csv", header=True, inferSchema=True)

# NOTE: dept_id 60 exists in employees but NOT in departments; 50 may have no employees.
for how in ["inner", "left", "right", "full"]:
    print(f"--- {how} join ---")
    emp.join(dept, on="dept_id", how=how).select("emp_id", "name", "dept_id", "dept_name").show(5)

# semi = rows of left that HAVE a match (only left columns returned)
emp.join(dept, "dept_id", "left_semi").count()
# anti = rows of left that have NO match
emp.join(dept, "dept_id", "left_anti").show()

# different column names -> explicit condition
dept2 = dept.withColumnRenamed("dept_id", "d_id")
emp.join(dept2, emp.dept_id == dept2.d_id, "inner").select("name", "dept_name").show(3)

# self join: employee -> manager
m = emp.alias("e").join(emp.alias("m"), F.col("e.manager_id") == F.col("m.emp_id"), "left")
m.select(F.col("e.name").alias("employee"), F.col("m.name").alias("manager")).show(10)

# cross join (cartesian) - use carefully
dept.crossJoin(spark.createDataFrame([("Q1",), ("Q2",)], ["quarter"])).show(4)

# join on multiple columns
# a.join(b, ["col1", "col2"], "inner")

spark.stop()
