"""
Day 16 - selectExpr() & Expressions

Topics:
  - selectExpr for SQL-style column expressions
  - expr() inside DataFrame API
  - CASE WHEN, cast, string ops in expr
  - Filtering with SQL string conditions
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day16")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])

# selectExpr = select() where every argument is a SQL expression string
emp.selectExpr(
    "emp_id",
    "upper(name) AS name_upper",
    "salary * 1.10 AS salary_after_hike",
    "cast(dept_id AS string) AS dept_str",
    "CASE WHEN salary >= 80000 THEN 'High' WHEN salary >= 50000 THEN 'Mid' ELSE 'Low/Unknown' END AS band",
    "coalesce(email, 'n/a') AS email",
    "year(join_date) AS join_year",
).show(5)

# expr() lets you use SQL syntax inside withColumn / filter
(emp.withColumn("tenure_days", F.expr("datediff(current_date(), join_date)"))
    .withColumn("email_domain", F.expr("substring_index(email, '@', -1)"))
    .filter(F.expr("dept_id IN (10, 20) AND salary IS NOT NULL"))
    .select("name", "tenure_days", "email_domain").show(5))

# Rename many columns with selectExpr
emp.selectExpr("emp_id AS id", "name AS employee_name", "city AS location").show(3)

# `*` + extra columns
emp.selectExpr("*", "salary / 12 AS monthly_salary").select("name", "monthly_salary").show(3)

# when do we prefer which? -> API is checked at build time, strings are checked at runtime.

spark.stop()
