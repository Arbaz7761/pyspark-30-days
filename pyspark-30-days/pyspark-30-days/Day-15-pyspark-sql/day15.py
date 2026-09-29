"""
Day 15 - PySpark SQL

Topics:
  - createOrReplaceTempView
  - spark.sql() with CTEs, joins, window functions
  - Global temp views
  - Mixing SQL and DataFrame API
  - Creating tables (saveAsTable)
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day15")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True).dropDuplicates(["emp_id"])
dept = spark.read.csv(f"{DATA}/departments.csv", header=True, inferSchema=True)
sales = spark.read.csv(f"{DATA}/sales.csv", header=True, inferSchema=True)

emp.createOrReplaceTempView("employees")
dept.createOrReplaceTempView("departments")
sales.createOrReplaceTempView("sales")

# basic query
spark.sql("SELECT name, salary FROM employees WHERE salary > 50000 ORDER BY salary DESC").show()

# join + aggregation
spark.sql("""
    SELECT d.dept_name, COUNT(*) AS headcount, ROUND(AVG(e.salary), 0) AS avg_salary
    FROM employees e
    JOIN departments d ON e.dept_id = d.dept_id
    GROUP BY d.dept_name
    ORDER BY headcount DESC
""").show()

# CTE + window function
spark.sql("""
    WITH monthly AS (
        SELECT region, date_trunc('month', to_date(order_date)) AS month, SUM(qty * price) AS revenue
        FROM sales
        WHERE status = 'Completed'
        GROUP BY region, date_trunc('month', to_date(order_date))
    )
    SELECT region, month, revenue,
           RANK() OVER (PARTITION BY region ORDER BY revenue DESC) AS rnk,
           LAG(revenue) OVER (PARTITION BY region ORDER BY month) AS prev_month_rev
    FROM monthly
    ORDER BY region, month
""").show(10)

# SQL result is a DataFrame -> continue with the API
result = spark.sql("SELECT * FROM employees").filter("city = 'Pune'").select("name", "city")
result.show()

# Global temp view (shared across sessions in the same app) lives in `global_temp`
emp.createOrReplaceGlobalTempView("emp_global")
spark.sql("SELECT COUNT(*) AS n FROM global_temp.emp_global").show()

# Catalog inspection
print([t.name for t in spark.catalog.listTables()])
spark.sql("SHOW TABLES").show()

spark.stop()
