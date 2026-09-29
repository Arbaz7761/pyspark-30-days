"""
Day 29 - Data Quality Validation

Topics:
  - Common DQ dimensions: completeness, uniqueness, validity, consistency, referential integrity
  - Rule-based validation framework
  - Quarantining bad records
  - DQ report + fail-fast threshold
  - Connecting to the Data Quality Monitor project idea
"""
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))   # repo root -> `common` package

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from common.spark_utils import get_spark, DATA, OUT

spark = get_spark("Day29")

emp = spark.read.csv(f"{DATA}/employees.csv", header=True, inferSchema=True)
dept = spark.read.csv(f"{DATA}/departments.csv", header=True, inferSchema=True)
total = emp.count()

# ---- Rules: (name, dimension, "violation" condition) ----
rules = [
    ("emp_id not null",        "completeness", F.col("emp_id").isNull()),
    ("salary not null",        "completeness", F.col("salary").isNull()),
    ("email not null",         "completeness", F.col("email").isNull()),
    ("salary >= 0",            "validity",     F.col("salary") < 0),
    ("email format",           "validity",     F.col("email").isNotNull() & ~F.col("email").rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$")),
    ("join_date not future",   "validity",     F.col("join_date") > F.current_date()),
]

results = []
for name, dim, violation in rules:
    bad = emp.filter(violation).count()
    results.append((name, dim, bad, round(100 * (total - bad) / total, 2)))

# ---- Uniqueness ----
dupes = emp.groupBy("emp_id").count().filter("count > 1").count()
results.append(("emp_id unique", "uniqueness", dupes, round(100 * (total - dupes) / total, 2)))

# ---- Referential integrity: every emp.dept_id must exist in departments ----
orphans = emp.join(dept, "dept_id", "left_anti").count()
results.append(("dept_id exists in departments", "integrity", orphans, round(100 * (total - orphans) / total, 2)))

report = spark.createDataFrame(results, ["rule", "dimension", "violations", "pass_pct"])
report.orderBy("pass_pct").show(truncate=False)

# ---- Quarantine: split good and bad rows ----
is_bad = F.lit(False)
for _, _, cond in rules:
    is_bad = is_bad | F.coalesce(cond, F.lit(False))
emp_flagged = emp.withColumn("dq_failed", is_bad)
clean = emp_flagged.filter(~F.col("dq_failed")).drop("dq_failed")
quarantine = emp_flagged.filter("dq_failed").drop("dq_failed")
print(f"clean={clean.count()} quarantine={quarantine.count()}")
clean.write.mode("overwrite").parquet(f"{OUT}/day29_clean")
quarantine.write.mode("overwrite").parquet(f"{OUT}/day29_quarantine")

# ---- Fail-fast threshold: stop pipeline if quality is too low ----
THRESHOLD = 50.0
worst = report.agg(F.min("pass_pct")).first()[0]
print(f"worst rule pass rate = {worst}%")
if worst < THRESHOLD:
    print(f"WARNING: DQ below {THRESHOLD}% -> a real pipeline would raise an exception and alert here.")
    # raise Exception("Data quality check failed")

spark.stop()
