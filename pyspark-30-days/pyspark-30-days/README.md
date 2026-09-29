# PySpark in 30 Days

A hands-on 30 day journey from reading a CSV to building an end-to-end **Bronze → Silver → Gold** pipeline with PySpark.
Every day has a runnable script with comments, plus a short README.

## Roadmap

### Week 1 - DataFrame Fundamentals (Day 1-7)
| Day | Topic |
|-----|-------|
| 01 | [File reading in PySpark — CSV, basic read options](Day-01-file-reading-csv/day01.py) |
| 02 | [Schema & CSV reading](Day-02-schema-and-csv-reading/day02.py) |
| 03 | [DataFrame transformations](Day-03-dataframe-transformations/day03.py) |
| 04 | [Filtering & Sorting](Day-04-filtering-and-sorting/day04.py) |
| 05 | [Missing data & Duplicates](Day-05-missing-data-and-duplicates/day05.py) |
| 06 | [Aggregations](Day-06-aggregations/day06.py) |
| 07 | [Joins](Day-07-joins/day07.py) |

### Week 2 - Transformations & Functions (Day 8-14)
| Day | Topic |
|-----|-------|
| 08 | [Window Functions](Day-08-window-functions/day08.py) |
| 09 | [when() / otherwise()](Day-09-when-otherwise/day09.py) |
| 10 | [Null handling & Data Quality](Day-10-null-handling-data-quality/day10.py) |
| 11 | [String Functions](Day-11-string-functions/day11.py) |
| 12 | [Date Functions](Day-12-date-functions/day12.py) |
| 13 | [Advanced Date Functions](Day-13-advanced-date-functions/day13.py) |
| 14 | [Arrays & Nested Data](Day-14-arrays-and-nested-data/day14.py) |

### Week 3 - SQL, Partitioning & Spark Internals (Day 15-21)
| Day | Topic |
|-----|-------|
| 15 | [PySpark SQL](Day-15-pyspark-sql/day15.py) |
| 16 | [selectExpr() & Expressions](Day-16-selectexpr-and-expressions/day16.py) |
| 17 | [Union / UnionByName](Day-17-union-and-unionbyname/day17.py) |
| 18 | [Repartition & Coalesce](Day-18-repartition-and-coalesce/day18.py) |
| 19 | [Cache & Persist](Day-19-cache-and-persist/day19.py) |
| 20 | [Spark Execution Model](Day-20-spark-execution-model/day20.py) |
| 21 | [Join Optimization](Day-21-join-optimization/day21.py) |

### Week 4 - Performance & Real-World Pipelines (Day 22-30)
| Day | Topic |
|-----|-------|
| 22 | [Debugging Slow Spark Jobs](Day-22-debugging-slow-spark-jobs/day22.py) |
| 23 | [Data Skew, Salting & AQE](Day-23-data-skew-salting-aqe/day23.py) |
| 24 | [Parquet & Partition Pruning](Day-24-parquet-and-partition-pruning/day24.py) |
| 25 | [Broadcast Join, Column Pruning & Cache](Day-25-broadcast-join-column-pruning-cache/day25.py) |
| 26 | [Built-in Functions vs UDF](Day-26-builtin-functions-vs-udf/day26.py) |
| 27 | [Practical PySpark ETL](Day-27-practical-pyspark-etl/day27.py) |
| 28 | [File Formats & Incremental Processing](Day-28-file-formats-and-incremental-processing/day28.py) |
| 29 | [Data Quality Validation](Day-29-data-quality-validation/day29.py) |
| 30 | [End-to-End Pipeline: Bronze → Silver → Gold](Day-30-end-to-end-bronze-silver-gold/day30.py) |

## Tech stack
Python 3.9+, PySpark 3.5+ / 4.x, Java 17+ (required by Spark), Pandas + PyArrow (for pandas_udf)

## Setup
```bash
git clone https://github.com/Arbaz7761/pyspark-30-days.git
cd pyspark-30-days
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python generate_data.py                                  # creates sample CSV/JSON in data/
python Day-01-file-reading-csv/day01.py                  # run any day
python run_all.py                                        # (optional) run all 30 days
```

## Repo structure
```
pyspark-30-days/
├── README.md
├── requirements.txt
├── generate_data.py        # creates sample datasets
├── run_all.py              # runs every day's script
├── common/spark_utils.py   # shared SparkSession + paths
├── data/                   # sample input data
├── output/                 # generated results (git-ignored)
└── Day-XX-<topic>/
    ├── dayXX.py
    └── README.md
```

## Sample datasets
| File | Used for |
|------|----------|
| employees.csv | reads, filters, nulls, duplicates, windows, joins |
| departments.csv | joins, SQL |
| customers.csv | joins, broadcast, medallion |
| sales.csv | aggregations, dates, partitioning, pipelines |
| orders_nested.json | arrays & nested data |
| raw_orders_dirty.csv | ETL and data-quality practice |

## About
Built by **Arbaz** while preparing for data engineering roles.
[GitHub](https://github.com/Arbaz7761) | [LinkedIn](https://linkedin.com/in/arbaz-ali-948407381)
