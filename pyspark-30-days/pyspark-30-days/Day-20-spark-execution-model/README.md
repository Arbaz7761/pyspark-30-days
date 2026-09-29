# Day 20 - Spark Execution Model

## What I learned
- Driver, executors, cluster manager
- Transformations (lazy) vs Actions
- Narrow vs Wide transformations
- DAG -> Jobs -> Stages -> Tasks
- explain() and reading physical plans
- Spark UI at http://localhost:4040

## Run it
```bash
# from the repo root
python Day-20-spark-execution-model/day20.py
```

## Files
- `day20.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
