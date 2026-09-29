# Day 22 - Debugging Slow Spark Jobs

## What I learned
- Systematic checklist for slow jobs
- Reading explain(formatted) and the Spark UI
- Detecting too many / too few partitions
- Repeated recomputation & missing cache
- Skew & shuffle spills
- Small files problem

## Run it
```bash
# from the repo root
python Day-22-debugging-slow-spark-jobs/day22.py
```

## Files
- `day22.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
