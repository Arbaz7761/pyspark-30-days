# Day 18 - Repartition & Coalesce

## What I learned
- What partitions are and why they matter
- getNumPartitions, spark_partition_id
- repartition (full shuffle, increase/decrease)
- coalesce (no shuffle, only decrease)
- repartition by column
- Rules of thumb for partition sizing

## Run it
```bash
# from the repo root
python Day-18-repartition-and-coalesce/day18.py
```

## Files
- `day18.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
