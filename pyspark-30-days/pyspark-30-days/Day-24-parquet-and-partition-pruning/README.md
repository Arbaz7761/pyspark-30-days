# Day 24 - Parquet & Partition Pruning

## What I learned
- Row vs columnar formats
- Writing & reading Parquet
- partitionBy on write
- Partition pruning in explain()
- Choosing partition columns (low cardinality)
- Compression

## Run it
```bash
# from the repo root
python Day-24-parquet-and-partition-pruning/day24.py
```

## Files
- `day24.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
