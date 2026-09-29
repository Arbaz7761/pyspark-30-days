# Day 28 - File Formats & Incremental Processing

## What I learned
- CSV vs JSON vs Parquet vs ORC (size, schema, speed)
- Choosing the right format
- Incremental load with high-water mark
- Append new data + dedupe on key (latest wins)
- Idempotent pipelines

## Run it
```bash
# from the repo root
python Day-28-file-formats-and-incremental-processing/day28.py
```

## Files
- `day28.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
