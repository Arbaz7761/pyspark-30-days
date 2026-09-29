# Day 02 - Schema & CSV reading

## What I learned
- Why inferSchema is slow / risky in production
- StructType / StructField
- DDL string schema
- Read modes: PERMISSIVE, DROPMALFORMED, FAILFAST
- Writing CSV back

## Run it
```bash
# from the repo root
python Day-02-schema-and-csv-reading/day02.py
```

## Files
- `day02.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
