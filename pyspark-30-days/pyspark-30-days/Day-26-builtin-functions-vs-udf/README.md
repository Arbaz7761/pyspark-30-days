# Day 26 - Built-in Functions vs UDF

## What I learned
- Why built-ins are faster (JVM + Catalyst)
- Python UDF cost (serialisation)
- pandas_udf (vectorised, Arrow)
- When a UDF is unavoidable
- Registering UDFs for SQL

## Run it
```bash
# from the repo root
python Day-26-builtin-functions-vs-udf/day26.py
```

## Files
- `day26.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
