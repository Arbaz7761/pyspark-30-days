# Day 30 - End-to-End Pipeline: Bronze → Silver → Gold

## What I learned
- Medallion architecture
- Bronze: raw + metadata, append only
- Silver: cleaned, typed, deduped, validated
- Gold: business-level aggregates
- Reusable layer functions
- Ready to port to Databricks / Delta

## Run it
```bash
# from the repo root
python Day-30-end-to-end-bronze-silver-gold/day30.py
```

## Files
- `day30.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
