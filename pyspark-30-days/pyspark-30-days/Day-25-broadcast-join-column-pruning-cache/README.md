# Day 25 - Broadcast Join, Column Pruning & Cache

## What I learned
- Broadcast join in practice
- Column pruning & predicate pushdown proof via explain
- Cache the right thing at the right point
- Combining the three in one pipeline

## Run it
```bash
# from the repo root
python Day-25-broadcast-join-column-pruning-cache/day25.py
```

## Files
- `day25.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
