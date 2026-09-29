# Day 21 - Join Optimization

## What I learned
- Sort-merge join vs broadcast hash join
- broadcast() hint & autoBroadcastJoinThreshold
- Filter and select BEFORE joining
- Join hints: broadcast, merge, shuffle_hash
- Reading join strategy in explain()

## Run it
```bash
# from the repo root
python Day-21-join-optimization/day21.py
```

## Files
- `day21.py` - runnable PySpark code with comments
- Sample data is read from `../data/` (created by `generate_data.py`)
