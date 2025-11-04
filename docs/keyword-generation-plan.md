# Keyword Generation — Option B (Cached Compute, no city/state)

## Day 1 (MVP)
- Deterministic tokenization + normalization
- Synonym expansion from `ckw_seeds`
- Respect `ckw_locked`; append `ckw_manual_extra`
- Stamp `ckw-1`
- Use `scripts/ckw_recompute.py` (dry-run first, then run on DB copy)

## How to run
1) Dry-run (no writes):    ./scripts/ckw_recompute.py --dry-run --limit 50
2) Full on DB **copy**:    cp -p providers.db providers.TEST.db && SQLITE_PATH=providers.TEST.db ./scripts/ckw_recompute.py
3) Inspect copy, then decide on real DB.

## Day 2 (later)
- Admin buttons (Dry-run / Recompute filtered / Recompute all)
- Version bump helper; import/export seeds; observability
