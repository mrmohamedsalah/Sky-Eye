# Silver Layer

The Silver layer creates the trusted clean table consumed by both Gold Analytics and Gold ML.

## Main Output

```text
AIRLINES.SILVER.FLIGHT_TRACES_CLEAN
```

## Main Responsibilities

- Normalize identity fields.
- Validate callsigns and squawk codes.
- Resolve altitude fields.
- Resolve vertical-rate fields.
- Remove stale records.
- Drop redundant and high-null columns.
- Keep the table clean but not yet BI-specific or ML-specific.

## Scripts

| Script | Purpose |
|---|---|
| `01_null_backfill_strategy.py` | Fills important fields using time-aware and aircraft-aware rules |
| `02_transform_silver.py` | Applies final Silver validation, type-casting, filtering, and column cleanup |
| `03_deduplicate_icao_timestamp.sql` | Removes duplicate records with same ICAO and timestamp |
