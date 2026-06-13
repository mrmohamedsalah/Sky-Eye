# Power BI Dashboards

Power BI is used for historical analytics and business reporting.

## Data Source

Power BI reads from Snowflake Gold Analytics tables.

## Main Tables

```text
AIRLINES.GOLD_ANALYTICS.FACT_FLIGHT_TRACE
AIRLINES.GOLD_ANALYTICS.DIM_AIRCRAFT
AIRLINES.GOLD_ANALYTICS.DIM_FLIGHT
AIRLINES.GOLD_ANALYTICS.DIM_DATE
AIRLINES.GOLD_ANALYTICS.DIM_TIME
AIRLINES.GOLD_ANALYTICS.DIM_SQUAWK
AIRLINES.GOLD_ANALYTICS.DIM_EMERGENCY
AIRLINES.GOLD_ANALYTICS.DIM_TRACE_FLAGS
```

## Main Report Views

- Aircraft count over time
- Average velocity over time
- Average altitude over time
- Flight phase distribution
- Vertical-rate distribution
- Top countries
- Active aircraft KPI
- Airborne vs on-ground split
- Altitude band distribution

## Screenshots

Power BI screenshots are stored in:

```text
docs/dashboards/
```

## Note

Large `.pbix` files should not be committed unless Git LFS is enabled.
