# Gold Analytics Layer

The Gold Analytics layer converts the clean Silver table into BI-ready analytical tables.

## Input

```text
AIRLINES.SILVER.FLIGHT_TRACES_CLEAN
```

## Outputs

```text
AIRLINES.GOLD_ANALYTICS.FLIGHT_TRACES_ANALYTICS
AIRLINES.GOLD_ANALYTICS.DIM_AIRCRAFT
AIRLINES.GOLD_ANALYTICS.DIM_FLIGHT
AIRLINES.GOLD_ANALYTICS.DIM_DATE
AIRLINES.GOLD_ANALYTICS.DIM_TIME
AIRLINES.GOLD_ANALYTICS.DIM_SQUAWK
AIRLINES.GOLD_ANALYTICS.DIM_EMERGENCY
AIRLINES.GOLD_ANALYTICS.DIM_TRACE_FLAGS
AIRLINES.GOLD_ANALYTICS.FACT_FLIGHT_TRACE
```

## Purpose

- Add BI-friendly labels.
- Classify callsigns.
- Categorize squawk codes.
- Add emergency flags.
- Create date and time attributes.
- Add climb-state labels.
- Build a star schema for Power BI.

## Fact Grain

One row per aircraft position report.
