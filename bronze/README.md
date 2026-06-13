# Bronze Layer

The Bronze layer stores raw or lightly parsed ADS-B historical trace records.

## Purpose

- Preserve source records before heavy business logic.
- Keep lineage using `SOURCE_FILE` and `INGESTION_TS`.
- Keep raw aircraft identity, time, position, motion, trace flags, navigation, and signal-integrity fields.
- Provide the immutable input to the Silver layer.

## Main Tables

```text
AIRLINES_DB.BRONZE.FLIGHT_TRACES_RAW
AIRLINES_DB.BRONZE.FLIGHT_TRACES
```

## Important Columns

- `ICAO`
- `REGISTRATION`
- `AIRCRAFT_TYPE`
- `DESCRIPTION`
- `DATA_DATE`
- `EVENT_TIMESTAMP`
- `LAT`
- `LON`
- `ALT_BARO`
- `ALT_GEOM`
- `GROUND_SPEED`
- `TRACK`
- `VERT_RATE`
- `TRACE_FLAGS`
- `POSITION_TYPE`
- `FLIGHT`
- `SQUAWK`
- `EMERGENCY`
- `CATEGORY`
- `SOURCE_FILE`
- `INGESTION_TS`

The full column dictionary is available in:

```text
docs/data_dictionary/flight_traces_raw_columns.md
```
