# FLIGHT_TRACES_RAW Column Dictionary

Table:

```text
AIRLINES_DB.BRONZE.FLIGHT_TRACES_RAW
```

This table stores raw parsed ADS-B trace observations before Silver cleaning.

## Aircraft Identity

### ICAO

Primary aircraft identifier from ADS-B / trace file.

- Type: `VARCHAR(10)`
- Meaning: 24-bit ICAO address, usually 6 hex characters.
- Notes: Values starting with `~` are non-ICAO addresses such as TIS-B or ground vehicles.

### REGISTRATION

Aircraft tail number / registration.

- Type: `VARCHAR(20)`
- Source: reference database field `r`
- Notes: Not every ADS-B message includes registration.

### AIRCRAFT_TYPE

Aircraft model/type code.

- Type: `VARCHAR(10)`
- Source: reference database field `t`
- Example: `A320`, `B738`

### DESCRIPTION

Human-readable aircraft description.

- Type: `VARCHAR(100)`
- Example: `AIRBUS A-320`

### DB_FLAGS

Database classification bitfield.

- Type: `NUMBER`
- Examples: military, interesting, PIA, LADD

## Time

### DATA_DATE

Calendar date used for partitioning and batch filtering.

### EVENT_TIMESTAMP

UTC timestamp of the aircraft observation.

## Position

### LAT

Latitude in decimal degrees.

### LON

Longitude in decimal degrees.

### ALT_BARO

Barometric altitude in feet or the string `ground`.

### ALT_GEOM

Geometric GNSS/INS altitude in feet.

## Motion

### GROUND_SPEED

Ground speed in knots.

### TRACK

True track over ground in degrees.

### VERT_RATE

Barometric vertical rate in feet per minute.

### GEOM_VERT_RATE

Geometric vertical rate in feet per minute.

### IAS

Indicated airspeed in knots.

### ROLL

Roll angle in degrees.

## Trace Flags

### TRACE_FLAGS

Raw bitfield from the trace array.

### IS_STALE

True when no position was received for 20+ seconds before this point.

### IS_NEW_LEG

True when this point marks the start of a new flight leg.

### IS_GEOM_RATE

True when the vertical-rate field is geometric.

### IS_GEOM_ALT

True when the altitude field is geometric.

## Source Quality

### POSITION_TYPE

Technology/source of the position message.

Examples:

- `adsb_icao`
- `mlat`
- `adsr_icao`
- `tisb_icao`
- `mode_s`
- `adsc`
- `other`

## Flight Identity

### FLIGHT

Broadcast callsign or flight number.

### SQUAWK

Mode A transponder code.

Special squawk codes:

- `7700`: emergency
- `7600`: radio failure
- `7500`: hijack

### EMERGENCY

ADS-B emergency/priority status.

### CATEGORY

ADS-B emitter category such as `A1`, `A3`, `A5`, `B1`, or `C1`.

## Navigation and Signal Integrity

Important fields:

- `NAV_ALTITUDE_MCP`
- `NAV_ALTITUDE_FMS`
- `NAV_HEADING`
- `NAV_QNH`
- `NIC`
- `RC`
- `NAC_P`
- `NAC_V`
- `SIL`
- `SIL_TYPE`
- `GVA`
- `SDA`
- `VERSION`

## Pipeline Metadata

### SOURCE_FILE

Source trace JSON file path.

### INGESTION_TS

Timestamp when the row was loaded into Snowflake.
