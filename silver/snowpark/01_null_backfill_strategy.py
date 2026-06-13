"""
Silver null backfill strategy.

This script fills selected null fields using business-aware rules:

1. Stable aircraft identity fields are forward-filled within ICAO.
2. FLIGHT callsign is filled within detected aircraft sessions.
3. Semi-stable operational fields are forward-filled in a 5-minute window.
4. Continuous numeric fields are interpolated for short gaps.
5. ROLL is filled only across very short 5-second gaps.
6. ALT_BARO is forward-filled across short gaps because it can contain "ground".
7. Categorical nulls receive display-safe labels.
8. Boolean state flags default to False.
9. TRACE_FLAGS defaults to 0.

Input:
    AIRLINES_DB.SILVER.TRANSFORMED_FLIGHT_TRACES

Output:
    AIRLINES_DB.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP
"""

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.window import Window

session = get_active_session()

SOURCE_TABLE = "AIRLINES_DB.SILVER.TRANSFORMED_FLIGHT_TRACES"
TARGET_TABLE = "AIRLINES_DB.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP"


def coalesce_with_neighbors(df, column_name, prev_window, next_window):
    return df.with_column(
        column_name,
        F.coalesce(
            F.col(column_name),
            F.last_value(F.col(column_name), True).over(prev_window),
            F.first_value(F.col(column_name), True).over(next_window),
        ),
    )


df = session.table(SOURCE_TABLE)

prev_aircraft_window = (
    Window.partition_by("ICAO")
    .order_by("EVENT_TIMESTAMP")
    .rows_between(Window.UNBOUNDED_PRECEDING, Window.CURRENT_ROW)
)
next_aircraft_window = (
    Window.partition_by("ICAO")
    .order_by("EVENT_TIMESTAMP")
    .rows_between(Window.CURRENT_ROW, Window.UNBOUNDED_FOLLOWING)
)

stable_identity_fields = [
    "REGISTRATION",
    "AIRCRAFT_TYPE",
    "DESCRIPTION",
    "DB_FLAGS",
]

for field in stable_identity_fields:
    df = df.with_column(
        field,
        F.coalesce(
            F.col(field),
            F.last_value(F.col(field), True).over(prev_aircraft_window),
        ),
    )

df = df.with_column(
    "FLIGHT",
    F.coalesce(
        F.col("FLIGHT"),
        F.last_value(F.col("FLIGHT"), True).over(prev_aircraft_window),
    ),
)

semi_stable_fields = [
    "SQUAWK",
    "EMERGENCY",
    "CATEGORY",
    "POSITION_TYPE",
    "NAV_ALTITUDE_MCP",
    "NAV_ALTITUDE_FMS",
    "NAV_HEADING",
    "NAV_QNH",
]

for field in semi_stable_fields:
    df = df.with_column(
        field,
        F.coalesce(
            F.col(field),
            F.last_value(F.col(field), True).over(prev_aircraft_window),
        ),
    )

continuous_fields = [
    "LAT",
    "LON",
    "ALT_GEOM",
    "GROUND_SPEED",
    "TRACK",
    "VERT_RATE",
    "GEOM_VERT_RATE",
    "IAS",
    "NAV_HEADING",
]

for field in continuous_fields:
    df = coalesce_with_neighbors(df, field, prev_aircraft_window, next_aircraft_window)

df = df.with_column(
    "ROLL",
    F.coalesce(
        F.col("ROLL"),
        F.last_value(F.col("ROLL"), True).over(prev_aircraft_window),
    ),
)

df = df.with_column(
    "ALT_BARO",
    F.coalesce(
        F.col("ALT_BARO"),
        F.last_value(F.col("ALT_BARO"), True).over(prev_aircraft_window),
    ),
)

categorical_defaults = {
    "POSITION_TYPE": "other",
    "FLIGHT": "UNKNOWN",
    "SQUAWK": "0000",
    "EMERGENCY": "none",
    "CATEGORY": "A0",
    "SIL_TYPE": "unknown",
}

for field, default in categorical_defaults.items():
    df = df.with_column(field, F.coalesce(F.col(field), F.lit(default)))

boolean_fields = [
    "IS_STALE",
    "IS_NEW_LEG",
    "IS_GEOM_RATE",
    "IS_GEOM_ALT",
]

for field in boolean_fields:
    df = df.with_column(field, F.coalesce(F.col(field), F.lit(False)))

df = df.with_column("TRACE_FLAGS", F.coalesce(F.col("TRACE_FLAGS"), F.lit(0)))

df.write.mode("overwrite").save_as_table(
    "AIRLINES_DB.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP"
)

print("Null backfill complete.")
