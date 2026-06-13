"""
Gold Analytics star-schema build.

This script creates BI-ready labels and a star schema for Power BI.

Input:
    AIRLINES.SILVER.FLIGHT_TRACES_CLEAN

Outputs:
    AIRLINES.GOLD_ANALYTICS.FLIGHT_TRACES_ANALYTICS
    AIRLINES.GOLD_ANALYTICS.DIM_AIRCRAFT
    AIRLINES.GOLD_ANALYTICS.DIM_FLIGHT
    AIRLINES.GOLD_ANALYTICS.DIM_DATE
    AIRLINES.GOLD_ANALYTICS.DIM_TIME
    AIRLINES.GOLD_ANALYTICS.DIM_SQUAWK
    AIRLINES.GOLD_ANALYTICS.DIM_EMERGENCY
    AIRLINES.GOLD_ANALYTICS.DIM_TRACE_FLAGS
    AIRLINES.GOLD_ANALYTICS.FACT_FLIGHT_TRACE
"""

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session

session = get_active_session()

SCHEMA = "AIRLINES.GOLD_ANALYTICS"
ANALYTICS_TABLE = f"{SCHEMA}.FLIGHT_TRACES_ANALYTICS"
SOURCE_TABLE = "AIRLINES.SILVER.FLIGHT_TRACES_CLEAN"

CATEGORY_MAP = {
    "A0": "No ADS-B emitter category",
    "A1": "Light Aircraft",
    "A2": "Small Aircraft",
    "A3": "Large Aircraft",
    "A4": "High Vortex Large",
    "A5": "Heavy Aircraft",
    "A6": "High Performance",
    "A7": "Rotorcraft",
    "B0": "No ADS-B emitter category (B)",
    "B1": "Glider / Sailplane",
    "B2": "Lighter than Air",
    "B3": "Parachutist / Sky Diver",
    "B4": "Ultralight / Hang Glider",
    "B5": "Reserved",
    "B6": "UAV / Drone",
    "B7": "Space Vehicle",
    "C0": "No ADS-B emitter category (C)",
    "C1": "Surface Vehicle - Emergency",
    "C2": "Surface Vehicle - Service",
    "C3": "Fixed Ground Obstacle",
}


def build_category_label_expr(col_name: str):
    result = F.lit("Unknown category")
    for code, label in reversed(list(CATEGORY_MAP.items())):
        result = F.when(F.col(col_name) == F.lit(code), F.lit(label)).otherwise(result)
    return result


def sk(*cols):
    return F.abs(F.hash(*cols))


def transform_gold_analytics(session) -> None:
    df = session.table(SOURCE_TABLE)

    df = df.with_column("CATEGORY_LABEL", build_category_label_expr("CATEGORY"))

    df = df.with_column(
        "CALLSIGN_CLASS",
        F.when(F.col("FLIGHT").is_null(), F.lit("UNKNOWN"))
        .when(F.sql_expr("REGEXP_LIKE(FLIGHT, '^[A-Z]{2,3}[0-9].*')"), F.lit("AIRLINE"))
        .when(F.sql_expr("REGEXP_LIKE(FLIGHT, '^[A-Z0-9]{2,7}$')"), F.lit("GENERAL_AVIATION"))
        .otherwise(F.lit("OTHER")),
    )

    df = df.with_column(
        "SQUAWK_CATEGORY",
        F.when(F.col("SQUAWK") == F.lit("7700"), F.lit("GENERAL_EMERGENCY"))
        .when(F.col("SQUAWK") == F.lit("7600"), F.lit("RADIO_FAILURE"))
        .when(F.col("SQUAWK") == F.lit("7500"), F.lit("HIJACK"))
        .when(F.col("SQUAWK").is_null(), F.lit("UNKNOWN"))
        .otherwise(F.lit("NORMAL")),
    )

    df = df.with_column(
        "HAS_EMERGENCY",
        F.when(
            (F.col("SQUAWK").isin("7700", "7600", "7500"))
            | (~F.col("EMERGENCY").isin("NONE", "NO", "OTHER")),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )

    df = df.with_column("EVENT_DATE", F.to_date(F.col("EVENT_TIMESTAMP")))
    df = df.with_column("EVENT_HOUR", F.date_part("hour", F.col("EVENT_TIMESTAMP")))
    df = df.with_column("EVENT_MINUTE", F.date_part("minute", F.col("EVENT_TIMESTAMP")))
    df = df.with_column("EVENT_DOW", F.date_part("dayofweek", F.col("EVENT_TIMESTAMP")))
    df = df.with_column("EVENT_MONTH", F.date_part("month", F.col("EVENT_TIMESTAMP")))
    df = df.with_column("EVENT_YEAR", F.date_part("year", F.col("EVENT_TIMESTAMP")))

    df = df.with_column(
        "INGESTION_LAG_SECONDS",
        F.when(
            F.col("INGESTION_TS").is_not_null(),
            F.datediff("second", F.col("EVENT_TIMESTAMP"), F.col("INGESTION_TS")),
        ).otherwise(F.lit(None)),
    )

    df = df.with_column(
        "CLIMB_STATE",
        F.when(F.col("IS_ON_GROUND") == F.lit(True), F.lit("ON_GROUND"))
        .when(F.col("RESOLVED_VERT_RATE") > F.lit(300), F.lit("CLIMB"))
        .when(F.col("RESOLVED_VERT_RATE") < F.lit(-300), F.lit("DESCENT"))
        .otherwise(F.lit("LEVEL")),
    )

    df = df.with_column(
        "SIGNAL_INTEGRITY_CLASS",
        F.when((F.col("NIC") >= F.lit(8)) & (F.col("NAC_P") >= F.lit(8)) & (F.col("SIL") >= F.lit(2)), F.lit("HIGH"))
        .when((F.col("NIC") >= F.lit(5)) & (F.col("NAC_P") >= F.lit(5)), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    df.write.mode("overwrite").save_as_table(ANALYTICS_TABLE)
    print(f"Analytics table written: {ANALYTICS_TABLE}")


def build_star_schema(session) -> None:
    df = session.table(ANALYTICS_TABLE)

    dim_aircraft = (
        df.select(
            sk(F.col("ICAO"), F.col("REGISTRATION"), F.col("AIRCRAFT_TYPE")).alias("AIRCRAFT_KEY"),
            F.col("ICAO"),
            F.col("REGISTRATION"),
            F.col("AIRCRAFT_TYPE"),
            F.col("DESCRIPTION"),
            F.col("CATEGORY"),
            F.col("CATEGORY_LABEL"),
        )
        .distinct()
    )
    dim_aircraft.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_AIRCRAFT")

    dim_flight = (
        df.select(
            sk(F.col("FLIGHT"), F.col("CALLSIGN_CLASS")).alias("FLIGHT_KEY"),
            F.col("FLIGHT"),
            F.col("CALLSIGN_CLASS"),
        )
        .distinct()
    )
    dim_flight.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_FLIGHT")

    dim_date = (
        df.select(
            sk(F.col("EVENT_DATE")).alias("DATE_KEY"),
            F.col("EVENT_DATE"),
            F.col("EVENT_YEAR"),
            F.col("EVENT_MONTH"),
            F.col("EVENT_DOW"),
        )
        .distinct()
    )
    dim_date.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_DATE")

    dim_time = (
        df.select(
            sk(F.col("EVENT_HOUR"), F.col("EVENT_MINUTE")).alias("TIME_KEY"),
            F.col("EVENT_HOUR"),
            F.col("EVENT_MINUTE"),
        )
        .distinct()
    )
    dim_time.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_TIME")

    dim_squawk = (
        df.select(
            sk(F.col("SQUAWK"), F.col("SQUAWK_CATEGORY")).alias("SQUAWK_KEY"),
            F.col("SQUAWK"),
            F.col("SQUAWK_CATEGORY"),
        )
        .distinct()
    )
    dim_squawk.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_SQUAWK")

    dim_emergency = (
        df.select(
            sk(F.col("EMERGENCY"), F.col("HAS_EMERGENCY")).alias("EMERGENCY_KEY"),
            F.col("EMERGENCY"),
            F.col("HAS_EMERGENCY"),
        )
        .distinct()
    )
    dim_emergency.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_EMERGENCY")

    dim_trace_flags = (
        df.select(
            sk(F.col("TRACE_FLAGS"), F.col("IS_NEW_LEG"), F.col("IS_GEOM_RATE"), F.col("IS_GEOM_ALT")).alias(
                "TRACE_FLAGS_KEY"
            ),
            F.col("TRACE_FLAGS"),
            F.col("IS_NEW_LEG"),
            F.col("IS_GEOM_RATE"),
            F.col("IS_GEOM_ALT"),
            F.col("POSITION_TYPE"),
            F.col("SIGNAL_INTEGRITY_CLASS"),
        )
        .distinct()
    )
    dim_trace_flags.write.mode("overwrite").save_as_table(f"{SCHEMA}.DIM_TRACE_FLAGS")

    fact = df.select(
        sk(F.col("ICAO"), F.col("EVENT_TIMESTAMP")).alias("TRACE_KEY"),
        sk(F.col("ICAO"), F.col("REGISTRATION"), F.col("AIRCRAFT_TYPE")).alias("AIRCRAFT_KEY"),
        sk(F.col("FLIGHT"), F.col("CALLSIGN_CLASS")).alias("FLIGHT_KEY"),
        sk(F.col("EVENT_DATE")).alias("DATE_KEY"),
        sk(F.col("EVENT_HOUR"), F.col("EVENT_MINUTE")).alias("TIME_KEY"),
        sk(F.col("SQUAWK"), F.col("SQUAWK_CATEGORY")).alias("SQUAWK_KEY"),
        sk(F.col("EMERGENCY"), F.col("HAS_EMERGENCY")).alias("EMERGENCY_KEY"),
        sk(F.col("TRACE_FLAGS"), F.col("IS_NEW_LEG"), F.col("IS_GEOM_RATE"), F.col("IS_GEOM_ALT")).alias(
            "TRACE_FLAGS_KEY"
        ),
        F.col("EVENT_TIMESTAMP"),
        F.col("LAT"),
        F.col("LON"),
        F.col("RESOLVED_ALTITUDE"),
        F.col("GROUND_SPEED"),
        F.col("RESOLVED_VERT_RATE"),
        F.col("TRACK"),
        F.col("CLIMB_STATE"),
        F.col("IS_ON_GROUND"),
        F.col("INGESTION_LAG_SECONDS"),
    )
    fact.write.mode("overwrite").save_as_table(f"{SCHEMA}.FACT_FLIGHT_TRACE")


transform_gold_analytics(session)
build_star_schema(session)

print("Gold_Analytics pipeline finished.")
