"""
Final Silver transformation.

This script performs universal cleaning, validation, type-casting, and resolving.
It produces one clean source of truth for both Gold Analytics and Gold ML.

Input:
    AIRLINES.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP

Output:
    AIRLINES.SILVER.FLIGHT_TRACES_CLEAN
"""

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.window import Window

session = get_active_session()

SOURCE_TABLE = "AIRLINES.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP"
TARGET_TABLE = "AIRLINES.SILVER.FLIGHT_TRACES_CLEAN"


def null_if_blank(column_name: str):
    cleaned = F.trim(F.col(column_name))
    return F.when(cleaned == F.lit(""), F.lit(None)).otherwise(cleaned)


def valid_range(column_name: str, minimum, maximum):
    value = F.col(column_name)
    return F.when((value >= F.lit(minimum)) & (value <= F.lit(maximum)), value).otherwise(
        F.lit(None)
    )


def transform_silver(session) -> None:
    df = session.table("AIRLINES.SILVER.TRANSFORMED_FLIGHT_TRACES_TEMP")

    df = df.with_column("ICAO", F.upper(F.trim(F.col("ICAO"))))
    df = df.with_column("REGISTRATION", F.upper(F.trim(F.col("REGISTRATION"))))
    df = df.with_column("AIRCRAFT_TYPE", F.upper(F.trim(F.col("AIRCRAFT_TYPE"))))
    df = df.with_column("DESCRIPTION", F.trim(F.col("DESCRIPTION")))
    df = df.with_column("CATEGORY", F.upper(F.trim(F.col("CATEGORY"))))

    df = df.filter(
        ~(
            F.col("REGISTRATION").is_null()
            & F.col("AIRCRAFT_TYPE").is_null()
            & F.col("DESCRIPTION").is_null()
        )
    )

    df = df.with_column("FLIGHT", F.upper(null_if_blank("FLIGHT")))
    df = df.with_column(
        "FLIGHT",
        F.when(F.sql_expr("REGEXP_LIKE(FLIGHT, '^[A-Z0-9 ]{2,12}$')"), F.col("FLIGHT")).otherwise(
            F.lit(None)
        ),
    )

    df = df.with_column("SQUAWK", null_if_blank("SQUAWK"))
    df = df.with_column(
        "SQUAWK",
        F.when(F.sql_expr("REGEXP_LIKE(SQUAWK, '^[0-7]{4}$')"), F.col("SQUAWK")).otherwise(
            F.lit(None)
        ),
    )

    df = df.with_column("EMERGENCY", F.upper(F.coalesce(null_if_blank("EMERGENCY"), F.lit("NONE"))))
    df = df.with_column(
        "EMERGENCY",
        F.when(
            F.col("EMERGENCY").isin(
                "NONE",
                "NO",
                "GENERAL",
                "LIFEGUARD",
                "MINFUEL",
                "NORDO",
                "UNLAWFUL",
                "DOWNED",
                "RESERVED",
            ),
            F.col("EMERGENCY"),
        ).otherwise(F.lit("OTHER")),
    )

    df = df.with_column(
        "IS_ON_GROUND",
        F.when(F.lower(F.col("ALT_BARO").cast("string")) == F.lit("ground"), F.lit(True)).otherwise(
            F.lit(False)
        ),
    )

    df = df.with_column(
        "ALT_BARO_FT",
        F.when(F.lower(F.col("ALT_BARO").cast("string")) == F.lit("ground"), F.lit(0.0)).otherwise(
            F.col("ALT_BARO").cast("double")
        ),
    )

    df = df.with_column(
        "RESOLVED_ALTITUDE",
        F.coalesce(
            F.when(F.col("IS_GEOM_ALT") == F.lit(True), F.col("ALT_GEOM")),
            F.col("ALT_BARO_FT"),
            F.col("ALT_GEOM"),
        ),
    )

    df = df.with_column(
        "RESOLVED_VERT_RATE",
        F.coalesce(
            F.when(F.col("IS_GEOM_RATE") == F.lit(True), F.col("GEOM_VERT_RATE")),
            F.col("VERT_RATE"),
            F.col("GEOM_VERT_RATE"),
        ),
    )

    df = df.with_column("GROUND_SPEED", valid_range("GROUND_SPEED", 0, 1200))
    df = df.with_column("TRACK", valid_range("TRACK", 0, 360))
    df = df.with_column("NAV_HEADING", valid_range("NAV_HEADING", 0, 360))
    df = df.with_column("NAV_QNH", valid_range("NAV_QNH", 800, 1100))
    df = df.with_column("LAT", valid_range("LAT", -90, 90))
    df = df.with_column("LON", valid_range("LON", -180, 180))

    integrity_ranges = {
        "NIC": (0, 11),
        "NAC_P": (0, 11),
        "NAC_V": (0, 4),
        "SIL": (0, 3),
        "GVA": (0, 2),
        "SDA": (0, 3),
        "VERSION": (0, 2),
    }

    for field, (minimum, maximum) in integrity_ranges.items():
        df = df.with_column(field, valid_range(field, minimum, maximum))

    df = df.with_column(
        "POSITION_TYPE",
        F.upper(F.regexp_replace(F.coalesce(null_if_blank("POSITION_TYPE"), F.lit("other")), "-", "_")),
    )
    df = df.with_column(
        "POSITION_TYPE",
        F.when(
            F.col("POSITION_TYPE").isin(
                "ADSB_ICAO",
                "MLAT",
                "ADSR_ICAO",
                "TISB_ICAO",
                "MODE_S",
                "ADSC",
                "OTHER",
            ),
            F.col("POSITION_TYPE"),
        ).otherwise(F.lit("OTHER")),
    )

    df = df.with_column("IS_STALE", F.coalesce(F.col("IS_STALE"), F.lit(False)))
    df = df.filter(F.col("IS_STALE") == F.lit(False))

    df = df.with_column(
        "AIRCRAFT_SESSION_ID",
        F.abs(F.hash(F.col("ICAO"), F.to_date(F.col("EVENT_TIMESTAMP")))),
    )

    _session_window = Window.partition_by("AIRCRAFT_SESSION_ID").order_by("EVENT_TIMESTAMP")

    df = df.drop(
        "ALT_BARO",
        "ALT_GEOM",
        "VERT_RATE",
        "GEOM_VERT_RATE",
        "IAS",
        "ROLL",
    )

    df.write.mode("overwrite").save_as_table(TARGET_TABLE)
    print(f"Silver table written: {TARGET_TABLE}")


transform_silver(session)
