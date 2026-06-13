"""
Gold ML feature engineering.

Creates ML-ready features from the cleaned Silver table.

Input:
    AIRLINES.SILVER.FLIGHT_TRACES_CLEAN

Suggested output:
    AIRLINES.GOLD_ML.PRESTAGING_FLIGHT_FEATURES
"""

from snowflake.snowpark import functions as F
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.window import Window

session = get_active_session()


SOURCE_TABLE = "AIRLINES.SILVER.FLIGHT_TRACES_CLEAN"
TARGET_TABLE = "AIRLINES.GOLD_ML.PRESTAGING_FLIGHT_FEATURES"


def build_features(session):
    df = session.table(SOURCE_TABLE)

    # Keep rows with usable position and time.
    df = df.filter(
        F.col("ICAO").is_not_null()
        & F.col("EVENT_TIMESTAMP").is_not_null()
        & F.col("LAT").is_not_null()
        & F.col("LON").is_not_null()
    )

    # Convert track angle into sine/cosine to avoid 0/360 wrap-around.
    df = df.with_column("TRACK_RAD", F.radians(F.col("TRACK")))
    df = df.with_column("TRACK_SIN", F.sin(F.col("TRACK_RAD")))
    df = df.with_column("TRACK_COS", F.cos(F.col("TRACK_RAD")))

    # Create event unix timestamp.
    df = df.with_column(
        "EVENT_UNIX_TS",
        F.date_part("epoch_second", F.col("EVENT_TIMESTAMP")),
    )

    # Lag features by aircraft.
    w = Window.partition_by("ICAO").order_by("EVENT_TIMESTAMP")

    df = df.with_column("PREV_LAT", F.lag("LAT").over(w))
    df = df.with_column("PREV_LON", F.lag("LON").over(w))
    df = df.with_column("PREV_ALTITUDE", F.lag("RESOLVED_ALTITUDE").over(w))
    df = df.with_column("PREV_GROUND_SPEED", F.lag("GROUND_SPEED").over(w))
    df = df.with_column("PREV_VERT_RATE", F.lag("RESOLVED_VERT_RATE").over(w))
    df = df.with_column("PREV_TRACK_SIN", F.lag("TRACK_SIN").over(w))
    df = df.with_column("PREV_TRACK_COS", F.lag("TRACK_COS").over(w))

    # Future target at next observation.
    df = df.with_column("NEXT_LAT", F.lead("LAT").over(w))
    df = df.with_column("NEXT_LON", F.lead("LON").over(w))
    df = df.with_column("NEXT_ALTITUDE", F.lead("RESOLVED_ALTITUDE").over(w))
    df = df.with_column("NEXT_GROUND_SPEED", F.lead("GROUND_SPEED").over(w))
    df = df.with_column("NEXT_VERT_RATE", F.lead("RESOLVED_VERT_RATE").over(w))
    df = df.with_column("NEXT_TRACK_SIN", F.lead("TRACK_SIN").over(w))
    df = df.with_column("NEXT_TRACK_COS", F.lead("TRACK_COS").over(w))

    # Basic model-ready filtering.
    df = df.filter(
        F.col("NEXT_LAT").is_not_null()
        & F.col("NEXT_LON").is_not_null()
        & F.col("NEXT_ALTITUDE").is_not_null()
    )

    df = df.drop("TRACK_RAD")

    df.write.mode("overwrite").save_as_table(TARGET_TABLE)
    print(f"Feature table written: {TARGET_TABLE}")


if __name__ == "__main__":
    build_features(session)
