"""
MinIO OpenSky Parquet -> Snowflake AIRLINES.GOLD_ML.STREAM_FLIGHT_DATA

This bridge reads streamed Parquet files from MinIO, converts OpenSky fields
into the ML serving schema, and writes rows into Snowflake.

The script tracks processed files using processed_files.txt.
"""

import os
import time

import numpy as np
import pandas as pd
import s3fs
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


# ==========================
# MinIO config
# ==========================

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET = os.environ.get("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "opensky-data")
MINIO_PREFIX = os.environ.get("MINIO_PREFIX", "opensky/Streaming/")


# ==========================
# Snowflake config
# ==========================

SF = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "AIRLINES"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "GOLD_ML"),
}

TARGET_TABLE = os.environ.get("SNOWFLAKE_STREAM_TABLE", "STREAM_FLIGHT_DATA")
PROCESSED_FILES_LOG = os.environ.get("PROCESSED_FILES_LOG", "processed_files.txt")
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))


CAT_MAP = {
    2: "A1",
    3: "A2",
    4: "A3",
    5: "A4",
    6: "A5",
    7: "A6",
    8: "B1",
    9: "B2",
    10: "B3",
    11: "C1",
    12: "C2",
    14: "C3",
    15: "C4",
}

COLS = [
    "ICAO",
    "SEGMENT_ID",
    "EVENT_UNIX_TS",
    "LAT",
    "LON",
    "RESOLVED_ALTITUDE",
    "GROUND_SPEED",
    "RESOLVED_VERT_RATE",
    "TRACK_SIN",
    "TRACK_COS",
    "NAV_HEADING_SIN",
    "NAV_HEADING_COS",
    "CATEGORY",
    "FLIGHT",
    "NIC",
    "SIL",
]


def get_minio_fs():
    return s3fs.S3FileSystem(
        key=MINIO_KEY,
        secret=MINIO_SECRET,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
        use_ssl=False,
    )


def get_conn():
    conn = snowflake.connector.connect(**SF)
    cur = conn.cursor()
    cur.execute(f"USE DATABASE {SF['database']}")
    cur.execute(f"USE SCHEMA {SF['schema']}")
    cur.execute(f"USE WAREHOUSE {SF['warehouse']}")
    cur.close()
    return conn


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    track_rad = np.radians(df["true_track"])
    track_sin = np.sin(track_rad)
    track_cos = np.cos(track_rad)

    out = pd.DataFrame(
        {
            "ICAO": df["icao24"].str.upper(),
            "SEGMENT_ID": None,
            "EVENT_UNIX_TS": df["last_contact"],
            "LAT": df["latitude"],
            "LON": df["longitude"],
            "RESOLVED_ALTITUDE": df["baro_altitude"] * 3.28084,
            "GROUND_SPEED": df["velocity"] * 1.94384,
            "RESOLVED_VERT_RATE": df["vertical_rate"] * 196.85,
            "TRACK_SIN": track_sin,
            "TRACK_COS": track_cos,
            "NAV_HEADING_SIN": track_sin,
            "NAV_HEADING_COS": track_cos,
            "CATEGORY": df["category"].map(CAT_MAP),
            "FLIGHT": df["callsign"].fillna("").astype(str).str.strip(),
            "NIC": 8,
            "SIL": 3,
        },
        columns=COLS,
    )

    out["CATEGORY"] = out["CATEGORY"].fillna("A3")
    return out.dropna(subset=["LAT", "LON"]).reset_index(drop=True)


def load_processed():
    if os.path.exists(PROCESSED_FILES_LOG):
        with open(PROCESSED_FILES_LOG, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_processed(name: str):
    with open(PROCESSED_FILES_LOG, "a", encoding="utf-8") as f:
        f.write(name + "\n")


def main():
    print("MinIO -> Snowflake bridge starting...")

    fs = get_minio_fs()
    processed = load_processed()
    conn = get_conn()

    while True:
        try:
            files = fs.glob(f"{MINIO_BUCKET}/{MINIO_PREFIX}/part-*.snappy.parquet")
            new_files = [path for path in files if path not in processed]

            if not new_files:
                print(f"No new files; sleeping {POLL_INTERVAL_SECONDS}s")
            else:
                print(f"{len(new_files)} new file(s)")

                if conn is None or conn.is_closed():
                    conn = get_conn()

                for path in new_files:
                    with fs.open(path, "rb") as fh:
                        raw = pd.read_parquet(fh)

                    if raw.empty:
                        mark_processed(path)
                        processed.add(path)
                        continue

                    transformed = transform_df(raw)

                    if transformed.empty:
                        mark_processed(path)
                        processed.add(path)
                        continue

                    ok, _, nrows, _ = write_pandas(
                        conn,
                        transformed,
                        TARGET_TABLE,
                        database=SF["database"],
                        schema=SF["schema"],
                        auto_create_table=False,
                        quote_identifiers=False,
                    )

                    if not ok:
                        raise RuntimeError(f"write_pandas failed for {path}")

                    mark_processed(path)
                    processed.add(path)
                    print(f"Loaded {nrows:,} rows <- {path}")

        except Exception as exc:
            print(f"Error: {exc!r} -- reconnecting next cycle")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            conn = None

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
