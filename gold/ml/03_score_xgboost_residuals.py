"""
Score XGBoost residual anomalies.

This script compares actual next state vs predicted next state.
The residuals become anomaly signals.
"""

import math
import os

import joblib
import pandas as pd


MODEL_DIR = os.environ.get("MODEL_DIR", "models/xgboost")


FEATURE_COLUMNS = [
    "LAT",
    "LON",
    "RESOLVED_ALTITUDE",
    "GROUND_SPEED",
    "RESOLVED_VERT_RATE",
    "TRACK_SIN",
    "TRACK_COS",
    "NIC",
    "NAC_P",
    "NAC_V",
    "SIL",
]


TARGET_COLUMNS = [
    "NEXT_LAT",
    "NEXT_LON",
    "NEXT_ALTITUDE",
    "NEXT_GROUND_SPEED",
    "NEXT_VERT_RATE",
    "NEXT_TRACK_SIN",
    "NEXT_TRACK_COS",
]


def haversine_km(lat1, lon1, lat2, lon2):
    radius_km = 6371.0

    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = (
        math.sin(dp / 2) ** 2
        + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    )

    return 2 * radius_km * math.asin(math.sqrt(a))


def score(df: pd.DataFrame) -> pd.DataFrame:
    X = df[FEATURE_COLUMNS].fillna(0).copy()

    for target in TARGET_COLUMNS:
        model_path = os.path.join(MODEL_DIR, f"{target.lower()}_model.joblib")
        model = joblib.load(model_path)
        df[f"PRED_{target}"] = model.predict(X)

    df["POSITION_ERROR_KM"] = df.apply(
        lambda row: haversine_km(
            row["NEXT_LAT"],
            row["NEXT_LON"],
            row["PRED_NEXT_LAT"],
            row["PRED_NEXT_LON"],
        ),
        axis=1,
    )

    df["ALTITUDE_ERROR_FT"] = (
        df["NEXT_ALTITUDE"] - df["PRED_NEXT_ALTITUDE"]
    ).abs()

    df["GROUND_SPEED_ERROR"] = (
        df["NEXT_GROUND_SPEED"] - df["PRED_NEXT_GROUND_SPEED"]
    ).abs()

    df["VERT_RATE_ERROR"] = (
        df["NEXT_VERT_RATE"] - df["PRED_NEXT_VERT_RATE"]
    ).abs()

    df["TRACK_ERROR"] = (
        (df["NEXT_TRACK_SIN"] - df["PRED_NEXT_TRACK_SIN"]).abs()
        + (df["NEXT_TRACK_COS"] - df["PRED_NEXT_TRACK_COS"]).abs()
    )

    df["XGB_ANOMALY_SCORE"] = (
        df["POSITION_ERROR_KM"]
        + df["ALTITUDE_ERROR_FT"] / 1000
        + df["GROUND_SPEED_ERROR"] / 100
        + df["VERT_RATE_ERROR"] / 1000
        + df["TRACK_ERROR"]
    )

    return df


if __name__ == "__main__":
    print("Load feature data into pandas, call score(df), then write results to Snowflake.")
