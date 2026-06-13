"""
Hybrid anomaly labels.

Combines XGBoost residual anomaly signals with Chronos sequence validation.
"""

import pandas as pd


def assign_primary_reason(row):
    errors = {
        "POSITION": row.get("POSITION_ERROR_KM", 0),
        "ALTITUDE": row.get("ALTITUDE_ERROR_FT", 0) / 1000,
        "SPEED": row.get("GROUND_SPEED_ERROR", 0) / 100,
        "VERTICAL_RATE": row.get("VERT_RATE_ERROR", 0) / 1000,
        "TRACK": row.get("TRACK_ERROR", 0),
    }

    return max(errors, key=errors.get)


def build_hybrid_labels(
    xgb_scores: pd.DataFrame,
    chronos_scores: pd.DataFrame,
    threshold: float = 5.0,
) -> pd.DataFrame:
    df = xgb_scores.merge(
        chronos_scores,
        on=["ICAO", "EVENT_TIMESTAMP"],
        how="left",
    )

    df["XGB_IS_ANOMALY"] = df["XGB_ANOMALY_SCORE"] >= threshold

    df["PRIMARY_ANOMALY_REASON"] = df.apply(
        assign_primary_reason,
        axis=1,
    )

    df["HYBRID_ANOMALY_LABEL"] = df.apply(
        lambda row: bool(row["XGB_IS_ANOMALY"])
        if pd.isna(row.get("CHRONOS_CONFIRMED"))
        else bool(row["XGB_IS_ANOMALY"] and row["CHRONOS_CONFIRMED"]),
        axis=1,
    )

    return df


if __name__ == "__main__":
    print("Load XGBoost and Chronos outputs, then build hybrid anomaly labels.")
