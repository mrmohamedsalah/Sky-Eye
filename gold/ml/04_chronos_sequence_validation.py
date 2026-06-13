"""
Chronos sequence validation.

Chronos is used as a secondary model to validate whether a recent aircraft
movement sequence supports or rejects the XGBoost anomaly signal.
"""

import pandas as pd


TARGET_COLUMNS = [
    "LAT",
    "LON",
    "RESOLVED_ALTITUDE",
    "GROUND_SPEED",
    "RESOLVED_VERT_RATE",
    "TRACK_SIN",
    "TRACK_COS",
]


def build_sequence_windows(
    df: pd.DataFrame,
    aircraft_col: str = "ICAO",
    time_col: str = "EVENT_TIMESTAMP",
    window_size: int = 32,
):
    """
    Build sequence windows per aircraft.

    This prepares the input structure for Chronos or any time-series model.
    """
    windows = []

    df = df.sort_values([aircraft_col, time_col])

    for icao, group in df.groupby(aircraft_col):
        group = group.reset_index(drop=True)

        if len(group) <= window_size:
            continue

        for i in range(window_size, len(group)):
            history = group.iloc[i - window_size : i]
            actual = group.iloc[i]

            windows.append(
                {
                    "ICAO": icao,
                    "EVENT_TIMESTAMP": actual[time_col],
                    "HISTORY": history[TARGET_COLUMNS].to_dict("records"),
                    "ACTUAL": actual[TARGET_COLUMNS].to_dict(),
                }
            )

    return windows


def score_chronos_placeholder(windows):
    """
    Placeholder for Chronos inference.

    Replace this with the exact Chronos predict_df implementation you used.
    """
    results = []

    for item in windows:
        results.append(
            {
                "ICAO": item["ICAO"],
                "EVENT_TIMESTAMP": item["EVENT_TIMESTAMP"],
                "CHRONOS_SCORE": None,
                "CHRONOS_CONFIRMED": None,
            }
        )

    return pd.DataFrame(results)


if __name__ == "__main__":
    print("Build sequence windows and run Chronos validation.")
