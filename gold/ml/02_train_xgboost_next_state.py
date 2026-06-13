"""
Train XGBoost next-state prediction models.

The model predicts the next aircraft state from the current aircraft state.
Large residuals between prediction and actual future state become anomaly signals.
"""

import os

import joblib
import pandas as pd
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor


FEATURE_TABLE = "AIRLINES.GOLD_ML.PRESTAGING_FLIGHT_FEATURES"
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


def train_models(df: pd.DataFrame):
    os.makedirs(MODEL_DIR, exist_ok=True)

    X = df[FEATURE_COLUMNS].fillna(0)

    metrics = {}

    for target in TARGET_COLUMNS:
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            shuffle=True,
        )

        model = XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42,
        )

        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        mae = mean_absolute_error(y_test, preds)

        model_path = os.path.join(MODEL_DIR, f"{target.lower()}_model.joblib")
        joblib.dump(model, model_path)

        metrics[target] = mae
        print(f"{target}: MAE={mae:.6f}, saved={model_path}")

    return metrics


if __name__ == "__main__":
    print(
        "Load training data from Snowflake into a pandas DataFrame, "
        "then call train_models(df)."
    )
