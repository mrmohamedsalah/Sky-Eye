# Gold ML Layer

The Gold ML layer detects abnormal aircraft behavior using trajectory prediction residuals and sequence validation.

## Core Idea

If a model can predict where an aircraft should be in the next 20 seconds, then a large difference between the predicted state and the actual state can reveal abnormal behavior.

## Models

### XGBoost

Primary one-step trajectory residual detector.

It predicts the next aircraft state:

- Latitude
- Longitude
- Altitude
- Ground speed
- Vertical rate
- Track direction

Residuals are converted into anomaly scores.

### Chronos

Secondary sequence-dynamics confirmation model.

Chronos validates whether the recent aircraft movement sequence supports the anomaly detected by XGBoost.

## Main Steps

1. Feature engineering.
2. XGBoost next-state training.
3. Residual calculation.
4. Category-aware thresholding.
5. Episode grouping.
6. Chronos sequence validation.
7. Hybrid anomaly label creation.

## Output Tables

Suggested Snowflake outputs:

```text
AIRLINES.GOLD_ML.PRESTAGING_FLIGHT_FEATURES
AIRLINES.GOLD_ML.XGBOOST_PREDICTIONS
AIRLINES.GOLD_ML.XGBOOST_RESIDUAL_SCORES
AIRLINES.GOLD_ML.CHRONOS_VALIDATION
AIRLINES.GOLD_ML.HYBRID_ANOMALY_LABELS
AIRLINES.GOLD_ML.STREAM_FLIGHT_DATA
```
