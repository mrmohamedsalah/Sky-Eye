# Streaming Pipeline

This folder contains the real-time ADS-B streaming pipeline.

## Flow

```text
OpenSky Flight API
        |
        v
Apache Kafka topic: opensky
        |
        v
Apache Spark Structured Streaming
        |
        v
Fan-out:
  1. MinIO raw Parquet storage
  2. ClickHouse OLAP table
        |
        v
Grafana live dashboards
```

A bridge script also promotes MinIO Parquet files into Snowflake for near real-time ML serving.

## Main Files

| File | Purpose |
|---|---|
| `producer_consumer/opensky_kafka_spark_pipeline.py` | Main OpenSky -> Kafka -> Spark -> MinIO/ClickHouse pipeline |
| `bridge/minio_to_snowflake_bridge.py` | Loads streamed MinIO Parquet into Snowflake ML table |
| `clickhouse/aircraft_states_schema.sql` | ClickHouse schema for live aircraft states |
| `lookups/icao_country_ranges.csv` | ICAO country lookup table used for enrichment |
| `docker/docker-compose.yml` | Local services for Kafka, MinIO, ClickHouse, and Grafana |

## Outputs

- MinIO path: `s3a://opensky-data/opensky/Streaming/`
- ClickHouse table: `opensky.aircraft_states`
- Grafana dashboards
- Snowflake ML serving feed
