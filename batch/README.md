# Batch Pipeline

This folder contains the historical ADS-B ingestion pipeline.

## Flow

```text
ADS-B historical archives
        |
        v
CloudShell / local extraction
        |
        v
AWS S3
        |
        v
Snowflake external stage
        |
        v
Bronze raw trace table
        |
        v
Silver cleaned table
        |
        v
Gold Analytics and Gold ML
```

## Main Steps

1. Download ADS-B historical archive files.
2. Concatenate archive parts into one tar file.
3. Extract trace JSON files.
4. Upload extracted files to S3.
5. Create Snowflake file format and external stage.
6. Copy JSON into raw staging table.
7. Flatten trace arrays into flight traces.
8. Validate row counts, aircraft counts, and timestamp coverage.
