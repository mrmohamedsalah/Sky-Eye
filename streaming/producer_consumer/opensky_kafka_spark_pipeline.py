import json
import logging
import os
import threading
import time
from itertools import chain

import clickhouse_connect
import requests
from kafka import KafkaProducer
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    conv,
    create_map,
    expr,
    from_json,
    from_unixtime,
    lit,
    round as spark_round,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    ByteType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("OpenSkyPipeline")


# ==========================
# Environment configuration
# ==========================

OPENSKY_USERNAME = os.environ.get("OPENSKY_USERNAME")
OPENSKY_PASSWORD = os.environ.get("OPENSKY_PASSWORD")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka1:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "opensky")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minio123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "opensky-data")
MINIO_PREFIX = os.environ.get("MINIO_PREFIX", "opensky/Streaming/")

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "opensky")

OPEN_SKY_URL = os.environ.get(
    "OPENSKY_STATES_URL",
    "https://opensky-network.org/api/states/all",
)
POLL_INTERVAL_SECONDS = int(os.environ.get("OPENSKY_POLL_INTERVAL_SECONDS", "10"))


CLICKHOUSE_DDL = """
CREATE TABLE IF NOT EXISTS opensky.aircraft_states
(
    icao24            String,
    callsign          String,
    origin_country    Nullable(String),
    event_time        DateTime,
    ingested_at       DateTime DEFAULT now(),
    latitude          Nullable(Float64),
    longitude         Nullable(Float64),
    baro_altitude     Nullable(Float64),
    geo_altitude      Nullable(Float64),
    altitude_ft       Nullable(Float64),
    altitude_band     String,
    velocity_kmh      Nullable(Float64),
    velocity_knots    Nullable(Float64),
    vertical_rate     Nullable(Float64),
    flight_phase      String,
    true_track        Nullable(Float64),
    squawk            String,
    spi               UInt8,
    on_ground         UInt8,
    position_source   String,
    aircraft_category String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, icao24)
TTL event_time + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;
"""

CLICKHOUSE_COLUMNS = [
    "icao24",
    "callsign",
    "origin_country",
    "event_time",
    "latitude",
    "longitude",
    "baro_altitude",
    "geo_altitude",
    "altitude_ft",
    "altitude_band",
    "velocity_kmh",
    "velocity_knots",
    "vertical_rate",
    "flight_phase",
    "true_track",
    "squawk",
    "spi",
    "on_ground",
    "position_source",
    "aircraft_category",
]

CATEGORY_MAP = {
    0: "NO_INFO",
    1: "NO_ADS_B",
    2: "LIGHT",
    3: "SMALL",
    4: "LARGE",
    5: "HIGH_VORTEX",
    6: "HEAVY",
    7: "PERF_MANEUVER",
    8: "ROTORCRAFT",
    9: "GLIDER",
    10: "LIGHTER_AIR",
    11: "PARACHUTIST",
    12: "ULTRALIGHT",
    13: "RESERVED",
    14: "UAV",
    15: "SPACE",
    16: "SURFACE_EMERG",
    17: "SURFACE_SVC",
    18: "POINT_OBSTACLE",
    19: "CLUSTER_OBSTACLE",
    20: "LINE_OBSTACLE",
}


class OpenSkyKafkaSparkPipeline:
    def __init__(self) -> None:
        self.url = OPEN_SKY_URL
        self.kafka_server = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TOPIC
        self.BUCKET_NAME = MINIO_BUCKET
        self.MINIO_PREFIX = MINIO_PREFIX.strip("/")

        self.spark = self._create_spark_session()
        self.clickhouse_client = self._init_clickhouse()
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda item: json.dumps(item).encode("utf-8"),
            linger_ms=50,
            retries=5,
        )

    def _create_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder.appName("SkyEyeOpenSkyKafkaSparkPipeline")
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                        "org.apache.hadoop:hadoop-aws:3.3.4",
                    ]
                ),
            )
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _init_clickhouse(self):
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
        client.command(CLICKHOUSE_DDL)
        return client

    @staticmethod
    def _schema() -> StructType:
        return StructType(
            [
                StructField("icao24", StringType()),
                StructField("callsign", StringType()),
                StructField("origin_country", StringType()),
                StructField("time_position", LongType()),
                StructField("last_contact", LongType()),
                StructField("longitude", DoubleType()),
                StructField("latitude", DoubleType()),
                StructField("baro_altitude", DoubleType()),
                StructField("on_ground", BooleanType()),
                StructField("velocity", DoubleType()),
                StructField("true_track", DoubleType()),
                StructField("vertical_rate", DoubleType()),
                StructField("sensors", ArrayType(IntegerType())),
                StructField("geo_altitude", DoubleType()),
                StructField("squawk", StringType()),
                StructField("spi", BooleanType()),
                StructField("position_source", IntegerType()),
                StructField("category", IntegerType()),
            ]
        )

    def _parse(self) -> DataFrame:
        raw = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_server)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )
        parsed = raw.selectExpr("CAST(value AS STRING) AS json_value")
        return parsed.select(from_json(col("json_value"), self._schema()).alias("data")).select(
            "data.*"
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        category_expr = create_map(*[lit(item) for item in chain(*CATEGORY_MAP.items())])

        transformed = (
            df.withColumn("icao24", when(col("icao24").isNull(), lit("UNKNOWN")).otherwise(col("icao24")))
            .withColumn("callsign", coalesce(expr("trim(callsign)"), lit("")))
            .withColumn("event_time", to_timestamp(from_unixtime(coalesce(col("last_contact"), col("time_position")))))
            .withColumn("latitude", col("latitude").cast(DoubleType()))
            .withColumn("longitude", col("longitude").cast(DoubleType()))
            .withColumn("baro_altitude", col("baro_altitude").cast(DoubleType()))
            .withColumn("geo_altitude", col("geo_altitude").cast(DoubleType()))
            .withColumn("altitude_ft", spark_round(coalesce(col("baro_altitude"), col("geo_altitude")) * lit(3.28084), 2))
            .withColumn(
                "altitude_band",
                when(col("on_ground") == lit(True), lit("GROUND"))
                .when(col("altitude_ft").isNull(), lit("UNKNOWN"))
                .when(col("altitude_ft") < lit(10000), lit("LOW"))
                .when(col("altitude_ft") < lit(25000), lit("MEDIUM"))
                .when(col("altitude_ft") < lit(40000), lit("HIGH"))
                .otherwise(lit("VERY_HIGH")),
            )
            .withColumn("velocity_kmh", spark_round(col("velocity") * lit(3.6), 2))
            .withColumn("velocity_knots", spark_round(col("velocity") * lit(1.94384), 2))
            .withColumn("vertical_rate", col("vertical_rate").cast(DoubleType()))
            .withColumn(
                "flight_phase",
                when(col("on_ground") == lit(True), lit("GROUND"))
                .when(col("vertical_rate") > lit(2.5), lit("CLIMB"))
                .when(col("vertical_rate") < lit(-2.5), lit("DESCENT"))
                .otherwise(lit("CRUISE")),
            )
            .withColumn("true_track", col("true_track").cast(DoubleType()))
            .withColumn("squawk", coalesce(col("squawk"), lit("")))
            .withColumn("spi", coalesce(col("spi").cast(ByteType()), lit(0).cast(ByteType())))
            .withColumn("on_ground", coalesce(col("on_ground").cast(ByteType()), lit(0).cast(ByteType())))
            .withColumn(
                "position_source",
                when(col("position_source") == lit(0), lit("ADS-B"))
                .when(col("position_source") == lit(1), lit("ASTERIX"))
                .when(col("position_source") == lit(2), lit("MLAT"))
                .when(col("position_source") == lit(3), lit("FLARM"))
                .otherwise(lit("UNKNOWN")),
            )
            .withColumn("aircraft_category", coalesce(category_expr[col("category")], lit("UNKNOWN")))
            .drop("sensors")
        )

        # conv is imported in the requested header; this derived field can help debug ICAO range joins.
        return transformed.withColumn("icao24_int", conv(col("icao24"), 16, 10).cast(LongType()))

    def _write_minio(self, df: DataFrame):
        return (
            df.writeStream.format("parquet")
            .option("path", f"s3a://{self.BUCKET_NAME}/{self.MINIO_PREFIX}")
            .option("checkpointLocation", f"s3a://{self.BUCKET_NAME}/{self.MINIO_PREFIX}/_checkpoints/minio")
            .outputMode("append")
            .start()
        )

    def _write_clickhouse(self, df: DataFrame):
        return (
            df.writeStream.foreachBatch(self.process_batch)
            .option("checkpointLocation", f"s3a://{self.BUCKET_NAME}/{self.MINIO_PREFIX}/_checkpoints/clickhouse")
            .outputMode("append")
            .start()
        )

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            log.info("Batch %s is empty", batch_id)
            return

        cached = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            out = cached.select(*CLICKHOUSE_COLUMNS).toPandas()
            if out.empty:
                log.info("Batch %s has no ClickHouse rows", batch_id)
                return

            self.clickhouse_client.insert_df(
                "opensky.aircraft_states",
                out,
                column_names=CLICKHOUSE_COLUMNS,
            )
            log.info("Batch %s wrote %,d rows to ClickHouse", batch_id, len(out))
        finally:
            cached.unpersist()

    @staticmethod
    def _state_array_to_record(state: list) -> dict:
        padded = list(state) + [None] * (18 - len(state))
        return {
            "icao24": padded[0],
            "callsign": padded[1],
            "origin_country": padded[2],
            "time_position": padded[3],
            "last_contact": padded[4],
            "longitude": padded[5],
            "latitude": padded[6],
            "baro_altitude": padded[7],
            "on_ground": padded[8],
            "velocity": padded[9],
            "true_track": padded[10],
            "vertical_rate": padded[11],
            "sensors": padded[12],
            "geo_altitude": padded[13],
            "squawk": padded[14],
            "spi": padded[15],
            "position_source": padded[16],
            "category": padded[17],
        }

    def produce_data(self) -> None:
        auth = None
        if OPENSKY_USERNAME and OPENSKY_PASSWORD:
            auth = (OPENSKY_USERNAME, OPENSKY_PASSWORD)

        while True:
            try:
                response = requests.get(
                    self.url,
                    auth=auth,
                    timeout=30,
                )
                response.raise_for_status()
                payload = response.json()
                states = payload.get("states") or []

                for state in states:
                    self.producer.send(self.topic, self._state_array_to_record(state))

                self.producer.flush()
                log.info("Produced %,d OpenSky records", len(states))
            except Exception:
                log.exception("OpenSky producer cycle failed")

            time.sleep(POLL_INTERVAL_SECONDS)

    def consume_data(self) -> None:
        parsed = self._parse()
        transformed = self._transform(parsed)

        minio_query = self._write_minio(transformed)
        clickhouse_query = self._write_clickhouse(transformed)

        log.info("Spark streaming consumers started")
        minio_query.awaitTermination()
        clickhouse_query.awaitTermination()

    def run(self) -> None:
        producer_thread = threading.Thread(target=self.produce_data, daemon=True)
        producer_thread.start()
        self.consume_data()


if __name__ == "__main__":
    pipeline = OpenSkyKafkaSparkPipeline()
    pipeline.run()
