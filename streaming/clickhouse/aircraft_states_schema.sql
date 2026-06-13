CREATE DATABASE IF NOT EXISTS opensky;

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
