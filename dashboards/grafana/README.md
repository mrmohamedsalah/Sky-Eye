# Grafana Dashboards

Grafana is used for near real-time monitoring of the streaming pipeline.

## Data Source

```text
ClickHouse: opensky.aircraft_states
```

## Main Panels

- Active aircraft trend
- Geo grid density
- Missing geo percentage
- Stream lag
- Missing key fields
- Phase distribution
- Fastest aircraft
- High-speed outliers
- Extreme vertical-rate events
- Top steep descents
- Source quality

## Screenshots

Dashboard screenshots are stored in:

```text
docs/dashboards/
```

## Optional Export

If available, export the Grafana dashboard JSON and save it here as:

```text
sky_eye_grafana_dashboard.json
```
