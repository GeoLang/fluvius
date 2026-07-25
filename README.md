# Fluvius

[![CI](https://github.com/GeoLang/fluvius/actions/workflows/ci.yml/badge.svg)](https://github.com/GeoLang/fluvius/actions)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)

Real-time geospatial stream processor. Sub-second latency processing for continuous spatial data streams — GPS tracks, IoT sensors, vehicle telemetry, drone feeds.

Zero JVM. Sub-MB footprint. Single binary.

[Documentation](https://geolang.github.io/fluvius/) · [GitHub](https://github.com/GeoLang/fluvius)

## Features

### Spatial Operators

- **Geofencing** — Multi-zone polygon enter/exit detection with per-entity state tracking
- **Proximity alerts** — Haversine distance triggers between moving entities
- **Trajectory analysis** — Speed, stop detection, distance accumulation, path smoothing
- **Spatial aggregation** — Real-time density grids, heatmaps, count/sum/mean/max/min
- **Map matching** — Snap GPS points to road network with confidence scoring

### Stream Processing

- **Complex Event Processing (CEP)** — Multi-step pattern sequences with spatial constraints and time windows
- **Windowing** — Tumbling, sliding, session, and count-based windows
- **Watermarks** — Event-time processing with configurable late-event tolerance
- **Temporal joins** — Correlate events across multiple streams by entity + time proximity
- **R-tree spatial index** — k-NN, radius, and bounding box queries over millions of entities

### Connectors

- **WebSocket** — Source and sink for real-time browser/client integration
- **File** — JSON lines input/output
- **Kafka** (rdkafka): consumer source and producer sink over JSON events, consumer groups. Build with `--features kafka` (vendored librdkafka via cmake, no system libs needed).
- **MQTT** (rumqttc): IoT pub/sub source and sink over JSON events, configurable QoS 0/1/2. Build with `--features mqtt` (pure Rust).

### Operations

- **Checkpointing** — Periodic state snapshots for crash recovery with automatic GC
- **Replay mode** — Replay historical data at 1x, 10x, 100x, or max speed
- **Prometheus metrics** — Built-in `/metrics` endpoint (events received/emitted/filtered/late, processing time)
- **Topology DSL (TOML)** — Declare full pipelines without writing code

## Quick Start

```bash
# Build from source
git clone https://github.com/GeoLang/fluvius.git
cd fluvius && cargo build --release
# Kafka/MQTT topologies need their features: cargo build --release --features kafka,mqtt

# Run with a TOML topology
fluvius run --topology pipeline.toml

# Or use individual commands
fluvius geofence --input events.jsonl --zone "warehouse:10.0,20.0,0.01"
fluvius proximity --input events.jsonl --radius 100.0
fluvius trajectory --input events.jsonl --max-speed 50.0

# Live WebSocket processing
fluvius serve --source-bind 127.0.0.1:9001 --sink-bind 127.0.0.1:9002
```

## Example Topology

```toml
[pipeline]
name = "fleet-monitoring"

[[pipeline.operators]]
type = "geofence"
name = "depot-zone"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]

[[pipeline.operators]]
type = "spatial_agg"
name = "density"
cell_size_deg = 0.1
function = "count"
threshold = 10

[pipeline.source]
type = "kafka"
brokers = ["localhost:9092"]
topic = "gps-events"
group_id = "fluvius-fleet"

[pipeline.sink]
type = "mqtt"
broker_url = "mqtt://localhost:1883"
topic = "alerts/geofence"

[pipeline.metrics]
enabled = true
port = 9090

[pipeline.checkpoint]
dir = "/var/lib/fluvius/checkpoints"
interval_secs = 30
max_retained = 5
```

`run --topology` wires the configured source and sink (file, websocket, kafka, mqtt) and applies `filter` operators. Stateful geo operators (geofence, proximity, trajectory) run via their own subcommands.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        fluvius-cli                               │
│  Commands: run │ serve │ geofence │ proximity │ trajectory      │
├─────────────────────────────────────────────────────────────────┤
│       fluvius-geo              │     fluvius-connectors          │
│  Geofence │ Proximity         │  WebSocket │ File               │
│  Trajectory │ Spatial Agg     │  Kafka │ MQTT                   │
│  Map Matching                 │                                 │
├─────────────────────────────────────────────────────────────────┤
│                       fluvius-core                               │
│  Pipeline │ Operators │ Windows │ Watermarks │ State            │
│  CEP │ Spatial Index │ Checkpoint │ Metrics │ Replay            │
│  Temporal Joins │ Topology DSL                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Comparison

| Feature | Fluvius | Kafka Streams | Apache Flink | Esri GeoEvent |
|---------|---------|---------------|--------------|---------------|
| Native spatial operators | ✓ | ✗ | ✗ | ✓ |
| R-tree spatial index | ✓ | ✗ | ✗ | ✗ |
| CEP + spatial | ✓ | ✗ | ✓ | ✗ |
| Zero JVM | ✓ | ✗ | ✗ | ✗ |
| Single binary | ✓ | ✗ | ✗ | ✗ |
| Sub-MB memory | ✓ | ✗ | ✗ | ✗ |
| TOML topology DSL | ✓ | ✗ | ✗ | ✗ |
| Map matching | ✓ | ✗ | ✗ | ✗ |
| Checkpointing | ✓ | ✓ | ✓ | ✓ |
| Prometheus metrics | ✓ | ✓ | ✓ | ✗ |
| Open source | ✓ | ✓ | ✓ | ✗ |

## License

AGPL-3.0-or-later, see [LICENSE](LICENSE).

Copyright (C) 2026 Grok Image Compression Inc.
