# Fluvius

[![CI](https://github.com/GeoLang/fluvius/actions/workflows/ci.yml/badge.svg)](https://github.com/GeoLang/fluvius/actions)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)

Real-time geospatial stream processor. Sub-second latency processing for continuous spatial data streams — GPS tracks, IoT sensors, vehicle telemetry, drone feeds.

Zero JVM. Single binary (4 MB release build with default features, larger with `--features kafka`).

[Documentation](https://geolang.github.io/fluvius/) · [GitHub](https://github.com/GeoLang/fluvius)

## Features

### Spatial Operators

- **Geofencing** — Multi-zone polygon enter/exit detection with per-entity state tracking
- **Proximity alerts** — Haversine distance triggers between moving entities
- **Trajectory analysis** — Speed, stop detection, distance accumulation, path smoothing
- **Spatial aggregation** — Real-time density grids, count/sum/mean/max/min
- **Map matching** — Snap GPS points to a road network with confidence scoring (`map_match` topology operator)

### Stream Processing

- **Complex Event Processing (CEP)** — Multi-step pattern sequences with spatial constraints and time windows
- **Windowing** — Tumbling, sliding, session and count windows. `[pipeline.window]` expires stateful operators when a window closes. Count windows close when they fill; time windows close against the event-time watermark
- **Watermarks** — Event-time processing with configurable late-event tolerance. Late events past `max_lateness_secs` are dropped
- **R-tree spatial index** — the proximity operator queries the index. k-NN, radius and bounding box queries are on the library type

### Connectors

- **WebSocket**: a source url with a `ws://` or `wss://` scheme is a remote feed the runner connects to, retrying with a doubling delay up to 30 s when the feed drops or refuses. A source given as `host:port`, and every sink, binds a listener and waits for clients instead. `wss://` needs `--features tls`
- **File** — JSON lines input/output
- **Kafka** (rdkafka): consumer source and producer sink over JSON events, consumer groups. Build with `--features kafka` (vendored librdkafka via cmake, no system libs needed).
- **MQTT** (rumqttc): IoT pub/sub source and sink over JSON events. A topology sets `username`, `qos` (0, 1 or 2) and `client_id` alongside the broker URL and topic, and names the environment variable holding the password in `password_env`. The password is never written in the topology file, and a run stops at startup if the named variable is unset. Build with `--features mqtt` (pure Rust).

### Operations

- **Replay mode** — Replay historical data at 1x, 10x, 100x, or max speed, from a topology or the library
- **Topology DSL (TOML)** — Declare full pipelines without writing code
- **Checkpointing**: `[pipeline.checkpoint]` restores every operator's state at startup and snapshots it on an interval and when the run ends, keeping the last `max_retained` snapshots
- **Prometheus metrics**: `[pipeline.metrics]` serves the exposition format over HTTP for the life of the run: events received, emitted, filtered and late, plus average operator time per event

## Quick Start

```bash
# Build from source
git clone https://github.com/GeoLang/fluvius.git
cd fluvius && cargo build --release
# Kafka, MQTT and wss:// feeds need their features: cargo build --release --features kafka,mqtt,tls

# Run with a TOML topology
fluvius run --topology pipeline.toml

# Or use individual commands
fluvius geofence --input events.jsonl --bounds "10.0,20.0,10.5,20.5" --zone-name warehouse
fluvius proximity --input events.jsonl --threshold 100.0
fluvius trajectory --input events.jsonl --max-speed 50.0

# Apply a topology to live WebSocket traffic
fluvius serve --topology pipeline.toml --source-bind 127.0.0.1:9001 --sink-bind 127.0.0.1:9002
```

## Example Topology

```toml
[pipeline]
name = "fleet-monitoring"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

# radius is in degrees, not meters
[[pipeline.operators]]
type = "geofence"
name = "depot-zone"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]

[[pipeline.operators]]
type = "trajectory"
name = "tracks"
max_speed_mps = 50.0

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

[[pipeline.operators]]
type = "rate_limit"
name = "cap"
max_per_second = 50.0

[pipeline.sink]
type = "mqtt"
broker_url = "mqtt://localhost:1883"
topic = "alerts/geofence"
```

`run --topology` wires the configured source and sink (file, websocket, kafka, mqtt) and chains the declared operators: `filter`, `geofence`, `proximity`, `trajectory`, `spatial_agg`, `cep`, `rate_limit`, `map_match`.

A `filter` drops the events it rejects, so nothing downstream sees them. `rate_limit` is a token bucket over the whole stream, not per entity: it passes `max_per_second` events, bursting up to one second's worth, and drops the rest. The stateful operators emit their alerts and pass the event on, they never drop it. When the stream ends they are flushed, which is when `trajectory` emits its per-entity summary.

`[pipeline.window]` expires stateful operators when a window closes. `[pipeline.watermark]` drops events older than the watermark plus `max_lateness_secs`:

```toml
[pipeline.window]
type = "tumbling"
duration_secs = 10

[pipeline.watermark]
max_lateness_secs = 2
```

`[pipeline.replay]` replaces the source with a recorded JSON lines file, paced by the event timestamps. `speed` is a multiplier over the recording, and `inf` replays as fast as the pipeline accepts events:

```toml
[pipeline.replay]
file = "historical.jsonl"
speed = 10.0
```

`[pipeline.metrics]` serves the counters in Prometheus exposition format for as long as the run lasts. An address it cannot bind fails the run rather than leaving the pipeline unobservable. Set `enabled = false` to keep the section without serving anything:

```toml
[pipeline.metrics]
enabled = true
bind = "127.0.0.1:9090"
path = "/metrics"
```

`[pipeline.checkpoint]` makes a run resumable. At startup the latest snapshot in `dir` is loaded into every stage, keyed by the operator name, and a new one is written every `interval_secs` and once when the run ends. The state is what an operator accumulated, so a resumed geofence knows which entities were already inside a zone and a resumed proximity operator knows where everything was:

```toml
[pipeline.checkpoint]
dir = "/var/lib/fluvius/checkpoints"
interval_secs = 30
max_retained = 3
```

Two stateful operators cannot share a name in a checkpointing topology, since the name is the key. The snapshot holds accumulated state only, never configuration, so the topology file stays the one place zones, thresholds and patterns are defined.

`serve` runs the same wiring against live WebSocket endpoints, replacing whatever source and sink the topology declares:

```bash
fluvius serve --topology pipeline.toml
```

Send events to the source socket as JSON, one per WebSocket message, and every alert the pipeline produces is broadcast to the clients connected to the sink socket. Only clients connected at the time receive an alert, nothing is buffered. A `[pipeline.replay]` section still wins over the source, so `serve` then broadcasts a recording instead of listening on the source socket.

The `geofence`, `proximity` and `trajectory` subcommands run a single operator over a file, without a topology.

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
│  Topology DSL                                                   │
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
| TOML topology DSL | ✓ | ✗ | ✗ | ✗ |
| Map matching | ✓ | ✗ | ✗ | ✗ |
| Checkpointing | ✓ | ✓ | ✓ | ✓ |
| Prometheus metrics | ✓ | ✓ | ✓ | ✗ |
| Open source | ✓ | ✓ | ✓ | ✗ |


## License

AGPL-3.0-or-later, see [LICENSE](LICENSE).

Copyright (C) 2026 Grok Image Compression Inc.
