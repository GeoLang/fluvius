# Fluvius

[![CI](https://github.com/GeoLang/fluvius/actions/workflows/ci.yml/badge.svg)](https://github.com/GeoLang/fluvius/actions)
[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg)](LICENSE)

A stream processor in Rust for moving-object events such as GPS tracks, sensor readings and vehicle telemetry, with geofence, proximity, trajectory, aggregation, CEP and map matching operators declared in a TOML topology.

It runs as one `fluvius` binary, with no JVM.

[Documentation](https://geolang.github.io/fluvius/) · [GitHub](https://github.com/GeoLang/fluvius)

## Features

### Spatial operators

- **Geofencing**: enter and exit events per entity and zone. The library takes any polygon. A topology zone is a centre and radius in degrees, drawn as a 64-segment circle, and the `geofence` subcommand takes a bounding box.
- **Proximity alerts**: an alert when two entities come within `radius_m` metres by haversine distance.
- **Trajectory analysis**: speed anomalies above `max_speed_mps`, a stop alert after 30 s at or below 1 m/s, a 3-point smoothed position, and a per-entity summary of distance and average speed when a window closes or the stream ends.
- **Spatial aggregation**: count, sum, mean, max or min per cell of a fixed lon/lat grid. A cell emits and resets once it holds `threshold` events. In a topology, sum, mean, max and min aggregate the event `speed`.
- **Map matching**: snaps each event to the nearest road given in the topology within `max_distance_m`, with a confidence that falls to 0 at that distance, and passes the snapped position downstream. An event with no road in range is marked `unmatched`. Events are matched one at a time, with no sequence model.

### Stream processing

- **Complex event processing**: per-entity pattern sequences within `within_secs`, each step a condition plus an optional `near = [lon, lat, radius_deg]`.
- **Windowing**: tumbling, sliding, session and count windows. `[pipeline.window]` flushes the stateful operators when a window closes. Count windows close when they fill, time windows close against the event-time watermark.
- **Watermarks**: with a window set, the watermark trails the newest event time by `max_lateness_secs`, and an event more than `max_lateness_secs` behind the watermark is dropped and counted as late.
- **R-tree spatial index**: entity positions kept current from the stream. The proximity operator is its only caller, through a bounding box query.

### Connectors

- **WebSocket**: a source url with a `ws://` or `wss://` scheme is a remote feed the runner connects to, retrying with a doubling delay up to 30 s when the feed drops or refuses. A source given as `host:port`, and every sink, binds a listener and waits for clients instead. `wss://` needs `--features tls`.
- **File**: JSON lines in, JSON lines appended out.
- **Stdout**: sink only, one JSON line per output.
- **Kafka** (rdkafka): consumer source and producer sink over JSON events. `group_id` defaults to `fluvius`. Needs `--features kafka`, which builds a vendored librdkafka with cmake and a C compiler.
- **MQTT** (rumqttc): subscriber source and publisher sink over JSON events. Needs `--features mqtt`. A topology sets `username`, `qos` (0, 1 or 2) and `client_id` alongside `broker_url` and `topic`, and names the environment variable holding the password in `password_env`. A run stops at startup if that variable is unset.

A default build rejects a topology that uses Kafka, MQTT or a `wss://` feed at startup and names the missing feature.

### Operations

- **Replay**: `[pipeline.replay]` replaces the source with a recorded file, paced by the event timestamps at any positive speed multiplier or `inf`.
- **Checkpointing**: `[pipeline.checkpoint]` restores every stateful operator at startup and snapshots them on an interval and when the run ends, keeping the last `max_retained` snapshots.
- **Prometheus metrics**: `[pipeline.metrics]` serves `fluvius_events_received_total`, `_emitted_total`, `_filtered_total`, `_late_total` and `fluvius_processing_time_avg_us` for the life of the run.

## Quick Start

```bash
cargo install --path crates/fluvius-cli
# with Kafka, MQTT and wss:// feeds
cargo install --path crates/fluvius-cli --features kafka,mqtt,tls

fluvius run --topology pipeline.toml

fluvius geofence --input events.jsonl --bounds "10.0,20.0,10.5,20.5" --zone-name warehouse
fluvius proximity --input events.jsonl --threshold 100.0
fluvius trajectory --input events.jsonl --max-speed 50.0

fluvius serve --topology pipeline.toml --source-bind 127.0.0.1:9001 --sink-bind 127.0.0.1:9002
```

Tagged releases ship prebuilt binaries for Linux and macOS on x86_64 and aarch64, built with default features only.

### Events

Every source reads one JSON event per line or message. `id`, `timestamp` (RFC 3339), `entity_id`, `lon`, `lat` and `properties` are required. `speed` (m/s), `heading` and `altitude` are optional. An event that does not parse is skipped.

```json
{"id":"e1","timestamp":"2026-01-01T12:00:00Z","entity_id":"truck-1","lon":10.0,"lat":20.0,"speed":12.5,"properties":{}}
```

Each output is `{"source_event": {...}, "operator": "<name>", "payload": {...}}`.

## Topology

```toml
[pipeline]
name = "fleet-monitoring"

[pipeline.source]
type = "file"
path = "events.jsonl"

[pipeline.sink]
type = "stdout"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[[pipeline.operators]]
type = "geofence"
name = "depot-zone"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]

[[pipeline.operators]]
type = "proximity"
name = "near"
radius_m = 50.0

[[pipeline.operators]]
type = "trajectory"
name = "tracks"
max_speed_mps = 50.0
```

Every operator takes a `name`. The other keys:

| `type` | Keys |
|--------|------|
| `filter` | `condition` |
| `geofence` | `zones = [{ name, center = [lon, lat], radius }]`, radius in degrees |
| `proximity` | `radius_m` |
| `trajectory` | `max_speed_mps`, `max_buffer` (default 1000) |
| `spatial_agg` | `cell_size_deg`, `function` (`count`, `sum`, `mean`, `max`, `min`), `threshold` |
| `cep` | `pattern = { name, within_secs, steps = [{ name, condition, near }] }`, `near` optional |
| `rate_limit` | `max_per_second` |
| `map_match` | `roads = [{ id, name, geometry = [[lon, lat], ...] }]`, `max_distance_m` (default 50) |

Sources and sinks:

| `type` | Keys |
|--------|------|
| `file` | `path` |
| `websocket` | `url` |
| `kafka` | `brokers`, `topic`, `group_id` (source only) |
| `mqtt` | `broker_url`, `topic`, `username`, `password_env`, `qos`, `client_id` |
| `stdout` | none, sink only |

A `filter` condition is one comparison of three whitespace-separated tokens, either `speed` against a number with `>`, `>=`, `<`, `<=`, `==` or `!=`, or `entity_id` against a quoted name with `==` or `!=`. A `cep` step takes its condition in the same form.

`filter` and `rate_limit` write every event they pass to the sink and drop the rest, so nothing downstream sees a dropped event. `map_match` writes every event, snapped or marked `unmatched`. `rate_limit` is a token bucket over the whole stream, not per entity: it passes `max_per_second` events, bursting up to one second's worth. The stateful operators write only their alerts and pass every event on. When the stream ends they are flushed, which is when `trajectory` writes its per-entity summaries.

`[pipeline.window]` flushes the stateful operators when a window closes. `[pipeline.watermark]` only takes effect with a window:

```toml
[pipeline.window]
type = "tumbling"
duration_secs = 10

[pipeline.watermark]
max_lateness_secs = 2
```

`sliding` takes `duration_secs` and `slide_secs`, `session` takes `gap_secs`, and `count` takes `count`.

`[pipeline.replay]` replaces the source. `speed` is a multiplier over the recording (default 1.0), and `inf` replays as fast as the pipeline accepts events:

```toml
[pipeline.replay]
file = "historical.jsonl"
speed = 10.0
```

`[pipeline.metrics]` serves the counters in Prometheus exposition format for as long as the run lasts. An address it cannot bind fails the run. Set `enabled = false` to keep the section without serving anything. These are the defaults:

```toml
[pipeline.metrics]
enabled = true
bind = "127.0.0.1:9090"
path = "/metrics"
```

`[pipeline.checkpoint]` makes a run resumable. At startup the latest snapshot in `dir` is loaded into every stateful operator by name, and a new one is written every `interval_secs` (default 60) and once when the run ends. A resumed geofence knows which entities were already inside a zone, and a resumed proximity operator knows where everything was:

```toml
[pipeline.checkpoint]
dir = "/var/lib/fluvius/checkpoints"
interval_secs = 30
max_retained = 3
```

Two stateful operators cannot share a name in a checkpointing topology, since the name is the key. A snapshot holds accumulated state only, never configuration, so zones, thresholds and patterns come from the topology file on every start.

### serve

`serve` runs the same wiring against two WebSocket listeners, replacing whatever source and sink the topology declares. `--source-bind` defaults to `127.0.0.1:9001` and `--sink-bind` to `127.0.0.1:9002`.

Send events to the source socket as JSON, one per message. Every output is broadcast to the clients connected to the sink socket at that moment, nothing is buffered. A `[pipeline.replay]` section still replaces the source, so `serve` then broadcasts a recording instead of listening on the source socket.

### Single-operator subcommands

`geofence`, `proximity` and `trajectory` run one operator over a file and print the alerts, with no topology. `run --input events.jsonl --output alerts.jsonl` copies every event through to the output file, and `--min-speed` keeps only events at or above that speed in m/s.

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

## License

AGPL-3.0-or-later, see [LICENSE](LICENSE).

Copyright (C) 2026 Grok Image Compression Inc.
