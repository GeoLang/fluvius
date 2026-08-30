//! Run a pipeline declared in a TOML topology, wiring configured sources and sinks
//! to the connector implementations. Kafka and MQTT are feature-gated; a topology
//! that references them while the feature is compiled out fails with a clear error.
//!
//! Both `fluvius run --topology` and `fluvius serve` come through here; serve only
//! swaps the source and sink for its websocket binds.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use fluvius_connectors::file;
use fluvius_core::cep::{CepEngine, Pattern, PatternStep};
use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::metrics::{Metrics, serve_metrics};
use fluvius_core::operator::{FilterOperator, RateLimitOperator};
use fluvius_core::pipeline::{Pipeline, Stage};
use fluvius_core::replay::{ReplaySpeed, Replayer};
use fluvius_core::topology::{
    MetricsConfig, OperatorConfig, PatternConfig, PipelineConfig, ReplayConfig, SinkConfig,
    SourceConfig, TopologyConfig, ZoneConfig,
};
use fluvius_geo::geofence::{GeofenceOperator, GeofenceZone};
use fluvius_geo::map_match::{MapMatchOperator, RoadNetwork, RoadSegment};
use fluvius_geo::proximity::ProximityOperator;
use fluvius_geo::spatial_agg::{AggregateFunction, GridConfig, SpatialAggregator};
use fluvius_geo::trajectory::TrajectoryOperator;
use geo::{Coord, LineString, Polygon};
use tokio::sync::mpsc;

const CHANNEL_CAPACITY: usize = 10_000;

/// Run a topology against live websocket endpoints. Whatever source and sink the
/// topology declares are replaced by the given binds, everything else is honoured.
pub async fn serve(
    mut config: TopologyConfig,
    source_bind: &str,
    sink_bind: &str,
) -> Result<(), String> {
    if config.pipeline.source.is_some() || config.pipeline.sink.is_some() {
        eprintln!("serve: ignoring the topology source/sink, using the websocket binds");
    }
    // stripped to host:port so a bind is never read back as a remote feed to dial
    let source_bind = ws_bind(source_bind);
    let sink_bind = ws_bind(sink_bind);
    config.pipeline.source = Some(SourceConfig::WebSocket {
        url: source_bind.clone(),
    });
    config.pipeline.sink = Some(SinkConfig::WebSocket {
        url: sink_bind.clone(),
    });

    println!("Starting Fluvius stream processor");
    println!("  Pipeline: {}", config.pipeline.name);
    println!("  Operators: {}", config.pipeline.operators.len());
    println!("  Source WebSocket: ws://{source_bind}");
    println!("  Sink WebSocket: ws://{sink_bind}");

    run_topology(config).await
}

/// Run a topology to completion.
pub async fn run_topology(config: TopologyConfig) -> Result<(), String> {
    let pipeline_cfg = config.pipeline;
    reject_unsupported_sections(&pipeline_cfg)?;

    let sink_cfg = pipeline_cfg
        .sink
        .ok_or_else(|| "topology has no [pipeline.sink]".to_string())?;

    let mut pipeline = Pipeline::new(pipeline_cfg.name);
    for op in &pipeline_cfg.operators {
        pipeline.add_stage(build_stage(op)?);
    }
    if let Some(window) = &pipeline_cfg.window {
        let lateness = pipeline_cfg
            .watermark
            .as_ref()
            .map_or(0, |w| w.max_lateness_secs);
        pipeline.set_window_lateness_secs(window.strategy()?, lateness);
    }

    let rx_in = match &pipeline_cfg.replay {
        Some(replay) => {
            if pipeline_cfg.source.is_some() {
                eprintln!(
                    "replay: ignoring the topology source, replaying {}",
                    replay.file
                );
            }
            replay_source(replay)?
        }
        None => {
            let source_cfg = pipeline_cfg
                .source
                .ok_or_else(|| "topology has no [pipeline.source]".to_string())?;
            resolve_source(&source_cfg)?
        }
    };
    let (tx_out, rx_out) = mpsc::channel(CHANNEL_CAPACITY);
    let sink_handle = spawn_sink(&sink_cfg, rx_out)?;
    let metrics_handle = spawn_metrics_endpoint(&pipeline_cfg.metrics, pipeline.metrics()).await?;

    let metrics = pipeline.run(rx_in, tx_out).await;
    // pipeline.run returns once the source channel closes, dropping tx_out, which
    // closes the sink channel and lets the sink task finish.
    let _ = sink_handle.await;
    if let Some(handle) = metrics_handle {
        handle.abort();
    }

    println!("Topology complete:");
    println!("  Events received: {}", metrics.events_received());
    println!("  Events emitted: {}", metrics.events_emitted());
    println!("  Events filtered: {}", metrics.events_filtered());
    println!("  Events late: {}", metrics.events_late());
    Ok(())
}

/// Reject the sections the runner cannot honour. The section parses so a config can
/// carry it, but checkpointing would need operator state in a store, which does not
/// exist.
fn reject_unsupported_sections(config: &PipelineConfig) -> Result<(), String> {
    if config.checkpoint.is_some() {
        return Err(
            "[pipeline.checkpoint] is configured but not supported: the runner keeps no state \
             store to snapshot, fluvius_core::checkpoint is a library API. Remove the section"
                .to_string(),
        );
    }
    Ok(())
}

/// Serve the pipeline counters for the life of the run when the topology asks for it.
async fn spawn_metrics_endpoint(
    config: &Option<MetricsConfig>,
    metrics: Metrics,
) -> Result<Option<tokio::task::JoinHandle<()>>, String> {
    let Some(config) = config.as_ref().filter(|c| c.enabled) else {
        return Ok(None);
    };

    let handle = serve_metrics(&config.bind, &config.path, metrics)
        .await
        .map_err(|e| {
            format!(
                "cannot serve [pipeline.metrics] on {}{}: {e}",
                config.bind, config.path
            )
        })?;
    println!("  Metrics: http://{}{}", config.bind, config.path);
    Ok(Some(handle))
}

/// Feed the pipeline from a recorded file, paced by the event timestamps.
fn replay_source(config: &ReplayConfig) -> Result<mpsc::Receiver<Event>, String> {
    let speed = match config.speed {
        s if s.is_infinite() && s.is_sign_positive() => ReplaySpeed::MaxSpeed,
        s if s > 0.0 => ReplaySpeed::Multiplied(s),
        s => return Err(format!("replay speed must be positive or inf, got {s}")),
    };

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let path = config.file.clone();
    tokio::spawn(async move {
        match file::read_jsonl(Path::new(&path)).await {
            Ok(events) => {
                Replayer::new(speed).replay(events, tx).await;
            }
            Err(e) => eprintln!("replay source error: {e}"),
        }
    });
    Ok(rx)
}

/// Build a pipeline stage from its config.
fn build_stage(config: &OperatorConfig) -> Result<Stage, String> {
    match config {
        OperatorConfig::Filter { name, condition } => {
            let predicate = compile_condition(condition)?;
            Ok(Stage::Map(Arc::new(FilterOperator::new(
                name.clone(),
                predicate,
            ))))
        }
        OperatorConfig::Geofence { name, zones } => {
            let mut op = GeofenceOperator::new(name.clone());
            for zone in zones {
                op.add_zone(GeofenceZone {
                    name: zone.name.clone(),
                    polygon: zone_polygon(zone),
                });
            }
            Ok(Stage::Stateful(Box::new(op)))
        }
        OperatorConfig::Proximity { name, radius_m } => Ok(Stage::Stateful(Box::new(
            ProximityOperator::new(name.clone(), *radius_m),
        ))),
        OperatorConfig::Trajectory {
            name,
            max_speed_mps,
            max_buffer,
        } => Ok(Stage::Stateful(Box::new(TrajectoryOperator::new(
            name.clone(),
            *max_buffer,
            *max_speed_mps,
        )))),
        OperatorConfig::SpatialAgg {
            name,
            cell_size_deg,
            function,
            threshold,
        } => Ok(Stage::Stateful(Box::new(SpatialAggregator::new(
            name.clone(),
            GridConfig::uniform(*cell_size_deg),
            parse_aggregate(function)?,
            None,
            *threshold,
        )))),
        // the engine names its outputs after the matched pattern, so the operator
        // name in the config has nothing to attach to
        OperatorConfig::Cep { name: _, pattern } => {
            let mut engine = CepEngine::new();
            engine.add_pattern(build_pattern(pattern)?);
            Ok(Stage::Stateful(Box::new(engine)))
        }
        OperatorConfig::RateLimit {
            name,
            max_per_second,
        } => {
            if !max_per_second.is_finite() || *max_per_second <= 0.0 {
                return Err(format!(
                    "rate_limit {name:?} needs a finite max_per_second above zero, got {max_per_second}"
                ));
            }
            Ok(Stage::Map(Arc::new(RateLimitOperator::new(
                name.clone(),
                *max_per_second,
            ))))
        }
        OperatorConfig::MapMatch {
            name,
            max_distance_m,
            roads,
        } => {
            let mut network = RoadNetwork::new(*max_distance_m);
            for road in roads {
                if road.geometry.len() < 2 {
                    return Err(format!(
                        "map_match {name:?} road {} needs at least two points",
                        road.id
                    ));
                }
                network.add_segment(RoadSegment {
                    id: road.id.clone(),
                    name: road.name.clone(),
                    geometry: LineString::from(
                        road.geometry
                            .iter()
                            .map(|[lon, lat]| (*lon, *lat))
                            .collect::<Vec<_>>(),
                    ),
                    speed_limit: None,
                    oneway: false,
                });
            }
            Ok(Stage::Map(Arc::new(MapMatchOperator::new(
                name.clone(),
                network,
            ))))
        }
    }
}

/// Approximate a configured zone as a polygon. Radius is in degrees, so the shape
/// is a circle in lon/lat space, not on the ground.
fn zone_polygon(zone: &ZoneConfig) -> Polygon<f64> {
    const SEGMENTS: usize = 64;
    let [lon, lat] = zone.center;
    let ring: Vec<Coord<f64>> = (0..=SEGMENTS)
        .map(|i| {
            let theta = std::f64::consts::TAU * i as f64 / SEGMENTS as f64;
            Coord {
                x: lon + zone.radius * theta.cos(),
                y: lat + zone.radius * theta.sin(),
            }
        })
        .collect();
    Polygon::new(LineString::from(ring), vec![])
}

fn parse_aggregate(function: &str) -> Result<AggregateFunction, String> {
    match function.to_ascii_lowercase().as_str() {
        "count" => Ok(AggregateFunction::Count),
        "sum" => Ok(AggregateFunction::Sum),
        "mean" => Ok(AggregateFunction::Mean),
        "max" => Ok(AggregateFunction::Max),
        "min" => Ok(AggregateFunction::Min),
        other => Err(format!("unsupported spatial_agg function: {other:?}")),
    }
}

fn build_pattern(config: &PatternConfig) -> Result<Pattern, String> {
    let mut steps = Vec::new();
    for step in &config.steps {
        steps.push(PatternStep {
            name: step.name.clone(),
            condition: compile_condition(&step.condition)?,
            near: step.near.map(|[lon, lat, radius]| (lon, lat, radius)),
        });
    }
    Ok(Pattern {
        name: config.name.clone(),
        steps,
        within: Duration::from_secs(config.within_secs),
    })
}

/// Compile a simple filter condition into a predicate.
/// Supports `speed <op> <number>` and `entity_id ==|!= '<value>'`.
type Predicate = Box<dyn Fn(&Event) -> bool + Send + Sync>;

fn compile_condition(condition: &str) -> Result<Predicate, String> {
    let tokens: Vec<&str> = condition.split_whitespace().collect();
    if tokens.len() != 3 {
        return Err(format!("unsupported filter condition: {condition:?}"));
    }
    let (field, op, value) = (tokens[0], tokens[1], tokens[2]);

    match field {
        "speed" => {
            let threshold: f64 = value
                .parse()
                .map_err(|_| format!("invalid speed value in condition: {condition:?}"))?;
            let op = op.to_string();
            Ok(Box::new(move |e: &Event| {
                let speed = e.speed.unwrap_or(0.0);
                match op.as_str() {
                    ">" => speed > threshold,
                    ">=" => speed >= threshold,
                    "<" => speed < threshold,
                    "<=" => speed <= threshold,
                    "==" => speed == threshold,
                    "!=" => speed != threshold,
                    _ => false,
                }
            }) as Box<dyn Fn(&Event) -> bool + Send + Sync>)
        }
        "entity_id" => {
            let wanted = value.trim_matches(['\'', '"']).to_string();
            let negate = match op {
                "==" => false,
                "!=" => true,
                _ => return Err(format!("unsupported operator for entity_id: {op:?}")),
            };
            Ok(Box::new(move |e: &Event| (e.entity_id == wanted) != negate)
                as Box<dyn Fn(&Event) -> bool + Send + Sync>)
        }
        _ => Err(format!("unsupported filter field: {field:?}")),
    }
}

/// Build a connector config from the topology fields, reading the password out of the
/// environment variable the topology names.
#[cfg(feature = "mqtt")]
fn mqtt_config(
    broker_url: &str,
    topic: &str,
    username: Option<&str>,
    password_env: Option<&str>,
    qos: Option<u8>,
    client_id: Option<&str>,
) -> Result<fluvius_connectors::mqtt::MqttConfig, String> {
    use fluvius_connectors::mqtt::{MqttConfig, MqttQos};

    let mut config = MqttConfig::new(broker_url, topic);
    if let Some(id) = client_id {
        config.client_id = id.to_string();
    }
    if let Some(level) = qos {
        let level = match level {
            0 => MqttQos::AtMostOnce,
            1 => MqttQos::AtLeastOnce,
            2 => MqttQos::ExactlyOnce,
            other => return Err(format!("mqtt qos must be 0, 1 or 2, got {other}")),
        };
        config = config.with_qos(level);
    }

    match (username, password_env) {
        (Some(user), Some(variable)) => {
            let password = std::env::var(variable).map_err(|_| {
                format!("mqtt password_env names {variable}, which is not set in the environment")
            })?;
            config = config.with_credentials(user, password);
        }
        (Some(user), None) => {
            return Err(format!(
                "mqtt username {user:?} needs a password_env naming the variable that holds its password"
            ));
        }
        (None, Some(variable)) => {
            return Err(format!("mqtt password_env {variable:?} needs a username"));
        }
        (None, None) => {}
    }

    Ok(config)
}

/// Resolve a source config into a receiver fed by a background task.
fn resolve_source(config: &SourceConfig) -> Result<mpsc::Receiver<Event>, String> {
    match config {
        SourceConfig::File { path } => {
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            let path = path.clone();
            tokio::spawn(async move {
                match file::read_jsonl(Path::new(&path)).await {
                    Ok(events) => {
                        for event in events {
                            if tx.send(event).await.is_err() {
                                break;
                            }
                        }
                    }
                    Err(e) => eprintln!("file source error: {e}"),
                }
            });
            Ok(rx)
        }
        SourceConfig::WebSocket { url } => {
            let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
            match remote_feed_url(url)? {
                Some(feed) => {
                    let feed = feed.to_string();
                    tokio::spawn(async move {
                        fluvius_connectors::websocket::ws_remote_source(&feed, tx).await;
                    });
                }
                None => {
                    let bind = ws_bind(url);
                    tokio::spawn(async move {
                        if let Err(e) = fluvius_connectors::websocket::ws_source(&bind, tx).await {
                            eprintln!("websocket source error: {e}");
                        }
                    });
                }
            }
            Ok(rx)
        }
        SourceConfig::Kafka {
            brokers,
            topic,
            group_id,
        } => {
            #[cfg(feature = "kafka")]
            {
                use fluvius_connectors::kafka::{KafkaConfig, KafkaSource};
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                let mut cfg = KafkaConfig::new(brokers.clone(), topic.clone());
                if let Some(group) = group_id {
                    cfg = cfg.with_group(group.clone());
                }
                tokio::spawn(async move {
                    if let Err(e) = KafkaSource::new(cfg).start(tx).await {
                        eprintln!("kafka source error: {e}");
                    }
                });
                Ok(rx)
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (brokers, topic, group_id);
                Err(
                    "topology source type 'kafka' requires building with --features kafka"
                        .to_string(),
                )
            }
        }
        SourceConfig::Mqtt {
            broker_url,
            topic,
            username,
            password_env,
            qos,
            client_id,
        } => {
            #[cfg(feature = "mqtt")]
            {
                use fluvius_connectors::mqtt::MqttSource;
                let cfg = mqtt_config(
                    broker_url,
                    topic,
                    username.as_deref(),
                    password_env.as_deref(),
                    *qos,
                    client_id.as_deref(),
                )?;
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                tokio::spawn(async move {
                    if let Err(e) = MqttSource::new(cfg).start(tx).await {
                        eprintln!("mqtt source error: {e}");
                    }
                });
                Ok(rx)
            }
            #[cfg(not(feature = "mqtt"))]
            {
                let _ = (broker_url, topic, username, password_env, qos, client_id);
                Err(
                    "topology source type 'mqtt' requires building with --features mqtt"
                        .to_string(),
                )
            }
        }
    }
}

/// Spawn a sink task that drains the pipeline output into the configured destination.
fn spawn_sink(
    config: &SinkConfig,
    mut rx: mpsc::Receiver<OutputEvent>,
) -> Result<tokio::task::JoinHandle<()>, String> {
    match config {
        SinkConfig::File { path } => {
            let path = path.clone();
            Ok(tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    if let Err(e) = file::append_jsonl(Path::new(&path), &event).await {
                        eprintln!("file sink error: {e}");
                        break;
                    }
                }
            }))
        }
        SinkConfig::Stdout => Ok(tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                match serde_json::to_string(&event) {
                    Ok(line) => println!("{line}"),
                    Err(e) => eprintln!("stdout sink error: {e}"),
                }
            }
        })),
        SinkConfig::WebSocket { url } => {
            let bind = ws_bind(url);
            Ok(tokio::spawn(async move {
                if let Err(e) = fluvius_connectors::websocket::WsSink::start(&bind, rx).await {
                    eprintln!("websocket sink error: {e}");
                }
            }))
        }
        SinkConfig::Kafka { brokers, topic } => {
            #[cfg(feature = "kafka")]
            {
                use fluvius_connectors::kafka::{KafkaConfig, KafkaSink};
                let cfg = KafkaConfig::new(brokers.clone(), topic.clone());
                let sink = KafkaSink::new(cfg).map_err(|e| e.to_string())?;
                Ok(tokio::spawn(async move {
                    while let Some(event) = rx.recv().await {
                        if let Err(e) = sink.send(&event).await {
                            eprintln!("kafka sink error: {e}");
                            break;
                        }
                    }
                }))
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (brokers, topic, &mut rx);
                Err(
                    "topology sink type 'kafka' requires building with --features kafka"
                        .to_string(),
                )
            }
        }
        SinkConfig::Mqtt {
            broker_url,
            topic,
            username,
            password_env,
            qos,
            client_id,
        } => {
            #[cfg(feature = "mqtt")]
            {
                use fluvius_connectors::mqtt::MqttSink;
                let cfg = mqtt_config(
                    broker_url,
                    topic,
                    username.as_deref(),
                    password_env.as_deref(),
                    *qos,
                    client_id.as_deref(),
                )?;
                let sink = MqttSink::new(cfg).map_err(|e| e.to_string())?;
                Ok(tokio::spawn(async move {
                    while let Some(event) = rx.recv().await {
                        if let Err(e) = sink.send(&event).await {
                            eprintln!("mqtt sink error: {e}");
                            break;
                        }
                    }
                }))
            }
            #[cfg(not(feature = "mqtt"))]
            {
                let _ = (
                    broker_url,
                    topic,
                    username,
                    password_env,
                    qos,
                    client_id,
                    &mut rx,
                );
                Err("topology sink type 'mqtt' requires building with --features mqtt".to_string())
            }
        }
    }
}

const PLAIN_WEBSOCKET_SCHEME: &str = "ws://";
const SECURE_WEBSOCKET_SCHEME: &str = "wss://";

/// A source url carrying a websocket scheme names a remote feed to connect to. A bare
/// `host:port` is an address to bind a listener on instead.
fn remote_feed_url(url: &str) -> Result<Option<&str>, String> {
    if url.starts_with(SECURE_WEBSOCKET_SCHEME) && !cfg!(feature = "tls") {
        return Err(format!(
            "topology source url {url:?} requires building with --features tls"
        ));
    }
    let is_feed =
        url.starts_with(PLAIN_WEBSOCKET_SCHEME) || url.starts_with(SECURE_WEBSOCKET_SCHEME);
    Ok(is_feed.then_some(url))
}

/// Strip the scheme and any path from a websocket url, leaving `host:port`.
fn ws_bind(url: &str) -> String {
    let no_scheme = url
        .strip_prefix(PLAIN_WEBSOCKET_SCHEME)
        .or_else(|| url.strip_prefix(SECURE_WEBSOCKET_SCHEME))
        .unwrap_or(url);
    no_scheme
        .split_once('/')
        .map(|(host, _)| host)
        .unwrap_or(no_scheme)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvius_core::topology::{PatternStepConfig, parse_topology};

    fn kafka_mqtt_toml() -> &'static str {
        r#"
[pipeline]
name = "iot-fleet"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 5.0"

[pipeline.source]
type = "kafka"
brokers = ["localhost:9092"]
topic = "gps-events"
group_id = "fluvius-fleet"

[pipeline.sink]
type = "mqtt"
broker_url = "mqtt://localhost:1883"
topic = "alerts/geofence"
"#
    }

    #[test]
    fn test_parse_kafka_mqtt_topology() {
        let config = parse_topology(kafka_mqtt_toml()).unwrap();
        let pipeline = config.pipeline;
        assert!(matches!(pipeline.source, Some(SourceConfig::Kafka { .. })));
        assert!(matches!(pipeline.sink, Some(SinkConfig::Mqtt { .. })));
        assert_eq!(pipeline.operators.len(), 1);
    }

    #[test]
    fn test_build_filter_operator() {
        let op = OperatorConfig::Filter {
            name: "moving".into(),
            condition: "speed > 5.0".into(),
        };
        assert!(matches!(build_stage(&op), Ok(Stage::Map(_))));
    }

    #[test]
    fn test_build_spatial_operators() {
        let stages = [
            OperatorConfig::Geofence {
                name: "depot".into(),
                zones: vec![ZoneConfig {
                    name: "depot".into(),
                    center: [10.0, 20.0],
                    radius: 0.01,
                }],
            },
            OperatorConfig::Proximity {
                name: "near".into(),
                radius_m: 100.0,
            },
            OperatorConfig::Trajectory {
                name: "tracks".into(),
                max_speed_mps: 50.0,
                max_buffer: 100,
            },
            OperatorConfig::SpatialAgg {
                name: "density".into(),
                cell_size_deg: 0.1,
                function: "count".into(),
                threshold: 10,
            },
        ];
        for cfg in &stages {
            assert!(
                matches!(build_stage(cfg), Ok(Stage::Stateful(_))),
                "{cfg:?} should build a stateful stage"
            );
        }
    }

    #[test]
    fn test_geofence_stage_alerts_on_entry() {
        let cfg = OperatorConfig::Geofence {
            name: "depot".into(),
            zones: vec![ZoneConfig {
                name: "depot".into(),
                center: [10.0, 20.0],
                radius: 0.01,
            }],
        };
        let Ok(Stage::Stateful(mut op)) = build_stage(&cfg) else {
            panic!("expected a stateful geofence stage");
        };

        assert!(op.process(&Event::now("truck", 11.0, 21.0)).is_empty());
        let alerts = op.process(&Event::now("truck", 10.0, 20.0));
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].payload["event"], "Enter");
        assert_eq!(alerts[0].payload["zone"], "depot");
    }

    #[test]
    fn test_spatial_agg_stage_emits_at_threshold() {
        let cfg = OperatorConfig::SpatialAgg {
            name: "density".into(),
            cell_size_deg: 0.1,
            function: "count".into(),
            threshold: 2,
        };
        let Ok(Stage::Stateful(mut op)) = build_stage(&cfg) else {
            panic!("expected a stateful spatial_agg stage");
        };

        assert!(op.process(&Event::now("v1", 10.0, 20.0)).is_empty());
        let cells = op.process(&Event::now("v2", 10.01, 20.01));
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].payload["count"], 2);
    }

    #[test]
    fn test_build_cep_stage() {
        let cfg = OperatorConfig::Cep {
            name: "stop-start".into(),
            pattern: PatternConfig {
                name: "stop_then_move".into(),
                within_secs: 60,
                steps: vec![
                    PatternStepConfig {
                        name: "stop".into(),
                        condition: "speed < 1.0".into(),
                        near: None,
                    },
                    PatternStepConfig {
                        name: "move".into(),
                        condition: "speed > 5.0".into(),
                        near: None,
                    },
                ],
            },
        };
        let Ok(Stage::Stateful(mut op)) = build_stage(&cfg) else {
            panic!("expected a stateful cep stage");
        };

        assert!(
            op.process(&Event::now("v1", 0.0, 0.0).with_speed(0.0))
                .is_empty()
        );
        let matches = op.process(&Event::now("v1", 0.0, 0.0).with_speed(10.0));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].payload["pattern"], "stop_then_move");
    }

    #[test]
    fn test_build_rate_limit_stage() {
        let cfg = OperatorConfig::RateLimit {
            name: "cap".into(),
            max_per_second: 10.0,
        };
        assert!(matches!(build_stage(&cfg), Ok(Stage::Map(_))));
    }

    /// A rate that cannot throttle anything is a config mistake, not a pass-through.
    #[test]
    fn test_build_rejects_a_rate_limit_without_a_usable_rate() {
        for rate in [0.0, -1.0, f64::INFINITY, f64::NAN] {
            let cfg = OperatorConfig::RateLimit {
                name: "cap".into(),
                max_per_second: rate,
            };
            let err = build_stage(&cfg).err().unwrap();
            assert!(err.contains("max_per_second"), "{rate}: {err}");
        }
    }

    #[test]
    fn test_build_rejects_bad_configs() {
        let bad_agg = OperatorConfig::SpatialAgg {
            name: "density".into(),
            cell_size_deg: 0.1,
            function: "median".into(),
            threshold: 10,
        };
        assert!(build_stage(&bad_agg).err().unwrap().contains("median"));
    }

    #[test]
    fn test_compile_condition_speed() {
        let pred = compile_condition("speed > 5.0").unwrap();
        assert!(pred(&Event::now("v", 0.0, 0.0).with_speed(6.0)));
        assert!(!pred(&Event::now("v", 0.0, 0.0).with_speed(4.0)));
    }

    #[test]
    fn test_compile_condition_entity() {
        let pred = compile_condition("entity_id == 'truck-1'").unwrap();
        assert!(pred(&Event::now("truck-1", 0.0, 0.0)));
        assert!(!pred(&Event::now("truck-2", 0.0, 0.0)));
    }

    #[test]
    fn test_compile_condition_rejects_garbage() {
        assert!(compile_condition("speed !!").is_err());
        assert!(compile_condition("altitude > 1").is_err());
    }

    #[test]
    fn test_compile_condition_covers_every_speed_operator() {
        let cases = [
            (">", 5.0, false),
            (">=", 5.0, true),
            ("<", 5.0, false),
            ("<=", 5.0, true),
            ("==", 5.0, true),
            ("!=", 5.0, false),
        ];
        for (op, speed, expected) in cases {
            let pred = compile_condition(&format!("speed {op} 5.0")).unwrap();
            assert_eq!(
                pred(&Event::now("v", 0.0, 0.0).with_speed(speed)),
                expected,
                "speed {speed} {op} 5.0"
            );
        }
    }

    /// An event without a speed reading is treated as stopped.
    #[test]
    fn test_compile_condition_treats_missing_speed_as_zero() {
        let pred = compile_condition("speed > 1.0").unwrap();
        assert!(!pred(&Event::now("v", 0.0, 0.0)));

        let pred = compile_condition("speed < 1.0").unwrap();
        assert!(pred(&Event::now("v", 0.0, 0.0)));
    }

    #[test]
    fn test_compile_condition_entity_id_rejects_ordering_operators() {
        let err = compile_condition("entity_id > 'truck-1'").err().unwrap();
        assert!(err.contains("entity_id"), "{err}");
    }

    #[test]
    fn test_compile_condition_entity_id_accepts_either_quote_style() {
        for condition in ["entity_id == 'truck-1'", "entity_id == \"truck-1\""] {
            let pred = compile_condition(condition).unwrap();
            assert!(pred(&Event::now("truck-1", 0.0, 0.0)), "{condition}");
        }

        let pred = compile_condition("entity_id != 'truck-1'").unwrap();
        assert!(pred(&Event::now("truck-2", 0.0, 0.0)));
        assert!(!pred(&Event::now("truck-1", 0.0, 0.0)));
    }

    #[test]
    fn test_compile_condition_rejects_bad_number() {
        assert!(compile_condition("speed > fast").is_err());
    }

    #[test]
    fn test_zone_polygon_wraps_the_configured_radius() {
        use geo::Point;
        use geo::algorithm::contains::Contains;

        let zone = ZoneConfig {
            name: "depot".into(),
            center: [10.0, 20.0],
            radius: 0.01,
        };
        let polygon = zone_polygon(&zone);

        assert!(polygon.contains(&Point::new(10.0, 20.0)), "center");
        assert!(polygon.contains(&Point::new(10.009, 20.0)), "inside radius");
        assert!(
            !polygon.contains(&Point::new(10.02, 20.0)),
            "outside radius"
        );
        // the corner of the bounding box is outside the circle
        assert!(!polygon.contains(&Point::new(10.009, 20.009)));
    }

    #[test]
    fn test_parse_aggregate_is_case_insensitive() {
        for name in ["count", "COUNT", "Sum", "mean", "MAX", "min"] {
            assert!(parse_aggregate(name).is_ok(), "{name}");
        }
        assert!(parse_aggregate("").is_err());
    }

    #[test]
    fn test_proximity_stage_alerts_between_entities() {
        let cfg = OperatorConfig::Proximity {
            name: "near".into(),
            radius_m: 1000.0,
        };
        let Ok(Stage::Stateful(mut op)) = build_stage(&cfg) else {
            panic!("expected a stateful proximity stage");
        };

        assert!(op.process(&Event::now("v1", 0.0, 0.0)).is_empty());
        let alerts = op.process(&Event::now("v2", 0.001, 0.0));
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].payload["alert"], "proximity");
        assert_eq!(alerts[0].payload["entity_b"], "v1");
    }

    #[test]
    fn test_trajectory_stage_alerts_on_speed_anomaly() {
        use chrono::{DateTime, Duration as ChronoDuration};

        let cfg = OperatorConfig::Trajectory {
            name: "tracks".into(),
            max_speed_mps: 50.0,
            max_buffer: 100,
        };
        let Ok(Stage::Stateful(mut op)) = build_stage(&cfg) else {
            panic!("expected a stateful trajectory stage");
        };

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        assert!(op.process(&Event::new("v1", 0.0, 0.0, ts)).is_empty());
        // 0.1 degrees in one second is far above 50 m/s
        let out = op.process(&Event::new("v1", 0.1, 0.1, ts + ChronoDuration::seconds(1)));
        assert!(out.iter().any(|o| o.payload["alert"] == "speed_anomaly"));

        let summary = op.on_window_close();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].payload["type"], "trajectory_summary");
    }

    #[test]
    fn test_build_rejects_a_cep_pattern_with_a_bad_condition() {
        let cfg = OperatorConfig::Cep {
            name: "bad".into(),
            pattern: PatternConfig {
                name: "p".into(),
                within_secs: 60,
                steps: vec![PatternStepConfig {
                    name: "step".into(),
                    condition: "altitude > 100".into(),
                    near: None,
                }],
            },
        };
        assert!(build_stage(&cfg).err().unwrap().contains("altitude"));
    }

    #[test]
    fn test_reject_unsupported_sections() {
        let with_checkpoint = parse_topology(
            "[pipeline]\nname = \"p\"\n\n[pipeline.checkpoint]\ndir = \"/tmp/cp\"\n",
        )
        .unwrap()
        .pipeline;
        let err = reject_unsupported_sections(&with_checkpoint).err().unwrap();
        assert!(
            err.contains("[pipeline.checkpoint]") && err.contains("not supported"),
            "{err}"
        );

        let absent = parse_topology("[pipeline]\nname = \"p\"\n")
            .unwrap()
            .pipeline;
        assert!(reject_unsupported_sections(&absent).is_ok());
    }

    /// A metrics section turned off explicitly binds nothing.
    #[tokio::test]
    async fn test_a_disabled_metrics_section_serves_nothing() {
        let disabled =
            parse_topology("[pipeline]\nname = \"p\"\n\n[pipeline.metrics]\nenabled = false\n")
                .unwrap()
                .pipeline;
        let handle = spawn_metrics_endpoint(&disabled.metrics, Metrics::new())
            .await
            .unwrap();
        assert!(handle.is_none());

        let absent = parse_topology("[pipeline]\nname = \"p\"\n")
            .unwrap()
            .pipeline;
        assert!(
            spawn_metrics_endpoint(&absent.metrics, Metrics::new())
                .await
                .unwrap()
                .is_none()
        );
    }

    /// An address the endpoint cannot take stops the run rather than starting one
    /// with no way to observe it.
    #[tokio::test]
    async fn test_an_unbindable_metrics_address_fails_the_run() {
        let config = Some(MetricsConfig {
            enabled: true,
            bind: "203.0.113.1:9090".to_string(),
            path: "/metrics".to_string(),
        });
        let err = spawn_metrics_endpoint(&config, Metrics::new())
            .await
            .err()
            .unwrap();
        assert!(err.contains("[pipeline.metrics]"), "{err}");
    }

    #[test]
    fn test_replay_source_rejects_a_bad_speed() {
        for speed in [0.0, -2.0, f64::NAN, f64::NEG_INFINITY] {
            let cfg = ReplayConfig {
                file: "history.jsonl".into(),
                speed,
            };
            let err = replay_source(&cfg).err().unwrap();
            assert!(err.contains("replay speed"), "{speed}: {err}");
        }
    }

    #[test]
    fn test_ws_bind_strips_scheme_and_path() {
        assert_eq!(ws_bind("ws://localhost:8080/events"), "localhost:8080");
        assert_eq!(ws_bind("127.0.0.1:9001"), "127.0.0.1:9001");
    }

    #[test]
    fn test_a_websocket_scheme_selects_a_remote_feed() {
        assert_eq!(
            remote_feed_url("ws://localhost:8080/events").unwrap(),
            Some("ws://localhost:8080/events")
        );
        assert_eq!(remote_feed_url("127.0.0.1:9001").unwrap(), None);
        assert_eq!(remote_feed_url("localhost:8080").unwrap(), None);
    }

    #[cfg(feature = "tls")]
    #[test]
    fn test_a_secure_websocket_scheme_selects_a_remote_feed() {
        assert_eq!(
            remote_feed_url("wss://feed.example.com/events").unwrap(),
            Some("wss://feed.example.com/events")
        );
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn test_a_secure_websocket_feed_requires_the_tls_feature() {
        let err = remote_feed_url("wss://feed.example.com/events")
            .err()
            .unwrap();
        assert!(err.contains("--features tls"), "{err}");
    }

    /// serve strips the scheme off its binds, so a bind written as a url still binds
    /// a listener instead of dialing itself.
    #[test]
    fn test_a_stripped_bind_is_never_a_remote_feed() {
        let bind = ws_bind("ws://127.0.0.1:9001/events");
        assert_eq!(remote_feed_url(&bind).unwrap(), None);
    }

    // Feature-gated errors when compiled without the connector feature.
    #[cfg(not(feature = "kafka"))]
    #[test]
    fn test_kafka_source_requires_feature() {
        let cfg = SourceConfig::Kafka {
            brokers: vec!["localhost:9092".into()],
            topic: "t".into(),
            group_id: None,
        };
        assert!(
            resolve_source(&cfg)
                .unwrap_err()
                .contains("--features kafka")
        );
    }

    /// A variable no test sets, so the runner has to report it missing.
    const UNSET_PASSWORD_VARIABLE: &str = "FLUVIUS_TEST_MQTT_PASSWORD_NEVER_SET";

    fn mqtt_source(username: Option<&str>, password_env: Option<&str>) -> SourceConfig {
        SourceConfig::Mqtt {
            broker_url: "mqtt://localhost:1883".into(),
            topic: "t".into(),
            username: username.map(String::from),
            password_env: password_env.map(String::from),
            qos: None,
            client_id: None,
        }
    }

    fn mqtt_sink(username: Option<&str>, password_env: Option<&str>) -> SinkConfig {
        SinkConfig::Mqtt {
            broker_url: "mqtt://localhost:1883".into(),
            topic: "t".into(),
            username: username.map(String::from),
            password_env: password_env.map(String::from),
            qos: None,
            client_id: None,
        }
    }

    #[cfg(not(feature = "mqtt"))]
    #[test]
    fn test_mqtt_source_requires_feature() {
        assert!(
            resolve_source(&mqtt_source(None, None))
                .unwrap_err()
                .contains("--features mqtt")
        );
    }

    /// The password variable is read when the run starts, so an unset one stops the
    /// pipeline before any event moves and says which variable to set.
    #[cfg(feature = "mqtt")]
    #[test]
    fn test_mqtt_source_fails_naming_an_unset_password_variable() {
        let err = resolve_source(&mqtt_source(Some("sensor"), Some(UNSET_PASSWORD_VARIABLE)))
            .err()
            .unwrap();
        assert!(err.contains(UNSET_PASSWORD_VARIABLE), "{err}");
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn test_mqtt_sink_fails_naming_an_unset_password_variable() {
        let (_tx, rx) = mpsc::channel(1);
        let err = spawn_sink(
            &mqtt_sink(Some("alerter"), Some(UNSET_PASSWORD_VARIABLE)),
            rx,
        )
        .err()
        .unwrap();
        assert!(err.contains(UNSET_PASSWORD_VARIABLE), "{err}");
    }

    /// Half a credential cannot reach the broker, so it is a config mistake rather
    /// than a silently dropped username.
    #[cfg(feature = "mqtt")]
    #[test]
    fn test_mqtt_rejects_half_a_credential() {
        let err = resolve_source(&mqtt_source(Some("sensor"), None))
            .err()
            .unwrap();
        assert!(err.contains("password_env"), "{err}");

        let err = resolve_source(&mqtt_source(None, Some(UNSET_PASSWORD_VARIABLE)))
            .err()
            .unwrap();
        assert!(err.contains("username"), "{err}");
    }

    #[cfg(feature = "mqtt")]
    #[test]
    fn test_mqtt_config_maps_qos_and_client_id() {
        use fluvius_connectors::mqtt::MqttQos;

        let config = mqtt_config(
            "mqtt://localhost:1883",
            "t",
            None,
            None,
            Some(2),
            Some("id"),
        )
        .expect("qos 2 is exactly once");
        assert!(matches!(config.qos, MqttQos::ExactlyOnce));
        assert_eq!(config.client_id, "id");

        let err = mqtt_config("mqtt://localhost:1883", "t", None, None, Some(3), None)
            .err()
            .unwrap();
        assert!(err.contains("qos"), "{err}");
    }

    #[cfg(not(feature = "kafka"))]
    #[test]
    fn test_kafka_sink_requires_feature() {
        let (_tx, rx) = mpsc::channel(1);
        let cfg = SinkConfig::Kafka {
            brokers: vec!["localhost:9092".into()],
            topic: "t".into(),
        };
        assert!(
            spawn_sink(&cfg, rx)
                .err()
                .unwrap()
                .contains("--features kafka")
        );
    }

    #[cfg(not(feature = "mqtt"))]
    #[test]
    fn test_mqtt_sink_requires_feature() {
        let (_tx, rx) = mpsc::channel(1);
        assert!(
            spawn_sink(&mqtt_sink(None, None), rx)
                .err()
                .unwrap()
                .contains("--features mqtt")
        );
    }

    /// The file source reads the events the sink would receive, so a bad path is
    /// reported by the task rather than crashing the run.
    #[tokio::test]
    async fn test_file_source_survives_a_missing_path() {
        let cfg = SourceConfig::File {
            path: "no/such/file.jsonl".into(),
        };
        let mut rx = resolve_source(&cfg).unwrap();
        assert!(rx.recv().await.is_none());
    }

    // File source and sink run end to end without a broker.
    #[tokio::test]
    async fn test_file_topology_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.jsonl");
        let output = dir.path().join("out.jsonl");

        let events = [
            Event::now("fast", 0.0, 0.0).with_speed(20.0),
            Event::now("slow", 0.0, 0.0).with_speed(1.0),
        ];
        let lines: Vec<String> = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap())
            .collect();
        std::fs::write(&input, lines.join("\n")).unwrap();

        let toml = format!(
            r#"
[pipeline]
name = "file-run"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 5.0"

[pipeline.source]
type = "file"
path = "{}"

[pipeline.sink]
type = "file"
path = "{}"
"#,
            // forward slashes keep the toml valid on windows paths
            input.display().to_string().replace('\\', "/"),
            output.display().to_string().replace('\\', "/")
        );

        let config = parse_topology(&toml).unwrap();
        run_topology(config).await.unwrap();

        let written = std::fs::read_to_string(&output).unwrap();
        let out_lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(out_lines.len(), 1, "only the fast event passes the filter");
        assert!(out_lines[0].contains("\"entity_id\":\"fast\""));
    }
}
