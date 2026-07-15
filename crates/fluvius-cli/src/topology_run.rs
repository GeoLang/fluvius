//! Run a pipeline declared in a TOML topology, wiring configured sources and sinks
//! to the connector implementations. Kafka and MQTT are feature-gated; a topology
//! that references them while the feature is compiled out fails with a clear error.

use std::path::Path;
use std::sync::Arc;

use fluvius_connectors::file;
use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::{FilterOperator, MapOperator};
use fluvius_core::pipeline::Pipeline;
use fluvius_core::topology::{OperatorConfig, SinkConfig, SourceConfig, TopologyConfig};
use tokio::sync::mpsc;

const CHANNEL_CAPACITY: usize = 10_000;

/// Run a topology to completion.
pub async fn run_topology(config: TopologyConfig) -> Result<(), String> {
    let pipeline_cfg = config.pipeline;
    let source_cfg = pipeline_cfg
        .source
        .ok_or_else(|| "topology has no [pipeline.source]".to_string())?;
    let sink_cfg = pipeline_cfg
        .sink
        .ok_or_else(|| "topology has no [pipeline.sink]".to_string())?;

    let mut pipeline = Pipeline::new(pipeline_cfg.name);
    for op in &pipeline_cfg.operators {
        pipeline.add_operator(build_operator(op)?);
    }

    let rx_in = resolve_source(&source_cfg)?;
    let (tx_out, rx_out) = mpsc::channel(CHANNEL_CAPACITY);
    let sink_handle = spawn_sink(&sink_cfg, rx_out)?;

    let metrics = pipeline.run(rx_in, tx_out).await;
    // pipeline.run returns once the source channel closes, dropping tx_out, which
    // closes the sink channel and lets the sink task finish.
    let _ = sink_handle.await;

    println!("Topology complete:");
    println!("  Events received: {}", metrics.events_received);
    println!("  Events emitted: {}", metrics.events_emitted);
    println!("  Events filtered: {}", metrics.events_filtered);
    Ok(())
}

/// Build a pipeline operator from its config. Only stateless (MapOperator) types
/// are supported by the topology runner; stateful geo operators are not chainable here.
fn build_operator(config: &OperatorConfig) -> Result<Arc<dyn MapOperator>, String> {
    match config {
        OperatorConfig::Filter { name, condition } => {
            let predicate = compile_condition(condition)?;
            Ok(Arc::new(FilterOperator::new(name.clone(), predicate)))
        }
        other => Err(format!(
            "topology operator '{}' is not supported by the run command yet",
            operator_kind(other)
        )),
    }
}

fn operator_kind(config: &OperatorConfig) -> &'static str {
    match config {
        OperatorConfig::Filter { .. } => "filter",
        OperatorConfig::Geofence { .. } => "geofence",
        OperatorConfig::Proximity { .. } => "proximity",
        OperatorConfig::RateLimit { .. } => "rate_limit",
        OperatorConfig::SpatialAgg { .. } => "spatial_agg",
        OperatorConfig::Cep { .. } => "cep",
    }
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
            let bind = ws_bind(url);
            tokio::spawn(async move {
                if let Err(e) = fluvius_connectors::websocket::ws_source(&bind, tx).await {
                    eprintln!("websocket source error: {e}");
                }
            });
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
        SourceConfig::Mqtt { broker_url, topic } => {
            #[cfg(feature = "mqtt")]
            {
                use fluvius_connectors::mqtt::{MqttConfig, MqttSource};
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                let cfg = MqttConfig::new(broker_url.clone(), topic.clone());
                tokio::spawn(async move {
                    if let Err(e) = MqttSource::new(cfg).start(tx).await {
                        eprintln!("mqtt source error: {e}");
                    }
                });
                Ok(rx)
            }
            #[cfg(not(feature = "mqtt"))]
            {
                let _ = (broker_url, topic);
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
        SinkConfig::Mqtt { broker_url, topic } => {
            #[cfg(feature = "mqtt")]
            {
                use fluvius_connectors::mqtt::{MqttConfig, MqttSink};
                let cfg = MqttConfig::new(broker_url.clone(), topic.clone());
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
                let _ = (broker_url, topic, &mut rx);
                Err("topology sink type 'mqtt' requires building with --features mqtt".to_string())
            }
        }
    }
}

/// Strip the scheme and any path from a websocket url, leaving `host:port`.
fn ws_bind(url: &str) -> String {
    let no_scheme = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
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
    use fluvius_core::topology::parse_topology;

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
        assert!(build_operator(&op).is_ok());
    }

    #[test]
    fn test_build_unsupported_operator_errors() {
        let op = OperatorConfig::Proximity {
            name: "near".into(),
            radius_m: 100.0,
        };
        let err = match build_operator(&op) {
            Err(e) => e,
            Ok(_) => panic!("expected an error for proximity"),
        };
        assert!(err.contains("proximity"));
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
    fn test_ws_bind_strips_scheme_and_path() {
        assert_eq!(ws_bind("ws://localhost:8080/events"), "localhost:8080");
        assert_eq!(ws_bind("127.0.0.1:9001"), "127.0.0.1:9001");
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

    #[cfg(not(feature = "mqtt"))]
    #[test]
    fn test_mqtt_source_requires_feature() {
        let cfg = SourceConfig::Mqtt {
            broker_url: "mqtt://localhost:1883".into(),
            topic: "t".into(),
        };
        assert!(
            resolve_source(&cfg)
                .unwrap_err()
                .contains("--features mqtt")
        );
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
