//! Topology DSL — declare pipelines from TOML configuration.
//!
//! Example TOML:
//! ```toml
//! [pipeline]
//! name = "vehicle-tracking"
//!
//! [[pipeline.operators]]
//! type = "filter"
//! name = "speed-filter"
//! condition = "speed > 5.0"
//!
//! [[pipeline.operators]]
//! type = "geofence"
//! name = "warehouse-zone"
//! zones = [{ name = "warehouse", center = [10.0, 20.0], radius = 0.01 }]
//!
//! [pipeline.source]
//! type = "websocket"
//! url = "ws://localhost:8080/events"
//!
//! [pipeline.sink]
//! type = "file"
//! path = "output.jsonl"
//! ```

use serde::{Deserialize, Serialize};

/// Top-level topology configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    pub pipeline: PipelineConfig,
}

/// Pipeline configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    #[serde(default)]
    pub operators: Vec<OperatorConfig>,
    pub source: Option<SourceConfig>,
    pub sink: Option<SinkConfig>,
    pub metrics: Option<MetricsConfig>,
    pub checkpoint: Option<CheckpointConfig>,
    pub replay: Option<ReplayConfig>,
}

/// Operator definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OperatorConfig {
    #[serde(rename = "filter")]
    Filter {
        name: String,
        /// Simple expression: "speed > 5.0", "entity_id == 'vehicle1'"
        condition: String,
    },
    #[serde(rename = "geofence")]
    Geofence {
        name: String,
        zones: Vec<ZoneConfig>,
    },
    #[serde(rename = "proximity")]
    Proximity { name: String, radius_m: f64 },
    #[serde(rename = "trajectory")]
    Trajectory {
        name: String,
        /// Speed above which an anomaly alert is emitted, in m/s.
        max_speed_mps: f64,
        #[serde(default = "default_max_buffer")]
        max_buffer: usize,
    },
    #[serde(rename = "rate_limit")]
    RateLimit { name: String, max_per_second: f64 },
    #[serde(rename = "spatial_agg")]
    SpatialAgg {
        name: String,
        cell_size_deg: f64,
        function: String,
        threshold: u64,
    },
    #[serde(rename = "cep")]
    Cep {
        name: String,
        pattern: PatternConfig,
    },
}

fn default_max_buffer() -> usize {
    1000
}

/// Zone configuration for geofence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZoneConfig {
    pub name: String,
    pub center: [f64; 2],
    pub radius: f64,
}

/// CEP pattern configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternConfig {
    pub name: String,
    pub within_secs: u64,
    pub steps: Vec<PatternStepConfig>,
}

/// CEP pattern step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternStepConfig {
    pub name: String,
    pub condition: String,
    pub near: Option<[f64; 3]>, // [lon, lat, radius_deg]
}

/// Source configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "websocket")]
    WebSocket { url: String },
    #[serde(rename = "file")]
    File { path: String },
    #[serde(rename = "kafka")]
    Kafka {
        brokers: Vec<String>,
        topic: String,
        group_id: Option<String>,
    },
    #[serde(rename = "mqtt")]
    Mqtt { broker_url: String, topic: String },
}

/// Sink configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    #[serde(rename = "websocket")]
    WebSocket { url: String },
    #[serde(rename = "file")]
    File { path: String },
    #[serde(rename = "kafka")]
    Kafka { brokers: Vec<String>, topic: String },
    #[serde(rename = "mqtt")]
    Mqtt { broker_url: String, topic: String },
    #[serde(rename = "stdout")]
    Stdout,
}

/// Metrics endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,
    #[serde(default = "default_metrics_port")]
    pub port: u16,
    #[serde(default = "default_metrics_path")]
    pub path: String,
}

fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_port() -> u16 {
    9090
}
fn default_metrics_path() -> String {
    "/metrics".to_string()
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
            path: default_metrics_path(),
        }
    }
}

/// Checkpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub dir: String,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    #[serde(default = "default_max_retained")]
    pub max_retained: usize,
}

fn default_interval_secs() -> u64 {
    60
}
fn default_max_retained() -> usize {
    5
}

/// Replay configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    pub file: String,
    /// Playback multiplier over the recorded timestamps. `inf` replays as fast as
    /// the pipeline can take the events.
    #[serde(default = "default_speed")]
    pub speed: f64,
}

fn default_speed() -> f64 {
    1.0
}

/// Parse a topology from TOML string.
pub fn parse_topology(toml_str: &str) -> Result<TopologyConfig, toml::de::Error> {
    toml::from_str(toml_str)
}

/// Load a topology from a file path.
pub fn load_topology(path: &std::path::Path) -> Result<TopologyConfig, TopologyError> {
    let content = std::fs::read_to_string(path)?;
    let config = parse_topology(&content)?;
    Ok(config)
}

/// Topology loading errors.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML parse error: {0}")]
    Parse(#[from] toml::de::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_topology() {
        let toml = r#"
[pipeline]
name = "vehicle-tracking"

[[pipeline.operators]]
type = "filter"
name = "speed-filter"
condition = "speed > 5.0"

[pipeline.source]
type = "websocket"
url = "ws://localhost:8080/events"

[pipeline.sink]
type = "file"
path = "output.jsonl"
"#;
        let config = parse_topology(toml).unwrap();
        assert_eq!(config.pipeline.name, "vehicle-tracking");
        assert_eq!(config.pipeline.operators.len(), 1);
        assert!(config.pipeline.source.is_some());
        assert!(config.pipeline.sink.is_some());
    }

    #[test]
    fn test_parse_trajectory_operator() {
        let toml = r#"
[pipeline]
name = "tracks"

[[pipeline.operators]]
type = "trajectory"
name = "tracks"
max_speed_mps = 50.0
"#;
        let config = parse_topology(toml).unwrap();
        let OperatorConfig::Trajectory {
            max_speed_mps,
            max_buffer,
            ..
        } = &config.pipeline.operators[0]
        else {
            panic!("expected a trajectory operator");
        };
        assert!((max_speed_mps - 50.0).abs() < 1e-10);
        assert_eq!(*max_buffer, 1000);
    }

    #[test]
    fn test_parse_full_topology() {
        let toml = r#"
[pipeline]
name = "iot-fleet"

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

[[pipeline.operators]]
type = "cep"
name = "stop-start"
[pipeline.operators.pattern]
name = "stop_then_move"
within_secs = 60
steps = [
    { name = "stop", condition = "speed < 1.0" },
    { name = "move", condition = "speed > 5.0" },
]

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
path = "/metrics"

[pipeline.checkpoint]
dir = "/tmp/fluvius-checkpoints"
interval_secs = 30
max_retained = 3

[pipeline.replay]
file = "historical.jsonl"
speed = 10.0
"#;
        let config = parse_topology(toml).unwrap();
        assert_eq!(config.pipeline.name, "iot-fleet");
        assert_eq!(config.pipeline.operators.len(), 3);
        assert!(config.pipeline.checkpoint.is_some());
        assert!(config.pipeline.replay.is_some());
        assert_eq!(config.pipeline.metrics.unwrap().port, 9090);
    }

    /// A pipeline is legal with no operators and no endpoints; the runner is what
    /// insists on a source and a sink.
    #[test]
    fn test_parse_minimal_pipeline() {
        let config = parse_topology("[pipeline]\nname = \"bare\"\n").unwrap();
        assert!(config.pipeline.operators.is_empty());
        assert!(config.pipeline.source.is_none());
        assert!(config.pipeline.sink.is_none());
        assert!(config.pipeline.checkpoint.is_none());
        assert!(config.pipeline.metrics.is_none());
    }

    #[test]
    fn test_parse_rejects_unknown_operator_type() {
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[[pipeline.operators]]
type = "teleport"
name = "nope"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("teleport"), "{err}");
    }

    #[test]
    fn test_parse_rejects_operator_missing_required_field() {
        // geofence without zones
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[[pipeline.operators]]
type = "geofence"
name = "depot"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("zones"), "{err}");
    }

    #[test]
    fn test_parse_rejects_zone_with_wrong_center_arity() {
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0], radius = 0.01 }]
"#,
        )
        .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn test_parse_rejects_unknown_source_type() {
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[pipeline.source]
type = "carrier-pigeon"
url = "nowhere"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("carrier-pigeon"), "{err}");
    }

    #[test]
    fn test_parse_rejects_source_missing_field() {
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[pipeline.source]
type = "kafka"
topic = "gps"
"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("brokers"), "{err}");
    }

    #[test]
    fn test_parse_rejects_malformed_toml() {
        assert!(parse_topology("[pipeline\nname = ").is_err());
        assert!(parse_topology("this is not toml at all {{{").is_err());
    }

    #[test]
    fn test_parse_rejects_missing_pipeline_section() {
        let err = parse_topology("[something_else]\nname = \"p\"\n").unwrap_err();
        assert!(err.to_string().contains("pipeline"), "{err}");
    }

    #[test]
    fn test_parse_rejects_wrong_field_type() {
        let err = parse_topology(
            r#"
[pipeline]
name = "p"

[[pipeline.operators]]
type = "proximity"
name = "near"
radius_m = "one hundred"
"#,
        )
        .unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn test_stdout_sink_needs_no_fields() {
        let config = parse_topology(
            r#"
[pipeline]
name = "p"

[pipeline.sink]
type = "stdout"
"#,
        )
        .unwrap();
        assert!(matches!(config.pipeline.sink, Some(SinkConfig::Stdout)));
    }

    #[test]
    fn test_section_defaults() {
        let config = parse_topology(
            r#"
[pipeline]
name = "p"

[pipeline.metrics]

[pipeline.checkpoint]
dir = "/var/lib/fluvius"

[pipeline.replay]
file = "history.jsonl"
"#,
        )
        .unwrap();
        let metrics = config.pipeline.metrics.unwrap();
        assert!(metrics.enabled);
        assert_eq!(metrics.port, 9090);
        assert_eq!(metrics.path, "/metrics");
        let checkpoint = config.pipeline.checkpoint.unwrap();
        assert_eq!(checkpoint.interval_secs, 60);
        assert_eq!(checkpoint.max_retained, 5);
        let replay = config.pipeline.replay.unwrap();
        assert!((replay.speed - 1.0).abs() < 1e-10);
    }

    /// `inf` is a legal TOML float and is what selects max-speed replay.
    #[test]
    fn test_replay_speed_accepts_infinity() {
        let config = parse_topology(
            r#"
[pipeline]
name = "p"

[pipeline.replay]
file = "history.jsonl"
speed = inf
"#,
        )
        .unwrap();
        assert!(config.pipeline.replay.unwrap().speed.is_infinite());
    }

    #[test]
    fn test_load_topology_reads_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.toml");
        std::fs::write(&path, "[pipeline]\nname = \"from-disk\"\n").unwrap();

        let config = load_topology(&path).unwrap();
        assert_eq!(config.pipeline.name, "from-disk");
    }

    #[test]
    fn test_load_topology_missing_file_is_io_error() {
        let err = load_topology(std::path::Path::new("no/such/pipeline.toml")).unwrap_err();
        assert!(matches!(err, TopologyError::Io(_)), "{err:?}");
    }

    #[test]
    fn test_load_topology_bad_content_is_parse_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "[pipeline\n").unwrap();

        let err = load_topology(&path).unwrap_err();
        assert!(matches!(err, TopologyError::Parse(_)), "{err:?}");
    }

    /// A parsed topology round-trips, so a config can be re-serialized without loss.
    #[test]
    fn test_topology_roundtrips_through_toml() {
        let original = parse_topology(
            r#"
[pipeline]
name = "roundtrip"

[[pipeline.operators]]
type = "proximity"
name = "near"
radius_m = 250.0

[pipeline.source]
type = "file"
path = "in.jsonl"

[pipeline.sink]
type = "stdout"
"#,
        )
        .unwrap();

        let reparsed = parse_topology(&toml::to_string(&original).unwrap()).unwrap();
        assert_eq!(reparsed.pipeline.name, "roundtrip");
        let OperatorConfig::Proximity { name, radius_m } = &reparsed.pipeline.operators[0] else {
            panic!("expected proximity");
        };
        assert_eq!(name, "near");
        assert!((radius_m - 250.0).abs() < 1e-10);
    }
}
