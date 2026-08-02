//! A TOML topology chaining a filter with two stateful spatial operators, run
//! end to end over the file connector.

use chrono::{Duration, Utc};
use fluvius_cli::topology_run;
use fluvius_core::event::Event;
use fluvius_core::topology::parse_topology;

#[tokio::test]
async fn topology_chains_filter_geofence_and_trajectory() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.jsonl");
    let output = dir.path().join("out.jsonl");

    let start = Utc::now();
    let events = [
        // outside the depot
        Event::new("truck-1", 10.05, 20.0, start).with_speed(10.0),
        // enters the depot
        Event::new("truck-1", 10.0, 20.0, start + Duration::seconds(300)).with_speed(10.0),
        // still inside, no new geofence alert
        Event::new("truck-1", 10.001, 20.001, start + Duration::seconds(600)).with_speed(10.0),
        // dropped by the filter, so it must never reach the geofence
        Event::new("parked", 10.0, 20.0, start).with_speed(0.0),
    ];
    let lines: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();
    std::fs::write(&input, lines.join("\n")).unwrap();

    let toml = format!(
        r#"
[pipeline]
name = "depot-watch"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{{ name = "depot", center = [10.0, 20.0], radius = 0.01 }}]

[[pipeline.operators]]
type = "trajectory"
name = "tracks"
max_speed_mps = 50.0

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
    topology_run::run_topology(config).await.unwrap();

    let written = std::fs::read_to_string(&output).unwrap();
    let outputs: Vec<serde_json::Value> = written
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();

    let geofence: Vec<&serde_json::Value> = outputs
        .iter()
        .filter(|o| o["operator"] == "depot")
        .collect();
    assert_eq!(geofence.len(), 1, "one entry alert: {outputs:#?}");
    assert_eq!(geofence[0]["payload"]["event"], "Enter");

    let summary = outputs
        .iter()
        .find(|o| o["payload"]["type"] == "trajectory_summary")
        .expect("trajectory flushes a summary when the stream ends");
    assert_eq!(summary["payload"]["entity_id"], "truck-1");
    assert_eq!(summary["payload"]["point_count"], 3);

    assert!(
        !outputs
            .iter()
            .any(|o| o["source_event"]["entity_id"] == "parked"),
        "the filter runs before the spatial operators"
    );
}

/// Run a topology over the file connector and hand back the emitted outputs. No
/// broker is involved, so this covers the whole engine in process.
async fn run_over_files(
    operators_toml: &str,
    events: &[Event],
) -> Result<Vec<serde_json::Value>, String> {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.jsonl");
    let output = dir.path().join("out.jsonl");

    let lines: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();
    std::fs::write(&input, lines.join("\n")).unwrap();

    let toml = format!(
        r#"
[pipeline]
name = "test-pipeline"
{operators_toml}

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
    topology_run::run_topology(config).await?;

    let written = std::fs::read_to_string(&output).unwrap_or_default();
    Ok(written
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect())
}

/// The three-operator chain the docs advertise, run end to end without a broker.
#[tokio::test]
async fn topology_chains_filter_geofence_and_proximity() {
    let start = Utc::now();
    let events = [
        Event::new("truck-1", 11.0, 21.0, start).with_speed(10.0),
        Event::new("truck-1", 10.0, 20.0, start + Duration::seconds(60)).with_speed(10.0),
        // ~52m from truck-1, inside the same zone
        Event::new("van-2", 10.0005, 20.0, start + Duration::seconds(120)).with_speed(5.0),
        // dropped by the filter
        Event::new("parked", 10.0, 20.0, start + Duration::seconds(180)).with_speed(0.0),
    ];

    let outputs = run_over_files(
        r#"
[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]

[[pipeline.operators]]
type = "proximity"
name = "too-close"
radius_m = 200.0
"#,
        &events,
    )
    .await
    .unwrap();

    let entries: Vec<&serde_json::Value> = outputs
        .iter()
        .filter(|o| o["operator"] == "depot" && o["payload"]["event"] == "Enter")
        .collect();
    assert_eq!(entries.len(), 2, "{outputs:#?}");

    let proximity: Vec<&serde_json::Value> = outputs
        .iter()
        .filter(|o| o["operator"] == "too-close")
        .collect();
    assert_eq!(proximity.len(), 1, "{outputs:#?}");
    assert_eq!(proximity[0]["payload"]["entity_a"], "van-2");
    assert_eq!(proximity[0]["payload"]["entity_b"], "truck-1");
    let distance = proximity[0]["payload"]["distance_meters"].as_f64().unwrap();
    assert!((40.0..70.0).contains(&distance), "distance was {distance}");

    assert!(
        !outputs
            .iter()
            .any(|o| o["source_event"]["entity_id"] == "parked")
    );
}

/// An entity that leaves and comes back is reported each time.
#[tokio::test]
async fn topology_reports_repeated_geofence_transitions() {
    let start = Utc::now();
    let path = [(10.0, 0), (11.0, 60), (10.0, 120), (11.0, 180)];
    let events: Vec<Event> = path
        .iter()
        .map(|(lon, offset)| {
            Event::new("truck-1", *lon, 20.0, start + Duration::seconds(*offset)).with_speed(10.0)
        })
        .collect();

    let outputs = run_over_files(
        r#"
[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]
"#,
        &events,
    )
    .await
    .unwrap();

    let transitions: Vec<&str> = outputs
        .iter()
        .filter(|o| o["operator"] == "depot")
        .map(|o| o["payload"]["event"].as_str().unwrap())
        .collect();
    assert_eq!(transitions, vec!["Enter", "Exit", "Enter", "Exit"]);
}

#[tokio::test]
async fn topology_without_operators_emits_nothing() {
    let outputs = run_over_files("", &[Event::now("v1", 0.0, 0.0).with_speed(10.0)])
        .await
        .unwrap();
    assert!(outputs.is_empty(), "{outputs:#?}");
}

#[tokio::test]
async fn topology_missing_a_source_is_rejected() {
    let config = parse_topology(
        r#"
[pipeline]
name = "no-source"

[pipeline.sink]
type = "stdout"
"#,
    )
    .unwrap();
    let err = topology_run::run_topology(config).await.unwrap_err();
    assert!(err.contains("source"), "{err}");
}

#[tokio::test]
async fn topology_missing_a_sink_is_rejected() {
    let config = parse_topology(
        r#"
[pipeline]
name = "no-sink"

[pipeline.source]
type = "file"
path = "in.jsonl"
"#,
    )
    .unwrap();
    let err = topology_run::run_topology(config).await.unwrap_err();
    assert!(err.contains("sink"), "{err}");
}

/// The throttle drops what it rejects, so a downstream operator never sees those
/// events. One token per second and a file source that delivers in microseconds
/// means exactly the first event gets through.
#[tokio::test]
async fn topology_rate_limit_drops_events_before_the_next_operator() {
    let start = Utc::now();
    let events: Vec<Event> = ["truck-1", "truck-2", "truck-3", "truck-4"]
        .iter()
        .enumerate()
        .map(|(i, id)| Event::new(*id, 10.0, 20.0, start + Duration::seconds(i as i64)))
        .collect();

    let outputs = run_over_files(
        r#"
[[pipeline.operators]]
type = "rate_limit"
name = "cap"
max_per_second = 1.0

[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]
"#,
        &events,
    )
    .await
    .unwrap();

    let passed: Vec<&serde_json::Value> =
        outputs.iter().filter(|o| o["operator"] == "cap").collect();
    assert_eq!(passed.len(), 1, "{outputs:#?}");
    assert_eq!(passed[0]["source_event"]["entity_id"], "truck-1");

    let entries: Vec<&serde_json::Value> = outputs
        .iter()
        .filter(|o| o["operator"] == "depot" && o["payload"]["event"] == "Enter")
        .collect();
    assert_eq!(entries.len(), 1, "only the event that passed the throttle");
    assert_eq!(entries[0]["source_event"]["entity_id"], "truck-1");
}

/// A replay section stands in for the source, reading the recorded file. `inf`
/// ignores the recorded gaps.
#[tokio::test]
async fn topology_replays_a_recorded_file() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("history.jsonl");
    let output = dir.path().join("out.jsonl");

    let start = Utc::now();
    let events = [
        Event::new("truck-1", 0.0, 0.0, start).with_speed(10.0),
        // an hour later in the recording, replayed without the wait
        Event::new("truck-2", 0.0, 0.0, start + Duration::hours(1)).with_speed(0.0),
        Event::new("truck-3", 0.0, 0.0, start + Duration::hours(2)).with_speed(20.0),
    ];
    let lines: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();
    std::fs::write(&input, lines.join("\n")).unwrap();

    let config = parse_topology(&format!(
        r#"
[pipeline]
name = "replay-run"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[pipeline.replay]
file = "{}"
speed = inf

[pipeline.sink]
type = "file"
path = "{}"
"#,
        input.display().to_string().replace('\\', "/"),
        output.display().to_string().replace('\\', "/")
    ))
    .unwrap();

    topology_run::run_topology(config).await.unwrap();

    let written = std::fs::read_to_string(&output).unwrap();
    let ids: Vec<String> = written
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            serde_json::from_str::<serde_json::Value>(l).unwrap()["source_event"]["entity_id"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(ids, vec!["truck-1", "truck-3"]);
}

#[tokio::test]
async fn topology_rejects_a_configured_metrics_endpoint() {
    let config = parse_topology(
        r#"
[pipeline]
name = "wants-metrics"

[pipeline.metrics]
enabled = true
port = 9090

[pipeline.source]
type = "file"
path = "in.jsonl"

[pipeline.sink]
type = "stdout"
"#,
    )
    .unwrap();
    let err = topology_run::run_topology(config).await.unwrap_err();
    assert!(
        err.contains("[pipeline.metrics]") && err.contains("not supported"),
        "{err}"
    );
}

#[tokio::test]
async fn topology_rejects_configured_checkpointing() {
    let config = parse_topology(
        r#"
[pipeline]
name = "wants-checkpoints"

[pipeline.checkpoint]
dir = "/tmp/fluvius-checkpoints"

[pipeline.source]
type = "file"
path = "in.jsonl"

[pipeline.sink]
type = "stdout"
"#,
    )
    .unwrap();
    let err = topology_run::run_topology(config).await.unwrap_err();
    assert!(
        err.contains("[pipeline.checkpoint]") && err.contains("not supported"),
        "{err}"
    );
}

/// A missing input file leaves the run empty rather than panicking.
#[tokio::test]
async fn topology_with_an_unreadable_source_produces_no_output() {
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");
    let config = parse_topology(&format!(
        r#"
[pipeline]
name = "missing-input"

[pipeline.source]
type = "file"
path = "{}"

[pipeline.sink]
type = "file"
path = "{}"
"#,
        dir.path()
            .join("does-not-exist.jsonl")
            .display()
            .to_string()
            .replace('\\', "/"),
        output.display().to_string().replace('\\', "/")
    ))
    .unwrap();

    topology_run::run_topology(config).await.unwrap();
    assert!(!output.exists() || std::fs::read_to_string(&output).unwrap().is_empty());
}
