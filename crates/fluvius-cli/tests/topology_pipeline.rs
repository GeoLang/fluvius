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
