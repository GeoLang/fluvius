//! A run writes its operator state to a checkpoint directory, and a fresh run started
//! from that directory picks up where the first one left off.

use std::path::Path;

use chrono::{Duration, Utc};
use fluvius_cli::topology_run;
use fluvius_core::event::Event;
use fluvius_core::topology::parse_topology;

/// Run a topology over the file connector, checkpointing into `checkpoint_dir`, and
/// hand back the outputs it emitted.
async fn run_checkpointed(
    operators_toml: &str,
    checkpoint_dir: &Path,
    events: &[Event],
) -> Vec<serde_json::Value> {
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
name = "resumable"
{operators_toml}

[pipeline.checkpoint]
dir = "{}"
interval_secs = 60
max_retained = 3

[pipeline.source]
type = "file"
path = "{}"

[pipeline.sink]
type = "file"
path = "{}"
"#,
        // forward slashes keep the toml valid on windows paths
        checkpoint_dir.display().to_string().replace('\\', "/"),
        input.display().to_string().replace('\\', "/"),
        output.display().to_string().replace('\\', "/")
    );

    let config = parse_topology(&toml).unwrap();
    topology_run::run_topology(config).await.unwrap();

    let written = std::fs::read_to_string(&output).unwrap_or_default();
    serde_json::Deserializer::from_str(&written)
        .into_iter::<serde_json::Value>()
        .map(|r| r.unwrap())
        .collect()
}

const GEOFENCE: &str = r#"
[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]
"#;

/// Whether an entity was inside a zone is what decides Enter against Exit, so an Exit
/// on the first event of the second run can only come from restored state.
#[tokio::test]
async fn a_second_run_resumes_the_geofence_state_of_the_first() {
    let checkpoints = tempfile::tempdir().unwrap();
    let start = Utc::now();

    let first = run_checkpointed(
        GEOFENCE,
        checkpoints.path(),
        &[Event::new("truck-1", 10.0, 20.0, start).with_speed(10.0)],
    )
    .await;
    let transitions: Vec<&str> = first
        .iter()
        .map(|o| o["payload"]["event"].as_str().unwrap())
        .collect();
    assert_eq!(transitions, vec!["Enter"]);

    // a fresh pipeline, fed only the event that leaves the zone
    let second = run_checkpointed(
        GEOFENCE,
        checkpoints.path(),
        &[Event::new("truck-1", 11.0, 21.0, start + Duration::seconds(60)).with_speed(10.0)],
    )
    .await;
    let transitions: Vec<&str> = second
        .iter()
        .map(|o| o["payload"]["event"].as_str().unwrap())
        .collect();
    assert_eq!(
        transitions,
        vec!["Exit"],
        "the restored state remembered truck-1 was inside: {second:#?}"
    );
}

/// Without a checkpoint to restore, the same second run reports an entry it never saw
/// leave, which is what makes the assertion above meaningful.
#[tokio::test]
async fn a_second_run_with_an_empty_checkpoint_dir_starts_clean() {
    let checkpoints = tempfile::tempdir().unwrap();
    let start = Utc::now();

    let outputs = run_checkpointed(
        GEOFENCE,
        checkpoints.path(),
        &[Event::new("truck-1", 11.0, 21.0, start).with_speed(10.0)],
    )
    .await;
    assert!(outputs.is_empty(), "no transition to report: {outputs:#?}");
}

/// Proximity remembers where every entity was, so a single event in the second run
/// alerts against a position the first run recorded.
#[tokio::test]
async fn a_second_run_resumes_proximity_positions() {
    const PROXIMITY: &str = r#"
[[pipeline.operators]]
type = "proximity"
name = "too-close"
radius_m = 200.0
"#;

    let checkpoints = tempfile::tempdir().unwrap();
    let start = Utc::now();

    let first = run_checkpointed(
        PROXIMITY,
        checkpoints.path(),
        &[Event::new("truck-1", 10.0, 20.0, start).with_speed(10.0)],
    )
    .await;
    assert!(first.is_empty(), "nothing to be near yet: {first:#?}");

    // ~52 m from where truck-1 was left
    let second = run_checkpointed(
        PROXIMITY,
        checkpoints.path(),
        &[Event::new("van-2", 10.0005, 20.0, start + Duration::seconds(60)).with_speed(5.0)],
    )
    .await;
    assert_eq!(second.len(), 1, "{second:#?}");
    assert_eq!(second[0]["payload"]["entity_a"], "van-2");
    assert_eq!(second[0]["payload"]["entity_b"], "truck-1");
}

/// Only `max_retained` checkpoint files survive.
#[tokio::test]
async fn checkpoints_are_pruned_to_the_retained_count() {
    let checkpoints = tempfile::tempdir().unwrap();
    let start = Utc::now();

    for i in 0..5 {
        run_checkpointed(
            GEOFENCE,
            checkpoints.path(),
            &[Event::new("truck-1", 10.0, 20.0, start + Duration::seconds(i)).with_speed(10.0)],
        )
        .await;
    }

    let files = std::fs::read_dir(checkpoints.path()).unwrap().count();
    assert_eq!(files, 3, "max_retained = 3");
}
