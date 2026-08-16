//! File-based source/sink — read from JSON lines files, write outputs.

use std::path::Path;

use fluvius_core::event::{Event, OutputEvent};
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

/// Read events from a JSON lines file.
pub async fn read_jsonl(path: &Path) -> Result<Vec<Event>, std::io::Error> {
    let file = fs::File::open(path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut events = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Event>(&line) {
            Ok(event) => events.push(event),
            Err(e) => {
                eprintln!("Warning: skipping malformed event line: {e}");
            }
        }
    }

    Ok(events)
}

/// Write output events to a JSON lines file.
pub async fn write_jsonl(path: &Path, events: &[OutputEvent]) -> Result<(), std::io::Error> {
    let mut file = fs::File::create(path).await?;

    for event in events {
        write_jsonl_line(&mut file, event).await?;
    }

    file.flush().await?;
    Ok(())
}

/// Append a single output event to a file.
pub async fn append_jsonl(path: &Path, event: &OutputEvent) -> Result<(), std::io::Error> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;

    write_jsonl_line(&mut file, event).await?;
    file.flush().await?;
    Ok(())
}

async fn write_jsonl_line(file: &mut fs::File, event: &OutputEvent) -> Result<(), std::io::Error> {
    let mut line = serde_json::to_string(event)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    line.push('\n');
    file.write_all(line.as_bytes()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvius_core::event::Event;

    #[tokio::test]
    async fn test_jsonl_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");

        // Write events
        let events = vec![Event::now("v1", 1.0, 2.0), Event::now("v2", 3.0, 4.0)];

        let mut file = fs::File::create(&path).await.unwrap();
        for e in &events {
            let line = serde_json::to_string(e).unwrap();
            file.write_all(line.as_bytes()).await.unwrap();
            file.write_all(b"\n").await.unwrap();
        }
        drop(file);

        // Read back
        let read_events = read_jsonl(&path).await.unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].entity_id, "v1");
        assert_eq!(read_events[1].entity_id, "v2");
        assert!((read_events[0].lon - 1.0).abs() < 1e-10);
    }

    #[tokio::test]
    async fn test_output_jsonl_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.jsonl");

        let outputs = vec![OutputEvent {
            source_event: Event::now("v1", 0.0, 0.0),
            operator: "test".into(),
            payload: serde_json::json!({"result": "ok"}),
        }];

        write_jsonl(&path, &outputs).await.unwrap();

        let content = fs::read_to_string(&path).await.unwrap();
        assert!(content.contains("\"operator\":\"test\""));
    }

    /// A junk line in an export is skipped, the good events around it still load.
    #[tokio::test]
    async fn test_read_skips_malformed_and_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.jsonl");

        let good = serde_json::to_string(&Event::now("v1", 1.0, 2.0)).unwrap();
        let also_good = serde_json::to_string(&Event::now("v2", 3.0, 4.0)).unwrap();
        let content = format!(
            "{good}\n\n   \nnot json at all\n{{\"entity_id\": \"missing-fields\"}}\n{also_good}\n"
        );
        fs::write(&path, content).await.unwrap();

        let events = read_jsonl(&path).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].entity_id, "v1");
        assert_eq!(events[1].entity_id, "v2");
    }

    #[tokio::test]
    async fn test_read_empty_file_yields_no_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");
        fs::write(&path, "").await.unwrap();

        assert!(read_jsonl(&path).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_read_missing_file_is_an_error() {
        let err = read_jsonl(Path::new("no/such/events.jsonl"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_append_accumulates_and_creates_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("alerts.jsonl");

        for i in 0..3 {
            let output = OutputEvent {
                source_event: Event::now(format!("v{i}"), 0.0, 0.0),
                operator: "geofence".into(),
                payload: serde_json::json!({ "seq": i }),
            };
            append_jsonl(&path, &output).await.unwrap();
        }

        let content = fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 3);
        assert!(lines[2].contains("\"seq\":2"));
        assert!(!content.contains("}{"));
    }

    /// write_jsonl replaces the file, so a rerun does not append to the old results.
    #[tokio::test]
    async fn test_write_truncates_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");

        let make = |name: &str| OutputEvent {
            source_event: Event::now(name, 0.0, 0.0),
            operator: "test".into(),
            payload: serde_json::json!({}),
        };

        write_jsonl(&path, &[make("first"), make("second")])
            .await
            .unwrap();
        write_jsonl(&path, &[make("only")]).await.unwrap();

        let content = fs::read_to_string(&path).await.unwrap();
        assert_eq!(content.lines().filter(|l| !l.is_empty()).count(), 1);
        assert!(content.contains("only"));
        assert!(!content.contains("first"));
    }
}
