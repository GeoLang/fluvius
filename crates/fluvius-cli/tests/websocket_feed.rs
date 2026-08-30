//! Runs a topology whose source is a remote websocket feed, against a server the test
//! spins up itself.

use std::path::Path;
use std::time::Duration;

use fluvius_cli::topology_run;
use fluvius_core::event::Event;
use fluvius_core::topology::parse_topology;
use futures::SinkExt;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

fn topology(feed_url: &str, output: &Path) -> String {
    format!(
        r#"
[pipeline]
name = "remote-feed"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[pipeline.source]
type = "websocket"
url = "{feed_url}"

[pipeline.sink]
type = "file"
path = "{}"
"#,
        // forward slashes keep the toml valid on windows paths
        output.display().to_string().replace('\\', "/")
    )
}

fn event_json(entity: &str) -> String {
    serde_json::to_string(&Event::now(entity, 10.0, 20.0).with_speed(10.0)).unwrap()
}

/// Wait for the file sink to have written `want` lines, then return them.
async fn wait_for_lines(output: &Path, want: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let lines: Vec<String> = std::fs::read_to_string(output)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect();
        if lines.len() >= want {
            return lines;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {} of {want} lines reached the sink: {lines:#?}",
            lines.len()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn topology_reads_events_from_a_remote_websocket_feed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let feed_url = format!("ws://{}", listener.local_addr().unwrap());

    let feed = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut server = accept_async(stream).await.unwrap();
        for entity in ["truck-1", "van-2"] {
            server
                .send(Message::Text(event_json(entity).into()))
                .await
                .unwrap();
        }
        // hold the socket open, a close would only make the runner reconnect
        std::future::pending::<()>().await;
    });

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");
    let config = parse_topology(&topology(&feed_url, &output)).unwrap();
    let run = tokio::spawn(async move { topology_run::run_topology(config).await });

    let lines = wait_for_lines(&output, 2).await;
    assert!(lines[0].contains("\"entity_id\":\"truck-1\""), "{lines:#?}");
    assert!(lines[1].contains("\"entity_id\":\"van-2\""), "{lines:#?}");

    run.abort();
    feed.abort();
}

/// A feed that hangs up is dialed again, so events published after the drop still
/// reach the sink.
#[tokio::test]
async fn topology_reconnects_to_a_feed_that_closes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let feed_url = format!("ws://{}", listener.local_addr().unwrap());

    let feed = tokio::spawn(async move {
        for entity in ["before-drop", "after-drop"] {
            let (stream, _) = listener.accept().await.unwrap();
            let mut server = accept_async(stream).await.unwrap();
            server
                .send(Message::Text(event_json(entity).into()))
                .await
                .unwrap();
            server.close(None).await.unwrap();
        }
        std::future::pending::<()>().await;
    });

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");
    let config = parse_topology(&topology(&feed_url, &output)).unwrap();
    let run = tokio::spawn(async move { topology_run::run_topology(config).await });

    let lines = wait_for_lines(&output, 2).await;
    assert!(
        lines[0].contains("\"entity_id\":\"before-drop\""),
        "{lines:#?}"
    );
    assert!(
        lines[1].contains("\"entity_id\":\"after-drop\""),
        "{lines:#?}"
    );

    run.abort();
    feed.abort();
}
