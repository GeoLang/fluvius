//! Scrapes the endpoint a running topology serves and checks the counters against the
//! events that went through it.

use std::path::Path;
use std::time::Duration;

use fluvius_cli::topology_run;
use fluvius_core::event::Event;
use fluvius_core::topology::parse_topology;
use futures::SinkExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const POLL_INTERVAL: Duration = Duration::from_millis(50);
const POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// Ask the OS for a free address, then hand it to the server under test.
async fn free_address() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address.to_string()
}

async fn connect(address: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let url = format!("ws://{address}");
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        if let Ok((stream, _)) = connect_async(url.as_str()).await {
            return stream;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the source socket never came up on {url}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn scrape(address: &str, path: &str) -> String {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    let mut stream = loop {
        match TcpStream::connect(address).await {
            Ok(stream) => break stream,
            Err(e) => assert!(
                tokio::time::Instant::now() < deadline,
                "the metrics endpoint never came up on {address}: {e}"
            ),
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    };
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

/// Wait for the file sink to have written `want` lines.
async fn wait_for_lines(output: &Path, want: usize) {
    let deadline = tokio::time::Instant::now() + POLL_TIMEOUT;
    loop {
        let written = std::fs::read_to_string(output).unwrap_or_default();
        let lines = written.lines().filter(|l| !l.is_empty()).count();
        if lines >= want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "only {lines} of {want} lines reached the sink"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

#[tokio::test]
async fn a_running_topology_serves_its_counters() {
    let source_bind = free_address().await;
    let metrics_bind = free_address().await;
    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.jsonl");

    let config = parse_topology(&format!(
        r#"
[pipeline]
name = "counted"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 5.0"

[pipeline.metrics]
enabled = true
bind = "{metrics_bind}"
path = "/metrics"

[pipeline.source]
type = "websocket"
url = "{source_bind}"

[pipeline.sink]
type = "file"
path = "{}"
"#,
        // forward slashes keep the toml valid on windows paths
        output.display().to_string().replace('\\', "/")
    ))
    .unwrap();

    let run = tokio::spawn(async move { topology_run::run_topology(config).await });

    let mut source = connect(&source_bind).await;
    // the slow one sits between the two the filter passes, so both outputs landing
    // means all three were counted
    for speed in [20.0, 1.0, 30.0] {
        let event = Event::now("truck-1", 10.0, 20.0).with_speed(speed);
        source
            .send(Message::Text(serde_json::to_string(&event).unwrap().into()))
            .await
            .unwrap();
    }
    wait_for_lines(&output, 2).await;

    let response = scrape(&metrics_bind, "/metrics").await;
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(
        response.contains("fluvius_events_received_total 3"),
        "{response}"
    );
    assert!(
        response.contains("fluvius_events_emitted_total 2"),
        "{response}"
    );
    assert!(
        response.contains("fluvius_events_filtered_total 1"),
        "{response}"
    );

    run.abort();
}

/// The endpoint answers only the configured path.
#[tokio::test]
async fn the_metrics_endpoint_refuses_other_paths() {
    let source_bind = free_address().await;
    let metrics_bind = free_address().await;

    let config = parse_topology(&format!(
        r#"
[pipeline]
name = "counted"

[pipeline.metrics]
enabled = true
bind = "{metrics_bind}"
path = "/observe"

[pipeline.source]
type = "websocket"
url = "{source_bind}"

[pipeline.sink]
type = "stdout"
"#
    ))
    .unwrap();

    let run = tokio::spawn(async move { topology_run::run_topology(config).await });
    connect(&source_bind).await;

    assert!(
        scrape(&metrics_bind, "/observe")
            .await
            .starts_with("HTTP/1.1 200 OK\r\n")
    );
    assert!(
        scrape(&metrics_bind, "/metrics")
            .await
            .starts_with("HTTP/1.1 404")
    );

    run.abort();
}
