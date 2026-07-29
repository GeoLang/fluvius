//! Drives a real `serve` session: events in over the websocket source, geofence
//! alerts out over the websocket sink.

use std::time::Duration;

use fluvius_cli::topology_run;
use fluvius_core::event::Event;
use fluvius_core::topology::parse_topology;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const TOPOLOGY: &str = r#"
[pipeline]
name = "live-geofence"

[[pipeline.operators]]
type = "filter"
name = "moving"
condition = "speed > 1.0"

[[pipeline.operators]]
type = "geofence"
name = "depot"
zones = [{ name = "depot", center = [10.0, 20.0], radius = 0.01 }]
"#;

/// Grab a port the OS says is free, then hand it to the server under test.
async fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

async fn connect(addr: &str) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
    let url = format!("ws://{addr}");
    for _ in 0..100 {
        if let Ok((stream, _)) = connect_async(url.as_str()).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("server never came up on {url}");
}

/// Collect sink messages until `want` matches one, so a test never depends on how
/// many unrelated alerts arrive first.
async fn wait_for(
    sink: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    seen: &mut Vec<serde_json::Value>,
    want: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(Ok(Message::Text(text))) = sink.next().await else {
                panic!("sink stream ended before the expected alert: {seen:#?}");
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            seen.push(value.clone());
            if want(&value) {
                return value;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for an alert, saw: {seen:#?}"))
}

async fn send_event(
    source: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
    entity: &str,
    lon: f64,
    lat: f64,
    speed: f64,
) {
    let event = Event::now(entity, lon, lat).with_speed(speed);
    source
        .send(Message::Text(serde_json::to_string(&event).unwrap().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn serve_applies_topology_to_live_events() {
    let source_bind = free_addr().await;
    let sink_bind = free_addr().await;
    let config = parse_topology(TOPOLOGY).unwrap();

    let serve_handle = tokio::spawn({
        let (source, sink) = (source_bind.clone(), sink_bind.clone());
        async move { topology_run::serve(config, &source, &sink).await }
    });

    // subscribe before feeding: the sink broadcasts, it does not buffer
    let mut sink_client = connect(&sink_bind).await;
    let mut source_client = connect(&source_bind).await;

    for (lon, lat) in [(11.0, 21.0), (10.0, 20.0)] {
        let event = Event::now("truck-1", lon, lat).with_speed(10.0);
        source_client
            .send(Message::Text(serde_json::to_string(&event).unwrap().into()))
            .await
            .unwrap();
    }

    let alert = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let Some(Ok(Message::Text(text))) = sink_client.next().await else {
                panic!("sink stream ended before the geofence alert");
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value["operator"] == "depot" {
                return value;
            }
        }
    })
    .await
    .expect("no geofence alert within 10s");

    assert_eq!(alert["payload"]["event"], "Enter");
    assert_eq!(alert["payload"]["zone"], "depot");
    assert_eq!(alert["source_event"]["entity_id"], "truck-1");

    serve_handle.abort();
}

/// The chain a user is told to write: a filter, a geofence and a proximity check,
/// all applied to live traffic.
#[tokio::test]
async fn serve_runs_a_filter_geofence_and_proximity_chain() {
    const CHAIN: &str = r#"
[pipeline]
name = "fleet"

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
"#;

    let source_bind = free_addr().await;
    let sink_bind = free_addr().await;
    let config = parse_topology(CHAIN).unwrap();

    let serve_handle = tokio::spawn({
        let (source, sink) = (source_bind.clone(), sink_bind.clone());
        async move { topology_run::serve(config, &source, &sink).await }
    });

    let mut sink_client = connect(&sink_bind).await;
    let mut source_client = connect(&source_bind).await;

    // stationary, so the filter drops it before any spatial operator sees it
    send_event(&mut source_client, "parked-3", 10.0, 20.0, 0.0).await;
    // outside the depot
    send_event(&mut source_client, "truck-1", 11.0, 21.0, 10.0).await;
    // into the depot
    send_event(&mut source_client, "truck-1", 10.0, 20.0, 10.0).await;
    // into the depot about 52m from truck-1
    send_event(&mut source_client, "van-2", 10.0005, 20.0, 5.0).await;

    let mut seen = Vec::new();
    let alert = wait_for(&mut sink_client, &mut seen, |v| {
        v["operator"] == "too-close"
    })
    .await;

    assert_eq!(alert["payload"]["alert"], "proximity");
    assert_eq!(alert["payload"]["entity_a"], "van-2");
    assert_eq!(alert["payload"]["entity_b"], "truck-1");
    let distance = alert["payload"]["distance_meters"].as_f64().unwrap();
    assert!((40.0..70.0).contains(&distance), "distance was {distance}");

    let entries: Vec<&serde_json::Value> = seen
        .iter()
        .filter(|v| v["operator"] == "depot" && v["payload"]["event"] == "Enter")
        .collect();
    assert_eq!(entries.len(), 2, "both vehicles entered: {seen:#?}");
    assert_eq!(entries[0]["source_event"]["entity_id"], "truck-1");
    assert_eq!(entries[1]["source_event"]["entity_id"], "van-2");

    assert!(
        !seen
            .iter()
            .any(|v| v["source_event"]["entity_id"] == "parked-3"),
        "the filter runs first, so the parked event produced nothing: {seen:#?}"
    );

    serve_handle.abort();
}

/// Junk on the source socket is skipped without killing the connection, so events
/// sent after it are still processed.
#[tokio::test]
async fn serve_skips_malformed_events_and_keeps_running() {
    let source_bind = free_addr().await;
    let sink_bind = free_addr().await;
    let config = parse_topology(TOPOLOGY).unwrap();

    let serve_handle = tokio::spawn({
        let (source, sink) = (source_bind.clone(), sink_bind.clone());
        async move { topology_run::serve(config, &source, &sink).await }
    });

    let mut sink_client = connect(&sink_bind).await;
    let mut source_client = connect(&source_bind).await;

    for junk in ["not json at all", "{}", r#"{"entity_id": "truck-1"}"#, "[]"] {
        source_client
            .send(Message::Text(junk.into()))
            .await
            .unwrap();
    }

    send_event(&mut source_client, "truck-1", 11.0, 21.0, 10.0).await;
    send_event(&mut source_client, "truck-1", 10.0, 20.0, 10.0).await;

    let mut seen = Vec::new();
    let alert = wait_for(&mut sink_client, &mut seen, |v| v["operator"] == "depot").await;
    assert_eq!(alert["payload"]["event"], "Enter");

    serve_handle.abort();
}

/// serve replaces whatever endpoints the topology declares, so a topology written
/// for brokers still runs over the websocket binds.
#[tokio::test]
async fn serve_overrides_broker_endpoints_from_the_topology() {
    let config = parse_topology(&format!(
        r#"{TOPOLOGY}
[pipeline.source]
type = "kafka"
brokers = ["localhost:9092"]
topic = "gps-events"

[pipeline.sink]
type = "mqtt"
broker_url = "mqtt://localhost:1883"
topic = "alerts"
"#
    ))
    .unwrap();

    let source_bind = free_addr().await;
    let sink_bind = free_addr().await;

    let serve_handle = tokio::spawn({
        let (source, sink) = (source_bind.clone(), sink_bind.clone());
        async move { topology_run::serve(config, &source, &sink).await }
    });

    let mut sink_client = connect(&sink_bind).await;
    let mut source_client = connect(&source_bind).await;

    send_event(&mut source_client, "truck-1", 11.0, 21.0, 10.0).await;
    send_event(&mut source_client, "truck-1", 10.0, 20.0, 10.0).await;

    let mut seen = Vec::new();
    let alert = wait_for(&mut sink_client, &mut seen, |v| v["operator"] == "depot").await;
    assert_eq!(alert["payload"]["event"], "Enter");

    serve_handle.abort();
}

#[tokio::test]
async fn serve_rejects_a_topology_with_an_unparsable_filter() {
    let config = parse_topology(
        r#"
[pipeline]
name = "broken-filter"

[[pipeline.operators]]
type = "filter"
name = "nonsense"
condition = "altitude > 100"
"#,
    )
    .unwrap();

    let err = topology_run::serve(config, &free_addr().await, &free_addr().await)
        .await
        .unwrap_err();
    assert!(err.contains("altitude"), "{err}");
}

#[tokio::test]
async fn serve_rejects_a_topology_it_cannot_build() {
    let config = parse_topology(
        r#"
[pipeline]
name = "broken"

[[pipeline.operators]]
type = "spatial_agg"
name = "density"
cell_size_deg = 0.1
function = "median"
threshold = 10
"#,
    )
    .unwrap();

    let err = topology_run::serve(config, &free_addr().await, &free_addr().await)
        .await
        .unwrap_err();
    assert!(err.contains("median"), "{err}");
}
