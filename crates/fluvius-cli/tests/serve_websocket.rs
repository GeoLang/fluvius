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
