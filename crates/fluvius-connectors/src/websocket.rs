//! WebSocket source/sink — receive and send events over WebSocket connections.

use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};
use tokio_tungstenite::{WebSocketStream, accept_async, connect_async};

use fluvius_core::event::{Event, OutputEvent};

const RECONNECT_FIRST_DELAY: Duration = Duration::from_millis(250);
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const RECONNECT_BACKOFF_FACTOR: u32 = 2;

/// Start a WebSocket server that receives events and sends them to the channel.
pub async fn ws_source(bind: &str, tx: mpsc::Sender<Event>) -> Result<(), std::io::Error> {
    let listener = TcpListener::bind(bind).await?;

    while let Ok((stream, _)) = listener.accept().await {
        let tx = tx.clone();
        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(ws_stream) => {
                    forward_events(ws_stream, &tx).await;
                }
                Err(e) => eprintln!("WebSocket accept error: {e}"),
            }
        });
    }

    Ok(())
}

/// Connect to a remote WebSocket feed and forward the events it publishes. A feed that
/// closes or refuses the connection is retried, the wait doubling up to
/// `RECONNECT_MAX_DELAY`. Returns once the receiver is gone or the url cannot be used.
pub async fn ws_remote_source(url: &str, tx: mpsc::Sender<Event>) {
    let mut delay = RECONNECT_FIRST_DELAY;

    while !tx.is_closed() {
        match connect_async(url).await {
            Ok((ws_stream, _)) => {
                delay = RECONNECT_FIRST_DELAY;
                if !forward_events(ws_stream, &tx).await {
                    return;
                }
                eprintln!("websocket feed {url} closed, reconnecting");
            }
            // a url tungstenite cannot use will not start working
            Err(TungsteniteError::Url(e)) => {
                eprintln!("websocket feed {url} is unusable: {e}");
                return;
            }
            Err(e) => eprintln!("websocket feed {url} unreachable: {e}"),
        }

        tokio::time::sleep(delay).await;
        delay = (delay * RECONNECT_BACKOFF_FACTOR).min(RECONNECT_MAX_DELAY);
    }
}

/// Forward the JSON events a socket publishes until it closes. false once the receiver
/// is gone, which is the one reason not to read another connection.
async fn forward_events<S>(ws_stream: WebSocketStream<S>, tx: &mpsc::Sender<Event>) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let (_, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(event) = serde_json::from_str::<Event>(&text)
                    && tx.send(event).await.is_err()
                {
                    return false;
                }
            }
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {}
        }
    }

    true
}

/// WebSocket sink — connects to clients and sends output events.
pub struct WsSink {
    _tx: mpsc::Sender<OutputEvent>,
}

impl WsSink {
    /// Start a WebSocket server that broadcasts output events to connected clients.
    pub async fn start(
        bind: &str,
        mut rx: mpsc::Receiver<OutputEvent>,
    ) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(bind).await?;
        let (broadcast_tx, _) = tokio::sync::broadcast::channel::<String>(1000);
        let broadcast_tx_clone = broadcast_tx.clone();

        // Spawn broadcaster
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Ok(json) = serde_json::to_string(&event) {
                    let _ = broadcast_tx_clone.send(json);
                }
            }
        });

        // Accept connections
        while let Ok((stream, _)) = listener.accept().await {
            let mut broadcast_rx = broadcast_tx.subscribe();
            tokio::spawn(async move {
                let ws_stream = match accept_async(stream).await {
                    Ok(ws) => ws,
                    Err(_) => return,
                };

                let (mut write, _): (SplitSink<_, Message>, _) = ws_stream.split();

                while let Ok(msg) = broadcast_rx.recv().await {
                    if write.send(Message::Text(msg.into())).await.is_err() {
                        break;
                    }
                }
            });
        }

        Ok(())
    }
}
