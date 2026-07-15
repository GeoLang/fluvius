//! MQTT connector, publish/subscribe events via MQTT (rumqttc).

use std::time::Duration;

use fluvius_core::event::{Event, OutputEvent};
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
use tokio::sync::mpsc;

/// MQTT connection configuration.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: MqttQos,
    pub keep_alive: Duration,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// MQTT Quality of Service levels.
#[derive(Debug, Clone, Copy)]
pub enum MqttQos {
    /// At most once (fire and forget).
    AtMostOnce,
    /// At least once (acknowledged delivery).
    AtLeastOnce,
    /// Exactly once (assured delivery).
    ExactlyOnce,
}

impl From<MqttQos> for QoS {
    fn from(q: MqttQos) -> Self {
        match q {
            MqttQos::AtMostOnce => QoS::AtMostOnce,
            MqttQos::AtLeastOnce => QoS::AtLeastOnce,
            MqttQos::ExactlyOnce => QoS::ExactlyOnce,
        }
    }
}

impl MqttConfig {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            client_id: "fluvius-mqtt".to_string(),
            qos: MqttQos::AtLeastOnce,
            keep_alive: Duration::from_secs(30),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_qos(mut self, qos: MqttQos) -> Self {
        self.qos = qos;
        self
    }

    /// Reject configs that cannot connect.
    pub fn validate(&self) -> Result<(), MqttError> {
        self.host_port()?;
        if self.topic.trim().is_empty() {
            return Err(MqttError::Config("topic must not be empty".into()));
        }
        Ok(())
    }

    /// Parse `mqtt://host:port`, `tcp://host:port`, or `host:port`.
    fn host_port(&self) -> Result<(String, u16), MqttError> {
        let raw = self
            .broker_url
            .strip_prefix("mqtt://")
            .or_else(|| self.broker_url.strip_prefix("tcp://"))
            .unwrap_or(&self.broker_url);
        let (host, port) = raw
            .split_once(':')
            .ok_or_else(|| MqttError::Config(format!("invalid broker url: {}", self.broker_url)))?;
        if host.is_empty() {
            return Err(MqttError::Config(format!(
                "invalid broker url: {}",
                self.broker_url
            )));
        }
        let port: u16 = port.parse().map_err(|_| {
            MqttError::Config(format!("invalid port in broker url: {}", self.broker_url))
        })?;
        Ok((host.to_string(), port))
    }

    fn options(&self) -> Result<MqttOptions, MqttError> {
        let (host, port) = self.host_port()?;
        let mut opts = MqttOptions::new(&self.client_id, host, port);
        opts.set_keep_alive(self.keep_alive);
        if let (Some(user), Some(pass)) = (&self.username, &self.password) {
            opts.set_credentials(user, pass);
        }
        Ok(opts)
    }
}

/// Serialize an output event to a JSON payload.
fn encode_output(event: &OutputEvent) -> Result<Vec<u8>, MqttError> {
    serde_json::to_vec(event).map_err(|e| MqttError::Serialization(e.to_string()))
}

/// Deserialize a JSON payload into an input event.
fn decode_event(bytes: &[u8]) -> Result<Event, MqttError> {
    serde_json::from_slice(bytes).map_err(|e| MqttError::Serialization(e.to_string()))
}

/// MQTT subscriber source, reads events from an MQTT topic.
pub struct MqttSource {
    config: MqttConfig,
}

impl MqttSource {
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }

    /// Subscribe and forward decoded events until the receiver is dropped.
    pub async fn start(&self, sender: mpsc::Sender<Event>) -> Result<(), MqttError> {
        self.config.validate()?;
        let (client, mut eventloop) = AsyncClient::new(self.config.options()?, 100);
        client
            .subscribe(&self.config.topic, self.config.qos.into())
            .await
            .map_err(|e| MqttError::Connection(e.to_string()))?;

        loop {
            match eventloop.poll().await {
                Ok(MqttEvent::Incoming(Packet::Publish(publish))) => {
                    match decode_event(&publish.payload) {
                        Ok(event) => {
                            if sender.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => eprintln!("Warning: skipping malformed mqtt message: {e}"),
                    }
                }
                Ok(_) => {}
                Err(e) => return Err(MqttError::Connection(e.to_string())),
            }
        }
        Ok(())
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

/// MQTT publisher sink, publishes output events to an MQTT topic.
pub struct MqttSink {
    config: MqttConfig,
    client: AsyncClient,
}

impl MqttSink {
    /// Connect and start driving the event loop so publishes flush.
    pub fn new(config: MqttConfig) -> Result<Self, MqttError> {
        config.validate()?;
        let (client, mut eventloop) = AsyncClient::new(config.options()?, 100);
        // the eventloop must be polled for publishes to reach the broker
        tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
        Ok(Self { config, client })
    }

    /// Publish an output event.
    pub async fn send(&self, event: &OutputEvent) -> Result<(), MqttError> {
        let payload = encode_output(event)?;
        self.client
            .publish(&self.config.topic, self.config.qos.into(), false, payload)
            .await
            .map_err(|e| MqttError::Publish(e.to_string()))?;
        Ok(())
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

/// MQTT-related errors.
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("MQTT config error: {0}")]
    Config(String),
    #[error("MQTT connection error: {0}")]
    Connection(String),
    #[error("MQTT publish error: {0}")]
    Publish(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_config() {
        let config = MqttConfig::new("mqtt://localhost:1883", "sensors/gps")
            .with_credentials("user", "pass")
            .with_qos(MqttQos::ExactlyOnce);
        assert_eq!(config.topic, "sensors/gps");
        assert!(config.username.is_some());
    }

    #[test]
    fn test_mqtt_config_validate() {
        assert!(
            MqttConfig::new("mqtt://localhost:1883", "t")
                .validate()
                .is_ok()
        );
        assert!(
            MqttConfig::new("tcp://localhost:1883", "t")
                .validate()
                .is_ok()
        );
        assert!(MqttConfig::new("localhost:1883", "t").validate().is_ok());
        assert!(MqttConfig::new("localhost", "t").validate().is_err());
        assert!(
            MqttConfig::new("mqtt://localhost:notaport", "t")
                .validate()
                .is_err()
        );
        assert!(
            MqttConfig::new("mqtt://localhost:1883", "")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn test_host_port_parse() {
        let (host, port) = MqttConfig::new("mqtt://broker.local:1884", "t")
            .host_port()
            .unwrap();
        assert_eq!(host, "broker.local");
        assert_eq!(port, 1884);
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let event = Event::now("drone-1", 5.0, 6.0).with_heading(90.0);
        let output = OutputEvent {
            source_event: event.clone(),
            operator: "proximity".into(),
            payload: serde_json::json!({"count": 3}),
        };
        let bytes = encode_output(&output).unwrap();
        let decoded: OutputEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.operator, "proximity");

        let event_bytes = serde_json::to_vec(&event).unwrap();
        let decoded_event = decode_event(&event_bytes).unwrap();
        assert_eq!(decoded_event.entity_id, "drone-1");
        assert_eq!(decoded_event.heading, Some(90.0));
    }

    #[test]
    fn test_decode_rejects_garbage() {
        assert!(decode_event(b"{bad").is_err());
    }

    /// Round-trip through a real mosquitto broker. Needs docker; run with:
    /// `cargo test -p fluvius-connectors --features mqtt -- --ignored mqtt_broker`
    #[tokio::test]
    #[ignore]
    async fn mqtt_broker_roundtrip() {
        use std::process::Command;

        let name = "fluvius-mqtt-test";
        let _ = Command::new("docker").args(["rm", "-f", name]).output();
        let run = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                name,
                "-p",
                "18831:1883",
                "eclipse-mosquitto:2",
                "mosquitto",
                "-c",
                "/mosquitto-no-auth.conf",
            ])
            .output()
            .expect("docker run");
        assert!(run.status.success(), "docker run failed: {run:?}");
        tokio::time::sleep(Duration::from_secs(2)).await;

        let config = MqttConfig::new("mqtt://127.0.0.1:18831", "fluvius/events");
        let source = MqttSource::new(config.clone());
        let (tx, mut rx) = mpsc::channel(16);
        let consume = tokio::spawn(async move { source.start(tx).await });

        // let the subscriber connect before publishing
        tokio::time::sleep(Duration::from_secs(1)).await;

        // publish an Event-shaped payload via a separate client
        let mut pub_cfg = MqttConfig::new("mqtt://127.0.0.1:18831", "fluvius/events");
        pub_cfg.client_id = "fluvius-mqtt-pub".into();
        let (pub_client, mut pub_loop) = AsyncClient::new(pub_cfg.options().unwrap(), 10);
        tokio::spawn(async move { while pub_loop.poll().await.is_ok() {} });
        let event = Event::now("boat-3", 10.0, 20.0);
        pub_client
            .publish(
                "fluvius/events",
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&event).unwrap(),
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(10), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");
        assert_eq!(received.entity_id, "boat-3");

        consume.abort();
        let _ = Command::new("docker").args(["rm", "-f", name]).output();
    }
}
