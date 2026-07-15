//! Kafka connector, produce/consume events via Apache Kafka (rdkafka).

use std::time::Duration;

use fluvius_core::event::{Event, OutputEvent};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::sync::mpsc;

/// Kafka connection configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub group_id: Option<String>,
    pub client_id: String,
    /// Consumer poll interval.
    pub poll_interval: Duration,
}

impl KafkaConfig {
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers,
            topic: topic.into(),
            group_id: None,
            client_id: "fluvius".to_string(),
            poll_interval: Duration::from_millis(100),
        }
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }

    /// Reject configs that cannot connect.
    pub fn validate(&self) -> Result<(), KafkaError> {
        if self.brokers.is_empty() || self.brokers.iter().any(|b| b.trim().is_empty()) {
            return Err(KafkaError::Config("at least one broker is required".into()));
        }
        if self.topic.trim().is_empty() {
            return Err(KafkaError::Config("topic must not be empty".into()));
        }
        Ok(())
    }

    fn bootstrap_servers(&self) -> String {
        self.brokers.join(",")
    }
}

/// Serialize an output event to a JSON payload.
fn encode_output(event: &OutputEvent) -> Result<Vec<u8>, KafkaError> {
    serde_json::to_vec(event).map_err(|e| KafkaError::Serialization(e.to_string()))
}

/// Deserialize a JSON payload into an input event.
fn decode_event(bytes: &[u8]) -> Result<Event, KafkaError> {
    serde_json::from_slice(bytes).map_err(|e| KafkaError::Serialization(e.to_string()))
}

/// Kafka consumer source, reads events from a Kafka topic.
pub struct KafkaSource {
    config: KafkaConfig,
}

impl KafkaSource {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    /// Consume events into the given channel until the receiver is dropped.
    pub async fn start(&self, sender: mpsc::Sender<Event>) -> Result<(), KafkaError> {
        self.config.validate()?;
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", self.config.bootstrap_servers())
            .set(
                "group.id",
                self.config
                    .group_id
                    .clone()
                    .unwrap_or_else(|| "fluvius".to_string()),
            )
            .set("client.id", &self.config.client_id)
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .create()
            .map_err(|e| KafkaError::Connection(e.to_string()))?;

        consumer
            .subscribe(&[&self.config.topic])
            .map_err(|e| KafkaError::Connection(e.to_string()))?;

        loop {
            match consumer.recv().await {
                Ok(msg) => {
                    let Some(payload) = msg.payload() else {
                        continue;
                    };
                    match decode_event(payload) {
                        Ok(event) => {
                            if sender.send(event).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => eprintln!("Warning: skipping malformed kafka message: {e}"),
                    }
                }
                Err(e) => return Err(KafkaError::Connection(e.to_string())),
            }
        }
        Ok(())
    }

    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

/// Kafka producer sink, writes output events to a Kafka topic.
pub struct KafkaSink {
    config: KafkaConfig,
    producer: FutureProducer,
}

impl KafkaSink {
    /// Build a producer connected to the configured brokers.
    pub fn new(config: KafkaConfig) -> Result<Self, KafkaError> {
        config.validate()?;
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", config.bootstrap_servers())
            .set("client.id", &config.client_id)
            .create()
            .map_err(|e| KafkaError::Connection(e.to_string()))?;
        Ok(Self { config, producer })
    }

    /// Publish an output event, keyed by source entity id.
    pub async fn send(&self, event: &OutputEvent) -> Result<(), KafkaError> {
        let payload = encode_output(event)?;
        let key = event.source_event.entity_id.clone();
        let record = FutureRecord::to(&self.config.topic)
            .payload(&payload)
            .key(&key);
        self.producer
            .send(record, Timeout::After(Duration::from_secs(5)))
            .await
            .map_err(|(e, _)| KafkaError::Connection(e.to_string()))?;
        Ok(())
    }

    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

/// Kafka-related errors.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Kafka config error: {0}")]
    Config(String),
    #[error("Kafka connection error: {0}")]
    Connection(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config() {
        let config = KafkaConfig::new(vec!["localhost:9092".to_string()], "events")
            .with_group("fluvius-group");
        assert_eq!(config.topic, "events");
        assert_eq!(config.group_id, Some("fluvius-group".to_string()));
    }

    #[test]
    fn test_kafka_config_validate() {
        assert!(
            KafkaConfig::new(vec!["localhost:9092".into()], "events")
                .validate()
                .is_ok()
        );
        assert!(KafkaConfig::new(vec![], "events").validate().is_err());
        assert!(
            KafkaConfig::new(vec!["localhost:9092".into()], "")
                .validate()
                .is_err()
        );
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let event = Event::now("v1", 1.0, 2.0).with_speed(9.5);
        let output = OutputEvent {
            source_event: event.clone(),
            operator: "geofence".into(),
            payload: serde_json::json!({"zone": "depot"}),
        };
        let bytes = encode_output(&output).unwrap();
        let decoded: OutputEvent = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.operator, "geofence");
        assert_eq!(decoded.source_event.entity_id, "v1");

        let event_bytes = serde_json::to_vec(&event).unwrap();
        let decoded_event = decode_event(&event_bytes).unwrap();
        assert_eq!(decoded_event.entity_id, "v1");
        assert_eq!(decoded_event.speed, Some(9.5));
    }

    #[test]
    fn test_decode_rejects_garbage() {
        assert!(decode_event(b"not json").is_err());
    }

    /// Round-trip through a real broker. Needs docker; run with:
    /// `cargo test -p fluvius-connectors --features kafka -- --ignored kafka_broker`
    #[tokio::test]
    #[ignore]
    async fn kafka_broker_roundtrip() {
        use std::process::Command;

        let name = "fluvius-kafka-test";
        let _ = Command::new("docker").args(["rm", "-f", name]).output();
        let run = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-d",
                "--name",
                name,
                "-p",
                "19092:19092",
                "docker.redpanda.com/redpandadata/redpanda:latest",
                "redpanda",
                "start",
                "--overprovisioned",
                "--smp",
                "1",
                "--memory",
                "512M",
                "--reserve-memory",
                "0M",
                "--node-id",
                "0",
                "--check=false",
                "--kafka-addr",
                "PLAINTEXT://0.0.0.0:19092",
                "--advertise-kafka-addr",
                "PLAINTEXT://127.0.0.1:19092",
            ])
            .output()
            .expect("docker run");
        assert!(run.status.success(), "docker run failed: {run:?}");

        // let the broker come up
        tokio::time::sleep(Duration::from_secs(8)).await;

        // create the topic up front so the consumer does not hit UnknownTopicOrPartition
        let created = Command::new("docker")
            .args(["exec", name, "rpk", "topic", "create", "fluvius-events"])
            .output()
            .expect("rpk topic create");
        assert!(created.status.success(), "topic create failed: {created:?}");

        let config = KafkaConfig::new(vec!["127.0.0.1:19092".into()], "fluvius-events")
            .with_group("fluvius-test");
        let sink = KafkaSink::new(config.clone()).expect("sink");
        let source = KafkaSource::new(config);

        let (tx, mut rx) = mpsc::channel(16);
        let consume = tokio::spawn(async move { source.start(tx).await });

        // give the consumer time to join and subscribe
        tokio::time::sleep(Duration::from_secs(3)).await;

        let output = OutputEvent {
            source_event: Event::now("truck-7", -1.0, 2.0),
            operator: "geofence".into(),
            payload: serde_json::json!({"zone": "depot"}),
        };
        // publish an Event-shaped payload the source can decode
        let event_producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:19092")
            .create()
            .unwrap();
        let event_bytes = serde_json::to_vec(&output.source_event).unwrap();
        event_producer
            .send(
                FutureRecord::<(), _>::to("fluvius-events").payload(&event_bytes),
                Timeout::After(Duration::from_secs(5)),
            )
            .await
            .unwrap();

        let received = tokio::time::timeout(Duration::from_secs(15), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed");
        assert_eq!(received.entity_id, "truck-7");

        // sink also publishes cleanly
        sink.send(&output).await.expect("sink send");

        consume.abort();
        let _ = Command::new("docker").args(["rm", "-f", name]).output();
    }
}
