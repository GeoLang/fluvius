//! Stream events — the fundamental data unit.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A timestamped event flowing through the stream processor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    /// Unique event identifier.
    pub id: String,
    /// Event timestamp (event time, not processing time).
    pub timestamp: DateTime<Utc>,
    /// Entity this event belongs to (e.g., vehicle ID).
    pub entity_id: String,
    /// Longitude.
    pub lon: f64,
    /// Latitude.
    pub lat: f64,
    /// Optional altitude in meters.
    pub altitude: Option<f64>,
    /// Optional heading in degrees.
    pub heading: Option<f64>,
    /// Optional speed in m/s.
    pub speed: Option<f64>,
    /// Additional properties.
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,
}

impl Event {
    /// Create a new event with minimal fields.
    pub fn new(entity_id: impl Into<String>, lon: f64, lat: f64, timestamp: DateTime<Utc>) -> Self {
        Self {
            id: uuid_v4(),
            timestamp,
            entity_id: entity_id.into(),
            lon,
            lat,
            altitude: None,
            heading: None,
            speed: None,
            properties: HashMap::new(),
        }
    }

    /// Create an event with the current time.
    pub fn now(entity_id: impl Into<String>, lon: f64, lat: f64) -> Self {
        Self::new(entity_id, lon, lat, Utc::now())
    }

    /// Add a property to the event.
    pub fn with_property(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.properties.insert(key.into(), value);
        self
    }

    /// Set speed.
    pub fn with_speed(mut self, speed: f64) -> Self {
        self.speed = Some(speed);
        self
    }

    /// Set heading.
    pub fn with_heading(mut self, heading: f64) -> Self {
        self.heading = Some(heading);
        self
    }
}

/// Simple UUID v4 generation without pulling in the uuid crate.
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let rand_part: u64 = nanos as u64 ^ 0xDEAD_BEEF_CAFE_BABE;
    format!("{:016x}-{:08x}", rand_part, nanos)
}

/// Output event emitted by operators.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputEvent {
    /// The triggering event(s).
    pub source_event: Event,
    /// What operator produced this output.
    pub operator: String,
    /// Output payload.
    pub payload: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let event = Event::now("vehicle-1", -73.9857, 40.7484);
        assert_eq!(event.entity_id, "vehicle-1");
        assert!((event.lon - (-73.9857)).abs() < 1e-10);
        assert!((event.lat - 40.7484).abs() < 1e-10);
        assert!(!event.id.is_empty());
    }

    #[test]
    fn test_event_with_properties() {
        let event = Event::now("drone-1", 0.0, 0.0)
            .with_speed(15.0)
            .with_heading(90.0)
            .with_property("battery", serde_json::json!(85));
        assert_eq!(event.speed, Some(15.0));
        assert_eq!(event.heading, Some(90.0));
        assert_eq!(event.properties["battery"], 85);
    }

    #[test]
    fn test_event_serialization() {
        let event = Event::now("test", 1.0, 2.0);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.entity_id, "test");
        assert!((deserialized.lon - 1.0).abs() < 1e-10);
    }

    #[test]
    fn test_event_without_properties_deserializes() {
        let json = r#"{"id":"e1","timestamp":"2026-09-23T00:00:00Z","entity_id":"v1","lon":1.0,"lat":2.0,"altitude":null,"heading":null,"speed":null}"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.entity_id, "v1");
        assert!(event.properties.is_empty());
    }
}
