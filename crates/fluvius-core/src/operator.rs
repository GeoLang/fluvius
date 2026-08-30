//! Stream operators — the processing units in a pipeline.

use crate::event::{Event, OutputEvent};

/// Trait for stateless stream operators that process one event at a time.
pub trait MapOperator: Send + Sync {
    /// Process a single event, optionally producing an output.
    fn process(&self, event: &Event) -> Option<OutputEvent>;

    /// Operator name for logging/metrics.
    fn name(&self) -> &str;
}

/// Trait for stateful operators that may buffer events.
pub trait StatefulOperator: Send + Sync {
    /// Process an event, potentially updating internal state.
    /// May produce zero or more outputs.
    fn process(&mut self, event: &Event) -> Vec<OutputEvent>;

    /// Called when a window expires — flush any buffered state.
    fn on_window_close(&mut self) -> Vec<OutputEvent>;

    /// Operator name.
    fn name(&self) -> &str;

    /// The state a checkpoint carries. Configuration stays out of it, a restored
    /// operator is built from the topology and only its accumulated state comes back.
    fn snapshot(&self) -> serde_json::Value;

    /// Replace the accumulated state with one taken by `snapshot`.
    fn restore(&mut self, state: serde_json::Value);
}

/// Read a checkpointed state, keeping what the operator already has when the value
/// does not fit. A checkpoint written by a different topology is the usual reason.
pub fn restored<T: serde::de::DeserializeOwned>(
    operator: &str,
    state: serde_json::Value,
) -> Option<T> {
    match serde_json::from_value(state) {
        Ok(value) => Some(value),
        Err(e) => {
            eprintln!("{operator}: ignoring a checkpoint it cannot read: {e}");
            None
        }
    }
}

/// A filter operator that passes through events matching a predicate.
pub struct FilterOperator {
    name: String,
    predicate: Box<dyn Fn(&Event) -> bool + Send + Sync>,
}

impl FilterOperator {
    pub fn new(
        name: impl Into<String>,
        predicate: impl Fn(&Event) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            predicate: Box::new(predicate),
        }
    }
}

impl MapOperator for FilterOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        if (self.predicate)(event) {
            Some(OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({"action": "pass"}),
            })
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform operator that modifies events.
pub struct TransformOperator {
    name: String,
    transform: Box<dyn Fn(&Event) -> Event + Send + Sync>,
}

impl TransformOperator {
    pub fn new(
        name: impl Into<String>,
        transform: impl Fn(&Event) -> Event + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            transform: Box::new(transform),
        }
    }
}

impl MapOperator for TransformOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        let transformed = (self.transform)(event);
        Some(OutputEvent {
            source_event: transformed,
            operator: self.name.clone(),
            payload: serde_json::json!({"action": "transform"}),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Rate limiter — emits at most N events per entity per window.
pub struct RateLimiter {
    name: String,
    max_per_entity: usize,
    counts: std::collections::HashMap<String, usize>,
}

impl RateLimiter {
    pub fn new(name: impl Into<String>, max_per_entity: usize) -> Self {
        Self {
            name: name.into(),
            max_per_entity,
            counts: std::collections::HashMap::new(),
        }
    }
}

impl StatefulOperator for RateLimiter {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let count = self.counts.entry(event.entity_id.clone()).or_insert(0);
        if *count < self.max_per_entity {
            *count += 1;
            vec![OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({"action": "pass", "count": *count}),
            }]
        } else {
            vec![]
        }
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        self.counts.clear();
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.counts)
    }

    fn restore(&mut self, state: serde_json::Value) {
        if let Some(counts) = restored(&self.name, state) {
            self.counts = counts;
        }
    }
}

/// Token bucket throttle: passes at most `max_per_second` events across the whole
/// stream, bursting up to one second's worth. It is a `MapOperator` so the events it
/// rejects are dropped and never reach the rest of the chain, unlike `RateLimiter`,
/// which caps a per-entity count and only withholds its own output.
pub struct RateLimitOperator {
    name: String,
    max_per_second: f64,
    bucket: std::sync::Mutex<Bucket>,
}

struct Bucket {
    tokens: f64,
    last_refill: std::time::Instant,
}

impl RateLimitOperator {
    /// `max_per_second` must be finite and greater than zero.
    pub fn new(name: impl Into<String>, max_per_second: f64) -> Self {
        Self {
            name: name.into(),
            max_per_second,
            bucket: std::sync::Mutex::new(Bucket {
                tokens: burst_capacity(max_per_second),
                last_refill: std::time::Instant::now(),
            }),
        }
    }
}

/// A rate below one per second still gets a single token to spend, otherwise it
/// could never pass anything.
fn burst_capacity(max_per_second: f64) -> f64 {
    max_per_second.max(1.0)
}

impl MapOperator for RateLimitOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        let mut bucket = self.bucket.lock().expect("rate limit bucket poisoned");
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.last_refill = now;
        bucket.tokens = (bucket.tokens + elapsed * self.max_per_second)
            .min(burst_capacity(self.max_per_second));

        if bucket.tokens < 1.0 {
            return None;
        }
        bucket.tokens -= 1.0;
        Some(OutputEvent {
            source_event: event.clone(),
            operator: self.name.clone(),
            payload: serde_json::json!({"action": "pass"}),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_operator() {
        let filter = FilterOperator::new("speed_filter", |e: &Event| e.speed.unwrap_or(0.0) > 10.0);

        let fast = Event::now("v1", 0.0, 0.0).with_speed(20.0);
        let slow = Event::now("v2", 0.0, 0.0).with_speed(5.0);

        assert!(filter.process(&fast).is_some());
        assert!(filter.process(&slow).is_none());
    }

    #[test]
    fn test_transform_operator() {
        let transform = TransformOperator::new("double_speed", |e: &Event| {
            let mut cloned = e.clone();
            cloned.speed = cloned.speed.map(|s| s * 2.0);
            cloned
        });

        let event = Event::now("v1", 0.0, 0.0).with_speed(10.0);
        let output = transform.process(&event).unwrap();
        assert_eq!(output.source_event.speed, Some(20.0));
    }

    #[test]
    fn test_rate_limiter() {
        let mut limiter = RateLimiter::new("limiter", 2);

        let e1 = Event::now("v1", 0.0, 0.0);
        let e2 = Event::now("v1", 1.0, 1.0);
        let e3 = Event::now("v1", 2.0, 2.0);
        let e4 = Event::now("v2", 3.0, 3.0);

        assert_eq!(limiter.process(&e1).len(), 1);
        assert_eq!(limiter.process(&e2).len(), 1);
        assert_eq!(limiter.process(&e3).len(), 0); // Rate limited
        assert_eq!(limiter.process(&e4).len(), 1); // Different entity

        // After window close, counts reset
        limiter.on_window_close();
        assert_eq!(limiter.process(&e1).len(), 1); // v1 can send again
    }

    #[test]
    fn test_rate_limiter_snapshot_carries_the_counts() {
        let mut limiter = RateLimiter::new("limiter", 1);
        assert_eq!(limiter.process(&Event::now("v1", 0.0, 0.0)).len(), 1);

        let mut resumed = RateLimiter::new("limiter", 1);
        resumed.restore(limiter.snapshot());
        assert_eq!(
            resumed.process(&Event::now("v1", 0.0, 0.0)).len(),
            0,
            "v1 had already spent its allowance"
        );
        assert_eq!(resumed.process(&Event::now("v2", 0.0, 0.0)).len(), 1);
    }

    #[test]
    fn test_restore_keeps_the_current_state_when_the_value_does_not_fit() {
        let mut limiter = RateLimiter::new("limiter", 1);
        limiter.process(&Event::now("v1", 0.0, 0.0));

        limiter.restore(serde_json::json!("not a count map"));
        assert_eq!(
            limiter.process(&Event::now("v1", 0.0, 0.0)).len(),
            0,
            "the count survived the unusable checkpoint"
        );
    }

    /// The burst is spent across entities, not per entity, and the events it rejects
    /// produce no output at all.
    #[test]
    fn test_rate_limit_operator_spends_its_burst() {
        let limiter = RateLimitOperator::new("throttle", 2.0);

        assert!(limiter.process(&Event::now("v1", 0.0, 0.0)).is_some());
        assert!(limiter.process(&Event::now("v2", 0.0, 0.0)).is_some());
        assert!(limiter.process(&Event::now("v3", 0.0, 0.0)).is_none());
    }

    /// Below one per second the bucket still holds a single token, so the operator
    /// is a throttle rather than a mute.
    #[test]
    fn test_rate_limit_operator_allows_one_below_a_rate_of_one() {
        let limiter = RateLimitOperator::new("throttle", 0.1);
        assert!(limiter.process(&Event::now("v1", 0.0, 0.0)).is_some());
        assert!(limiter.process(&Event::now("v1", 0.0, 0.0)).is_none());
    }

    #[test]
    fn test_rate_limit_operator_refills_over_time() {
        let limiter = RateLimitOperator::new("throttle", 100.0);
        for _ in 0..100 {
            limiter.process(&Event::now("v1", 0.0, 0.0));
        }
        assert!(limiter.process(&Event::now("v1", 0.0, 0.0)).is_none());

        // 50ms at 100/s is worth 5 tokens
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(limiter.process(&Event::now("v1", 0.0, 0.0)).is_some());
    }
}
