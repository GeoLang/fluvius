//! Complex Event Processing — detect temporal/spatial patterns in event streams.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::event::{Event, OutputEvent};
use crate::operator::StatefulOperator;

/// A condition that an event must satisfy to match a pattern step.
pub type EventCondition = Box<dyn Fn(&Event) -> bool + Send + Sync>;

/// A single step in a CEP pattern.
pub struct PatternStep {
    pub name: String,
    pub condition: EventCondition,
    /// Optional spatial constraint: (lon, lat, radius_deg).
    pub near: Option<(f64, f64, f64)>,
}

/// A complete CEP pattern: a sequence of steps that must occur within a time window.
pub struct Pattern {
    pub name: String,
    pub steps: Vec<PatternStep>,
    /// Maximum time window for the entire pattern to complete.
    pub within: Duration,
}

/// Tracks in-progress pattern matches per entity.
#[derive(serde::Serialize, serde::Deserialize)]
struct MatchState {
    /// Index of next step to match.
    current_step: usize,
    /// Timestamp when first step matched.
    started_at: DateTime<Utc>,
    /// Matched events so far.
    matched_events: Vec<Event>,
}

/// CEP engine that evaluates patterns against incoming events.
pub struct CepEngine {
    patterns: Vec<Pattern>,
    /// entity_id -> pattern_index -> match state
    state: HashMap<String, HashMap<usize, MatchState>>,
}

impl CepEngine {
    pub fn new() -> Self {
        Self {
            patterns: Vec::new(),
            state: HashMap::new(),
        }
    }

    /// Register a pattern.
    pub fn add_pattern(&mut self, pattern: Pattern) {
        self.patterns.push(pattern);
    }

    /// Process an event, returning any completed pattern matches.
    pub fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();

        for (pattern_idx, pattern) in self.patterns.iter().enumerate() {
            let entity_state = self.state.entry(event.entity_id.clone()).or_default();

            // Check if this event matches the current step of an in-progress match
            let match_state = entity_state.entry(pattern_idx).or_insert(MatchState {
                current_step: 0,
                started_at: event.timestamp,
                matched_events: Vec::new(),
            });

            // Check timeout — reset if expired
            let elapsed = event
                .timestamp
                .signed_duration_since(match_state.started_at);
            if elapsed.num_milliseconds() > pattern.within.as_millis() as i64
                && match_state.current_step > 0
            {
                // Reset
                match_state.current_step = 0;
                match_state.matched_events.clear();
                match_state.started_at = event.timestamp;
            }

            let step_idx = match_state.current_step;
            if step_idx >= pattern.steps.len() {
                continue;
            }

            let step = &pattern.steps[step_idx];

            // Check condition
            if !(step.condition)(event) {
                continue;
            }

            // Check spatial constraint
            if let Some((lon, lat, radius)) = step.near {
                let dist = ((event.lon - lon).powi(2) + (event.lat - lat).powi(2)).sqrt();
                if dist > radius {
                    continue;
                }
            }

            // Step matched
            if match_state.current_step == 0 {
                match_state.started_at = event.timestamp;
            }
            match_state.matched_events.push(event.clone());
            match_state.current_step += 1;

            // Check if pattern complete
            if match_state.current_step >= pattern.steps.len() {
                let matched = match_state.matched_events.clone();
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: format!("cep:{}", pattern.name),
                    payload: serde_json::json!({
                        "pattern": pattern.name,
                        "entity_id": event.entity_id,
                        "steps_matched": matched.len(),
                        "duration_ms": event.timestamp
                            .signed_duration_since(matched[0].timestamp)
                            .num_milliseconds(),
                    }),
                });

                // Reset state for this pattern
                match_state.current_step = 0;
                match_state.matched_events.clear();
            }
        }

        outputs
    }
}

impl Default for CepEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StatefulOperator for CepEngine {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        // inherent method, the trait one is what we are defining here
        CepEngine::process(self, event)
    }

    /// Drops partial matches: a pattern that spans a window boundary would report
    /// a bogus duration.
    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        self.state.clear();
        vec![]
    }

    fn name(&self) -> &str {
        "cep"
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.state)
    }

    fn restore(&mut self, state: serde_json::Value) {
        if let Some(partial_matches) = crate::operator::restored("cep", state) {
            self.state = partial_matches;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;
    use chrono::TimeDelta;

    #[test]
    fn test_simple_sequence_pattern() {
        let mut engine = CepEngine::new();

        engine.add_pattern(Pattern {
            name: "stop_then_move".to_string(),
            steps: vec![
                PatternStep {
                    name: "stop".to_string(),
                    condition: Box::new(|e| e.speed.unwrap_or(0.0) < 0.5),
                    near: None,
                },
                PatternStep {
                    name: "move".to_string(),
                    condition: Box::new(|e| e.speed.unwrap_or(0.0) > 5.0),
                    near: None,
                },
            ],
            within: Duration::from_secs(60),
        });

        let now = Utc::now();

        // Stopped event
        let mut e1 = Event::new("v1", 0.0, 0.0, now);
        e1.speed = Some(0.0);
        let results = engine.process(&e1);
        assert!(results.is_empty());

        // Moving event
        let mut e2 = Event::new("v1", 0.1, 0.0, now + TimeDelta::seconds(10));
        e2.speed = Some(10.0);
        let results = engine.process(&e2);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].operator, "cep:stop_then_move");
    }

    #[test]
    fn test_pattern_timeout() {
        let mut engine = CepEngine::new();

        engine.add_pattern(Pattern {
            name: "quick_ab".to_string(),
            steps: vec![
                PatternStep {
                    name: "a".to_string(),
                    condition: Box::new(|e| e.properties.contains_key("type_a")),
                    near: None,
                },
                PatternStep {
                    name: "b".to_string(),
                    condition: Box::new(|e| e.properties.contains_key("type_b")),
                    near: None,
                },
            ],
            within: Duration::from_secs(5),
        });

        let now = Utc::now();

        let mut e1 = Event::now("v1", 0.0, 0.0);
        e1.timestamp = now;
        e1.properties
            .insert("type_a".to_string(), serde_json::json!(true));
        engine.process(&e1);

        // Too late — 10 seconds later
        let mut e2 = Event::now("v1", 0.0, 0.0);
        e2.timestamp = now + TimeDelta::seconds(10);
        e2.properties
            .insert("type_b".to_string(), serde_json::json!(true));
        let results = engine.process(&e2);
        assert!(results.is_empty()); // Pattern timed out and reset
    }

    #[test]
    fn test_spatial_constraint() {
        let mut engine = CepEngine::new();

        engine.add_pattern(Pattern {
            name: "near_warehouse".to_string(),
            steps: vec![PatternStep {
                name: "arrive".to_string(),
                condition: Box::new(|_| true),
                near: Some((10.0, 20.0, 0.5)),
            }],
            within: Duration::from_secs(60),
        });

        // Too far
        let e1 = Event::now("v1", 50.0, 50.0);
        assert!(engine.process(&e1).is_empty());

        // Close enough
        let e2 = Event::now("v1", 10.1, 20.1);
        assert_eq!(engine.process(&e2).len(), 1);
    }

    fn stop_then_move() -> Pattern {
        Pattern {
            name: "stop_then_move".to_string(),
            steps: vec![
                PatternStep {
                    name: "stop".to_string(),
                    condition: Box::new(|e: &Event| e.speed.unwrap_or(0.0) < 1.0),
                    near: None,
                },
                PatternStep {
                    name: "move".to_string(),
                    condition: Box::new(|e: &Event| e.speed.unwrap_or(0.0) > 5.0),
                    near: None,
                },
            ],
            within: Duration::from_secs(60),
        }
    }

    /// A restored engine holds the half-finished match, so the step that completes the
    /// pattern still completes it.
    #[test]
    fn test_cep_snapshot_carries_partial_matches() {
        let mut engine = CepEngine::new();
        engine.add_pattern(stop_then_move());
        assert!(
            engine
                .process(&Event::now("v1", 0.0, 0.0).with_speed(0.0))
                .is_empty()
        );

        let mut resumed = CepEngine::new();
        resumed.add_pattern(stop_then_move());
        resumed.restore(StatefulOperator::snapshot(&engine));

        let matches = resumed.process(&Event::now("v1", 0.0, 0.0).with_speed(10.0));
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].payload["pattern"], "stop_then_move");
    }
}
