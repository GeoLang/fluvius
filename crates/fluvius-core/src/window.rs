//! Windowing strategies for stream aggregation.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::event::Event;

/// Window assignment strategy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowStrategy {
    /// Fixed-size non-overlapping windows.
    Tumbling { duration: Duration },
    /// Overlapping windows with a slide interval.
    Sliding { size: Duration, slide: Duration },
    /// Session windows that close after inactivity.
    Session { gap: Duration },
    /// Count-based windows.
    Count { size: usize },
}

/// A window instance holding accumulated events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Window {
    /// Window start time.
    pub start: DateTime<Utc>,
    /// Window end time.
    pub end: DateTime<Utc>,
    /// Events in this window, keyed by entity_id.
    pub events: HashMap<String, Vec<Event>>,
}

impl Window {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start,
            end,
            events: HashMap::new(),
        }
    }

    /// Add an event to this window.
    pub fn add(&mut self, event: Event) {
        self.events
            .entry(event.entity_id.clone())
            .or_default()
            .push(event);
    }

    /// Total event count across all entities.
    pub fn count(&self) -> usize {
        self.events.values().map(|v| v.len()).sum()
    }

    /// Check if a timestamp falls within this window.
    pub fn contains(&self, ts: &DateTime<Utc>) -> bool {
        *ts >= self.start && *ts < self.end
    }

    /// Get all unique entity IDs in this window.
    pub fn entities(&self) -> Vec<&str> {
        self.events.keys().map(|k| k.as_str()).collect()
    }
}

/// Manages window lifecycle — creation, assignment, expiration.
pub struct WindowManager {
    strategy: WindowStrategy,
    windows: Vec<Window>,
}

impl WindowManager {
    pub fn new(strategy: WindowStrategy) -> Self {
        Self {
            strategy,
            windows: Vec::new(),
        }
    }

    /// Assign an event to the appropriate window(s), creating new windows as needed.
    pub fn assign(&mut self, event: Event) -> Vec<usize> {
        let ts = event.timestamp;
        let mut assigned = Vec::new();

        match &self.strategy {
            WindowStrategy::Tumbling { duration } => {
                let duration_ms = duration.num_milliseconds();
                if duration_ms <= 0 {
                    return assigned;
                }
                let window_start_ms = (ts.timestamp_millis() / duration_ms) * duration_ms;
                let window_start = DateTime::from_timestamp_millis(window_start_ms).unwrap();
                let window_end = window_start + *duration;

                // Find or create the window
                let idx = self.windows.iter().position(|w| w.start == window_start);

                let idx = match idx {
                    Some(i) => i,
                    None => {
                        self.windows.push(Window::new(window_start, window_end));
                        self.windows.len() - 1
                    }
                };

                self.windows[idx].add(event);
                assigned.push(idx);
            }
            WindowStrategy::Sliding { size, slide } => {
                let size_ms = size.num_milliseconds();
                let slide_ms = slide.num_milliseconds();
                if size_ms <= 0 || slide_ms <= 0 {
                    return assigned;
                }

                // Event belongs to all windows where start <= ts < start + size
                let ts_ms = ts.timestamp_millis();
                let earliest_start = ((ts_ms - size_ms + slide_ms) / slide_ms) * slide_ms;

                let mut start_ms = earliest_start;
                while start_ms <= ts_ms {
                    let window_start = DateTime::from_timestamp_millis(start_ms).unwrap();
                    let window_end = window_start + *size;

                    if ts >= window_start && ts < window_end {
                        let idx = self.windows.iter().position(|w| w.start == window_start);

                        let idx = match idx {
                            Some(i) => i,
                            None => {
                                self.windows.push(Window::new(window_start, window_end));
                                self.windows.len() - 1
                            }
                        };

                        self.windows[idx].add(event.clone());
                        assigned.push(idx);
                    }
                    start_ms += slide_ms;
                }
            }
            WindowStrategy::Session { gap } => {
                // Find existing session window for this entity within the gap
                let entity_id = &event.entity_id;
                let gap_duration = *gap;

                let idx = self.windows.iter().position(|w| {
                    if let Some(events) = w.events.get(entity_id.as_str())
                        && let Some(last) = events.last()
                    {
                        return ts - last.timestamp < gap_duration;
                    }
                    false
                });

                let idx = match idx {
                    Some(i) => {
                        // Extend the window end
                        self.windows[i].end = ts + gap_duration;
                        i
                    }
                    None => {
                        self.windows.push(Window::new(ts, ts + gap_duration));
                        self.windows.len() - 1
                    }
                };

                self.windows[idx].add(event);
                assigned.push(idx);
            }
            WindowStrategy::Count { size } => {
                // Use last window if it has room, otherwise create new
                let idx = if let Some(last) = self.windows.last() {
                    if last.count() < *size {
                        self.windows.len() - 1
                    } else {
                        self.windows.push(Window::new(ts, ts));
                        self.windows.len() - 1
                    }
                } else {
                    self.windows.push(Window::new(ts, ts));
                    0
                };

                self.windows[idx].add(event);
                self.windows[idx].end = ts;
                assigned.push(idx);
            }
        }

        assigned
    }

    /// Get expired windows: time-based when `end` is at or before the watermark,
    /// count-based when `count() >= size`.
    pub fn expire(&mut self, watermark: &DateTime<Utc>) -> Vec<Window> {
        let count_size = if let WindowStrategy::Count { size } = &self.strategy {
            Some(*size)
        } else {
            None
        };
        let (expired, active): (Vec<_>, Vec<_>) = match count_size {
            Some(size) => self.windows.drain(..).partition(|w| w.count() >= size),
            None => self.windows.drain(..).partition(|w| w.end <= *watermark),
        };
        self.windows = active;
        expired
    }

    /// Get a reference to all current windows.
    pub fn windows(&self) -> &[Window] {
        &self.windows
    }

    /// The windows still open, so a checkpoint can carry the events they hold. The
    /// strategy is left out, it comes from the topology rather than the checkpoint.
    pub fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.windows)
    }

    pub fn restore(&mut self, state: serde_json::Value) {
        if let Some(windows) = crate::operator::restored("windows", state) {
            self.windows = windows;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window() {
        let strategy = WindowStrategy::Tumbling {
            duration: Duration::seconds(10),
        };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let e1 = Event::new("a", 0.0, 0.0, ts);
        let e2 = Event::new("b", 1.0, 1.0, ts + Duration::seconds(3));
        let e3 = Event::new("a", 2.0, 2.0, ts + Duration::seconds(11));

        let idx1 = mgr.assign(e1);
        let idx2 = mgr.assign(e2);
        let idx3 = mgr.assign(e3);

        // e1 and e2 in same window, e3 in different
        assert_eq!(idx1, idx2);
        assert_ne!(idx1, idx3);
        assert_eq!(mgr.windows().len(), 2);
        assert_eq!(mgr.windows()[0].count(), 2);
        assert_eq!(mgr.windows()[1].count(), 1);
    }

    #[test]
    fn test_sliding_window() {
        let strategy = WindowStrategy::Sliding {
            size: Duration::seconds(10),
            slide: Duration::seconds(5),
        };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1007, 0).unwrap();
        let e1 = Event::new("a", 0.0, 0.0, ts);

        let assigned = mgr.assign(e1);
        // Event at t=1007 with size=10, slide=5 should be in 2 windows: [1005,1015) and [1000,1010)
        assert_eq!(assigned.len(), 2);
    }

    #[test]
    fn test_session_window() {
        let strategy = WindowStrategy::Session {
            gap: Duration::seconds(5),
        };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let e1 = Event::new("a", 0.0, 0.0, ts);
        let e2 = Event::new("a", 1.0, 1.0, ts + Duration::seconds(3));
        let e3 = Event::new("a", 2.0, 2.0, ts + Duration::seconds(20));

        mgr.assign(e1);
        mgr.assign(e2);
        mgr.assign(e3);

        // e1 and e2 same session (gap < 5s), e3 new session
        assert_eq!(mgr.windows().len(), 2);
        assert_eq!(mgr.windows()[0].count(), 2);
        assert_eq!(mgr.windows()[1].count(), 1);
    }

    #[test]
    fn test_count_window() {
        let strategy = WindowStrategy::Count { size: 3 };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        for i in 0..7 {
            let e = Event::new(
                format!("e{i}"),
                i as f64,
                0.0,
                ts + Duration::seconds(i as i64),
            );
            mgr.assign(e);
        }

        assert_eq!(mgr.windows().len(), 3); // 3 + 3 + 1
        assert_eq!(mgr.windows()[0].count(), 3);
        assert_eq!(mgr.windows()[1].count(), 3);
        assert_eq!(mgr.windows()[2].count(), 1);

        let expired = mgr.expire(&ts);
        assert_eq!(expired.len(), 2);
        assert_eq!(expired[0].count(), 3);
        assert_eq!(expired[1].count(), 3);
        assert_eq!(mgr.windows().len(), 1);
        assert_eq!(mgr.windows()[0].count(), 1);
    }

    #[test]
    fn test_count_window_expires_when_full() {
        let strategy = WindowStrategy::Count { size: 3 };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        for i in 0..3 {
            mgr.assign(Event::new(
                format!("e{i}"),
                i as f64,
                0.0,
                ts + Duration::seconds(i as i64),
            ));
        }

        let expired = mgr.expire(&ts);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].count(), 3);
        assert_eq!(mgr.windows().len(), 0);
    }

    #[test]
    fn test_count_window_partial_does_not_expire() {
        let strategy = WindowStrategy::Count { size: 3 };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        mgr.assign(Event::new("e0", 0.0, 0.0, ts));
        mgr.assign(Event::new("e1", 1.0, 0.0, ts + Duration::seconds(1)));

        let expired = mgr.expire(&(ts + Duration::hours(24)));
        assert!(expired.is_empty());
        assert_eq!(mgr.windows().len(), 1);
        assert_eq!(mgr.windows()[0].count(), 2);
    }

    /// A restored manager holds the events the open windows had accumulated, so a
    /// count window fills at the size it was already partway to.
    #[test]
    fn test_window_snapshot_carries_open_windows() {
        let mut mgr = WindowManager::new(WindowStrategy::Count { size: 3 });
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        mgr.assign(Event::new("e0", 0.0, 0.0, ts));
        mgr.assign(Event::new("e1", 1.0, 0.0, ts + Duration::seconds(1)));

        let mut resumed = WindowManager::new(WindowStrategy::Count { size: 3 });
        resumed.restore(mgr.snapshot());
        assert_eq!(resumed.windows().len(), 1);
        assert_eq!(resumed.windows()[0].count(), 2);

        resumed.assign(Event::new("e2", 2.0, 0.0, ts + Duration::seconds(2)));
        assert_eq!(resumed.expire(&ts).len(), 1);
    }

    #[test]
    fn test_window_expiration() {
        let strategy = WindowStrategy::Tumbling {
            duration: Duration::seconds(10),
        };
        let mut mgr = WindowManager::new(strategy);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        mgr.assign(Event::new("a", 0.0, 0.0, ts));
        mgr.assign(Event::new("b", 0.0, 0.0, ts + Duration::seconds(15)));

        let watermark = ts + Duration::seconds(12);
        let expired = mgr.expire(&watermark);

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].count(), 1);
        assert_eq!(mgr.windows().len(), 1);
    }
}
