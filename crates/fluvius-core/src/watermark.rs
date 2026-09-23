//! Watermark tracking for event-time progress.

use chrono::{DateTime, Duration, Utc};

/// Tracks the progress of event time across a stream.
/// Events with timestamps older than the watermark are considered late.
pub struct Watermark {
    /// Current watermark position.
    current: DateTime<Utc>,
    /// Maximum allowed lateness before events are dropped.
    max_lateness: Duration,
    /// How many late events have been observed.
    late_count: u64,
}

impl Watermark {
    /// Create a new watermark with the given max lateness tolerance.
    pub fn new(max_lateness: Duration) -> Self {
        Self {
            current: DateTime::from_timestamp(0, 0).unwrap(),
            max_lateness,
            late_count: 0,
        }
    }

    /// Advance the watermark based on an observed event timestamp.
    /// Returns false, counting the event as late, when it is before the watermark.
    pub fn advance(&mut self, event_time: &DateTime<Utc>) -> bool {
        if *event_time < self.current {
            self.late_count += 1;
            return false;
        }
        self.current = self.current.max(*event_time - self.max_lateness);
        true
    }

    /// Get the current watermark position.
    pub fn current(&self) -> &DateTime<Utc> {
        &self.current
    }

    /// Get the count of late events observed.
    pub fn late_count(&self) -> u64 {
        self.late_count
    }

    /// Check if a timestamp is considered late relative to current watermark.
    pub fn is_late(&self, ts: &DateTime<Utc>) -> bool {
        *ts < self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_advance() {
        let mut wm = Watermark::new(Duration::seconds(5));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(110, 0).unwrap();

        assert!(wm.advance(&ts1));
        assert!(wm.advance(&ts2));
        // Watermark should be at ts2 - 5s = 105
        assert_eq!(*wm.current(), DateTime::from_timestamp(105, 0).unwrap());
    }

    #[test]
    fn test_late_events() {
        let mut wm = Watermark::new(Duration::seconds(2));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(90, 0).unwrap(); // Very late

        wm.advance(&ts1);
        let accepted = wm.advance(&ts2);
        assert!(!accepted); // Too late (ts2=90, watermark=98)
        assert_eq!(wm.late_count(), 1);
    }

    #[test]
    fn test_within_lateness_tolerance() {
        let mut wm = Watermark::new(Duration::seconds(10));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(92, 0).unwrap(); // Out of order but within tolerance

        wm.advance(&ts1); // watermark = 90
        let accepted = wm.advance(&ts2);
        assert!(accepted);
        assert_eq!(wm.late_count(), 0);
    }

    #[test]
    fn test_event_exactly_at_the_watermark_is_kept() {
        let mut wm = Watermark::new(Duration::seconds(10));
        wm.advance(&DateTime::from_timestamp(100, 0).unwrap());
        assert!(wm.advance(&DateTime::from_timestamp(90, 0).unwrap()));
        assert_eq!(wm.late_count(), 0);
    }

    #[test]
    fn test_event_one_second_before_the_watermark_is_dropped() {
        let mut wm = Watermark::new(Duration::seconds(10));
        wm.advance(&DateTime::from_timestamp(100, 0).unwrap());
        assert!(!wm.advance(&DateTime::from_timestamp(89, 0).unwrap()));
        assert_eq!(wm.late_count(), 1);
    }

    #[test]
    fn test_out_of_order_event_does_not_move_the_watermark_back() {
        let mut wm = Watermark::new(Duration::seconds(10));
        wm.advance(&DateTime::from_timestamp(100, 0).unwrap());
        assert!(wm.advance(&DateTime::from_timestamp(95, 0).unwrap()));
        assert_eq!(*wm.current(), DateTime::from_timestamp(90, 0).unwrap());
        assert!(!wm.advance(&DateTime::from_timestamp(76, 0).unwrap()));
    }
}
