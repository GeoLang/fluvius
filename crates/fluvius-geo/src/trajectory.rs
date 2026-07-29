//! Trajectory processing — smoothing, anomaly detection, statistics.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::StatefulOperator;
use geo::Point;
use geo::algorithm::line_measures::{Distance, Haversine};

/// A trajectory point with timestamp.
#[derive(Debug, Clone)]
pub struct TrajectoryPoint {
    pub lon: f64,
    pub lat: f64,
    pub timestamp: DateTime<Utc>,
}

/// Trajectory operator — tracks entity movement, computes statistics, detects anomalies.
pub struct TrajectoryOperator {
    name: String,
    /// Maximum buffer size per entity.
    max_buffer: usize,
    /// Speed anomaly threshold (m/s). If computed speed exceeds this, emit alert.
    max_speed: f64,
    /// Entity trajectory buffers.
    trajectories: HashMap<String, Vec<TrajectoryPoint>>,
}

impl TrajectoryOperator {
    pub fn new(name: impl Into<String>, max_buffer: usize, max_speed: f64) -> Self {
        Self {
            name: name.into(),
            max_buffer,
            max_speed,
            trajectories: HashMap::new(),
        }
    }

    /// Compute speed between two points in m/s.
    fn compute_speed(p1: &TrajectoryPoint, p2: &TrajectoryPoint) -> f64 {
        let pt1 = Point::new(p1.lon, p1.lat);
        let pt2 = Point::new(p2.lon, p2.lat);
        let distance = Haversine::distance(pt1, pt2);
        let time_diff = (p2.timestamp - p1.timestamp).num_milliseconds() as f64 / 1000.0;
        if time_diff > 0.0 {
            distance / time_diff
        } else {
            0.0
        }
    }

    /// Compute total distance of a trajectory in meters.
    pub fn total_distance(points: &[TrajectoryPoint]) -> f64 {
        points
            .windows(2)
            .map(|w| {
                let p1 = Point::new(w[0].lon, w[0].lat);
                let p2 = Point::new(w[1].lon, w[1].lat);
                Haversine::distance(p1, p2)
            })
            .sum()
    }

    /// Simple moving average smoothing.
    pub fn smooth_position(points: &[TrajectoryPoint], window: usize) -> Option<(f64, f64)> {
        if points.len() < window {
            return None;
        }
        let recent = &points[points.len() - window..];
        let avg_lon = recent.iter().map(|p| p.lon).sum::<f64>() / window as f64;
        let avg_lat = recent.iter().map(|p| p.lat).sum::<f64>() / window as f64;
        Some((avg_lon, avg_lat))
    }
}

impl StatefulOperator for TrajectoryOperator {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();

        let point = TrajectoryPoint {
            lon: event.lon,
            lat: event.lat,
            timestamp: event.timestamp,
        };

        let trajectory = self
            .trajectories
            .entry(event.entity_id.clone())
            .or_default();

        let previous = trajectory.last().cloned();

        // Buffer the point first, so the stats below cover the leg that just happened
        trajectory.push(point);
        if trajectory.len() > self.max_buffer {
            trajectory.remove(0);
        }

        // Check for speed anomaly
        if let Some(previous) = previous {
            let current = trajectory.last().expect("just pushed");
            let computed_speed = Self::compute_speed(&previous, current);

            if computed_speed > self.max_speed {
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "alert": "speed_anomaly",
                        "entity_id": event.entity_id,
                        "computed_speed_mps": computed_speed,
                        "max_speed_mps": self.max_speed,
                    }),
                });
            }

            // Emit trajectory stats
            let total_distance = Self::total_distance(trajectory);
            outputs.push(OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({
                    "type": "trajectory_update",
                    "entity_id": event.entity_id,
                    "point_count": trajectory.len(),
                    "total_distance_meters": total_distance,
                    "current_speed_mps": computed_speed,
                }),
            });
        }

        outputs
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();

        // Emit summary for each entity on window close
        for (entity_id, points) in &self.trajectories {
            if points.len() >= 2 {
                let total_distance = Self::total_distance(points);
                let duration =
                    (points.last().unwrap().timestamp - points[0].timestamp).num_seconds() as f64;
                let avg_speed = if duration > 0.0 {
                    total_distance / duration
                } else {
                    0.0
                };

                outputs.push(OutputEvent {
                    source_event: Event::now(entity_id.as_str(), 0.0, 0.0),
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "type": "trajectory_summary",
                        "entity_id": entity_id,
                        "point_count": points.len(),
                        "total_distance_meters": total_distance,
                        "duration_seconds": duration,
                        "avg_speed_mps": avg_speed,
                    }),
                });
            }
        }

        self.trajectories.clear();
        outputs
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_trajectory_basic() {
        let mut op = TrajectoryOperator::new("traj", 100, 100.0);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let e1 = Event::new("v1", 0.0, 0.0, ts);
        let out1 = op.process(&e1);
        assert!(out1.is_empty()); // First point, no stats yet

        let e2 = Event::new("v1", 0.001, 0.001, ts + Duration::seconds(10));
        let out2 = op.process(&e2);
        assert!(!out2.is_empty()); // Should have trajectory_update
        assert_eq!(out2.last().unwrap().payload["type"], "trajectory_update");
    }

    #[test]
    fn test_speed_anomaly_detection() {
        let mut op = TrajectoryOperator::new("traj", 100, 50.0); // Max 50 m/s (~180 km/h)

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let e1 = Event::new("v1", 0.0, 0.0, ts);
        op.process(&e1);

        // Jump 10km in 1 second (10000 m/s — obviously anomalous)
        let e2 = Event::new("v1", 0.1, 0.1, ts + Duration::seconds(1));
        let out = op.process(&e2);

        let anomaly = out.iter().find(|o| o.payload["alert"] == "speed_anomaly");
        assert!(anomaly.is_some());
    }

    #[test]
    fn test_window_close_summary() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        op.process(&Event::new("v1", 0.0, 0.0, ts));
        op.process(&Event::new("v1", 0.001, 0.001, ts + Duration::seconds(10)));
        op.process(&Event::new("v1", 0.002, 0.002, ts + Duration::seconds(20)));

        let summaries = op.on_window_close();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].payload["type"], "trajectory_summary");
        assert_eq!(summaries[0].payload["point_count"], 3);
        assert!(
            summaries[0].payload["total_distance_meters"]
                .as_f64()
                .unwrap()
                > 0.0
        );
    }

    /// The running total covers every leg travelled so far, including the one that
    /// produced this update. It used to lag a leg behind `point_count`.
    #[test]
    fn test_update_distance_includes_the_current_leg() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        let update_at = |op: &mut TrajectoryOperator, i: i64| {
            let out = op.process(&Event::new(
                "v1",
                0.0,
                i as f64 * 0.01,
                ts + Duration::seconds(10 * i),
            ));
            out.last().map(|o| o.payload.clone())
        };

        assert!(
            update_at(&mut op, 0).is_none(),
            "first point has no leg yet"
        );

        // each 0.01 degree step north is about 1111.95m
        let leg = 1111.95;
        let mut distances = Vec::new();
        for i in 1..4 {
            let payload = update_at(&mut op, i).unwrap();
            assert_eq!(payload["type"], "trajectory_update");
            assert_eq!(payload["point_count"], i + 1);
            distances.push(payload["total_distance_meters"].as_f64().unwrap());
        }

        for (idx, distance) in distances.iter().enumerate() {
            let expected = leg * (idx + 1) as f64;
            assert!(
                (distance - expected).abs() < 1.0,
                "point {}: distance {distance} should be about {expected}",
                idx + 2
            );
        }
        // the fourth point sits three legs from the start
        assert!((distances[2] - 3335.85).abs() < 1.0, "{:?}", distances);
    }

    /// The update total and the close summary agree on how far an entity went.
    #[test]
    fn test_update_and_summary_report_the_same_distance() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        let mut last_update_distance = 0.0;
        for i in 0..4 {
            let out = op.process(&Event::new(
                "v1",
                0.0,
                i as f64 * 0.01,
                ts + Duration::seconds(10 * i),
            ));
            if let Some(update) = out.last() {
                last_update_distance = update.payload["total_distance_meters"].as_f64().unwrap();
            }
        }

        let summary = op.on_window_close().remove(0);
        let summary_distance = summary.payload["total_distance_meters"].as_f64().unwrap();

        assert_eq!(summary.payload["point_count"], 4);
        assert!(
            (last_update_distance - summary_distance).abs() < 1e-9,
            "update said {last_update_distance}, summary said {summary_distance}"
        );
    }

    /// Two readings with the same timestamp must not divide by zero or fire a
    /// bogus anomaly.
    #[test]
    fn test_identical_timestamps_give_zero_speed() {
        let mut op = TrajectoryOperator::new("traj", 100, 1.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("v1", 0.0, 0.0, ts));
        let out = op.process(&Event::new("v1", 0.5, 0.5, ts));

        assert!(
            !out.iter().any(|o| o.payload["alert"] == "speed_anomaly"),
            "no anomaly without elapsed time: {out:?}"
        );
        let update = out.last().unwrap();
        assert_eq!(update.payload["type"], "trajectory_update");
        assert_eq!(update.payload["current_speed_mps"], 0.0);
    }

    /// An out-of-order reading is reported as zero speed rather than negative.
    #[test]
    fn test_backwards_timestamp_gives_zero_speed() {
        let mut op = TrajectoryOperator::new("traj", 100, 1.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("v1", 0.0, 0.0, ts));
        let out = op.process(&Event::new("v1", 0.5, 0.5, ts - Duration::seconds(30)));

        assert!(!out.iter().any(|o| o.payload["alert"] == "speed_anomaly"));
        assert_eq!(out.last().unwrap().payload["current_speed_mps"], 0.0);
    }

    #[test]
    fn test_speed_below_max_raises_no_anomaly() {
        let mut op = TrajectoryOperator::new("traj", 100, 50.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("v1", 0.0, 0.0, ts));
        // ~111m in 10s = ~11 m/s, well under the 50 m/s limit
        let out = op.process(&Event::new("v1", 0.001, 0.0, ts + Duration::seconds(10)));

        assert_eq!(out.len(), 1, "just the update, no anomaly");
        assert_eq!(out[0].payload["type"], "trajectory_update");
    }

    /// The buffer keeps the newest points only, which is what the close summary counts.
    #[test]
    fn test_buffer_evicts_oldest_points() {
        let mut op = TrajectoryOperator::new("traj", 2, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        for i in 0..5 {
            op.process(&Event::new(
                "v1",
                i as f64 * 0.001,
                0.0,
                ts + Duration::seconds(10 * i),
            ));
        }

        let summaries = op.on_window_close();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].payload["point_count"], 2);
    }

    #[test]
    fn test_single_point_entity_gets_no_summary() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("lonely", 0.0, 0.0, ts));
        op.process(&Event::new("moving", 0.0, 0.0, ts));
        op.process(&Event::new(
            "moving",
            0.001,
            0.0,
            ts + Duration::seconds(10),
        ));

        let summaries = op.on_window_close();
        assert_eq!(summaries.len(), 1, "one point is not a trajectory");
        assert_eq!(summaries[0].payload["entity_id"], "moving");
    }

    #[test]
    fn test_window_close_clears_state() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("v1", 0.0, 0.0, ts));
        op.process(&Event::new("v1", 0.001, 0.0, ts + Duration::seconds(10)));
        assert_eq!(op.on_window_close().len(), 1);
        assert!(op.on_window_close().is_empty(), "state was flushed");

        // the next point starts a fresh trajectory, so it emits no update
        assert!(
            op.process(&Event::new("v1", 0.002, 0.0, ts + Duration::seconds(20)))
                .is_empty()
        );
    }

    #[test]
    fn test_summary_average_speed_matches_distance_over_duration() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        op.process(&Event::new("v1", 0.0, 0.0, ts));
        op.process(&Event::new("v1", 0.001, 0.0, ts + Duration::seconds(10)));
        op.process(&Event::new("v1", 0.002, 0.0, ts + Duration::seconds(20)));

        let summary = op.on_window_close().remove(0);
        let distance = summary.payload["total_distance_meters"].as_f64().unwrap();
        let duration = summary.payload["duration_seconds"].as_f64().unwrap();
        let avg = summary.payload["avg_speed_mps"].as_f64().unwrap();

        assert_eq!(duration, 20.0);
        assert!((distance - 222.0).abs() < 4.0, "distance was {distance}");
        assert!((avg - distance / duration).abs() < 1e-9);
    }

    /// Every entity gets its own buffer and its own summary.
    #[test]
    fn test_summaries_are_per_entity() {
        let mut op = TrajectoryOperator::new("traj", 100, 1000.0);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        for entity in ["a", "b"] {
            op.process(&Event::new(entity, 0.0, 0.0, ts));
            op.process(&Event::new(entity, 0.001, 0.0, ts + Duration::seconds(10)));
        }

        let mut names: Vec<String> = op
            .on_window_close()
            .iter()
            .map(|s| s.payload["entity_id"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_smoothing_needs_a_full_window() {
        let points: Vec<TrajectoryPoint> = (0..2)
            .map(|i| TrajectoryPoint {
                lon: i as f64,
                lat: 0.0,
                timestamp: Utc::now(),
            })
            .collect();
        assert!(TrajectoryOperator::smooth_position(&points, 3).is_none());
        assert!(TrajectoryOperator::smooth_position(&[], 1).is_none());
    }

    #[test]
    fn test_smoothing() {
        let points: Vec<TrajectoryPoint> = (0..5)
            .map(|i| TrajectoryPoint {
                lon: i as f64 * 0.001,
                lat: i as f64 * 0.001,
                timestamp: Utc::now(),
            })
            .collect();

        let smoothed = TrajectoryOperator::smooth_position(&points, 3).unwrap();
        // Average of last 3: (0.002 + 0.003 + 0.004) / 3 = 0.003
        assert!((smoothed.0 - 0.003).abs() < 1e-10);
        assert!((smoothed.1 - 0.003).abs() < 1e-10);
    }
}
