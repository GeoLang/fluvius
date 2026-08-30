//! Proximity detection — distance-based alerts between entities.

use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::StatefulOperator;
use fluvius_core::spatial_index::SpatialIndex;
use geo::Point;
use geo::algorithm::line_measures::{Distance, Haversine};

const METERS_PER_DEGREE_LATITUDE: f64 = 111_320.0;

/// widens the query box so a neighbour sitting exactly on the threshold survives rounding
const BOUNDING_BOX_PADDING_FACTOR: f64 = 1.01;

const MINIMUM_LONGITUDE: f64 = -180.0;
const MAXIMUM_LONGITUDE: f64 = 180.0;

/// Proximity operator — emits alerts when entities come within a threshold distance.
pub struct ProximityOperator {
    name: String,
    /// Distance threshold in meters.
    threshold_meters: f64,
    /// Last known position per entity.
    index: SpatialIndex,
}

impl ProximityOperator {
    pub fn new(name: impl Into<String>, threshold_meters: f64) -> Self {
        Self {
            name: name.into(),
            threshold_meters,
            index: SpatialIndex::new(),
        }
    }
}

/// Longitude span covering `latitude_half_width_degrees` of ground distance at `latitude`,
/// falling back to the whole globe at a pole or across the antimeridian.
fn longitude_range(longitude: f64, latitude: f64, latitude_half_width_degrees: f64) -> (f64, f64) {
    if latitude.abs() + latitude_half_width_degrees >= 90.0 {
        return (MINIMUM_LONGITUDE, MAXIMUM_LONGITUDE);
    }

    let half_width_degrees = latitude_half_width_degrees / latitude.to_radians().cos();
    let minimum = longitude - half_width_degrees;
    let maximum = longitude + half_width_degrees;
    if minimum < MINIMUM_LONGITUDE || maximum > MAXIMUM_LONGITUDE {
        return (MINIMUM_LONGITUDE, MAXIMUM_LONGITUDE);
    }

    (minimum, maximum)
}

impl StatefulOperator for ProximityOperator {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();
        let event_point = Point::new(event.lon, event.lat);

        let latitude_half_width_degrees =
            self.threshold_meters / METERS_PER_DEGREE_LATITUDE * BOUNDING_BOX_PADDING_FACTOR;
        let (minimum_longitude, maximum_longitude) =
            longitude_range(event.lon, event.lat, latitude_half_width_degrees);
        let candidates = self.index.query_bbox(
            minimum_longitude,
            event.lat - latitude_half_width_degrees,
            maximum_longitude,
            event.lat + latitude_half_width_degrees,
        );

        for other_id in candidates {
            if other_id == event.entity_id {
                continue;
            }
            let Some([lon, lat]) = self.index.get_position(&other_id) else {
                continue;
            };
            let other_point = Point::new(lon, lat);
            let distance = Haversine::distance(event_point, other_point);

            if distance <= self.threshold_meters {
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "alert": "proximity",
                        "entity_a": event.entity_id,
                        "entity_b": other_id,
                        "distance_meters": distance,
                        "threshold_meters": self.threshold_meters,
                    }),
                });
            }
        }

        // Update this entity's position
        self.index.update(event);

        outputs
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        // Clear stale positions on window close
        self.index.clear();
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.index.positions())
    }

    fn restore(&mut self, state: serde_json::Value) {
        if let Some(positions) = fluvius_core::operator::restored(&self.name, state) {
            self.index.replace(positions);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proximity_alert() {
        let mut op = ProximityOperator::new("proximity", 1000.0); // 1km threshold

        // Vehicle 1 at a position
        let e1 = Event::now("v1", -73.9857, 40.7484); // NYC
        let out1 = op.process(&e1);
        assert!(out1.is_empty()); // No other entities yet

        // Vehicle 2 very close (same block ~50m)
        let e2 = Event::now("v2", -73.9855, 40.7486);
        let out2 = op.process(&e2);
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].payload["alert"], "proximity");
        assert_eq!(out2[0].payload["entity_a"], "v2");
        assert_eq!(out2[0].payload["entity_b"], "v1");
    }

    #[test]
    fn test_no_alert_when_far() {
        let mut op = ProximityOperator::new("proximity", 100.0); // 100m threshold

        let e1 = Event::now("v1", -73.9857, 40.7484); // NYC
        op.process(&e1);

        // Vehicle 2 in London — very far
        let e2 = Event::now("v2", -0.1278, 51.5074);
        let out2 = op.process(&e2);
        assert!(out2.is_empty());
    }

    #[test]
    fn test_self_not_triggered() {
        let mut op = ProximityOperator::new("proximity", 1000.0);

        let e1 = Event::now("v1", 0.0, 0.0);
        op.process(&e1);

        // Same entity updates position — should not trigger self-alert
        let e2 = Event::now("v1", 0.0001, 0.0001);
        let out = op.process(&e2);
        assert!(out.is_empty());
    }

    /// Measure the pair distance once, then use it as the threshold: the check is
    /// inclusive, and a hair under it goes quiet.
    #[test]
    fn test_threshold_is_inclusive() {
        let (a, b) = (Event::now("v1", 0.0, 0.0), Event::now("v2", 0.001, 0.0));

        let mut measure = ProximityOperator::new("proximity", 1e9);
        measure.process(&a);
        let distance = measure.process(&b)[0].payload["distance_meters"]
            .as_f64()
            .unwrap();
        assert!(distance > 100.0, "sanity: {distance}");

        let mut at_threshold = ProximityOperator::new("proximity", distance);
        at_threshold.process(&a);
        assert_eq!(
            at_threshold.process(&b).len(),
            1,
            "equal to the threshold alerts"
        );

        let mut under = ProximityOperator::new("proximity", distance * 0.999);
        under.process(&a);
        assert!(under.process(&b).is_empty(), "just past the threshold");
    }

    /// An event near several entities alerts once per neighbour.
    #[test]
    fn test_alert_per_nearby_entity() {
        let mut op = ProximityOperator::new("proximity", 1000.0);
        op.process(&Event::now("v1", 0.0, 0.0));
        op.process(&Event::now("v2", 0.001, 0.0));
        // far away, must not be counted
        op.process(&Event::now("far", 40.0, 40.0));

        let out = op.process(&Event::now("v3", 0.0, 0.001));
        assert_eq!(out.len(), 2);

        let mut others: Vec<&str> = out
            .iter()
            .map(|o| o.payload["entity_b"].as_str().unwrap())
            .collect();
        others.sort();
        assert_eq!(others, vec!["v1", "v2"]);
        assert!(out.iter().all(|o| o.payload["entity_a"] == "v3"));
    }

    /// Only the latest position counts: once an entity drives off, arriving at its
    /// old spot is not an alert.
    #[test]
    fn test_uses_latest_position_only() {
        let mut op = ProximityOperator::new("proximity", 500.0);
        op.process(&Event::now("v1", 0.0, 0.0));
        // v1 moves far away
        op.process(&Event::now("v1", 40.0, 40.0));

        // v2 arrives where v1 used to be
        assert!(op.process(&Event::now("v2", 0.0, 0.0)).is_empty());
    }

    #[test]
    fn test_window_close_clears_positions() {
        let mut op = ProximityOperator::new("proximity", 1000.0);
        op.process(&Event::now("v1", 0.0, 0.0));

        assert!(op.on_window_close().is_empty(), "close emits nothing");

        // v1's position was dropped, so v2 has nothing to be near
        assert!(op.process(&Event::now("v2", 0.001, 0.0)).is_empty());
        // but v2 is now tracked, so v3 alongside it does alert
        assert_eq!(op.process(&Event::now("v3", 0.001, 0.0)).len(), 1);
    }

    /// Neighbours a few hundred metres apart across the 180th meridian still alert,
    /// even though their longitudes differ by almost 360 degrees.
    #[test]
    fn test_alert_across_the_antimeridian() {
        let mut op = ProximityOperator::new("proximity", 1000.0);
        op.process(&Event::now("east", 179.9995, 0.0));

        let out = op.process(&Event::now("west", -179.9995, 0.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_b"], "east");
        let distance = out[0].payload["distance_meters"].as_f64().unwrap();
        assert!(distance < 1000.0, "distance was {distance}");
    }

    /// At latitude 60 a degree of longitude is half as wide as at the equator, so the
    /// query box has to widen by the same factor to catch this pair.
    #[test]
    fn test_longitude_box_widens_with_latitude() {
        let mut alerting = ProximityOperator::new("proximity", 700.0);
        alerting.process(&Event::now("v1", 10.0, 60.0));
        // about 556 m apart
        let out = alerting.process(&Event::now("v2", 10.01, 60.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_b"], "v1");

        let mut quiet = ProximityOperator::new("proximity", 500.0);
        quiet.process(&Event::now("v1", 10.0, 60.0));
        assert!(quiet.process(&Event::now("v2", 10.01, 60.0)).is_empty());
    }

    /// A restored operator queries the positions the snapshot carried, so a neighbour
    /// it never saw arrive still triggers an alert.
    #[test]
    fn test_proximity_snapshot_carries_positions() {
        let mut op = ProximityOperator::new("proximity", 1000.0);
        op.process(&Event::now("v1", 0.0, 0.0));

        let mut resumed = ProximityOperator::new("proximity", 1000.0);
        resumed.restore(op.snapshot());

        let out = resumed.process(&Event::now("v2", 0.001, 0.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_b"], "v1");
    }

    #[test]
    fn test_alert_payload_reports_distance_and_threshold() {
        let mut op = ProximityOperator::new("proximity", 1000.0);
        op.process(&Event::now("v1", 0.0, 0.0));
        let out = op.process(&Event::now("v2", 0.001, 0.0));

        let payload = &out[0].payload;
        assert_eq!(payload["alert"], "proximity");
        assert_eq!(payload["threshold_meters"], 1000.0);
        let distance = payload["distance_meters"].as_f64().unwrap();
        // 0.001 degrees of longitude at the equator is about 111m
        assert!((distance - 111.0).abs() < 2.0, "distance was {distance}");
        assert_eq!(out[0].source_event.entity_id, "v2");
        assert_eq!(out[0].operator, "proximity");
    }
}
