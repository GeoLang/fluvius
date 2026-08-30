//! Geofence operator — detect entry/exit events for polygon boundaries.

use std::collections::HashMap;

use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::StatefulOperator;
use geo::algorithm::contains::Contains;
use geo::{Point, Polygon};

/// A named geofence zone.
#[derive(Debug, Clone)]
pub struct GeofenceZone {
    pub name: String,
    pub polygon: Polygon<f64>,
}

/// Geofence event type.
#[derive(Debug, Clone, PartialEq)]
pub enum GeofenceEvent {
    Enter,
    Exit,
    Inside,
    Outside,
}

/// Geofence operator that tracks entity positions relative to defined zones.
pub struct GeofenceOperator {
    name: String,
    zones: Vec<GeofenceZone>,
    /// entity_id -> zone_name -> was_inside
    state: HashMap<String, HashMap<String, bool>>,
}

impl GeofenceOperator {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            zones: Vec::new(),
            state: HashMap::new(),
        }
    }

    /// Add a geofence zone.
    pub fn add_zone(&mut self, zone: GeofenceZone) {
        self.zones.push(zone);
    }

    /// Check which zone(s) a point is inside. Returns (zone_name, is_inside).
    fn check_point(&self, lon: f64, lat: f64) -> Vec<(String, bool)> {
        let point = Point::new(lon, lat);
        self.zones
            .iter()
            .map(|z| (z.name.clone(), z.polygon.contains(&point)))
            .collect()
    }
}

impl StatefulOperator for GeofenceOperator {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();
        let checks = self.check_point(event.lon, event.lat);

        let entity_state = self.state.entry(event.entity_id.clone()).or_default();

        for (zone_name, is_inside) in checks {
            let was_inside = entity_state.get(&zone_name).copied().unwrap_or(false);
            entity_state.insert(zone_name.clone(), is_inside);

            let geofence_event = match (was_inside, is_inside) {
                (false, true) => GeofenceEvent::Enter,
                (true, false) => GeofenceEvent::Exit,
                (true, true) => GeofenceEvent::Inside,
                (false, false) => GeofenceEvent::Outside,
            };

            // Only emit on transitions (enter/exit)
            if geofence_event == GeofenceEvent::Enter || geofence_event == GeofenceEvent::Exit {
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "zone": zone_name,
                        "event": format!("{:?}", geofence_event),
                        "entity_id": event.entity_id,
                    }),
                });
            }
        }

        outputs
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!(self.state)
    }

    fn restore(&mut self, state: serde_json::Value) {
        if let Some(inside) = fluvius_core::operator::restored(&self.name, state) {
            self.state = inside;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::{Coord, LineString};

    fn square_zone(name: &str, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> GeofenceZone {
        let exterior = LineString::from(vec![
            Coord { x: min_x, y: min_y },
            Coord { x: max_x, y: min_y },
            Coord { x: max_x, y: max_y },
            Coord { x: min_x, y: max_y },
            Coord { x: min_x, y: min_y },
        ]);
        GeofenceZone {
            name: name.into(),
            polygon: Polygon::new(exterior, vec![]),
        }
    }

    #[test]
    fn test_geofence_enter_exit() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));

        // Start outside
        let e1 = Event::now("car-1", -5.0, -5.0);
        let out1 = op.process(&e1);
        assert!(out1.is_empty()); // Outside -> Outside = no event

        // Enter zone
        let e2 = Event::now("car-1", 5.0, 5.0);
        let out2 = op.process(&e2);
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].payload["event"], "Enter");
        assert_eq!(out2[0].payload["zone"], "zone_a");

        // Stay inside
        let e3 = Event::now("car-1", 7.0, 7.0);
        let out3 = op.process(&e3);
        assert!(out3.is_empty()); // Inside -> Inside = no event

        // Exit zone
        let e4 = Event::now("car-1", 15.0, 15.0);
        let out4 = op.process(&e4);
        assert_eq!(out4.len(), 1);
        assert_eq!(out4[0].payload["event"], "Exit");
    }

    #[test]
    fn test_geofence_multiple_entities() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("warehouse", 0.0, 0.0, 10.0, 10.0));

        // Car 1 enters
        let out = op.process(&Event::now("car-1", 5.0, 5.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_id"], "car-1");

        // Car 2 enters independently
        let out = op.process(&Event::now("car-2", 3.0, 3.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_id"], "car-2");
    }

    #[test]
    fn test_geofence_multiple_zones() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));
        op.add_zone(square_zone("zone_b", 5.0, 5.0, 15.0, 15.0));

        // Enter overlapping region (enters both zones)
        let out = op.process(&Event::now("v1", 7.0, 7.0));
        assert_eq!(out.len(), 2); // Enter zone_a AND enter zone_b
    }

    /// A point exactly on the edge counts as outside, so a vehicle stopped on the
    /// boundary never triggers an entry.
    #[test]
    fn test_geofence_boundary_point_counts_as_outside() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));

        assert!(op.process(&Event::now("v1", 0.0, 5.0)).is_empty(), "edge");
        assert!(op.process(&Event::now("v1", 0.0, 0.0)).is_empty(), "corner");

        // a hair inside is an entry
        let out = op.process(&Event::now("v1", 0.000_001, 5.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["event"], "Enter");
    }

    #[test]
    fn test_geofence_reentry_emits_enter_again() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));

        let path = [
            (5.0, 5.0, Some("Enter")),
            (20.0, 20.0, Some("Exit")),
            (6.0, 6.0, Some("Enter")),
            (7.0, 7.0, None),
            (30.0, 30.0, Some("Exit")),
        ];

        for (lon, lat, expected) in path {
            let out = op.process(&Event::now("v1", lon, lat));
            match expected {
                Some(kind) => {
                    assert_eq!(out.len(), 1, "at ({lon},{lat})");
                    assert_eq!(out[0].payload["event"], kind, "at ({lon},{lat})");
                }
                None => assert!(out.is_empty(), "at ({lon},{lat}): {out:?}"),
            }
        }
    }

    /// One move that leaves one zone and enters another emits both transitions.
    #[test]
    fn test_geofence_move_between_disjoint_zones() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));
        op.add_zone(square_zone("zone_b", 20.0, 20.0, 30.0, 30.0));

        assert_eq!(op.process(&Event::now("v1", 5.0, 5.0)).len(), 1);

        let out = op.process(&Event::now("v1", 25.0, 25.0));
        assert_eq!(out.len(), 2);

        let mut seen: Vec<(String, String)> = out
            .iter()
            .map(|o| {
                (
                    o.payload["zone"].as_str().unwrap().to_string(),
                    o.payload["event"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                ("zone_a".to_string(), "Exit".to_string()),
                ("zone_b".to_string(), "Enter".to_string()),
            ]
        );
    }

    /// Each entity carries its own zone state: one leaving does not emit for the other.
    #[test]
    fn test_geofence_entity_state_is_independent() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("warehouse", 0.0, 0.0, 10.0, 10.0));

        assert_eq!(op.process(&Event::now("car-1", 5.0, 5.0)).len(), 1);
        assert_eq!(op.process(&Event::now("car-2", 6.0, 6.0)).len(), 1);

        let out = op.process(&Event::now("car-1", 50.0, 50.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["entity_id"], "car-1");
        assert_eq!(out[0].payload["event"], "Exit");

        // car-2 never moved, so it stays inside and stays quiet
        assert!(op.process(&Event::now("car-2", 6.5, 6.5)).is_empty());
    }

    /// A restored operator knows which entities were already inside, so the next event
    /// outside reads as an exit rather than nothing.
    #[test]
    fn test_geofence_snapshot_carries_zone_membership() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("warehouse", 0.0, 0.0, 10.0, 10.0));
        assert_eq!(op.process(&Event::now("car-1", 5.0, 5.0)).len(), 1);

        let mut resumed = GeofenceOperator::new("geofence");
        resumed.add_zone(square_zone("warehouse", 0.0, 0.0, 10.0, 10.0));
        resumed.restore(op.snapshot());

        let out = resumed.process(&Event::now("car-1", 50.0, 50.0));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].payload["event"], "Exit");
    }

    #[test]
    fn test_geofence_without_zones_emits_nothing() {
        let mut op = GeofenceOperator::new("geofence");
        assert!(op.process(&Event::now("v1", 5.0, 5.0)).is_empty());
        assert!(op.on_window_close().is_empty());
    }

    /// Geofence state survives a window close, so an entity already inside does not
    /// re-enter after a flush.
    #[test]
    fn test_geofence_window_close_keeps_membership() {
        let mut op = GeofenceOperator::new("geofence");
        op.add_zone(square_zone("zone_a", 0.0, 0.0, 10.0, 10.0));

        assert_eq!(op.process(&Event::now("v1", 5.0, 5.0)).len(), 1);
        assert!(op.on_window_close().is_empty());
        assert!(
            op.process(&Event::now("v1", 6.0, 6.0)).is_empty(),
            "still inside after the flush"
        );
    }
}
