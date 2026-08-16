//! Map matching — snap GPS points to a road network.

use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::MapOperator;
use geo::algorithm::line_measures::{Distance, Haversine};
use geo::{Coord, Line, LineString, Point};

/// A road segment in the network.
#[derive(Debug, Clone)]
pub struct RoadSegment {
    pub id: String,
    pub name: Option<String>,
    pub geometry: LineString<f64>,
    pub speed_limit: Option<f64>,
    pub oneway: bool,
}

/// Result of snapping a point to the road network.
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// The matched road segment ID.
    pub segment_id: String,
    /// Road name if available.
    pub road_name: Option<String>,
    /// Snapped position on the road.
    pub snapped_lon: f64,
    pub snapped_lat: f64,
    /// Distance from original point to snapped point (meters).
    pub distance_m: f64,
    /// Confidence (0.0-1.0, based on distance).
    pub confidence: f64,
}

/// A simple road network for map matching.
pub struct RoadNetwork {
    segments: Vec<RoadSegment>,
    /// Maximum distance to consider a match (meters).
    max_distance_m: f64,
}

impl RoadNetwork {
    pub fn new(max_distance_m: f64) -> Self {
        Self {
            segments: Vec::new(),
            max_distance_m,
        }
    }

    /// Add a road segment.
    pub fn add_segment(&mut self, segment: RoadSegment) {
        self.segments.push(segment);
    }

    /// Number of segments in the network.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Snap a GPS point to the nearest road segment.
    pub fn match_point(&self, lon: f64, lat: f64) -> Option<MatchResult> {
        let point = Point::new(lon, lat);
        let mut best: Option<(f64, f64, f64, usize)> = None; // (dist, snap_lon, snap_lat, seg_idx)

        for (idx, seg) in self.segments.iter().enumerate() {
            if let Some((snap_lon, snap_lat, dist)) = nearest_point_on_line(&point, &seg.geometry)
                && dist < self.max_distance_m
                && (best.is_none() || dist < best.unwrap().0)
            {
                best = Some((dist, snap_lon, snap_lat, idx));
            }
        }

        best.map(|(dist, snap_lon, snap_lat, idx)| {
            let seg = &self.segments[idx];
            let confidence = (1.0 - dist / self.max_distance_m).max(0.0);
            MatchResult {
                segment_id: seg.id.clone(),
                road_name: seg.name.clone(),
                snapped_lon: snap_lon,
                snapped_lat: snap_lat,
                distance_m: dist,
                confidence,
            }
        })
    }

    /// Match a sequence of points (trajectory), returning matched points.
    pub fn match_trajectory(&self, points: &[(f64, f64)]) -> Vec<Option<MatchResult>> {
        points
            .iter()
            .map(|(lon, lat)| self.match_point(*lon, *lat))
            .collect()
    }
}

/// Find the nearest point on a linestring to a given point.
/// Returns (lon, lat, distance_meters).
fn nearest_point_on_line(point: &Point<f64>, line: &LineString<f64>) -> Option<(f64, f64, f64)> {
    let coords: Vec<Coord<f64>> = line.coords().cloned().collect();
    if coords.len() < 2 {
        return None;
    }

    let mut best_dist = f64::INFINITY;
    let mut best_point = coords[0];

    for window in coords.windows(2) {
        let seg = Line::new(window[0], window[1]);
        let (proj, dist) = project_point_on_segment(point, &seg);
        if dist < best_dist {
            best_dist = dist;
            best_point = proj;
        }
    }

    Some((best_point.x, best_point.y, best_dist))
}

/// Project a point onto a line segment, returning the projected coord and distance in meters.
fn project_point_on_segment(point: &Point<f64>, segment: &Line<f64>) -> (Coord<f64>, f64) {
    let a = segment.start;
    let b = segment.end;

    let dx = b.x - a.x;
    let dy = b.y - a.y;
    let len_sq = dx * dx + dy * dy;

    let projected = if len_sq == 0.0 {
        a
    } else {
        let t = ((point.x() - a.x) * dx + (point.y() - a.y) * dy) / len_sq;
        let t = t.clamp(0.0, 1.0);
        Coord {
            x: a.x + t * dx,
            y: a.y + t * dy,
        }
    };

    let dist = Haversine::distance(Point::from(projected), *point);
    (projected, dist)
}

/// Snaps each event onto `network` and forwards the snapped coordinates.
pub struct MapMatchOperator {
    name: String,
    network: RoadNetwork,
}

impl MapMatchOperator {
    pub fn new(name: impl Into<String>, network: RoadNetwork) -> Self {
        Self {
            name: name.into(),
            network,
        }
    }
}

impl MapOperator for MapMatchOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        match self.network.match_point(event.lon, event.lat) {
            Some(m) => {
                let mut snapped = event.clone();
                snapped.lon = m.snapped_lon;
                snapped.lat = m.snapped_lat;
                Some(OutputEvent {
                    source_event: snapped,
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "type": "map_match",
                        "segment_id": m.segment_id,
                        "road_name": m.road_name,
                        "distance_m": m.distance_m,
                        "confidence": m.confidence,
                    }),
                })
            }
            None => Some(OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({
                    "type": "map_match",
                    "unmatched": true,
                }),
            }),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::LineString;

    fn sample_network() -> RoadNetwork {
        let mut net = RoadNetwork::new(100.0);
        net.add_segment(RoadSegment {
            id: "road1".to_string(),
            name: Some("Main Street".to_string()),
            geometry: LineString::from(vec![(0.0, 0.0), (1.0, 0.0)]),
            speed_limit: Some(50.0),
            oneway: false,
        });
        net.add_segment(RoadSegment {
            id: "road2".to_string(),
            name: Some("Cross Avenue".to_string()),
            geometry: LineString::from(vec![(0.5, -0.5), (0.5, 0.5)]),
            speed_limit: Some(30.0),
            oneway: false,
        });
        net
    }

    #[test]
    fn operator_snaps_and_forwards() {
        use chrono::Utc;
        use fluvius_core::event::Event;
        use fluvius_core::operator::MapOperator;

        let op = MapMatchOperator::new("snap", sample_network());
        let event = Event::new("v1", 0.2, 0.0001, Utc::now());
        let out = op.process(&event).expect("match emits");
        assert_eq!(out.payload["segment_id"], "road1");
        assert!((out.source_event.lat).abs() < 1e-6);
        assert!((out.source_event.lon - 0.2).abs() < 1e-6);
    }

    #[test]
    fn test_match_point_near_road() {
        let net = sample_network();
        // Point slightly above Main Street, away from the intersection
        let result = net.match_point(0.1, 0.0001);
        assert!(result.is_some());
        let m = result.unwrap();
        assert_eq!(m.segment_id, "road1");
        assert!(m.distance_m < 20.0); // Should be very close
        assert!(m.confidence > 0.8);
    }

    #[test]
    fn test_match_point_too_far() {
        let net = sample_network();
        // Point very far from any road
        let result = net.match_point(50.0, 50.0);
        assert!(result.is_none());
    }

    #[test]
    fn test_match_trajectory() {
        let net = sample_network();
        let points = vec![(0.1, 0.0001), (0.5, 0.0001), (0.9, 0.0001)];
        let results = net.match_trajectory(&points);
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_some()));
    }
}
