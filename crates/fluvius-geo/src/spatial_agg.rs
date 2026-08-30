//! Spatial aggregation — real-time density grids, clustering, and heatmap cells.

use std::collections::HashMap;

use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::StatefulOperator;

/// A grid cell identifier.
#[derive(Debug, Clone, Hash, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CellId {
    pub col: i64,
    pub row: i64,
}

/// Configuration for a spatial grid.
#[derive(Debug, Clone)]
pub struct GridConfig {
    /// Cell width in degrees.
    pub cell_width: f64,
    /// Cell height in degrees.
    pub cell_height: f64,
    /// Grid origin longitude.
    pub origin_lon: f64,
    /// Grid origin latitude.
    pub origin_lat: f64,
}

impl GridConfig {
    /// Create a grid with uniform cell size.
    pub fn uniform(cell_size_deg: f64) -> Self {
        Self {
            cell_width: cell_size_deg,
            cell_height: cell_size_deg,
            origin_lon: -180.0,
            origin_lat: -90.0,
        }
    }

    /// Get the cell ID for a coordinate.
    pub fn cell_for(&self, lon: f64, lat: f64) -> CellId {
        CellId {
            col: ((lon - self.origin_lon) / self.cell_width).floor() as i64,
            row: ((lat - self.origin_lat) / self.cell_height).floor() as i64,
        }
    }

    /// Get the center coordinate of a cell.
    pub fn cell_center(&self, cell: &CellId) -> (f64, f64) {
        let lon = self.origin_lon + (cell.col as f64 + 0.5) * self.cell_width;
        let lat = self.origin_lat + (cell.row as f64 + 0.5) * self.cell_height;
        (lon, lat)
    }
}

/// Aggregation function for cells.
#[derive(Debug, Clone, Copy)]
pub enum AggregateFunction {
    Count,
    Sum,
    Mean,
    Max,
    Min,
}

/// A cell's accumulated state.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct CellState {
    count: u64,
    sum: f64,
    max: f64,
    min: f64,
    entities: Vec<String>,
}

/// Spatial aggregation operator that accumulates events into grid cells.
pub struct SpatialAggregator {
    name: String,
    grid: GridConfig,
    function: AggregateFunction,
    /// The property to aggregate (if applicable). For Count, this is ignored.
    value_field: Option<String>,
    cells: HashMap<CellId, CellState>,
    /// Emit threshold — emit output when a cell reaches this count.
    emit_threshold: u64,
}

impl SpatialAggregator {
    pub fn new(
        name: impl Into<String>,
        grid: GridConfig,
        function: AggregateFunction,
        value_field: Option<String>,
        emit_threshold: u64,
    ) -> Self {
        Self {
            name: name.into(),
            grid,
            function,
            value_field,
            cells: HashMap::new(),
            emit_threshold,
        }
    }

    /// Process an event, potentially emitting an aggregation output.
    pub fn process(&mut self, event: &Event) -> Option<OutputEvent> {
        let cell = self.grid.cell_for(event.lon, event.lat);
        let state = self.cells.entry(cell.clone()).or_default();

        state.count += 1;
        if !state.entities.contains(&event.entity_id) {
            state.entities.push(event.entity_id.clone());
        }

        // Extract value from event
        let value = self
            .value_field
            .as_ref()
            .and_then(|f| event.properties.get(f))
            .and_then(|v| v.as_f64())
            .or(event.speed);

        if let Some(val) = value {
            state.sum += val;
            if state.count == 1 {
                state.max = val;
                state.min = val;
            } else {
                state.max = state.max.max(val);
                state.min = state.min.min(val);
            }
        }

        // Check emit threshold
        if state.count >= self.emit_threshold {
            let (center_lon, center_lat) = self.grid.cell_center(&cell);
            let agg_value = match self.function {
                AggregateFunction::Count => state.count as f64,
                AggregateFunction::Sum => state.sum,
                AggregateFunction::Mean => state.sum / state.count as f64,
                AggregateFunction::Max => state.max,
                AggregateFunction::Min => state.min,
            };

            let output = OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({
                    "cell": {"col": cell.col, "row": cell.row},
                    "center": {"lon": center_lon, "lat": center_lat},
                    "aggregate": format!("{:?}", self.function),
                    "value": agg_value,
                    "count": state.count,
                    "unique_entities": state.entities.len(),
                }),
            };

            // Reset cell
            self.cells.remove(&cell);
            Some(output)
        } else {
            None
        }
    }

    /// Get current density map (all cells with their counts).
    pub fn density_map(&self) -> Vec<(CellId, u64)> {
        self.cells
            .iter()
            .map(|(k, v)| (k.clone(), v.count))
            .collect()
    }

    /// Reset all cells.
    pub fn reset(&mut self) {
        self.cells.clear();
    }
}

impl StatefulOperator for SpatialAggregator {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        // inherent method, the trait one is what we are defining here
        SpatialAggregator::process(self, event)
            .into_iter()
            .collect()
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        self.reset();
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// A list of pairs, not a map: a `CellId` is not a JSON object key.
    fn snapshot(&self) -> serde_json::Value {
        let cells: Vec<(&CellId, &CellState)> = self.cells.iter().collect();
        serde_json::json!(cells)
    }

    fn restore(&mut self, state: serde_json::Value) {
        let restored: Option<Vec<(CellId, CellState)>> =
            fluvius_core::operator::restored(&self.name, state);
        if let Some(cells) = restored {
            self.cells = cells.into_iter().collect();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluvius_core::event::Event;

    #[test]
    fn test_grid_cell_assignment() {
        let grid = GridConfig::uniform(1.0);
        let cell = grid.cell_for(10.5, 20.3);
        // 10.5 - (-180) = 190.5, floor(190.5/1.0) = 190
        assert_eq!(cell.col, 190);
        // 20.3 - (-90) = 110.3, floor(110.3/1.0) = 110
        assert_eq!(cell.row, 110);
    }

    #[test]
    fn test_aggregator_count() {
        let grid = GridConfig::uniform(1.0);
        let mut agg = SpatialAggregator::new("density", grid, AggregateFunction::Count, None, 3);

        let e1 = Event::now("v1", 10.0, 20.0);
        let e2 = Event::now("v2", 10.1, 20.1);
        let e3 = Event::now("v3", 10.2, 20.2);

        assert!(agg.process(&e1).is_none());
        assert!(agg.process(&e2).is_none());
        let out = agg.process(&e3).unwrap();
        assert_eq!(out.operator, "density");

        let payload = &out.payload;
        assert_eq!(payload["count"], 3);
        assert_eq!(payload["unique_entities"], 3);
    }

    /// A restored aggregator keeps the partial cell counts, so the threshold is
    /// reached by the events the first run did not already have.
    #[test]
    fn test_aggregator_snapshot_carries_the_cells() {
        let mut agg = SpatialAggregator::new(
            "density",
            GridConfig::uniform(1.0),
            AggregateFunction::Count,
            None,
            3,
        );
        assert!(agg.process(&Event::now("v1", 10.0, 20.0)).is_none());
        assert!(agg.process(&Event::now("v2", 10.1, 20.1)).is_none());

        let mut resumed = SpatialAggregator::new(
            "density",
            GridConfig::uniform(1.0),
            AggregateFunction::Count,
            None,
            3,
        );
        StatefulOperator::restore(&mut resumed, StatefulOperator::snapshot(&agg));

        let out = SpatialAggregator::process(&mut resumed, &Event::now("v3", 10.2, 20.2)).unwrap();
        assert_eq!(out.payload["count"], 3);
        assert_eq!(out.payload["unique_entities"], 3);
    }

    #[test]
    fn test_aggregator_mean() {
        let grid = GridConfig::uniform(1.0);
        let mut agg = SpatialAggregator::new("speed_avg", grid, AggregateFunction::Mean, None, 2);

        let mut e1 = Event::now("v1", 10.0, 20.0);
        e1.speed = Some(60.0);
        let mut e2 = Event::now("v2", 10.1, 20.1);
        e2.speed = Some(40.0);

        agg.process(&e1);
        let out = agg.process(&e2).unwrap();
        assert_eq!(out.payload["value"], 50.0);
    }
}
