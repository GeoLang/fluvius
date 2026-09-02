//! R-tree spatial index for fast spatial queries over entities.

use rstar::{AABB, RTree, primitives::GeomWithData};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::event::Event;

/// A point entry in the spatial index, carrying entity_id.
pub type SpatialEntry = GeomWithData<[f64; 2], String>;

/// Thread-safe spatial index that maintains current positions of all entities.
#[derive(Clone)]
pub struct SpatialIndex {
    inner: Arc<RwLock<SpatialIndexInner>>,
}

struct SpatialIndexInner {
    tree: RTree<SpatialEntry>,
    /// Current position per entity for efficient updates.
    positions: HashMap<String, [f64; 2]>,
}

impl SpatialIndex {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(SpatialIndexInner {
                tree: RTree::new(),
                positions: HashMap::new(),
            })),
        }
    }

    /// Update an entity's position from an event.
    pub fn update(&self, event: &Event) {
        let mut inner = self.inner.write().unwrap();
        let new_pos = [event.lon, event.lat];

        // Remove old position if exists
        if let Some(old_pos) = inner.positions.get(&event.entity_id) {
            let old_entry = SpatialEntry::new(*old_pos, event.entity_id.clone());
            inner.tree.remove(&old_entry);
        }

        // Insert new position
        inner
            .tree
            .insert(SpatialEntry::new(new_pos, event.entity_id.clone()));
        inner.positions.insert(event.entity_id.clone(), new_pos);
    }

    /// Query entities within a bounding box (min_lon, min_lat, max_lon, max_lat).
    pub fn query_bbox(
        &self,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        let aabb = AABB::from_corners([min_lon, min_lat], [max_lon, max_lat]);
        inner
            .tree
            .locate_in_envelope(&aabb)
            .map(|e| e.data.clone())
            .collect()
    }

    /// Get the current position of an entity.
    pub fn get_position(&self, entity_id: &str) -> Option<[f64; 2]> {
        let inner = self.inner.read().unwrap();
        inner.positions.get(entity_id).copied()
    }

    /// Remove an entity from the index.
    pub fn remove(&self, entity_id: &str) {
        let mut inner = self.inner.write().unwrap();
        if let Some(pos) = inner.positions.remove(entity_id) {
            let entry = SpatialEntry::new(pos, entity_id.to_string());
            inner.tree.remove(&entry);
        }
    }

    /// Every entity's current position.
    pub fn positions(&self) -> HashMap<String, [f64; 2]> {
        let inner = self.inner.read().unwrap();
        inner.positions.clone()
    }

    /// Drop what is indexed and rebuild it from the given positions.
    pub fn replace(&self, positions: HashMap<String, [f64; 2]>) {
        let mut inner = self.inner.write().unwrap();
        inner.tree = RTree::bulk_load(
            positions
                .iter()
                .map(|(entity_id, position)| SpatialEntry::new(*position, entity_id.clone()))
                .collect(),
        );
        inner.positions = positions;
    }

    /// Drop every indexed entity.
    pub fn clear(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.tree = RTree::new();
        inner.positions.clear();
    }

    /// Number of entities currently indexed.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.positions.len()
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for SpatialIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Event;

    #[test]
    fn test_spatial_index_update_and_query() {
        let idx = SpatialIndex::new();

        let e1 = Event::now("v1", 10.0, 20.0);
        let e2 = Event::now("v2", 10.1, 20.1);
        let e3 = Event::now("v3", 50.0, 50.0);

        idx.update(&e1);
        idx.update(&e2);
        idx.update(&e3);

        assert_eq!(idx.len(), 3);

        // Query bbox around v1 and v2
        let results = idx.query_bbox(9.9, 19.9, 10.2, 20.2);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&"v1".to_string()));
        assert!(results.contains(&"v2".to_string()));
    }

    #[test]
    fn test_position_update() {
        let idx = SpatialIndex::new();
        idx.update(&Event::now("v1", 0.0, 0.0));
        assert_eq!(idx.get_position("v1"), Some([0.0, 0.0]));

        // Update position
        idx.update(&Event::now("v1", 5.0, 5.0));
        assert_eq!(idx.get_position("v1"), Some([5.0, 5.0]));
        assert_eq!(idx.len(), 1); // Still just one entity
    }

    #[test]
    fn test_remove() {
        let idx = SpatialIndex::new();
        idx.update(&Event::now("v1", 0.0, 0.0));
        idx.remove("v1");
        assert_eq!(idx.len(), 0);
        assert_eq!(idx.get_position("v1"), None);
    }

    #[test]
    fn test_clear() {
        let idx = SpatialIndex::new();
        idx.update(&Event::now("v1", 0.0, 0.0));
        idx.update(&Event::now("v2", 1.0, 1.0));

        idx.clear();

        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert_eq!(idx.get_position("v1"), None);
        assert!(idx.query_bbox(-10.0, -10.0, 10.0, 10.0).is_empty());

        // the cleared index still takes new entities
        idx.update(&Event::now("v3", 2.0, 2.0));
        assert_eq!(idx.query_bbox(-10.0, -10.0, 10.0, 10.0), vec!["v3"]);
    }
}
