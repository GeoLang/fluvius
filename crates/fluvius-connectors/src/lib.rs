//! # fluvius-connectors
//!
//! Source and sink adapters for external systems.

pub mod file;
#[cfg(feature = "kafka")]
pub mod kafka;
#[cfg(feature = "mqtt")]
pub mod mqtt;
pub mod websocket;
