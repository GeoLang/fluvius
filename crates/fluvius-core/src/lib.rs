//! # fluvius-core
//!
//! Real-time stream processing engine for geospatial data.
//! Provides windowing, operators, watermarks, and event-time processing.

pub mod cep;
pub mod checkpoint;
pub mod event;
pub mod metrics;
pub mod operator;
pub mod pipeline;
pub mod replay;
pub mod spatial_index;
pub mod state;
pub mod topology;
pub mod watermark;
pub mod window;
