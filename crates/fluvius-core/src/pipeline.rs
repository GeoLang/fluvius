//! Stream processing pipeline — connects sources, operators, and sinks.

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::event::{Event, OutputEvent};
use crate::operator::{MapOperator, StatefulOperator};

/// One step in a pipeline's operator chain.
pub enum Stage {
    /// Stateless operator. Producing no output drops the event and stops the chain,
    /// so this is how filtering is expressed.
    Map(Arc<dyn MapOperator>),
    /// Stateful operator. Emits zero or more outputs and never drops the event:
    /// geofence, proximity and friends alert on what they see, they don't filter.
    Stateful(Box<dyn StatefulOperator>),
}

/// A processing pipeline that connects a source channel to operators and a sink.
pub struct Pipeline {
    name: String,
    stages: Vec<Stage>,
}

/// Pipeline execution metrics.
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    pub events_received: u64,
    pub events_emitted: u64,
    pub events_filtered: u64,
}

impl Pipeline {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stages: Vec::new(),
        }
    }

    /// Append a stage to the operator chain.
    pub fn add_stage(&mut self, stage: Stage) {
        self.stages.push(stage);
    }

    /// Add a stateless operator to the pipeline.
    pub fn add_operator(&mut self, op: Arc<dyn MapOperator>) {
        self.add_stage(Stage::Map(op));
    }

    /// Get pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the pipeline, processing events from the receiver and sending outputs to the sender.
    /// Returns once the input channel closes, after flushing the stateful stages.
    pub async fn run(
        &mut self,
        mut input: mpsc::Receiver<Event>,
        output: mpsc::Sender<OutputEvent>,
    ) -> PipelineMetrics {
        let mut metrics = PipelineMetrics::default();

        while let Some(event) = input.recv().await {
            metrics.events_received += 1;
            let mut current = event;

            for stage in &mut self.stages {
                match stage {
                    Stage::Map(op) => match op.process(&current) {
                        Some(out) => {
                            current = out.source_event.clone();
                            if output.send(out).await.is_err() {
                                return metrics;
                            }
                            metrics.events_emitted += 1;
                        }
                        None => {
                            metrics.events_filtered += 1;
                            break;
                        }
                    },
                    Stage::Stateful(op) => {
                        for out in op.process(&current) {
                            if output.send(out).await.is_err() {
                                return metrics;
                            }
                            metrics.events_emitted += 1;
                        }
                    }
                }
            }
        }

        for stage in &mut self.stages {
            if let Stage::Stateful(op) = stage {
                for out in op.on_window_close() {
                    if output.send(out).await.is_err() {
                        return metrics;
                    }
                    metrics.events_emitted += 1;
                }
            }
        }

        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{FilterOperator, RateLimiter};

    #[tokio::test]
    async fn test_pipeline_basic() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_operator(Arc::new(FilterOperator::new("pass_all", |_| true)));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        // Send events
        let e1 = Event::now("v1", 0.0, 0.0);
        let e2 = Event::now("v2", 1.0, 1.0);
        tx_in.send(e1).await.unwrap();
        tx_in.send(e2).await.unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received, 2);
        assert_eq!(metrics.events_emitted, 2);

        let out1 = rx_out.recv().await.unwrap();
        assert_eq!(out1.source_event.entity_id, "v1");
    }

    #[tokio::test]
    async fn test_pipeline_with_filter() {
        let mut pipeline = Pipeline::new("filtered");
        pipeline.add_operator(Arc::new(FilterOperator::new("fast_only", |e: &Event| {
            e.speed.unwrap_or(0.0) > 10.0
        })));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        tx_in
            .send(Event::now("v1", 0.0, 0.0).with_speed(20.0))
            .await
            .unwrap();
        tx_in
            .send(Event::now("v2", 0.0, 0.0).with_speed(5.0))
            .await
            .unwrap();
        tx_in
            .send(Event::now("v3", 0.0, 0.0).with_speed(30.0))
            .await
            .unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received, 3);
        assert_eq!(metrics.events_emitted, 2);
        assert_eq!(metrics.events_filtered, 1);

        let out1 = rx_out.recv().await.unwrap();
        assert_eq!(out1.source_event.entity_id, "v1");
        let out2 = rx_out.recv().await.unwrap();
        assert_eq!(out2.source_event.entity_id, "v3");
    }

    /// Emits one output per event plus a summary on close, so a single test covers
    /// multi-output stages and the end-of-stream flush.
    struct Counter {
        seen: u64,
    }

    impl StatefulOperator for Counter {
        fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
            self.seen += 1;
            vec![OutputEvent {
                source_event: event.clone(),
                operator: "counter".into(),
                payload: serde_json::json!({"seen": self.seen}),
            }]
        }

        fn on_window_close(&mut self) -> Vec<OutputEvent> {
            vec![OutputEvent {
                source_event: Event::now("summary", 0.0, 0.0),
                operator: "counter".into(),
                payload: serde_json::json!({"total": self.seen}),
            }]
        }

        fn name(&self) -> &str {
            "counter"
        }
    }

    #[tokio::test]
    async fn test_pipeline_filter_then_stateful() {
        let mut pipeline = Pipeline::new("mixed");
        pipeline.add_operator(Arc::new(FilterOperator::new("fast_only", |e: &Event| {
            e.speed.unwrap_or(0.0) > 10.0
        })));
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        for speed in [20.0, 5.0, 30.0] {
            tx_in
                .send(Event::now("v1", 0.0, 0.0).with_speed(speed))
                .await
                .unwrap();
        }
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received, 3);
        assert_eq!(metrics.events_filtered, 1);
        // 2 filter passes + 2 counter outputs + 1 close summary
        assert_eq!(metrics.events_emitted, 5);

        let mut outputs = Vec::new();
        while let Some(out) = rx_out.recv().await {
            outputs.push(out);
        }
        let counted: Vec<_> = outputs.iter().filter(|o| o.operator == "counter").collect();
        assert_eq!(counted.len(), 3);
        assert_eq!(counted[2].payload["total"], 2);
    }

    #[tokio::test]
    async fn test_stateful_stage_does_not_drop_events() {
        let mut pipeline = Pipeline::new("rate_limited");
        pipeline.add_stage(Stage::Stateful(Box::new(RateLimiter::new("limit", 1))));
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        tx_in.send(Event::now("v1", 0.0, 0.0)).await.unwrap();
        tx_in.send(Event::now("v1", 1.0, 1.0)).await.unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_filtered, 0);

        let mut counted = 0;
        while let Some(out) = rx_out.recv().await {
            if out.payload.get("seen").is_some() {
                counted += 1;
            }
        }
        // the rate limiter swallowed its own second output, the downstream stage
        // still saw both events
        assert_eq!(counted, 2);
    }
}
