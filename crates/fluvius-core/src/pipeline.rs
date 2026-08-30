//! Stream processing pipeline — connects sources, operators, and sinks.

use chrono::Duration;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::event::{Event, OutputEvent};
use crate::metrics::Metrics;
use crate::operator::{MapOperator, StatefulOperator};
use crate::watermark::Watermark;
use crate::window::{WindowManager, WindowStrategy};

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
    windows: Option<WindowManager>,
    watermark: Option<Watermark>,
    metrics: Metrics,
}

impl Pipeline {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            stages: Vec::new(),
            windows: None,
            watermark: None,
            metrics: Metrics::new(),
        }
    }

    /// The counters this pipeline updates as it runs. The handle is shared, so a
    /// metrics endpoint holding one sees the counts move.
    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    /// Expire stateful stages when a tumbling (or other) window closes, using
    /// event-time watermarks. Late events past `max_lateness` are dropped.
    pub fn set_window(&mut self, strategy: WindowStrategy, max_lateness: Duration) {
        self.windows = Some(WindowManager::new(strategy));
        self.watermark = Some(Watermark::new(max_lateness));
    }

    pub fn set_window_lateness_secs(&mut self, strategy: WindowStrategy, max_lateness_secs: u64) {
        self.set_window(strategy, Duration::seconds(max_lateness_secs as i64));
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
    ) -> Metrics {
        let metrics = self.metrics.clone();

        while let Some(event) = input.recv().await {
            metrics.inc_received();

            if let Some(wm) = &mut self.watermark
                && !wm.advance(&event.timestamp)
            {
                metrics.inc_late();
                continue;
            }
            if let (Some(wm), Some(windows)) = (&self.watermark, &mut self.windows) {
                for _expired in windows.expire(wm.current()) {
                    if !flush_stateful(&mut self.stages, &output, &metrics).await {
                        return metrics;
                    }
                }
            }

            let mut current = event;
            let mut processing_time = std::time::Duration::ZERO;

            for stage in &mut self.stages {
                let started = std::time::Instant::now();
                match stage {
                    Stage::Map(op) => {
                        let produced = op.process(&current);
                        processing_time += started.elapsed();
                        match produced {
                            Some(out) => {
                                current = out.source_event.clone();
                                if output.send(out).await.is_err() {
                                    return metrics;
                                }
                                metrics.inc_emitted();
                            }
                            None => {
                                metrics.inc_filtered();
                                break;
                            }
                        }
                    }
                    Stage::Stateful(op) => {
                        let produced = op.process(&current);
                        processing_time += started.elapsed();
                        for out in produced {
                            if output.send(out).await.is_err() {
                                return metrics;
                            }
                            metrics.inc_emitted();
                        }
                    }
                }
            }
            metrics.record_processing_time(processing_time.as_micros() as u64);

            if let Some(windows) = &mut self.windows {
                windows.assign(current);
            }
        }

        let _ = flush_stateful(&mut self.stages, &output, &metrics).await;
        metrics
    }
}

/// true if the sink is still accepting
async fn flush_stateful(
    stages: &mut [Stage],
    output: &mpsc::Sender<OutputEvent>,
    metrics: &Metrics,
) -> bool {
    for stage in stages.iter_mut() {
        if let Stage::Stateful(op) = stage {
            for out in op.on_window_close() {
                if output.send(out).await.is_err() {
                    return false;
                }
                metrics.inc_emitted();
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::{FilterOperator, RateLimiter};
    use crate::window::WindowStrategy;
    use chrono::DateTime;

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
        assert_eq!(metrics.events_received(), 2);
        assert_eq!(metrics.events_emitted(), 2);

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
        assert_eq!(metrics.events_received(), 3);
        assert_eq!(metrics.events_emitted(), 2);
        assert_eq!(metrics.events_filtered(), 1);

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
            let total = self.seen;
            self.seen = 0;
            vec![OutputEvent {
                source_event: Event::now("summary", 0.0, 0.0),
                operator: "counter".into(),
                payload: serde_json::json!({"total": total}),
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
        assert_eq!(metrics.events_received(), 3);
        assert_eq!(metrics.events_filtered(), 1);
        // 2 filter passes + 2 counter outputs + 1 close summary
        assert_eq!(metrics.events_emitted(), 5);

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
        assert_eq!(metrics.events_filtered(), 0);

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

    /// A pipeline with no stages consumes its input and emits nothing.
    #[tokio::test]
    async fn test_empty_pipeline_emits_nothing() {
        let mut pipeline = Pipeline::new("empty");
        assert_eq!(pipeline.name(), "empty");

        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        tx_in.send(Event::now("v1", 0.0, 0.0)).await.unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received(), 1);
        assert_eq!(metrics.events_emitted(), 0);
        assert!(rx_out.recv().await.is_none());
    }

    /// A filter placed after a stateful stage no longer shields it: stage order is
    /// what decides which events reach the operator.
    #[tokio::test]
    async fn test_stage_order_decides_what_the_operator_sees() {
        let mut pipeline = Pipeline::new("filter_last");
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));
        pipeline.add_operator(Arc::new(FilterOperator::new("fast_only", |e: &Event| {
            e.speed.unwrap_or(0.0) > 10.0
        })));

        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        for speed in [20.0, 5.0] {
            tx_in
                .send(Event::now("v1", 0.0, 0.0).with_speed(speed))
                .await
                .unwrap();
        }
        drop(tx_in);

        pipeline.run(rx_in, tx_out).await;

        let mut outputs = Vec::new();
        while let Some(out) = rx_out.recv().await {
            outputs.push(out);
        }
        let total = outputs
            .iter()
            .find(|o| o.payload.get("total").is_some())
            .expect("close summary");
        assert_eq!(total.payload["total"], 2, "the counter saw the slow event");
    }

    /// Stateful stages are flushed in order when the stream ends.
    #[tokio::test]
    async fn test_close_flushes_every_stateful_stage() {
        let mut pipeline = Pipeline::new("two_counters");
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));

        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        tx_in.send(Event::now("v1", 0.0, 0.0)).await.unwrap();
        drop(tx_in);

        pipeline.run(rx_in, tx_out).await;

        let mut summaries = 0;
        while let Some(out) = rx_out.recv().await {
            if out.payload.get("total").is_some() {
                summaries += 1;
            }
        }
        assert_eq!(summaries, 2);
    }

    /// If the sink goes away the run stops instead of blocking forever.
    #[tokio::test]
    async fn test_run_stops_when_the_sink_is_dropped() {
        let mut pipeline = Pipeline::new("dead_sink");
        pipeline.add_operator(Arc::new(FilterOperator::new("pass_all", |_| true)));

        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, rx_out) = mpsc::channel(10);
        drop(rx_out);

        for _ in 0..3 {
            tx_in.send(Event::now("v1", 0.0, 0.0)).await.unwrap();
        }
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_emitted(), 0);
        assert_eq!(metrics.events_received(), 1, "gave up on the first send");
    }

    /// A stateless stage hands the event it emitted to the next stage, so a
    /// transform is visible downstream.
    #[tokio::test]
    async fn test_map_stage_feeds_its_output_downstream() {
        use crate::operator::TransformOperator;

        let mut pipeline = Pipeline::new("transform_then_filter");
        pipeline.add_operator(Arc::new(TransformOperator::new("boost", |e: &Event| {
            let mut cloned = e.clone();
            cloned.speed = Some(cloned.speed.unwrap_or(0.0) + 100.0);
            cloned
        })));
        pipeline.add_operator(Arc::new(FilterOperator::new("fast_only", |e: &Event| {
            e.speed.unwrap_or(0.0) > 50.0
        })));

        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        tx_in
            .send(Event::now("v1", 0.0, 0.0).with_speed(1.0))
            .await
            .unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(
            metrics.events_filtered(),
            0,
            "the boost got it past the filter"
        );
        assert_eq!(metrics.events_emitted(), 2);

        let boosted = rx_out.recv().await.unwrap();
        assert_eq!(boosted.source_event.speed, Some(101.0));
    }

    #[tokio::test]
    async fn tumbling_window_flushes_stateful_when_it_closes() {
        let mut pipeline = Pipeline::new("windows");
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));
        pipeline.set_window(
            WindowStrategy::Tumbling {
                duration: Duration::seconds(10),
            },
            Duration::seconds(0),
        );

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        tx_in.send(Event::new("v1", 0.0, 0.0, ts)).await.unwrap();
        tx_in
            .send(Event::new("v1", 0.0, 0.0, ts + Duration::seconds(11)))
            .await
            .unwrap();
        drop(tx_in);

        pipeline.run(rx_in, tx_out).await;

        let mut totals = Vec::new();
        while let Some(out) = rx_out.recv().await {
            if let Some(t) = out.payload.get("total") {
                totals.push(t.as_u64().unwrap());
            }
        }
        // first window closes on the second event; end of stream flushes the next
        assert_eq!(totals, vec![1, 1]);
    }

    #[tokio::test]
    async fn count_window_flushes_stateful_when_it_fills() {
        let mut pipeline = Pipeline::new("count-windows");
        pipeline.add_stage(Stage::Stateful(Box::new(Counter { seen: 0 })));
        pipeline.set_window(WindowStrategy::Count { size: 3 }, Duration::seconds(0));

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, mut rx_out) = mpsc::channel(10);
        for i in 0..4 {
            tx_in
                .send(Event::new("v1", 0.0, 0.0, ts + Duration::seconds(i)))
                .await
                .unwrap();
        }
        drop(tx_in);

        pipeline.run(rx_in, tx_out).await;

        let mut totals = Vec::new();
        while let Some(out) = rx_out.recv().await {
            if let Some(t) = out.payload.get("total") {
                totals.push(t.as_u64().unwrap());
            }
        }
        // first count window closes on the 4th event; end of stream flushes the rest
        assert_eq!(totals, vec![3, 1]);
    }

    #[tokio::test]
    async fn late_events_past_the_watermark_are_dropped() {
        let mut pipeline = Pipeline::new("late");
        pipeline.add_operator(Arc::new(FilterOperator::new("pass", |_| true)));
        pipeline.set_window(
            WindowStrategy::Tumbling {
                duration: Duration::seconds(10),
            },
            Duration::seconds(2),
        );

        let ts = DateTime::from_timestamp(100, 0).unwrap();
        let (tx_in, rx_in) = mpsc::channel(10);
        let (tx_out, _rx_out) = mpsc::channel(10);
        tx_in.send(Event::new("v1", 0.0, 0.0, ts)).await.unwrap();
        tx_in
            .send(Event::new("v2", 0.0, 0.0, ts - Duration::seconds(10)))
            .await
            .unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_late(), 1);
        assert_eq!(metrics.events_received(), 2);
        assert_eq!(metrics.events_emitted(), 1);
    }
}
