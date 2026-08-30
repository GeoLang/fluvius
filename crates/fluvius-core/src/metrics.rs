//! Prometheus-compatible metrics for pipeline observability.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// Metric names are prefixed with this.
const METRICS_PREFIX: &str = "fluvius";
/// A request head longer than this is not a metrics scrape.
const REQUEST_HEAD_LIMIT: usize = 8 * 1024;
const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Metrics collector for pipeline operators.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    events_received: AtomicU64,
    events_emitted: AtomicU64,
    events_filtered: AtomicU64,
    events_late: AtomicU64,
    processing_time_us: AtomicU64,
    processing_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                events_received: AtomicU64::new(0),
                events_emitted: AtomicU64::new(0),
                events_filtered: AtomicU64::new(0),
                events_late: AtomicU64::new(0),
                processing_time_us: AtomicU64::new(0),
                processing_count: AtomicU64::new(0),
            }),
        }
    }

    pub fn inc_received(&self) {
        self.inner.events_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_emitted(&self) {
        self.inner.events_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_filtered(&self) {
        self.inner.events_filtered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_late(&self) {
        self.inner.events_late.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_processing_time(&self, microseconds: u64) {
        self.inner
            .processing_time_us
            .fetch_add(microseconds, Ordering::Relaxed);
        self.inner.processing_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn events_received(&self) -> u64 {
        self.inner.events_received.load(Ordering::Relaxed)
    }

    pub fn events_emitted(&self) -> u64 {
        self.inner.events_emitted.load(Ordering::Relaxed)
    }

    pub fn events_filtered(&self) -> u64 {
        self.inner.events_filtered.load(Ordering::Relaxed)
    }

    pub fn events_late(&self) -> u64 {
        self.inner.events_late.load(Ordering::Relaxed)
    }

    pub fn avg_processing_time_us(&self) -> f64 {
        let count = self.inner.processing_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.inner.processing_time_us.load(Ordering::Relaxed) as f64 / count as f64
    }

    /// Render metrics in Prometheus exposition format.
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "# HELP {prefix}_events_received_total Total events received\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_received_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_received_total {}\n",
            self.events_received()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_emitted_total Total events emitted\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_emitted_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_emitted_total {}\n",
            self.events_emitted()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_filtered_total Total events filtered out\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_filtered_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_filtered_total {}\n",
            self.events_filtered()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_late_total Total late events\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_late_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_late_total {}\n",
            self.events_late()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_processing_time_avg_us Average processing time in microseconds\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_processing_time_avg_us gauge\n"));
        out.push_str(&format!(
            "{prefix}_processing_time_avg_us {:.2}\n",
            self.avg_processing_time_us()
        ));

        out
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.inner.events_received.store(0, Ordering::Relaxed);
        self.inner.events_emitted.store(0, Ordering::Relaxed);
        self.inner.events_filtered.store(0, Ordering::Relaxed);
        self.inner.events_late.store(0, Ordering::Relaxed);
        self.inner.processing_time_us.store(0, Ordering::Relaxed);
        self.inner.processing_count.store(0, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Serve the counters over HTTP/1.1 at `path`. Binding happens before returning, so a
/// taken address is reported to the caller, and the returned task serves scrapes until
/// it is aborted or dropped. The workspace has no HTTP server dependency and one
/// endpoint answering one route does not earn one.
pub async fn serve_metrics(
    bind: &str,
    path: &str,
    metrics: Metrics,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind).await?;
    let path = path.to_string();

    Ok(tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let path = path.clone();
            let metrics = metrics.clone();
            tokio::spawn(async move { answer_scrape(stream, &path, &metrics).await });
        }
    }))
}

async fn answer_scrape(mut stream: TcpStream, path: &str, metrics: &Metrics) {
    let Some(head) = read_request_head(&mut stream).await else {
        return;
    };
    let response = respond(&head, path, metrics);
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Read up to the blank line that ends the request head. A scrape carries no body.
async fn read_request_head(stream: &mut TcpStream) -> Option<String> {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];

    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..read]);
        if head.len() > REQUEST_HEAD_LIMIT {
            return None;
        }
    }

    String::from_utf8(head).ok()
}

/// The full HTTP response to one request head.
fn respond(head: &str, path: &str, metrics: &Metrics) -> String {
    let mut request_line = head.lines().next().unwrap_or_default().split_whitespace();
    let method = request_line.next().unwrap_or_default();
    let target = request_line.next().unwrap_or_default();
    let target = target.split('?').next().unwrap_or(target);

    if method != "GET" {
        return http_response("405 Method Not Allowed", "text/plain", "only GET is served");
    }
    if target != path {
        return http_response("404 Not Found", "text/plain", "no such path");
    }
    http_response(
        "200 OK",
        EXPOSITION_CONTENT_TYPE,
        &metrics.to_prometheus(METRICS_PREFIX),
    )
}

fn http_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let m = Metrics::new();
        m.inc_received();
        m.inc_received();
        m.inc_emitted();
        m.inc_filtered();
        m.inc_late();
        m.record_processing_time(100);
        m.record_processing_time(200);

        assert_eq!(m.events_received(), 2);
        assert_eq!(m.events_emitted(), 1);
        assert_eq!(m.events_filtered(), 1);
        assert_eq!(m.events_late(), 1);
        assert_eq!(m.avg_processing_time_us(), 150.0);
    }

    #[test]
    fn test_prometheus_format() {
        let m = Metrics::new();
        m.inc_received();
        let output = m.to_prometheus("fluvius");
        assert!(output.contains("fluvius_events_received_total 1"));
        assert!(output.contains("# TYPE fluvius_events_received_total counter"));
    }

    #[test]
    fn test_respond_serves_the_exposition_on_the_configured_path() {
        let m = Metrics::new();
        m.inc_received();

        let response = respond("GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n", "/metrics", &m);
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(response.contains(EXPOSITION_CONTENT_TYPE), "{response}");
        assert!(
            response.contains("fluvius_events_received_total 1"),
            "{response}"
        );

        // a scrape may carry a query string
        let with_query = respond("GET /metrics?x=1 HTTP/1.1\r\n\r\n", "/metrics", &m);
        assert!(
            with_query.starts_with("HTTP/1.1 200 OK\r\n"),
            "{with_query}"
        );
    }

    #[test]
    fn test_respond_rejects_other_paths_and_methods() {
        let m = Metrics::new();

        let other_path = respond("GET /admin HTTP/1.1\r\n\r\n", "/metrics", &m);
        assert!(other_path.starts_with("HTTP/1.1 404"), "{other_path}");

        let post = respond("POST /metrics HTTP/1.1\r\n\r\n", "/metrics", &m);
        assert!(post.starts_with("HTTP/1.1 405"), "{post}");

        let garbage = respond("", "/metrics", &m);
        assert!(garbage.starts_with("HTTP/1.1 405"), "{garbage}");
    }

    /// The body length has to match what a client is told to read.
    #[test]
    fn test_response_declares_its_body_length() {
        let response = http_response("200 OK", "text/plain", "body");
        assert!(response.contains("Content-Length: 4\r\n"), "{response}");
        assert!(response.ends_with("\r\n\r\nbody"), "{response}");
    }

    /// Ask the OS for a free address, then hand it to the server under test.
    async fn free_address() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        address.to_string()
    }

    async fn scrape(address: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: {address}\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn test_serve_metrics_answers_a_scrape_over_tcp() {
        let metrics = Metrics::new();
        metrics.inc_received();
        metrics.inc_received();
        metrics.inc_emitted();

        let address = free_address().await;
        let server = serve_metrics(&address, "/metrics", metrics)
            .await
            .expect("bind the metrics endpoint");

        let response = scrape(&address, "/metrics").await;
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
        assert!(
            response.contains("fluvius_events_received_total 2"),
            "{response}"
        );
        assert!(
            response.contains("fluvius_events_emitted_total 1"),
            "{response}"
        );

        // the endpoint keeps answering, one scrape does not end it
        let again = scrape(&address, "/nope").await;
        assert!(again.starts_with("HTTP/1.1 404"), "{again}");

        server.abort();
    }

    #[tokio::test]
    async fn test_serve_metrics_reports_an_address_it_cannot_bind() {
        let taken = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = taken.local_addr().unwrap().to_string();

        assert!(
            serve_metrics(&address, "/metrics", Metrics::new())
                .await
                .is_err()
        );
    }
}
