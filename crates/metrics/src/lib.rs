//! Shared Prometheus metrics support for Rayflow processes.
//!
//! [`Metrics`] owns an isolated Prometheus registry and can be cloned into the
//! components that update metrics. Metric handles returned by [`Metrics::register`]
//! are backed by atomics, so recording does not require taking the registry lock.
//! The registry lock is only held while metrics are registered or encoded for a
//! scrape request.

use std::{
    fmt,
    sync::{Arc, RwLock},
};

use prometheus_client::{
    encoding::{EncodeLabelSet, text},
    metrics::gauge::Gauge,
    metrics::{counter::Counter, family::Family, histogram::Histogram},
    registry::{Metric, Registry, Unit},
};

/// Content type used by the Prometheus text exposition format.
pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// An isolated registry that can be shared by a Rayflow process's components.
///
/// A registry is intentionally owned by this value rather than being global.
/// This keeps separate server, pilot, and agent processes independent and lets
/// tests create a fresh registry without relying on global state.
#[derive(Clone, Debug, Default)]
pub struct Metrics {
    registry: Arc<RwLock<Registry>>,
}

impl Metrics {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a registry with the common process build metric installed.
    pub fn with_build_info() -> Self {
        let metrics = Self::new();
        let build_info = metrics.register(
            "rayflow_build_info",
            "Build information for this Rayflow process",
            Gauge::<i64>::default(),
        );
        build_info.set(1);
        metrics
    }

    /// Registers the standard HTTP request counter and duration histogram.
    pub fn register_http_metrics(&self) -> HttpMetrics {
        HttpMetrics {
            requests: self.register(
                "rayflow_http_requests",
                "HTTP requests handled by this Rayflow process",
                Family::default(),
            ),
            duration: self.register(
                "rayflow_http_request_duration_seconds",
                "HTTP request duration in seconds",
                Family::new_with_constructor(http_duration_histogram as fn() -> Histogram),
            ),
        }
    }

    /// Registers a metric and returns a handle that can be updated by callers.
    ///
    /// The metric is cloned so the returned handle and the registry retain the
    /// same underlying metric state. Metric implementations supplied by
    /// `prometheus-client` use atomics or internal locks for updates.
    pub fn register<M>(&self, name: impl Into<String>, help: impl Into<String>, metric: M) -> M
    where
        M: Metric + Clone,
    {
        let handle = metric.clone();
        self.registry
            .write()
            .expect("metrics registry lock poisoned")
            .register(name, help, metric);
        handle
    }

    /// Registers a metric with an explicit OpenMetrics unit.
    pub fn register_with_unit<M>(
        &self,
        name: impl Into<String>,
        help: impl Into<String>,
        unit: Unit,
        metric: M,
    ) -> M
    where
        M: Metric + Clone,
    {
        let handle = metric.clone();
        self.registry
            .write()
            .expect("metrics registry lock poisoned")
            .register_with_unit(name, help, unit, metric);
        handle
    }

    /// Runs a registration operation while holding the registry write lock.
    ///
    /// This is useful for advanced collectors or sub-registries that need an
    /// API not covered by [`Metrics::register`]. The closure should only mutate
    /// registration state and must not retain references to the registry.
    pub fn with_registry<R>(&self, operation: impl FnOnce(&mut Registry) -> R) -> R {
        let mut registry = self
            .registry
            .write()
            .expect("metrics registry lock poisoned");
        operation(&mut registry)
    }

    /// Encodes all registered metrics in Prometheus's text exposition format.
    pub fn encode(&self) -> Result<String, fmt::Error> {
        let mut output = String::new();
        self.encode_into(&mut output)?;
        Ok(output)
    }

    /// Encodes all registered metrics into a formatter.
    pub fn encode_into<W>(&self, writer: &mut W) -> Result<(), fmt::Error>
    where
        W: fmt::Write,
    {
        let registry = self
            .registry
            .read()
            .expect("metrics registry lock poisoned");
        text::encode(writer, &registry)
    }
}

/// Standard HTTP metrics shared by the server exporter and middleware.
#[derive(Clone, Debug)]
pub struct HttpMetrics {
    requests: Family<EndpointLabels, Counter>,
    duration: Family<EndpointLabels, Histogram, fn() -> Histogram>,
}

fn http_duration_histogram() -> Histogram {
    Histogram::new([
        0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    ])
}

impl HttpMetrics {
    /// Records one completed request.
    pub fn observe(
        &self,
        method: impl Into<String>,
        route: impl Into<String>,
        status: u16,
        seconds: f64,
    ) {
        let labels = EndpointLabels {
            method: method.into(),
            route: route.into(),
            status: status.to_string(),
        };
        self.requests.get_or_create(&labels).inc();
        self.duration.get_or_create(&labels).observe(seconds);
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct EndpointLabels {
    method: String,
    route: String,
    status: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_client::metrics::{counter::Counter, gauge::Gauge};

    #[test]
    fn encoding_an_empty_registry_is_valid_prometheus_text() {
        let metrics = Metrics::new();

        assert_eq!(metrics.encode().unwrap(), "# EOF\n");
    }

    #[test]
    fn registered_metric_updates_are_encoded() {
        let metrics = Metrics::new();
        let requests = metrics.register(
            "requests",
            "Total requests processed",
            Counter::<u64>::default(),
        );
        let in_flight = metrics.register("in_flight", "Current requests", Gauge::<i64>::default());

        requests.inc_by(3);
        in_flight.set(2);

        let encoded = metrics.encode().unwrap();
        assert!(encoded.contains("# HELP requests Total requests processed.\n"));
        assert!(encoded.contains("# TYPE requests counter\n"));
        assert!(encoded.contains("requests_total 3\n"));
        assert!(encoded.contains("# HELP in_flight Current requests.\n"));
        assert!(encoded.contains("# TYPE in_flight gauge\n"));
        assert!(encoded.contains("in_flight 2\n"));
        assert!(encoded.ends_with("# EOF\n"));
    }

    #[test]
    fn cloned_metrics_share_registry_and_handles() {
        let metrics = Metrics::new();
        let clone = metrics.clone();
        let counter = metrics.register("events", "Events observed", Counter::<u64>::default());

        counter.inc();

        assert_eq!(clone.encode().unwrap(), metrics.encode().unwrap());
        assert!(clone.encode().unwrap().contains("events_total 1\n"));
    }

    #[test]
    fn build_info_is_available_when_requested() {
        let encoded = Metrics::with_build_info().encode().unwrap();
        assert!(encoded.contains("rayflow_build_info 1\n"));
    }

    #[test]
    fn http_metrics_encode_bounded_endpoint_labels_and_duration() {
        let metrics = Metrics::new();
        let http = metrics.register_http_metrics();
        http.observe("GET", "/health", 200, 0.125);
        let encoded = metrics.encode().unwrap();
        assert!(encoded.contains(
            "rayflow_http_requests_total{method=\"GET\",route=\"/health\",status=\"200\"} 1"
        ));
        assert!(encoded.contains("rayflow_http_request_duration_seconds_count{method=\"GET\",route=\"/health\",status=\"200\"} 1"));
    }
}
