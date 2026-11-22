//! Process metrics, served in the Prometheus text format.
//!
//! A `Metrics` is a set of handles to shared counters. Cloning it copies
//! the handles, not the numbers, so every task keeps its own clone and all
//! of them add to the same totals.

use std::sync::Arc;
use std::time::Duration;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::{Registry, Unit};

use crate::risk::Venue;

/// The content type of `Metrics::render`.
pub const CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct VenueLabel {
    venue: &'static str,
}

impl From<Venue> for VenueLabel {
    fn from(venue: Venue) -> Self {
        Self {
            venue: venue.as_str(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EncodeLabelSet)]
struct KindLabel {
    kind: &'static str,
}

/// 10 microseconds to 1.3 seconds, doubling.
fn decision_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.000_01, 2.0, 18))
}

/// 1 millisecond to 8 seconds, doubling.
fn write_histogram() -> Histogram {
    Histogram::new(exponential_buckets(0.001, 2.0, 14))
}

#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    feed_to_decision: Family<VenueLabel, Histogram, fn() -> Histogram>,
    events: Family<KindLabel, Counter>,
    reconnects: Family<VenueLabel, Counter>,
    resyncs: Family<VenueLabel, Counter>,
    log_write: Histogram,
    log_write_failures: Counter,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let feed_to_decision =
            Family::new_with_constructor(decision_histogram as fn() -> Histogram);
        registry.register_with_unit(
            "feed_to_decision",
            "Time from reading a mark price frame off the socket to the engine \
             having rechecked every account against it",
            Unit::Seconds,
            feed_to_decision.clone(),
        );

        let events = Family::default();
        registry.register(
            "engine_events",
            "Account events the engine emitted",
            events.clone(),
        );

        let reconnects = Family::default();
        registry.register(
            "feed_reconnects",
            "Venue sessions that ended and were reconnected",
            reconnects.clone(),
        );

        let resyncs = Family::default();
        registry.register(
            "book_resyncs",
            "Books cleared and resubscribed after a desync",
            resyncs.clone(),
        );

        let log_write = write_histogram();
        registry.register_with_unit(
            "event_log_write",
            "Time to commit one batch of events to the event log",
            Unit::Seconds,
            log_write.clone(),
        );

        let log_write_failures = Counter::default();
        registry.register(
            "event_log_write_failures",
            "Event log writes that failed and were retried",
            log_write_failures.clone(),
        );

        Self {
            registry: Arc::new(registry),
            feed_to_decision,
            events,
            reconnects,
            resyncs,
            log_write,
            log_write_failures,
        }
    }

    /// The engine finished applying a mark that was read off the socket
    /// `elapsed` ago.
    pub fn mark_applied(&self, venue: Venue, elapsed: Duration) {
        self.feed_to_decision
            .get_or_create(&venue.into())
            .observe(elapsed.as_secs_f64());
    }

    pub fn event(&self, kind: &'static str) {
        self.events.get_or_create(&KindLabel { kind }).inc();
    }

    pub fn reconnect(&self, venue: Venue) {
        self.reconnects.get_or_create(&venue.into()).inc();
    }

    pub fn resync(&self, venue: Venue) {
        self.resyncs.get_or_create(&venue.into()).inc();
    }

    pub fn log_written(&self, elapsed: Duration) {
        self.log_write.observe(elapsed.as_secs_f64());
    }

    pub fn log_write_failed(&self) {
        self.log_write_failures.inc();
    }

    /// Every metric in the OpenMetrics text format.
    pub fn render(&self) -> String {
        let mut text = String::new();
        // Writing to a `String` cannot fail.
        let _ = encode(&mut text, &self.registry);
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clones_add_to_the_same_totals() {
        let metrics = Metrics::new();
        let clone = metrics.clone();
        metrics.reconnect(Venue::Okx);
        clone.reconnect(Venue::Okx);
        clone.event("filled");
        clone.mark_applied(Venue::Hyperliquid, Duration::from_micros(300));

        let text = metrics.render();
        assert!(
            text.contains("feed_reconnects_total{venue=\"okx\"} 2"),
            "{text}"
        );
        assert!(
            text.contains("engine_events_total{kind=\"filled\"} 1"),
            "{text}"
        );
        assert!(
            text.contains("feed_to_decision_seconds_count{venue=\"hyperliquid\"} 1"),
            "{text}"
        );
        // 300 microseconds lands in the 320 microsecond bucket, not the 160.
        assert!(
            text.contains(
                "feed_to_decision_seconds_bucket{le=\"0.00016\",venue=\"hyperliquid\"} 0"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "feed_to_decision_seconds_bucket{le=\"0.00032\",venue=\"hyperliquid\"} 1"
            ),
            "{text}"
        );
    }
}
