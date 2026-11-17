#![expect(
    dead_code,
    reason = "support helpers are used by only some test targets"
)]

//! In-memory OpenTelemetry metric helpers for integration tests.

pub mod fake_s3;

use std::sync::Arc;

use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{
    AggregatedMetrics, GaugeDataPoint, HistogramDataPoint, Metric, MetricData, ResourceMetrics,
    ScopeMetrics, SumDataPoint,
};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use s3_sidecar::metrics::Metrics;

/// A test meter, exporter, and provider. Keep this value alive for the test.
#[derive(Clone)]
pub struct TestMetrics {
    /// Metrics that the sidecar records to.
    pub metrics: Arc<Metrics>,
    exporter: InMemoryMetricExporter,
    provider: SdkMeterProvider,
}

impl TestMetrics {
    /// Build a test meter that exports to memory.
    #[must_use]
    pub fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let metrics = Arc::new(Metrics::new(&provider.meter("test")));
        Self {
            metrics,
            exporter,
            provider,
        }
    }

    /// Flush the meter and return the collected metrics.
    #[must_use]
    pub fn collect(&self) -> Vec<ResourceMetrics> {
        self.provider.force_flush().unwrap();
        self.exporter.get_finished_metrics().unwrap()
    }
}

impl Default for TestMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Find one metric by name.
#[must_use]
pub fn metric<'a>(data: &'a [ResourceMetrics], name: &str) -> Option<&'a Metric> {
    data.iter()
        .flat_map(ResourceMetrics::scope_metrics)
        .flat_map(ScopeMetrics::metrics)
        .find(|metric| metric.name() == name)
}

/// Return the value of a `u64` counter, summed over all data points.
#[must_use]
pub fn counter(data: &[ResourceMetrics], name: &str) -> u64 {
    let Some(metric) = metric(data, name) else {
        return 0;
    };
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        return 0;
    };
    sum.data_points()
        .map(SumDataPoint::value)
        .fold(0, u64::saturating_add)
}

/// Return the value of a `u64` counter with one matching attribute.
#[must_use]
pub fn counter_with(data: &[ResourceMetrics], name: &str, key: &str, value: &str) -> u64 {
    let Some(metric) = metric(data, name) else {
        return 0;
    };
    let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() else {
        return 0;
    };
    sum.data_points()
        .filter(|point| {
            point
                .attributes()
                .any(|attribute| attribute.key.as_str() == key && attribute.value.as_str() == value)
        })
        .map(SumDataPoint::value)
        .fold(0, u64::saturating_add)
}

/// Return the last value of a `u64` gauge.
#[must_use]
pub fn gauge(data: &[ResourceMetrics], name: &str) -> u64 {
    let Some(metric) = metric(data, name) else {
        return 0;
    };
    let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
        return 0;
    };
    gauge
        .data_points()
        .map(GaugeDataPoint::value)
        .next()
        .unwrap_or(0)
}

/// Return the most recent value of a `u64` gauge across all exports.
#[must_use]
pub fn latest_gauge(data: &[ResourceMetrics], name: &str) -> u64 {
    let mut latest = None;
    for resource in data {
        for scope in resource.scope_metrics() {
            for item in scope.metrics() {
                if item.name() == name
                    && let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = item.data()
                {
                    for point in gauge.data_points() {
                        latest = Some(point.value());
                    }
                }
            }
        }
    }
    latest.unwrap_or(0)
}

/// Return the last value of a `u64` gauge with one matching attribute.
#[must_use]
pub fn gauge_with(data: &[ResourceMetrics], name: &str, key: &str, value: &str) -> u64 {
    let Some(metric) = metric(data, name) else {
        return 0;
    };
    let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
        return 0;
    };
    gauge
        .data_points()
        .find(|point| {
            point
                .attributes()
                .any(|attribute| attribute.key.as_str() == key && attribute.value.as_str() == value)
        })
        .map_or(0, GaugeDataPoint::value)
}

/// Return the sample count of a histogram.
#[must_use]
pub fn histogram_count(data: &[ResourceMetrics], name: &str) -> u64 {
    let Some(metric) = metric(data, name) else {
        return 0;
    };
    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
        return 0;
    };
    histogram
        .data_points()
        .map(HistogramDataPoint::count)
        .fold(0, u64::saturating_add)
}

/// Return the sum of the values recorded in a histogram.
#[must_use]
pub fn histogram_sum(data: &[ResourceMetrics], name: &str) -> f64 {
    let Some(metric) = metric(data, name) else {
        return 0.0;
    };
    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
        return 0.0;
    };
    histogram.data_points().map(HistogramDataPoint::sum).sum()
}
