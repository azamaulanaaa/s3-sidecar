//! Per-function OpenTelemetry instruments.

use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};

/// Instruments for the cache and cluster data paths.
#[derive(Debug)]
pub struct Metrics {
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    bytes_served: Counter<u64>,
    origin_bytes: Counter<u64>,
    revalidations: Counter<u64>,
    sync_success: Counter<u64>,
    sync_failure: Counter<u64>,
    quarantine: Counter<u64>,
    evictions: Counter<u64>,
    fallbacks: Counter<u64>,
    pending_writes: Gauge<u64>,
    sync_lag: Gauge<u64>,
    tracked_bytes: Gauge<u64>,
    cache_max_bytes: Gauge<u64>,
    cache_unlimited: Gauge<u64>,
    cluster_committed_writes: Counter<u64>,
    cluster_aborted_writes: Counter<u64>,
    cluster_read_local_hits: Counter<u64>,
    cluster_missing_blob_reads: Counter<u64>,
    cluster_gc_deletions: Counter<u64>,
    cluster_read_index_calls: Counter<u64>,
    cluster_peer_fetches: Counter<u64>,
    cluster_read_repairs: Counter<u64>,
    cluster_replication_success: Counter<u64>,
    cluster_replication_failures: Counter<u64>,
    cluster_replication_queue_depth: Gauge<u64>,
    cluster_replication_oldest_age: Gauge<u64>,
    cluster_reconcile_checked: Counter<u64>,
    cluster_reconcile_present: Counter<u64>,
    cluster_reconcile_missing: Counter<u64>,
    cluster_owner_blobs_repaired: Counter<u64>,
    cluster_owner_blobs_unrecoverable: Counter<u64>,
    cluster_write_ack: Counter<u64>,
    cluster_quorum_success: Counter<u64>,
    cluster_quorum_failure: Counter<u64>,
    cluster_commit_duration: Histogram<f64>,
    cluster_sync_replication_duration: Histogram<f64>,
    cluster_replica_acks: Counter<u64>,
    cluster_replica_ack_failures: Counter<u64>,
    cluster_tombstones: Gauge<u64>,
    cluster_applied_index: Gauge<u64>,
    cluster_term: Gauge<u64>,
    raft_term: Gauge<u64>,
    raft_last_log_index: Gauge<u64>,
    raft_last_applied: Gauge<u64>,
    raft_snapshot_index: Gauge<u64>,
    raft_elections: Counter<u64>,
    raft_leader: Gauge<u64>,
    consul_session_renewals: Counter<u64>,
    consul_session_losses: Counter<u64>,
    consul_leader_changes: Counter<u64>,
    consul_materialized: Gauge<u64>,
    consul_leader: Gauge<u64>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new(&opentelemetry::global::meter("s3-sidecar-noop"))
    }
}

impl Metrics {
    /// Build the instruments from one meter.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn new(meter: &Meter) -> Self {
        Self {
            cache_hits: counter(meter, "s3_sidecar.cache.hits", "Cache hits."),
            cache_misses: counter(meter, "s3_sidecar.cache.misses", "Cache misses."),
            bytes_served: bytes_counter(
                meter,
                "s3_sidecar.cache.bytes_served",
                "Bytes sent to clients from the cache.",
            ),
            origin_bytes: bytes_counter(
                meter,
                "s3_sidecar.cache.origin_bytes",
                "Bytes fetched from origin.",
            ),
            revalidations: counter(
                meter,
                "s3_sidecar.cache.revalidations",
                "Conditional origin requests that returned 304.",
            ),
            sync_success: counter(
                meter,
                "s3_sidecar.cache.sync.success",
                "Syncs that reached origin.",
            ),
            sync_failure: counter(
                meter,
                "s3_sidecar.cache.sync.failure",
                "Sync attempts that failed.",
            ),
            quarantine: counter(
                meter,
                "s3_sidecar.cache.quarantine",
                "Entries moved to quarantine.",
            ),
            evictions: counter(
                meter,
                "s3_sidecar.cache.evictions",
                "Cache entries removed by eviction.",
            ),
            fallbacks: counter(
                meter,
                "s3_sidecar.cache.fallbacks",
                "Cache failures that fell back to origin.",
            ),
            pending_writes: gauge(
                meter,
                "s3_sidecar.cache.pending_writes",
                "Pending write and delete journal entries.",
                "1",
            ),
            sync_lag: gauge(
                meter,
                "s3_sidecar.cache.sync_lag",
                "Age of the oldest pending journal entry.",
                "s",
            ),
            tracked_bytes: gauge(
                meter,
                "s3_sidecar.cache.tracked_bytes",
                "Bytes tracked by the LRU index.",
                "By",
            ),
            cache_max_bytes: gauge(
                meter,
                "s3_sidecar.cache.max_bytes",
                "Configured cache limit. Zero means unlimited.",
                "By",
            ),
            cache_unlimited: gauge(
                meter,
                "s3_sidecar.cache.unlimited",
                "One when the cache has no size limit.",
                "1",
            ),
            cluster_committed_writes: counter(
                meter,
                "s3_sidecar.cluster.committed_writes",
                "Cluster writes that reached the committed state.",
            ),
            cluster_aborted_writes: counter(
                meter,
                "s3_sidecar.cluster.aborted_writes",
                "Cluster writes that were rejected and removed.",
            ),
            cluster_read_local_hits: counter(
                meter,
                "s3_sidecar.cluster.read_local_hits",
                "Reads served from local metadata and a local blob.",
            ),
            cluster_missing_blob_reads: counter(
                meter,
                "s3_sidecar.cluster.missing_blob_reads",
                "Reads where the blob was missing.",
            ),
            cluster_gc_deletions: counter(
                meter,
                "s3_sidecar.cluster.gc_deletions",
                "Blobs removed by cluster garbage collection.",
            ),
            cluster_read_index_calls: counter(
                meter,
                "s3_sidecar.cluster.read_index_calls",
                "Calls to the coordinator read index.",
            ),
            cluster_peer_fetches: counter(
                meter,
                "s3_sidecar.cluster.peer_fetches",
                "Reads served by a peer blob fetch.",
            ),
            cluster_read_repairs: counter(
                meter,
                "s3_sidecar.cluster.read_repairs",
                "Peer reads that stored the blob locally.",
            ),
            cluster_replication_success: counter(
                meter,
                "s3_sidecar.cluster.replication.success",
                "Replication operations that reached a target.",
            ),
            cluster_replication_failures: counter(
                meter,
                "s3_sidecar.cluster.replication.failures",
                "Replication failures by status.",
            ),
            cluster_replication_queue_depth: gauge(
                meter,
                "s3_sidecar.cluster.replication.queue_depth",
                "Pending replication jobs.",
                "1",
            ),
            cluster_replication_oldest_age: gauge(
                meter,
                "s3_sidecar.cluster.replication.oldest_age",
                "Age of the oldest pending replication job.",
                "s",
            ),
            cluster_reconcile_checked: counter(
                meter,
                "s3_sidecar.cluster.reconcile.checked",
                "Replica targets checked by reconciliation.",
            ),
            cluster_reconcile_present: counter(
                meter,
                "s3_sidecar.cluster.reconcile.present",
                "Replica targets present during reconciliation.",
            ),
            cluster_reconcile_missing: counter(
                meter,
                "s3_sidecar.cluster.reconcile.missing",
                "Replica targets missing during reconciliation.",
            ),
            cluster_owner_blobs_repaired: counter(
                meter,
                "s3_sidecar.cluster.owner_blobs.repaired",
                "Owner blobs restored by reconciliation.",
            ),
            cluster_owner_blobs_unrecoverable: counter(
                meter,
                "s3_sidecar.cluster.owner_blobs.unrecoverable",
                "Owner blobs that no replica could supply.",
            ),
            cluster_write_ack: counter(
                meter,
                "s3_sidecar.cluster.write_ack",
                "Accepted writes by acknowledgement policy.",
            ),
            cluster_quorum_success: counter(
                meter,
                "s3_sidecar.cluster.quorum.success",
                "Quorum writes that reached the required acknowledgements.",
            ),
            cluster_quorum_failure: counter(
                meter,
                "s3_sidecar.cluster.quorum.failure",
                "Quorum writes that did not reach the required acknowledgements.",
            ),
            cluster_commit_duration: histogram(
                meter,
                "s3_sidecar.cluster.commit.duration",
                "Duration of one cluster commit.",
            ),
            cluster_sync_replication_duration: histogram(
                meter,
                "s3_sidecar.cluster.sync_replication.duration",
                "Duration of one synchronous peer copy.",
            ),
            cluster_replica_acks: counter(
                meter,
                "s3_sidecar.cluster.replica_acks",
                "Peer copies that acknowledged the blob.",
            ),
            cluster_replica_ack_failures: counter(
                meter,
                "s3_sidecar.cluster.replica_ack_failures",
                "Peer copies that failed or timed out.",
            ),
            cluster_tombstones: gauge(
                meter,
                "s3_sidecar.cluster.tombstones",
                "Current cluster tombstone count.",
                "1",
            ),
            cluster_applied_index: gauge(
                meter,
                "s3_sidecar.cluster.applied_index",
                "Applied metadata index.",
                "1",
            ),
            cluster_term: gauge(
                meter,
                "s3_sidecar.cluster.term",
                "Current coordinator term.",
                "1",
            ),
            raft_term: gauge(meter, "s3_sidecar.raft.term", "Current raft term.", "1"),
            raft_last_log_index: gauge(
                meter,
                "s3_sidecar.raft.last_log_index",
                "Last raft log index.",
                "1",
            ),
            raft_last_applied: gauge(
                meter,
                "s3_sidecar.raft.last_applied",
                "Last applied raft log index.",
                "1",
            ),
            raft_snapshot_index: gauge(
                meter,
                "s3_sidecar.raft.snapshot_index",
                "Last snapshot raft log index.",
                "1",
            ),
            raft_elections: counter(
                meter,
                "s3_sidecar.raft.elections",
                "Observed raft leader elections.",
            ),
            raft_leader: gauge(
                meter,
                "s3_sidecar.raft.leader",
                "Current raft leader. One means the named node.",
                "1",
            ),
            consul_session_renewals: counter(
                meter,
                "s3_sidecar.consul.session.renewals",
                "Successful Consul session renewals.",
            ),
            consul_session_losses: counter(
                meter,
                "s3_sidecar.consul.session.losses",
                "Consul sessions that ended or failed to renew.",
            ),
            consul_leader_changes: counter(
                meter,
                "s3_sidecar.consul.leader.changes",
                "Consul leadership acquisitions.",
            ),
            consul_materialized: gauge(
                meter,
                "s3_sidecar.consul.materialized",
                "One when the first Consul reconcile completed.",
                "1",
            ),
            consul_leader: gauge(
                meter,
                "s3_sidecar.consul.leader",
                "Current Consul leader. One means the named node.",
                "1",
            ),
        }
    }

    /// Record a cache hit.
    pub fn hit(&self) {
        self.cache_hits.add(1, &[]);
    }

    /// Record a cache miss.
    pub fn miss(&self) {
        self.cache_misses.add(1, &[]);
    }

    /// Add bytes served from the cache.
    pub fn add_served_bytes(&self, bytes: u64) {
        self.bytes_served.add(bytes, &[]);
    }

    /// Add bytes fetched from origin.
    pub fn add_origin_bytes(&self, bytes: u64) {
        self.origin_bytes.add(bytes, &[]);
    }

    /// Record one revalidation.
    pub fn revalidated(&self) {
        self.revalidations.add(1, &[]);
    }

    /// Record one sync success.
    pub fn synced(&self) {
        self.sync_success.add(1, &[]);
    }

    /// Record one sync failure.
    pub fn sync_failed(&self) {
        self.sync_failure.add(1, &[]);
    }

    /// Record one quarantine.
    pub fn quarantined(&self) {
        self.quarantine.add(1, &[]);
    }

    /// Record one eviction.
    pub fn evicted(&self) {
        self.evictions.add(1, &[]);
    }

    /// Record one fallback to origin.
    pub fn fallback(&self) {
        self.fallbacks.add(1, &[]);
    }

    /// Set the cache size limit gauges.
    pub fn set_cache_limit(&self, max_bytes: u64, unlimited: bool) {
        self.cache_max_bytes.record(max_bytes, &[]);
        self.cache_unlimited.record(u64::from(unlimited), &[]);
    }

    /// Set the tracked byte gauge.
    pub fn set_tracked(&self, bytes: u64) {
        self.tracked_bytes.record(bytes, &[]);
    }

    /// Set the pending write and sync lag gauges.
    pub fn set_pending_stats(&self, pending: u64, lag_seconds: u64) {
        self.pending_writes.record(pending, &[]);
        self.sync_lag.record(lag_seconds, &[]);
    }

    /// Record one committed cluster write.
    pub fn committed_write(&self) {
        self.cluster_committed_writes.add(1, &[]);
    }

    /// Record one rejected and removed cluster write.
    pub fn aborted_write(&self) {
        self.cluster_aborted_writes.add(1, &[]);
    }

    /// Record the latency of one commit.
    pub fn commit_latency(&self, elapsed: Duration) {
        self.cluster_commit_duration.record(millis(elapsed), &[]);
    }

    /// Record one local cluster read.
    pub fn cluster_read_local(&self) {
        self.cluster_read_local_hits.add(1, &[]);
    }

    /// Record one committed read with a missing blob.
    pub fn missing_blob(&self) {
        self.cluster_missing_blob_reads.add(1, &[]);
    }

    /// Record one garbage collection deletion.
    pub fn gc_deleted(&self) {
        self.cluster_gc_deletions.add(1, &[]);
    }

    /// Record one `read_index` call.
    pub fn read_index_called(&self) {
        self.cluster_read_index_calls.add(1, &[]);
    }

    /// Record one peer blob fetch.
    pub fn peer_fetch(&self) {
        self.cluster_peer_fetches.add(1, &[]);
    }

    /// Record one read-repair write.
    pub fn read_repaired(&self) {
        self.cluster_read_repairs.add(1, &[]);
    }

    /// Record one successful replication operation.
    pub fn replication_success(&self) {
        self.cluster_replication_success.add(1, &[]);
    }

    /// Record one replication failure by status or transport class.
    pub fn replication_failure(&self, status: &str) {
        self.cluster_replication_failures
            .add(1, &[KeyValue::new("status", status.to_owned())]);
    }

    /// Set the replication queue gauges.
    pub fn set_replication_queue(&self, depth: u64, oldest_age_seconds: u64) {
        self.cluster_replication_queue_depth.record(depth, &[]);
        self.cluster_replication_oldest_age
            .record(oldest_age_seconds, &[]);
    }

    /// Record one reconciliation pass.
    pub fn set_reconcile(&self, checked: u64, present: u64, missing: u64) {
        self.cluster_reconcile_checked.add(checked, &[]);
        self.cluster_reconcile_present.add(present, &[]);
        self.cluster_reconcile_missing.add(missing, &[]);
    }

    /// Record one owner blob restored by reconciliation.
    pub fn owner_blob_repaired(&self) {
        self.cluster_owner_blobs_repaired.add(1, &[]);
    }

    /// Record one owner blob that no replica could supply.
    pub fn owner_blob_unrecoverable(&self) {
        self.cluster_owner_blobs_unrecoverable.add(1, &[]);
    }

    /// Count one write attempt with an acknowledgement policy.
    pub fn write_ack(&self, policy: &str) {
        self.cluster_write_ack
            .add(1, &[KeyValue::new("policy", policy.to_owned())]);
    }

    /// Record one quorum write that reached the required acknowledgements.
    pub fn quorum_success(&self) {
        self.cluster_quorum_success.add(1, &[]);
    }

    /// Record one quorum write that did not reach the required acknowledgements.
    pub fn quorum_failure(&self) {
        self.cluster_quorum_failure.add(1, &[]);
    }

    /// Record the latency of one synchronous peer copy.
    pub fn sync_replication(&self, elapsed: Duration) {
        self.cluster_sync_replication_duration
            .record(millis(elapsed), &[]);
    }

    /// Record one peer that acknowledged a blob.
    pub fn replica_ack(&self) {
        self.cluster_replica_acks.add(1, &[]);
    }

    /// Record one peer copy that failed or timed out.
    pub fn replica_ack_failure(&self) {
        self.cluster_replica_ack_failures.add(1, &[]);
    }

    /// Record one successful Consul session renewal.
    pub fn consul_session_renewed(&self) {
        self.consul_session_renewals.add(1, &[]);
    }

    /// Record one lost Consul session.
    pub fn consul_session_lost(&self) {
        self.consul_session_losses.add(1, &[]);
    }

    /// Record one Consul leadership acquisition.
    pub fn consul_leader_changed(&self) {
        self.consul_leader_changes.add(1, &[]);
    }

    /// Set the raft leader name gauge.
    pub fn set_raft_leader(&self, leader: Option<&str>) {
        record_leader(&self.raft_leader, leader);
    }

    /// Set the Consul leader name gauge.
    pub fn set_consul_leader(&self, leader: Option<&str>) {
        record_leader(&self.consul_leader, leader);
    }

    /// Set the raft state gauges.
    pub fn set_raft_state(&self, term: u64, last_log: u64, applied: u64, snapshot: u64) {
        self.raft_term.record(term, &[]);
        self.raft_last_log_index.record(last_log, &[]);
        self.raft_last_applied.record(applied, &[]);
        self.raft_snapshot_index.record(snapshot, &[]);
    }

    /// Record one observed raft leader election.
    pub fn add_raft_election(&self) {
        self.raft_elections.add(1, &[]);
    }

    /// Set the Consul materialized gauge.
    pub fn set_consul_materialized(&self, materialized: bool) {
        self.consul_materialized
            .record(u64::from(materialized), &[]);
    }

    /// Set the coordinator gauges.
    pub fn set_cluster_state(&self, applied_index: u64, term: u64, tombstones: u64) {
        self.cluster_applied_index.record(applied_index, &[]);
        self.cluster_term.record(term, &[]);
        self.cluster_tombstones.record(tombstones, &[]);
    }
}

fn counter(meter: &Meter, name: &'static str, description: &'static str) -> Counter<u64> {
    meter
        .u64_counter(name)
        .with_description(description)
        .with_unit("1")
        .build()
}

fn bytes_counter(meter: &Meter, name: &'static str, description: &'static str) -> Counter<u64> {
    meter
        .u64_counter(name)
        .with_description(description)
        .with_unit("By")
        .build()
}

fn gauge(
    meter: &Meter,
    name: &'static str,
    description: &'static str,
    unit: &'static str,
) -> Gauge<u64> {
    meter
        .u64_gauge(name)
        .with_description(description)
        .with_unit(unit)
        .build()
}

fn histogram(meter: &Meter, name: &'static str, description: &'static str) -> Histogram<f64> {
    meter
        .f64_histogram(name)
        .with_description(description)
        .with_unit("ms")
        .build()
}

fn millis(elapsed: Duration) -> f64 {
    // The histogram stores floating-point milliseconds. Precision loss above
    // 2^53 ms is acceptable for a latency value.
    #[allow(clippy::cast_precision_loss)]
    let value = elapsed.as_secs_f64() * 1_000.0;
    value
}

fn record_leader(gauge: &Gauge<u64>, leader: Option<&str>) {
    match leader {
        Some(leader) if !leader.is_empty() => {
            gauge.record(1, &[KeyValue::new("id", leader.to_owned())]);
        }
        _ => {
            gauge.record(0, &[KeyValue::new("id", String::new())]);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::data::{
        AggregatedMetrics, GaugeDataPoint, MetricData, ResourceMetrics, ScopeMetrics, SumDataPoint,
    };
    use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

    use super::*;

    fn test_metrics() -> (Metrics, InMemoryMetricExporter, SdkMeterProvider) {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let metrics = Metrics::new(&provider.meter("test"));
        (metrics, exporter, provider)
    }

    fn collect(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<ResourceMetrics> {
        provider.force_flush().unwrap();
        exporter.get_finished_metrics().unwrap()
    }

    fn metric<'a>(data: &'a [ResourceMetrics], name: &str) -> &'a MetricData<u64> {
        data.iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .find(|metric| metric.name() == name)
            .map(|metric| match metric.data() {
                AggregatedMetrics::U64(data) => data,
                AggregatedMetrics::F64(_) | AggregatedMetrics::I64(_) => {
                    panic!("the metric is not a u64 metric")
                }
            })
            .unwrap()
    }

    fn counter_value(data: &[ResourceMetrics], name: &str, key: &str, value: &str) -> u64 {
        let MetricData::Sum(sum) = metric(data, name) else {
            panic!("the metric is not a sum")
        };
        sum.data_points()
            .filter(|point| {
                point.attributes().any(|attribute| {
                    attribute.key.as_str() == key && attribute.value.as_str() == value
                })
            })
            .map(SumDataPoint::value)
            .sum()
    }

    fn counter_total(data: &[ResourceMetrics], name: &str) -> u64 {
        let MetricData::Sum(sum) = metric(data, name) else {
            panic!("the metric is not a sum")
        };
        sum.data_points().map(SumDataPoint::value).sum()
    }

    fn gauge_value(data: &[ResourceMetrics], name: &str) -> u64 {
        let MetricData::Gauge(gauge) = metric(data, name) else {
            panic!("the metric is not a gauge")
        };
        gauge
            .data_points()
            .map(GaugeDataPoint::value)
            .next()
            .unwrap()
    }

    #[test]
    fn counter_increments() {
        let (metrics, exporter, provider) = test_metrics();
        metrics.hit();
        metrics.hit();
        let data = collect(&provider, &exporter);
        assert_eq!(counter_total(&data, "s3_sidecar.cache.hits"), 2);
    }

    #[test]
    fn attribute_counter_splits_by_policy_and_status() {
        let (metrics, exporter, provider) = test_metrics();
        metrics.write_ack("leader");
        metrics.write_ack("quorum");
        metrics.write_ack("quorum");
        metrics.replication_failure("timeout");
        metrics.replication_failure("transport");
        let data = collect(&provider, &exporter);
        assert_eq!(
            counter_value(&data, "s3_sidecar.cluster.write_ack", "policy", "leader"),
            1
        );
        assert_eq!(
            counter_value(&data, "s3_sidecar.cluster.write_ack", "policy", "quorum"),
            2
        );
        assert_eq!(
            counter_value(
                &data,
                "s3_sidecar.cluster.replication.failures",
                "status",
                "timeout"
            ),
            1
        );
    }

    #[test]
    fn gauge_records_the_last_value() {
        let (metrics, exporter, provider) = test_metrics();
        metrics.set_tracked(10);
        metrics.set_tracked(42);
        let data = collect(&provider, &exporter);
        assert_eq!(gauge_value(&data, "s3_sidecar.cache.tracked_bytes"), 42);
    }

    #[test]
    fn histogram_records_a_duration() {
        let (metrics, exporter, provider) = test_metrics();
        metrics.commit_latency(Duration::from_millis(25));
        let data = collect(&provider, &exporter);
        data.iter()
            .flat_map(ResourceMetrics::scope_metrics)
            .flat_map(ScopeMetrics::metrics)
            .find(|metric| metric.name() == "s3_sidecar.cluster.commit.duration")
            .map(|metric| {
                let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data() else {
                    panic!("the metric is not a histogram")
                };
                let point = histogram.data_points().next().unwrap();
                assert_eq!(point.count(), 1);
                assert!((point.sum() - 25.0).abs() < f64::EPSILON);
            })
            .unwrap();
    }

    #[test]
    fn noop_meter_accepts_every_method() {
        let metrics = Metrics::default();
        metrics.hit();
        metrics.miss();
        metrics.add_served_bytes(1);
        metrics.add_origin_bytes(1);
        metrics.revalidated();
        metrics.synced();
        metrics.sync_failed();
        metrics.quarantined();
        metrics.evicted();
        metrics.fallback();
        metrics.set_cache_limit(1, false);
        metrics.set_tracked(1);
        metrics.set_pending_stats(1, 1);
        metrics.committed_write();
        metrics.aborted_write();
        metrics.commit_latency(Duration::from_millis(1));
        metrics.cluster_read_local();
        metrics.missing_blob();
        metrics.gc_deleted();
        metrics.read_index_called();
        metrics.peer_fetch();
        metrics.read_repaired();
        metrics.replication_success();
        metrics.replication_failure("transport");
        metrics.set_replication_queue(1, 1);
        metrics.set_reconcile(1, 1, 1);
        metrics.owner_blob_repaired();
        metrics.owner_blob_unrecoverable();
        metrics.write_ack("leader");
        metrics.quorum_success();
        metrics.quorum_failure();
        metrics.sync_replication(Duration::from_millis(1));
        metrics.replica_ack();
        metrics.replica_ack_failure();
        metrics.set_raft_leader(Some("a"));
        metrics.set_consul_leader(None);
        metrics.set_raft_state(1, 1, 1, 1);
        metrics.add_raft_election();
        metrics.set_consul_materialized(true);
        metrics.set_cluster_state(1, 1, 1);
    }
}
