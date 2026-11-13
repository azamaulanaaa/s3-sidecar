//! Per-function counters and a Prometheus text rendering.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

/// Counters for the cache data path.
#[derive(Debug, Default)]
pub struct Metrics {
    /// Cache hits.
    pub cache_hits: AtomicU64,
    /// Cache misses.
    pub cache_misses: AtomicU64,
    /// Bytes sent to clients from the cache.
    pub bytes_served: AtomicU64,
    /// Bytes fetched from origin.
    pub origin_bytes: AtomicU64,
    /// Conditional origin requests that returned 304.
    pub revalidations: AtomicU64,
    /// Syncs that reached origin.
    pub sync_success: AtomicU64,
    /// Sync attempts that failed.
    pub sync_failure: AtomicU64,
    /// Entries moved to quarantine.
    pub quarantine: AtomicU64,
    /// Cache entries removed by eviction.
    pub evictions: AtomicU64,
    /// Fallbacks from the cache to origin.
    pub fallbacks: AtomicU64,
    /// Bytes tracked by the LRU index.
    pub tracked_bytes: AtomicU64,
    /// Configured cache limit. Zero means unlimited.
    pub cache_max_bytes: AtomicU64,
    /// One when the cache is unlimited.
    pub cache_unlimited: AtomicU64,
    /// Cluster writes that reached the committed state.
    pub cluster_committed_writes: AtomicU64,
    /// Cluster writes that were rejected and removed.
    pub cluster_aborted_writes: AtomicU64,
    /// Total commit latency in microseconds.
    pub cluster_commit_latency_micros: AtomicU64,
    /// Reads served from local metadata and a local blob.
    pub cluster_read_local_hits: AtomicU64,
    /// Reads where the metadata was committed but the blob was missing.
    pub cluster_missing_blob_reads: AtomicU64,
    /// Current tombstone count.
    pub cluster_tombstones: AtomicU64,
    /// Blobs removed by cluster garbage collection.
    pub cluster_gc_deletions: AtomicU64,
    /// Applied metadata index.
    pub cluster_applied_index: AtomicU64,
    /// Current coordinator term.
    pub cluster_term: AtomicU64,
    /// Calls to `read_index`.
    pub cluster_read_index_calls: AtomicU64,
    /// Reads served by a peer blob fetch.
    pub cluster_peer_fetches: AtomicU64,
    /// Peer reads that stored the blob locally.
    pub cluster_read_repairs: AtomicU64,
    /// Replication operations that reached a target.
    pub cluster_replication_success: AtomicU64,
    /// Replication failures by status or transport class.
    pub cluster_replication_failures: Mutex<BTreeMap<String, u64>>,
    /// Pending replication jobs.
    pub cluster_replication_queue_depth: AtomicU64,
    /// Age of the oldest pending replication job in seconds.
    pub cluster_replication_oldest_age_seconds: AtomicU64,
    /// Replica targets checked by reconciliation.
    pub cluster_reconcile_checked: AtomicU64,
    /// Replica targets present during reconciliation.
    pub cluster_reconcile_present: AtomicU64,
    /// Replica targets missing during reconciliation.
    pub cluster_reconcile_missing: AtomicU64,
    /// Owner blobs restored by reconciliation.
    pub cluster_owner_blobs_repaired: AtomicU64,
    /// Owner blobs that no replica could supply.
    pub cluster_owner_blobs_unrecoverable: AtomicU64,
    /// Accepted writes by acknowledgement policy.
    pub cluster_write_ack_total: Mutex<BTreeMap<String, u64>>,
    /// Quorum writes that reached the required acknowledgements.
    pub cluster_quorum_success: AtomicU64,
    /// Quorum writes that did not reach the required acknowledgements.
    pub cluster_quorum_failure: AtomicU64,
    /// Total synchronous replication latency in microseconds.
    pub cluster_sync_replication_latency_micros: AtomicU64,
    /// Synchronous peer copy attempts.
    pub cluster_sync_replication_count: AtomicU64,
    /// Peer copies that acknowledged the blob.
    pub cluster_replica_acks: AtomicU64,
    /// Peer copies that failed or timed out.
    pub cluster_replica_ack_failures: AtomicU64,
    /// Current raft term.
    pub raft_term: AtomicU64,
    /// Last raft log index.
    pub raft_last_log_index: AtomicU64,
    /// Last applied raft log index.
    pub raft_last_applied: AtomicU64,
    /// Last snapshot log index.
    pub raft_snapshot_index: AtomicU64,
    /// Observed raft leader elections.
    pub raft_elections: AtomicU64,
    /// Current raft leader name.
    pub raft_leader_id: Mutex<String>,
    /// Successful Consul session renewals.
    pub consul_session_renewals: AtomicU64,
    /// Consul sessions that ended or failed to renew.
    pub consul_session_losses: AtomicU64,
    /// Consul leadership acquisitions.
    pub consul_leader_changes: AtomicU64,
    /// One when the first Consul reconcile completed.
    pub consul_materialized: AtomicU64,
    /// Current Consul leader name.
    pub consul_leader_id: Mutex<String>,
}

impl Metrics {
    /// Record a cache hit.
    pub fn hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to a byte counter.
    pub fn add_bytes(counter: &AtomicU64, bytes: u64) {
        counter.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record one revalidation.
    pub fn revalidated(&self) {
        self.revalidations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one sync success.
    pub fn synced(&self) {
        self.sync_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one sync failure.
    pub fn sync_failed(&self) {
        self.sync_failure.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one quarantine.
    pub fn quarantined(&self) {
        self.quarantine.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one eviction.
    pub fn evicted(&self) {
        self.evictions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one fallback to origin.
    pub fn fallback(&self) {
        self.fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the tracked byte gauge.
    pub fn set_tracked(&self, bytes: u64) {
        self.tracked_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Record one committed cluster write.
    pub fn committed_write(&self) {
        self.cluster_committed_writes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one rejected and removed cluster write.
    pub fn aborted_write(&self) {
        self.cluster_aborted_writes.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the latency of one commit.
    pub fn commit_latency(&self, elapsed: std::time::Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.cluster_commit_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
    }

    /// Record one local cluster read.
    pub fn cluster_read_local(&self) {
        self.cluster_read_local_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one committed read with a missing blob.
    pub fn missing_blob(&self) {
        self.cluster_missing_blob_reads
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one garbage collection deletion.
    pub fn gc_deleted(&self) {
        self.cluster_gc_deletions.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one `read_index` call.
    pub fn read_index_called(&self) {
        self.cluster_read_index_calls
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one peer blob fetch.
    pub fn peer_fetch(&self) {
        self.cluster_peer_fetches.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one read-repair write.
    pub fn read_repaired(&self) {
        self.cluster_read_repairs.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one successful replication operation.
    pub fn replication_success(&self) {
        self.cluster_replication_success
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one replication failure by status or transport class.
    pub fn replication_failure(&self, status: &str) {
        let mut failures = self
            .cluster_replication_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *failures.entry(status.to_owned()).or_insert(0) += 1;
    }

    /// Set the replication queue gauges.
    pub fn set_replication_queue(&self, depth: u64, oldest_age_seconds: u64) {
        self.cluster_replication_queue_depth
            .store(depth, Ordering::Relaxed);
        self.cluster_replication_oldest_age_seconds
            .store(oldest_age_seconds, Ordering::Relaxed);
    }

    /// Record one reconciliation pass.
    pub fn set_reconcile(&self, checked: u64, present: u64, missing: u64) {
        self.cluster_reconcile_checked
            .store(checked, Ordering::Relaxed);
        self.cluster_reconcile_present
            .store(present, Ordering::Relaxed);
        self.cluster_reconcile_missing
            .store(missing, Ordering::Relaxed);
    }

    /// Record one owner blob restored by reconciliation.
    pub fn owner_blob_repaired(&self) {
        self.cluster_owner_blobs_repaired
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one owner blob that no replica could supply.
    pub fn owner_blob_unrecoverable(&self) {
        self.cluster_owner_blobs_unrecoverable
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Count one write attempt with an acknowledgement policy.
    pub fn write_ack(&self, policy: &str) {
        let mut counts = self
            .cluster_write_ack_total
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        *counts.entry(policy.to_owned()).or_insert(0) += 1;
    }

    /// Record one quorum write that reached the required acknowledgements.
    pub fn quorum_success(&self) {
        self.cluster_quorum_success.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one quorum write that did not reach the required acknowledgements.
    pub fn quorum_failure(&self) {
        self.cluster_quorum_failure.fetch_add(1, Ordering::Relaxed);
    }

    /// Record the latency of one synchronous peer copy.
    pub fn sync_replication(&self, elapsed: std::time::Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        self.cluster_sync_replication_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.cluster_sync_replication_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record one peer that acknowledged a blob.
    pub fn replica_ack(&self) {
        self.cluster_replica_acks.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one peer copy that failed or timed out.
    pub fn replica_ack_failure(&self) {
        self.cluster_replica_ack_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Set the raft leader name.
    pub fn set_raft_leader(&self, leader: Option<&str>) {
        set_leader(&self.raft_leader_id, leader);
    }

    /// Set the Consul leader name.
    pub fn set_consul_leader(&self, leader: Option<&str>) {
        set_leader(&self.consul_leader_id, leader);
    }

    /// Set the coordinator gauges.
    pub fn set_cluster_state(&self, applied_index: u64, term: u64, tombstones: u64) {
        self.cluster_applied_index
            .store(applied_index, Ordering::Relaxed);
        self.cluster_term.store(term, Ordering::Relaxed);
        self.cluster_tombstones.store(tombstones, Ordering::Relaxed);
    }
}

fn set_leader(target: &Mutex<String>, leader: Option<&str>) {
    let mut value = target.lock().unwrap_or_else(PoisonError::into_inner);
    value.clear();
    if let Some(leader) = leader {
        value.push_str(leader);
    }
}

/// Render Prometheus text for the metrics.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn render(metrics: &Metrics, pending: u64, sync_lag_seconds: u64) -> String {
    use std::fmt::Write as _;
    let hits = metrics.cache_hits.load(Ordering::Relaxed);
    let misses = metrics.cache_misses.load(Ordering::Relaxed);
    let total = hits.saturating_add(misses);
    let ratio = if total == 0 {
        0.0
    } else {
        // The ratio is a display value. Precision loss is acceptable here.
        #[allow(clippy::cast_precision_loss)]
        let value = hits as f64 / total as f64;
        value
    };

    let mut output = String::new();
    push_counter(
        &mut output,
        "s3_sidecar_cache_hits_total",
        "Cache hits.",
        hits,
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_misses_total",
        "Cache misses.",
        misses,
    );
    output.push_str("# TYPE s3_sidecar_cache_hit_ratio gauge\n");
    let _ = writeln!(output, "s3_sidecar_cache_hit_ratio {ratio:.6}");
    push_counter(
        &mut output,
        "s3_sidecar_cache_bytes_served_total",
        "Bytes served from the cache.",
        metrics.bytes_served.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_origin_bytes_fetched_total",
        "Bytes fetched from origin.",
        metrics.origin_bytes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_revalidations_total",
        "Conditional origin requests that returned 304.",
        metrics.revalidations.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_fallbacks_total",
        "Cache failures that fell back to origin.",
        metrics.fallbacks.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_pending_writes",
        "Pending write and delete journal entries.",
        pending,
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_sync_lag_seconds",
        "Age of the oldest pending journal entry.",
        sync_lag_seconds,
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_sync_success_total",
        "Syncs that reached origin.",
        metrics.sync_success.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_sync_failure_total",
        "Sync attempts that failed.",
        metrics.sync_failure.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_quarantine_total",
        "Entries moved to quarantine.",
        metrics.quarantine.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_evictions_total",
        "Cache entries removed by eviction.",
        metrics.evictions.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_tracked_bytes",
        "Bytes tracked by the LRU index.",
        metrics.tracked_bytes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_max_bytes",
        "Configured cache limit. Zero means unlimited.",
        metrics.cache_max_bytes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cache_unlimited",
        "One when the cache has no size limit.",
        metrics.cache_unlimited.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_committed_writes_total",
        "Cluster writes that reached the committed state.",
        metrics.cluster_committed_writes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_aborted_writes_total",
        "Cluster writes that were rejected and removed.",
        metrics.cluster_aborted_writes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_commit_latency_micros_total",
        "Total cluster commit latency in microseconds.",
        metrics
            .cluster_commit_latency_micros
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_read_local_hits_total",
        "Reads served from local metadata and a local blob.",
        metrics.cluster_read_local_hits.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_missing_blob_reads_total",
        "Reads where the blob was missing.",
        metrics.cluster_missing_blob_reads.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_tombstones",
        "Current cluster tombstone count.",
        metrics.cluster_tombstones.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_gc_deletions_total",
        "Blobs removed by cluster garbage collection.",
        metrics.cluster_gc_deletions.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_applied_index",
        "Applied metadata index.",
        metrics.cluster_applied_index.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_term",
        "Current coordinator term.",
        metrics.cluster_term.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_read_index_calls_total",
        "Calls to the coordinator read index.",
        metrics.cluster_read_index_calls.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_peer_fetches_total",
        "Reads served by a peer blob fetch.",
        metrics.cluster_peer_fetches.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_read_repairs_total",
        "Peer reads that stored the blob locally.",
        metrics.cluster_read_repairs.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_replication_success_total",
        "Replication operations that reached a target.",
        metrics.cluster_replication_success.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_replication_queue_depth",
        "Pending replication jobs.",
        metrics
            .cluster_replication_queue_depth
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_replication_oldest_age_seconds",
        "Age of the oldest pending replication job in seconds.",
        metrics
            .cluster_replication_oldest_age_seconds
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_reconcile_checked_total",
        "Replica targets checked by reconciliation.",
        metrics.cluster_reconcile_checked.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_reconcile_present_total",
        "Replica targets present during reconciliation.",
        metrics.cluster_reconcile_present.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_reconcile_missing_total",
        "Replica targets missing during reconciliation.",
        metrics.cluster_reconcile_missing.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_owner_blobs_repaired_total",
        "Owner blobs restored by reconciliation.",
        metrics.cluster_owner_blobs_repaired.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_owner_blobs_unrecoverable_total",
        "Owner blobs that no replica could supply.",
        metrics
            .cluster_owner_blobs_unrecoverable
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_quorum_success_total",
        "Quorum writes that reached the required acknowledgements.",
        metrics.cluster_quorum_success.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_quorum_failure_total",
        "Quorum writes that did not reach the required acknowledgements.",
        metrics.cluster_quorum_failure.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_sync_replication_latency_micros_total",
        "Total synchronous replication latency in microseconds.",
        metrics
            .cluster_sync_replication_latency_micros
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_sync_replication_count_total",
        "Synchronous peer copy attempts.",
        metrics
            .cluster_sync_replication_count
            .load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_replica_acks_total",
        "Peer copies that acknowledged the blob.",
        metrics.cluster_replica_acks.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_cluster_replica_ack_failures_total",
        "Peer copies that failed or timed out.",
        metrics.cluster_replica_ack_failures.load(Ordering::Relaxed),
    );
    {
        let counts = metrics
            .cluster_write_ack_total
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        output.push_str(
            "# HELP s3_sidecar_cluster_write_ack_total Accepted writes by acknowledgement policy.\n",
        );
        output.push_str("# TYPE s3_sidecar_cluster_write_ack_total gauge\n");
        for (policy, count) in counts.iter() {
            use std::fmt::Write as _;
            let _ = writeln!(
                output,
                "s3_sidecar_cluster_write_ack_total{{policy=\"{}\"}} {count}",
                escape_label(policy)
            );
        }
    }
    {
        let failures = metrics
            .cluster_replication_failures
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        output.push_str(
            "# HELP s3_sidecar_cluster_replication_failures_total Replication failures by status.\n",
        );
        output.push_str("# TYPE s3_sidecar_cluster_replication_failures_total gauge\n");
        for (status, count) in failures.iter() {
            use std::fmt::Write as _;
            let _ = writeln!(
                output,
                "s3_sidecar_cluster_replication_failures_total{{status=\"{}\"}} {count}",
                escape_label(status)
            );
        }
    }
    push_counter(
        &mut output,
        "s3_sidecar_raft_term",
        "Current raft term.",
        metrics.raft_term.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_raft_last_log_index",
        "Last raft log index.",
        metrics.raft_last_log_index.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_raft_last_applied",
        "Last applied raft log index.",
        metrics.raft_last_applied.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_raft_snapshot_index",
        "Last snapshot raft log index.",
        metrics.raft_snapshot_index.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_raft_elections_total",
        "Observed raft leader elections.",
        metrics.raft_elections.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_consul_session_renewals_total",
        "Successful Consul session renewals.",
        metrics.consul_session_renewals.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_consul_session_losses_total",
        "Consul sessions that ended or failed to renew.",
        metrics.consul_session_losses.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_consul_leader_changes_total",
        "Consul leadership acquisitions.",
        metrics.consul_leader_changes.load(Ordering::Relaxed),
    );
    push_counter(
        &mut output,
        "s3_sidecar_consul_materialized",
        "One when the first Consul reconcile completed.",
        metrics.consul_materialized.load(Ordering::Relaxed),
    );
    let consul_leader = metrics
        .consul_leader_id
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    output.push_str(
        "# HELP s3_sidecar_consul_leader Current Consul leader. One means the named node.\n",
    );
    output.push_str("# TYPE s3_sidecar_consul_leader gauge\n");
    let _ = writeln!(
        output,
        "s3_sidecar_consul_leader{{id=\"{}\"}} 1",
        escape_label(&consul_leader)
    );
    drop(consul_leader);
    let leader = metrics
        .raft_leader_id
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    output
        .push_str("# HELP s3_sidecar_raft_leader Current raft leader. One means the named node.\n");
    output.push_str("# TYPE s3_sidecar_raft_leader gauge\n");
    let _ = writeln!(
        output,
        "s3_sidecar_raft_leader{{id=\"{}\"}} 1",
        escape_label(&leader)
    );
    drop(leader);
    output
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn push_counter(output: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write as _;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}
