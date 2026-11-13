//! Per-function counters and a Prometheus text rendering.

use std::sync::atomic::{AtomicU64, Ordering};

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
    output
}

fn push_counter(output: &mut String, name: &str, help: &str, value: u64) {
    use std::fmt::Write as _;
    let _ = writeln!(output, "# HELP {name} {help}");
    let _ = writeln!(output, "# TYPE {name} gauge");
    let _ = writeln!(output, "{name} {value}");
}
