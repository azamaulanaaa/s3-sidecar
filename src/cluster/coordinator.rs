//! Coordinator trait and the single-node backend.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use super::state::{Applied, BucketConfigRecord, StateMachine};

/// Reason that the state machine rejected an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// A conditional request failed.
    PreconditionFailed,
    /// The caller used a stale per-key version.
    Conflict,
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PreconditionFailed => f.write_str("a request condition failed"),
            Self::Conflict => f.write_str("the key version changed"),
        }
    }
}

/// A coordination problem.
#[derive(Debug, thiserror::Error)]
pub enum CoordError {
    /// The entry carries a term that is not current.
    #[error("proposal term {actual} is stale; the current term is {expected}")]
    StaleTerm {
        /// Current term.
        expected: u64,
        /// Term in the entry.
        actual: u64,
    },
    /// The state machine rejected the entry.
    #[error("the state machine rejected the entry: {0}")]
    Rejected(RejectReason),
    /// The metadata store failed.
    #[error("the metadata store failed: {0}")]
    Storage(String),
    /// The backend is not part of this build.
    #[error("{0}")]
    NotImplemented(&'static str),
}

/// A commit proposal.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version that the caller observed.
    pub base_version: u64,
    /// Backing blob identifier.
    pub blob: String,
    /// Object size in bytes.
    pub size: u64,
    /// Body checksum.
    pub checksum: String,
    /// Content type.
    pub content_type: Option<String>,
    /// Entity tag without quotes.
    pub etag: String,
    /// User metadata without the `x-amz-meta-` prefix.
    pub user_metadata: BTreeMap<String, String>,
    /// Node that accepted the write.
    pub owner: String,
    /// Nodes that hold a replica.
    pub replicas: Vec<String>,
    /// Reject the entry when a committed key exists.
    pub if_none_match: bool,
    /// Reject the entry when the stored `ETag` does not match.
    pub if_match: Option<String>,
    /// Cache-Control value.
    pub cache_control: Option<String>,
    /// Content-Encoding value.
    pub content_encoding: Option<String>,
    /// Content-Disposition value.
    pub content_disposition: Option<String>,
}

/// One log entry.
#[derive(Debug, Clone)]
pub enum Entry {
    /// Make a staged blob visible.
    Commit {
        /// Coordinator term.
        term: u64,
        /// Commit details.
        request: Box<CommitRequest>,
    },
    /// Remove a pending multipart key.
    Abort {
        /// Coordinator term.
        term: u64,
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },
    /// Replace a key with a tombstone.
    Delete {
        /// Coordinator term.
        term: u64,
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Node that accepted the delete.
        owner: String,
    },
    /// Store bucket consistency settings.
    BucketConfig {
        /// Coordinator term.
        term: u64,
        /// Bucket name.
        bucket: String,
        /// Stored settings.
        request: BucketConfigRecord,
    },
}

impl Entry {
    /// Return the term carried by the entry.
    #[must_use]
    pub fn term(&self) -> u64 {
        match self {
            Self::Commit { term, .. }
            | Self::Abort { term, .. }
            | Self::Delete { term, .. }
            | Self::BucketConfig { term, .. } => *term,
        }
    }
}

/// The metadata authority for cluster mode.
pub trait Coordinator: Send + Sync {
    /// Return the current term.
    fn term(&self) -> u64;

    /// Return true when this node can commit entries.
    fn is_leader(&self) -> bool;

    /// Return the last applied index.
    fn applied_index(&self) -> u64;

    /// Apply or replicate one entry.
    fn propose(&self, entry: Entry) -> impl Future<Output = Result<Applied, CoordError>> + Send;

    /// Return an applied index that is safe to read from.
    fn read_index(&self) -> impl Future<Output = Result<u64, CoordError>> + Send;
}

/// A single-node coordinator. It starts at term 1 and is always the leader.
pub struct SingleNode {
    term: u64,
    state: Arc<StateMachine>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl SingleNode {
    /// Build a single-node coordinator.
    #[must_use]
    pub fn new(state: Arc<StateMachine>, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            term: 1,
            state,
            metrics,
        }
    }

    /// Return the metadata state machine.
    #[must_use]
    pub fn state(&self) -> &Arc<StateMachine> {
        &self.state
    }

    /// Refresh the applied index and term gauges.
    pub fn refresh_metrics(&self) {
        let tombstones = self.state.tombstone_count().unwrap_or(0);
        self.metrics
            .set_cluster_state(self.state.applied_index(), self.term, tombstones);
    }
}

impl Coordinator for SingleNode {
    fn term(&self) -> u64 {
        self.term
    }

    fn is_leader(&self) -> bool {
        true
    }

    fn applied_index(&self) -> u64 {
        self.state.applied_index()
    }

    fn propose(&self, entry: Entry) -> impl Future<Output = Result<Applied, CoordError>> + Send {
        let term = entry.term();
        let state = Arc::clone(&self.state);
        async move {
            if term != 1 {
                return Err(CoordError::StaleTerm {
                    expected: 1,
                    actual: term,
                });
            }
            let applied = match entry {
                Entry::Commit { term, request } => state.apply_commit(term, &request)?,
                Entry::Abort { term, bucket, key } => state.apply_abort(term, &bucket, &key)?,
                Entry::Delete {
                    term,
                    bucket,
                    key,
                    owner,
                } => state.apply_delete(term, &bucket, &key, &owner)?,
                Entry::BucketConfig {
                    term,
                    bucket,
                    request,
                } => state.apply_bucket_config(term, &bucket, request)?,
            };
            Ok(applied)
        }
    }

    fn read_index(&self) -> impl Future<Output = Result<u64, CoordError>> + Send {
        self.metrics.read_index_called();
        let index = self.state.applied_index();
        async move { Ok(index) }
    }
}
