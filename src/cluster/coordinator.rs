//! Coordinator trait, the single-node backend, and the shared entry types.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use futures_util::future::BoxFuture;
use openraft::LogId;
use openraft::vote::RaftLeaderId;

use super::raft::CommittedLeaderIdOf;

use super::raft::LogIdOf;
use super::state::{Applied, ApplyOutcome, BucketConfigRecord, MetadataStore};

/// Reason that the state machine rejected an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RejectReason {
    /// A conditional request failed.
    PreconditionFailed,
    /// The caller used a stale per-key version.
    Conflict,
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
    /// The cluster has no leader.
    #[error("the cluster has no leader")]
    Unavailable,
    /// The metadata store or raft backend failed.
    #[error("the metadata store failed: {0}")]
    Storage(String),
    /// The backend is not part of this build.
    #[error("{0}")]
    NotImplemented(&'static str),
}

/// A commit proposal.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// Commit time in Unix seconds. The state machine applies this value.
    pub committed_at: i64,
}

/// One log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        /// Delete time in Unix seconds.
        deleted_at: i64,
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

impl fmt::Display for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Commit { request, .. } => write!(
                f,
                "Commit({}/{}, blob={})",
                request.bucket, request.key, request.blob
            ),
            Self::Abort { bucket, key, .. } => write!(f, "Abort({bucket}/{key})"),
            Self::Delete { bucket, key, .. } => write!(f, "Delete({bucket}/{key})"),
            Self::BucketConfig { bucket, .. } => write!(f, "BucketConfig({bucket})"),
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
    fn propose(&self, entry: Entry) -> BoxFuture<'_, Result<Applied, CoordError>>;

    /// Return an applied index that is safe to read from.
    fn read_index(&self) -> BoxFuture<'_, Result<u64, CoordError>>;

    /// Refresh one key or bucket prefix from the authority before a read.
    ///
    /// Backends that apply locally do nothing. Consul uses a consistent read.
    fn refresh_read<'a>(
        &'a self,
        bucket: &'a str,
        key: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), CoordError>> {
        let _ = (bucket, key);
        Box::pin(async { Ok(()) })
    }
}

/// A single-node coordinator. It starts at term 1 and is always the leader.
pub struct SingleNode {
    term: u64,
    state: Arc<MetadataStore>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl SingleNode {
    /// Build a single-node coordinator.
    #[must_use]
    pub fn new(state: Arc<MetadataStore>, metrics: Arc<crate::metrics::Metrics>) -> Self {
        Self {
            term: 1,
            state,
            metrics,
        }
    }

    /// Return the metadata state machine.
    #[must_use]
    pub fn state(&self) -> &Arc<MetadataStore> {
        &self.state
    }

    /// Refresh the applied index and term gauges.
    pub fn refresh_metrics(&self) {
        let tombstones = self.state.tombstone_count().unwrap_or(0);
        self.metrics
            .set_cluster_state(self.state.applied_index(), self.term, tombstones);
    }
}

fn local_log_id(term: u64, index: u64) -> LogIdOf {
    LogId::new(CommittedLeaderIdOf::new(term, "local".to_owned()), index)
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

    fn propose(&self, entry: Entry) -> BoxFuture<'_, Result<Applied, CoordError>> {
        Box::pin(async move {
            let term = entry.term();
            if term != self.term {
                return Err(CoordError::StaleTerm {
                    expected: self.term,
                    actual: term,
                });
            }
            let index = self.state.applied_index().saturating_add(1);
            let outcome = self.state.apply_entry(&local_log_id(term, index), &entry)?;
            match outcome {
                ApplyOutcome::Applied(result) => Ok(Applied {
                    index,
                    term,
                    result,
                }),
                ApplyOutcome::Rejected(reason) => Err(CoordError::Rejected(reason)),
                ApplyOutcome::Duplicate => Ok(Applied {
                    index: self.state.applied_index(),
                    term,
                    result: super::state::ApplyResult::Blank,
                }),
            }
        })
    }

    fn read_index(&self) -> BoxFuture<'_, Result<u64, CoordError>> {
        self.metrics.read_index_called();
        let index = self.state.applied_index();
        Box::pin(async move { Ok(index) })
    }
}
