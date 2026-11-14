//! The raft [`Coordinator`] implementation.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures_util::future::BoxFuture;
use openraft::ReadPolicy;
use openraft::vote::RaftLeaderId;
use serde::{Deserialize, Serialize};

use super::network::TOKEN_HEADER;
use super::{LogIdOf, RaftNode};
use crate::cluster::coordinator::{CoordError, Coordinator, Entry};
use crate::cluster::state::{Applied, ApplyOutcome, ApplyResult, MetadataStore};
use crate::metrics::Metrics;

/// Reply from the leader for one forwarded proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeReply {
    /// Accepted log id.
    pub log_id: LogIdOf,
    /// Apply outcome.
    pub outcome: ApplyOutcome,
}

/// Reply from the leader for one read index request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ReadIndexReply {
    /// Log index that the caller must apply before it serves the read.
    pub index: u64,
}

/// A coordinator backed by one openraft node.
pub struct RaftCoordinator {
    store: Arc<MetadataStore>,
    metrics: Arc<Metrics>,
    node_id: String,
    runtime: OnceLock<Arc<RaftNode>>,
}

impl RaftCoordinator {
    /// Build one raft coordinator.
    #[must_use]
    pub fn new(store: Arc<MetadataStore>, metrics: Arc<Metrics>, node_id: String) -> Self {
        Self {
            store,
            metrics,
            node_id,
            runtime: OnceLock::new(),
        }
    }

    /// Attach the running raft node.
    ///
    /// # Errors
    ///
    /// Returns an error when the node was already attached.
    pub fn attach(&self, node: Arc<RaftNode>) -> Result<(), CoordError> {
        self.runtime
            .set(node)
            .map_err(|_| CoordError::Storage("the raft node is already attached".to_owned()))
    }

    /// Return the running raft node.
    #[must_use]
    pub fn node(&self) -> Option<&Arc<RaftNode>> {
        self.runtime.get()
    }

    /// Refresh the exported gauges.
    pub fn refresh_metrics(&self) {
        if let Some(node) = self.node() {
            node.refresh_metrics();
        } else {
            let tombstones = self.store.tombstone_count().unwrap_or(0);
            self.metrics
                .set_cluster_state(self.store.applied_index(), 0, tombstones);
        }
    }

    fn node_id(&self) -> &str {
        &self.node_id
    }

    async fn write_local(&self, node: &Arc<RaftNode>, entry: Entry) -> Result<Applied, CoordError> {
        let response = node.client_write(entry).await?;
        applied_from_reply(&response.log_id, response.data)
    }

    async fn forward_propose(&self, entry: Entry) -> Result<Applied, CoordError> {
        let node = self.node().ok_or(CoordError::Unavailable)?;
        let leader = node.leader().ok_or(CoordError::Unavailable)?;
        if leader == self.node_id() {
            return self.write_local(node, entry).await;
        }
        let address = node.node_addr(&leader).ok_or_else(|| {
            CoordError::Storage(format!("no address for raft leader \"{leader}\""))
        })?;
        let url = format!("{}/_raft/propose", address.trim_end_matches('/'));
        let response = node
            .http()
            .post(url)
            .header(TOKEN_HEADER, node.secret())
            .timeout(node.timeout())
            .json(&entry)
            .send()
            .await
            .map_err(|_| CoordError::Unavailable)?;
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(CoordError::Unavailable);
        }
        if !response.status().is_success() {
            return Err(CoordError::Storage(format!(
                "raft leader returned status {}",
                response.status()
            )));
        }
        let reply: ProposeReply = response
            .json()
            .await
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        applied_from_reply(&reply.log_id, reply.outcome)
    }

    async fn read_index_local(&self, node: &Arc<RaftNode>) -> Result<u64, CoordError> {
        let read_log_id = node
            .raft()
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(read_log_id.index())
    }

    async fn forward_read_index(&self) -> Result<u64, CoordError> {
        let node = self.node().ok_or(CoordError::Unavailable)?;
        let leader = node.leader().ok_or(CoordError::Unavailable)?;
        if leader == self.node_id() {
            return self.read_index_local(node).await;
        }
        let address = node.node_addr(&leader).ok_or_else(|| {
            CoordError::Storage(format!("no address for raft leader \"{leader}\""))
        })?;
        let url = format!("{}/_raft/read_index", address.trim_end_matches('/'));
        let response = node
            .http()
            .post(url)
            .header(TOKEN_HEADER, node.secret())
            .timeout(node.timeout())
            .json(&serde_json::json!({}))
            .send()
            .await
            .map_err(|_| CoordError::Unavailable)?;
        if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Err(CoordError::Unavailable);
        }
        if !response.status().is_success() {
            return Err(CoordError::Storage(format!(
                "raft leader returned status {}",
                response.status()
            )));
        }
        let reply: ReadIndexReply = response
            .json()
            .await
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(reply.index)
    }
}

impl Coordinator for RaftCoordinator {
    fn term(&self) -> u64 {
        self.node().map_or(0, |node| node.term())
    }

    fn is_leader(&self) -> bool {
        self.node().is_some_and(|node| node.is_leader())
    }

    fn applied_index(&self) -> u64 {
        self.node()
            .map_or_else(|| self.store.applied_index(), |node| node.applied_index())
    }

    fn propose(&self, entry: Entry) -> BoxFuture<'_, Result<Applied, CoordError>> {
        Box::pin(async move {
            let Some(node) = self.node().cloned() else {
                return Err(CoordError::Unavailable);
            };
            if node.is_leader() {
                match self.write_local(&node, entry.clone()).await {
                    Ok(applied) => Ok(applied),
                    Err(CoordError::Unavailable) => self.forward_propose(entry).await,
                    Err(error) => Err(error),
                }
            } else {
                self.forward_propose(entry).await
            }
        })
    }

    fn read_index(&self) -> BoxFuture<'_, Result<u64, CoordError>> {
        self.metrics.read_index_called();
        Box::pin(async move {
            let Some(node) = self.node().cloned() else {
                return Err(CoordError::Unavailable);
            };
            let index = if node.is_leader() {
                self.read_index_local(&node).await?
            } else {
                self.forward_read_index().await?
            };
            let timeout = Duration::from_secs(5);
            node.raft()
                .wait(Some(timeout))
                .applied_index_at_least(Some(index), "linearizable read barrier")
                .await
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            Ok(index)
        })
    }
}

fn applied_from_reply(log_id: &LogIdOf, outcome: ApplyOutcome) -> Result<Applied, CoordError> {
    match outcome {
        ApplyOutcome::Applied(result) => Ok(Applied {
            index: log_id.index,
            term: log_id.leader_id.term(),
            result,
        }),
        ApplyOutcome::Rejected(reason) => Err(CoordError::Rejected(reason)),
        ApplyOutcome::Duplicate => Ok(Applied {
            index: log_id.index,
            term: log_id.leader_id.term(),
            result: ApplyResult::Blank,
        }),
    }
}
