//! In-memory raft network for tests.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use openraft::BasicNode;
use openraft::errors::{RPCError, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, VoteRequest, VoteResponse,
};

use super::{RaftHandle, SnapshotOf, TypeConfig, VoteOf};

/// Shared map from node name to raft handle.
pub type Registry = Arc<Mutex<BTreeMap<String, RaftHandle>>>;

/// Build an empty registry.
#[must_use]
pub fn registry() -> Registry {
    Arc::new(Mutex::new(BTreeMap::new()))
}

/// Add one raft handle to a registry.
pub fn register(registry: &Registry, node_id: &str, raft: RaftHandle) {
    lock(registry).insert(node_id.to_owned(), raft);
}

/// Remove one raft handle from a registry.
pub fn unregister(registry: &Registry, node_id: &str) {
    lock(registry).remove(node_id);
}

fn lock(registry: &Registry) -> MutexGuard<'_, BTreeMap<String, RaftHandle>> {
    registry.lock().unwrap_or_else(PoisonError::into_inner)
}

/// In-memory network factory.
#[derive(Debug, Clone)]
pub struct MemoryNetworkFactory {
    registry: Registry,
}

impl MemoryNetworkFactory {
    /// Build one factory on a registry.
    #[must_use]
    pub fn new(registry: Registry) -> Self {
        Self { registry }
    }
}

#[expect(
    clippy::unused_async_trait_impl,
    reason = "the openraft trait requires async methods"
)]
impl RaftNetworkFactory<TypeConfig> for MemoryNetworkFactory {
    type Network = MemoryNetwork;

    async fn new_client(&mut self, target: String, _node: &BasicNode) -> Self::Network {
        MemoryNetwork {
            target,
            registry: Arc::clone(&self.registry),
        }
    }
}

/// One in-memory network client.
#[derive(Debug)]
pub struct MemoryNetwork {
    target: String,
    registry: Registry,
}

impl MemoryNetwork {
    fn handle(&self) -> Result<RaftHandle, RPCError<TypeConfig>> {
        lock(&self.registry)
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                RPCError::Unreachable(Unreachable::from_string(format!(
                    "raft node \"{}\" is not registered",
                    self.target
                )))
            })
    }
}

impl RaftNetworkV2<TypeConfig> for MemoryNetwork {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let raft = self.handle()?;
        raft.append_entries(rpc)
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let raft = self.handle()?;
        raft.vote(rpc)
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf,
        snapshot: SnapshotOf,
        _cancel: impl std::future::Future<Output = openraft::errors::ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let raft = self.handle()?;
        raft.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|error| {
                StreamingError::Unreachable(Unreachable::from_string(error.to_string()))
            })
    }
}
