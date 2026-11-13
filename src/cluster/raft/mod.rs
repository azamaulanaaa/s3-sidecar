//! The `openraft` backend for the raft consensus mode.

pub mod coordinator;
pub mod http;
pub mod memory;
pub mod network;
pub mod store;

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use openraft::network::NetSnapshot;
use openraft::rt::WatchReceiver;
use openraft::{BasicNode, Config as RaftSettings, Raft, RaftNetworkFactory, SnapshotPolicy};

use self::store::{RaftLogStore, RaftStore};
use super::coordinator::{CoordError, Entry};
use super::state::MetadataStore;
use crate::config::Config;
use crate::metrics::Metrics;

openraft::declare_raft_types!(
    /// Type configuration for the sidecar raft cluster.
    pub TypeConfig:
        D = Entry,
        R = super::state::ApplyOutcome,
        NodeId = String,
        Node = BasicNode,
);

/// Alias for one log id.
pub type LogIdOf = openraft::type_config::alias::LogIdOf<TypeConfig>;
/// Alias for one committed leader id.
pub type CommittedLeaderIdOf = openraft::type_config::alias::CommittedLeaderIdOf<TypeConfig>;
/// Alias for one vote.
pub type VoteOf = openraft::type_config::alias::VoteOf<TypeConfig>;
/// Alias for one entry.
pub type EntryOf = openraft::type_config::alias::EntryOf<TypeConfig>;
/// Alias for one stored membership.
pub type StoredMembershipOf = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
/// Alias for one snapshot metadata value.
pub type SnapshotMetaOf = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
/// Alias for one snapshot.
pub type SnapshotOf = openraft::type_config::alias::SnapshotOf<TypeConfig, Vec<u8>>;

/// Alias for one raft handle.
pub type RaftHandle = Raft<TypeConfig, RaftStore>;

/// A running raft node.
pub struct RaftNode {
    raft: RaftHandle,
    store: Arc<MetadataStore>,
    node_id: String,
    nodes: BTreeMap<String, BasicNode>,
    secret: String,
    http: reqwest::Client,
    timeout: Duration,
    metrics: Arc<Metrics>,
    election_state: Mutex<ElectionState>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ElectionState {
    term: u64,
    leader_seen: bool,
}

impl RaftNode {
    /// Start one raft node with the HTTP network transport.
    ///
    /// # Errors
    ///
    /// Returns an error when the raft configuration or the storage is not
    /// valid.
    pub async fn start(
        store: Arc<MetadataStore>,
        config: &Config,
        metrics: Arc<Metrics>,
    ) -> Result<Arc<Self>, CoordError> {
        let network = network::HttpNetworkFactory::new(&config.consensus.raft)?;
        Self::start_with_network(store, config, metrics, network).await
    }

    /// Start one raft node with a custom network transport.
    ///
    /// # Errors
    ///
    /// Returns an error when the raft configuration or the storage is not
    /// valid.
    pub async fn start_with_network<N>(
        store: Arc<MetadataStore>,
        config: &Config,
        metrics: Arc<Metrics>,
        network: N,
    ) -> Result<Arc<Self>, CoordError>
    where
        N: RaftNetworkFactory<TypeConfig>,
        N::Network: NetSnapshot<TypeConfig, SnapshotData = Vec<u8>>,
    {
        let raft_config = &config.consensus.raft;
        let nodes: BTreeMap<String, BasicNode> = raft_config
            .nodes
            .iter()
            .map(|node| {
                (
                    node.id.clone(),
                    BasicNode {
                        addr: node.addr.clone(),
                    },
                )
            })
            .collect();
        let settings = RaftSettings {
            cluster_name: "s3-sidecar".to_owned(),
            election_timeout_min: 300,
            election_timeout_max: 700,
            heartbeat_interval: 100,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(64),
            ..RaftSettings::default()
        };
        let settings = settings
            .validate()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let log_store = RaftLogStore::new(store.database());
        let state_machine = RaftStore::new(Arc::clone(&store));
        let raft = Raft::new(
            raft_config.node_id.clone(),
            Arc::new(settings),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|error| CoordError::Storage(error.to_string()))?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(raft_config.rpc_timeout.as_duration())
            .read_timeout(raft_config.rpc_timeout.as_duration())
            .build()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let node = Arc::new(Self {
            raft,
            store,
            node_id: raft_config.node_id.clone(),
            nodes: nodes.clone(),
            secret: raft_config.secret.expose_secret().to_owned(),
            http,
            timeout: raft_config.rpc_timeout.as_duration(),
            metrics,
            election_state: Mutex::new(ElectionState::default()),
        });
        let bootstrap = raft_config.bootstrap || nodes.len() == 1;
        if bootstrap
            && !node
                .raft
                .is_initialized()
                .await
                .map_err(|error| CoordError::Storage(error.to_string()))?
        {
            node.raft
                .initialize(nodes)
                .await
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        node.refresh_metrics();
        Ok(node)
    }

    /// Return the raft handle.
    #[must_use]
    pub fn raft(&self) -> &RaftHandle {
        &self.raft
    }

    /// Return the metadata store.
    #[must_use]
    pub fn store(&self) -> &Arc<MetadataStore> {
        &self.store
    }

    /// Return the node name.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Return the cluster members.
    #[must_use]
    pub fn nodes(&self) -> &BTreeMap<String, BasicNode> {
        &self.nodes
    }

    /// Return the shared internal token.
    #[must_use]
    pub fn secret(&self) -> &str {
        &self.secret
    }

    /// Return the HTTP client for internal calls.
    #[must_use]
    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    /// Return the internal request timeout.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Return the current term from the raft metrics.
    #[must_use]
    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow_watched().current_term
    }

    /// Return true when this node is the leader.
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow_watched().state.is_leader()
    }

    /// Return the last applied index.
    #[must_use]
    pub fn applied_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow_watched()
            .last_applied
            .as_ref()
            .map_or(0, |log_id| log_id.index)
    }

    /// Return the current leader name.
    #[must_use]
    pub fn leader(&self) -> Option<String> {
        self.raft.metrics().borrow_watched().current_leader.clone()
    }

    /// Return the address of one node.
    #[must_use]
    pub fn node_addr(&self, id: &str) -> Option<String> {
        self.nodes.get(id).map(|node| node.addr.clone())
    }

    /// Return true when the node has a leader and has applied one entry.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow_watched();
        metrics.current_leader.is_some() && metrics.last_applied.is_some()
    }

    /// Return the readiness reason.
    #[must_use]
    pub fn not_ready_reason(&self) -> Option<&'static str> {
        let receiver = self.raft.metrics();
        let metrics = receiver.borrow_watched();
        if metrics.current_leader.is_none() {
            Some("no raft leader")
        } else if metrics.last_applied.is_none() {
            Some("no applied raft entry")
        } else {
            None
        }
    }

    /// Write one entry through the local leader.
    ///
    /// # Errors
    ///
    /// Returns an unavailable error when this node is not the leader or the
    /// cluster cannot commit the entry.
    pub async fn client_write(
        &self,
        entry: Entry,
    ) -> Result<openraft::raft::ClientWriteResponse<TypeConfig>, CoordError> {
        use openraft::errors::{ClientWriteError, RaftError};
        match self.raft.client_write(entry).await {
            Ok(response) => Ok(response),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(CoordError::Unavailable)
            }
            Err(error) => Err(CoordError::Storage(error.to_string())),
        }
    }

    /// Refresh the exported raft gauges.
    pub fn refresh_metrics(&self) {
        let raft = self.raft.metrics().borrow_watched().clone();
        let term = raft.current_term;
        let last_log = raft.last_log_index.unwrap_or(0);
        let applied = raft.last_applied.as_ref().map_or(0, |log_id| log_id.index);
        let snapshot = raft.snapshot.as_ref().map_or(0, |log_id| log_id.index);
        let mut election = self
            .election_state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let leader_seen = raft.current_leader.is_some();
        if term > election.term && leader_seen {
            self.metrics.raft_elections.fetch_add(1, Ordering::Relaxed);
        }
        election.term = term;
        election.leader_seen = leader_seen;
        drop(election);
        self.metrics.raft_term.store(term, Ordering::Relaxed);
        self.metrics
            .raft_last_log_index
            .store(last_log, Ordering::Relaxed);
        self.metrics
            .raft_last_applied
            .store(applied, Ordering::Relaxed);
        self.metrics
            .raft_snapshot_index
            .store(snapshot, Ordering::Relaxed);
        self.metrics.set_raft_leader(raft.current_leader.as_deref());
        self.metrics
            .set_cluster_state(applied, term, self.store.tombstone_count().unwrap_or(0));
    }
}
