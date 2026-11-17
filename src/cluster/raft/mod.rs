//! The `openraft` backend for the raft consensus mode.

pub mod coordinator;
pub mod http;
pub mod memory;
pub mod network;
pub mod store;

use std::collections::BTreeMap;
use std::fmt;
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

/// Smallest election timeout in milliseconds.
const MIN_ELECTION_TIMEOUT_MS: u64 = 300;

/// Smallest heartbeat interval in milliseconds.
const MIN_HEARTBEAT_INTERVAL_MS: u64 = 50;

/// Timer settings for one raft node, in milliseconds.
#[derive(Debug, Clone, Copy)]
struct RaftTimers {
    election_min: u64,
    election_max: u64,
    heartbeat: u64,
}

/// Return the timer settings for one RPC timeout.
///
/// The leader lease equals the maximum election timeout. The lease must not be
/// shorter than the RPC timeout, or a slow but valid request can let the lease
/// expire.
fn raft_timers(rpc_timeout: Duration) -> RaftTimers {
    let rpc_ms = u64::try_from(rpc_timeout.as_millis()).unwrap_or(u64::MAX);
    let election_max = rpc_ms.max(MIN_ELECTION_TIMEOUT_MS);
    let election_min = (election_max / 2).max(MIN_ELECTION_TIMEOUT_MS / 2);
    let heartbeat = (election_min / 3).max(MIN_HEARTBEAT_INTERVAL_MS);
    RaftTimers {
        election_min,
        election_max,
        heartbeat,
    }
}

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

impl fmt::Debug for RaftNode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RaftNode")
            .field("raft", &self.raft)
            .field("store", &self.store)
            .field("node_id", &self.node_id)
            .field("nodes", &self.nodes)
            .field("secret", &"[REDACTED]")
            .field("http", &self.http)
            .field("timeout", &self.timeout)
            .field("metrics", &self.metrics)
            .field("election_state", &self.election_state)
            .finish()
    }
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
        let timers = raft_timers(raft_config.rpc_timeout.as_duration());
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
            election_timeout_min: timers.election_min,
            election_timeout_max: timers.election_max,
            heartbeat_interval: timers.heartbeat,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(64),
            ..RaftSettings::default()
        };
        let settings = settings.validate().map_err(CoordError::backend)?;
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
        .map_err(CoordError::backend)?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(raft_config.rpc_timeout.as_duration())
            .read_timeout(raft_config.rpc_timeout.as_duration())
            .build()
            .map_err(CoordError::backend)?;
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
                .map_err(CoordError::backend)?
        {
            node.raft
                .initialize(nodes)
                .await
                .map_err(CoordError::backend)?;
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
        use openraft::errors::{ClientWriteError, Fatal, RaftError};
        match self.raft.client_write(entry).await {
            Ok(response) => Ok(response),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(CoordError::Unavailable)
            }
            Err(RaftError::APIError(ClientWriteError::LogEntryDiscarded(error))) => {
                Err(CoordError::Indeterminate(error.to_string()))
            }
            Err(RaftError::Fatal(Fatal::StorageError(error))) => Err(CoordError::backend(error)),
            Err(RaftError::Fatal(_)) => Err(CoordError::Unavailable),
            Err(error) => Err(CoordError::backend(error)),
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
            self.metrics.add_raft_election();
        }
        election.term = term;
        election.leader_seen = leader_seen;
        drop(election);
        self.metrics
            .set_raft_state(term, last_log, applied, snapshot);
        self.metrics.set_raft_leader(raft.current_leader.as_deref());
        self.metrics
            .set_cluster_state(applied, term, self.store.tombstone_count().unwrap_or(0));
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn raft_timers_keep_the_lease_above_the_rpc_timeout() {
        let timers = raft_timers(Duration::from_secs(2));
        assert_eq!(timers.election_max, 2_000);
        assert!(timers.election_min < timers.election_max);
        assert!(timers.heartbeat < timers.election_min);
    }

    #[test]
    fn raft_timers_keep_a_floor_for_a_small_timeout() {
        let timers = raft_timers(Duration::ZERO);
        assert_eq!(timers.election_max, MIN_ELECTION_TIMEOUT_MS);
        assert_eq!(timers.election_min, MIN_ELECTION_TIMEOUT_MS / 2);
        assert_eq!(timers.heartbeat, MIN_HEARTBEAT_INTERVAL_MS);
    }
}
