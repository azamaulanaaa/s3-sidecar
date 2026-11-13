//! Consul coordinator: leader election, metadata proposals, and
//! materialization.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use super::api::{
    BUCKET_PREFIX, ConsulApi, ConsulError, KvEntry, LEADER_KEY, META_PREFIX, WATCH_WAIT,
};
use crate::cluster::coordinator::{CommitRequest, CoordError, Coordinator, Entry, RejectReason};
use crate::cluster::state::{
    Applied, ApplyResult, BucketConfigRecord, Metadata, MetadataStore, ObjectState, unix_now,
};
use crate::config::ConsulConfig;
use crate::metrics::Metrics;

/// Header that carries the internal proposal token.
pub const TOKEN_HEADER: &str = "x-s3-sidecar-consul-token";
/// Path of the internal proposal endpoint.
pub const PROPOSE_PATH: &str = "/_coord/propose";

const MAX_CAS_ATTEMPTS: usize = 5;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);

/// Leader record stored at [`LEADER_KEY`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRecord {
    /// Node name.
    pub id: String,
    /// Base URL that peers use to reach the node.
    pub addr: String,
}

/// Reply from the leader for one forwarded proposal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ProposeReply {
    /// The leader applied the entry.
    Applied(Applied),
    /// The leader rejected the entry.
    Rejected(RejectReason),
}

#[derive(Debug, Clone, Default)]
struct Leadership {
    session: Option<String>,
    leader: Option<LeaderRecord>,
    term: u64,
    holds_leader: bool,
}

/// A coordinator backed by Consul sessions and KV compare-and-swap.
pub struct ConsulCoordinator {
    api: Arc<dyn ConsulApi>,
    config: ConsulConfig,
    store: Arc<MetadataStore>,
    metrics: Arc<Metrics>,
    http: reqwest::Client,
    leadership: Mutex<Leadership>,
    applied: AtomicU64,
    materialized: AtomicBool,
    started: AtomicBool,
}

impl ConsulCoordinator {
    /// Build one Consul coordinator.
    #[must_use]
    pub fn new(
        store: Arc<MetadataStore>,
        metrics: Arc<Metrics>,
        config: ConsulConfig,
        api: Arc<dyn ConsulApi>,
    ) -> Self {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(FORWARD_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self {
            api,
            config,
            store,
            metrics,
            http,
            leadership: Mutex::new(Leadership::default()),
            applied: AtomicU64::new(0),
            materialized: AtomicBool::new(false),
            started: AtomicBool::new(false),
        }
    }

    /// Start the election, renewal, and materialization loops.
    ///
    /// The first session is best effort. The loops retry when Consul is not
    /// available yet.
    pub async fn start(self: &Arc<Self>) {
        if self.started.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(error) = self.ensure_session().await {
            warn!(%error, "consul session create failed; the election loop will retry");
        }
        spawn_loop(Arc::downgrade(self), election_loop);
        spawn_loop(Arc::downgrade(self), renewal_loop);
        spawn_loop(Arc::downgrade(self), watch_loop);
    }

    /// Return the shared internal proposal token.
    #[must_use]
    pub fn secret(&self) -> &str {
        self.config.secret.expose_secret()
    }

    /// Return the node name.
    #[must_use]
    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    /// Return the current leader record.
    #[must_use]
    pub fn leader(&self) -> Option<LeaderRecord> {
        self.snapshot().leader
    }

    /// Return the current Consul session identifier.
    #[must_use]
    pub fn session(&self) -> Option<String> {
        self.snapshot().session
    }

    /// Return true when the first full reconcile completed and a leader exists.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.materialized.load(Ordering::Acquire) && self.leader().is_some()
    }

    /// Return the readiness reason.
    #[must_use]
    pub fn not_ready_reason(&self) -> Option<&'static str> {
        if !self.materialized.load(Ordering::Acquire) {
            return Some("consul metadata is not materialized");
        }
        if self.leader().is_none() {
            return Some("no consul leader");
        }
        None
    }

    /// Apply one forwarded proposal on the leader.
    ///
    /// # Errors
    ///
    /// Returns an error when this node is not the leader, the term is stale,
    /// or Consul fails.
    pub async fn apply_forwarded(&self, entry: Entry) -> Result<Applied, CoordError> {
        self.apply_entry(entry).await
    }

    /// Read one key or one bucket prefix from Consul with a consistent read,
    /// then update the local store.
    ///
    /// # Errors
    ///
    /// Returns an error when the Consul read fails.
    pub async fn refresh_read(&self, bucket: &str, key: Option<&str>) -> Result<(), CoordError> {
        if let Some(key) = key {
            let path = metadata_key(bucket, key);
            let entry = self.api.get(&path, true).await.map_err(map_consul)?;
            match entry {
                Some(entry) => self.materialize_entry(&entry)?,
                None => self.store.remove_materialized_metadata(bucket, key)?,
            }
            return Ok(());
        }
        let prefix = bucket_prefix(bucket);
        let result = self.api.list(&prefix, true).await.map_err(map_consul)?;
        self.reconcile_bucket(bucket, &result.entries)
    }

    /// Delete expired tombstones from Consul. Only the leader writes.
    ///
    /// # Errors
    ///
    /// Returns an error when a Consul operation fails.
    pub async fn expire_tombstones(&self, max_age: Duration) -> Result<u64, CoordError> {
        if !self.is_leader() {
            return Ok(0);
        }
        let now = unix_now();
        let age = i64::try_from(max_age.as_secs()).unwrap_or(i64::MAX);
        let result = self.api.list(META_PREFIX, true).await.map_err(map_consul)?;
        let mut removed = 0;
        for entry in result.entries {
            let Some((_, _, metadata)) = decode_metadata_entry(&entry)? else {
                continue;
            };
            if metadata.state != ObjectState::Tombstone {
                continue;
            }
            if metadata
                .deleted_at
                .is_none_or(|deleted| now.saturating_sub(deleted) < age)
            {
                continue;
            }
            if self
                .api
                .delete_cas(&entry.key, entry.modify_index)
                .await
                .map_err(map_consul)?
            {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// Refresh the exported cluster gauges.
    pub fn refresh_metrics(&self) {
        let leadership = self.snapshot();
        self.metrics.set_cluster_state(
            self.applied.load(Ordering::Acquire),
            leadership.term,
            self.store.tombstone_count().unwrap_or(0),
        );
        self.metrics
            .set_consul_leader(leadership.leader.as_ref().map(|leader| leader.id.as_str()));
        self.metrics.consul_materialized.store(
            u64::from(self.materialized.load(Ordering::Acquire)),
            Ordering::Relaxed,
        );
    }

    fn snapshot(&self) -> Leadership {
        self.leadership
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn ensure_session(&self) -> Result<String, CoordError> {
        if let Some(session) = self.snapshot().session {
            return Ok(session);
        }
        let session = self
            .api
            .create_session(self.config.session_ttl.as_duration())
            .await
            .map_err(map_consul)?;
        {
            let mut state = self
                .leadership
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.session.is_none() {
                state.session = Some(session.clone());
            }
        }
        Ok(session)
    }

    async fn step_down(&self, reason: &str) {
        let session = {
            let mut state = self
                .leadership
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let session = state.session.take();
            state.holds_leader = false;
            state.leader = None;
            state.term = 0;
            session
        };
        if let Some(session) = session
            && self.api.destroy_session(&session).await.is_err()
        {
            warn!("consul session destroy failed");
        }
        self.metrics
            .consul_session_losses
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.set_consul_leader(None);
        warn!(reason, "consul leadership lost");
    }

    async fn election_tick(&self) -> Result<(), CoordError> {
        let session = self.ensure_session().await?;
        if self.snapshot().holds_leader {
            let entry = self.api.get(LEADER_KEY, true).await.map_err(map_consul)?;
            let still_leader = entry
                .as_ref()
                .is_some_and(|entry| entry.session.as_deref() == Some(session.as_str()));
            if still_leader {
                return Ok(());
            }
            self.step_down("the consul leader key is not held").await;
            return Ok(());
        }
        let record = LeaderRecord {
            id: self.config.node_id.clone(),
            addr: self.config.advertise_addr.trim_end_matches('/').to_owned(),
        };
        let value =
            serde_json::to_vec(&record).map_err(|error| CoordError::Storage(error.to_string()))?;
        let acquired = self
            .api
            .acquire(LEADER_KEY, value, &session)
            .await
            .map_err(map_consul)?;
        let entry = self.api.get(LEADER_KEY, true).await.map_err(map_consul)?;
        let mut state = self
            .leadership
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if acquired {
            let term = entry.as_ref().map_or(0, |entry| entry.modify_index);
            let changed = state.leader.as_ref() != Some(&record);
            state.leader = Some(record);
            state.term = term;
            state.holds_leader = true;
            drop(state);
            if changed {
                self.metrics
                    .consul_leader_changes
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.metrics.set_consul_leader(Some(self.node_id()));
            info!(term, "consul leader acquired");
            return Ok(());
        }
        if let Some(entry) = entry {
            let leader = parse_leader(&entry)?;
            let term = entry.modify_index;
            state.leader = Some(leader);
            state.term = term;
            state.holds_leader = false;
            self.metrics
                .set_consul_leader(state.leader.as_ref().map(|leader| leader.id.as_str()));
        } else {
            state.leader = None;
            state.term = 0;
            state.holds_leader = false;
            self.metrics.set_consul_leader(None);
        }
        Ok(())
    }

    async fn renew_tick(&self) {
        let session = self.snapshot().session;
        let Some(session) = session else {
            return;
        };
        match self.api.renew_session(&session).await {
            Ok(true) => {
                self.metrics
                    .consul_session_renewals
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(false) => self.step_down("the consul session ended").await,
            Err(error) => {
                warn!(%error, "consul session renewal failed");
                self.step_down("the consul session renewal failed").await;
            }
        }
    }

    async fn current_leader(&self) -> Result<LeaderRecord, CoordError> {
        if let Some(leader) = self.snapshot().leader {
            return Ok(leader);
        }
        let entry = self
            .api
            .get(LEADER_KEY, true)
            .await
            .map_err(map_consul)?
            .ok_or(CoordError::Unavailable)?;
        let leader = parse_leader(&entry)?;
        {
            let mut state = self
                .leadership
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            state.term = entry.modify_index;
            state.leader = Some(leader.clone());
            state.holds_leader = false;
        }
        Ok(leader)
    }

    async fn forward_propose(&self, entry: Entry) -> Result<Applied, CoordError> {
        let leader = self.current_leader().await?;
        if leader.id == self.config.node_id && !self.is_leader() {
            return Err(CoordError::Unavailable);
        }
        let entry = retag_entry(entry, self.term());
        let url = format!("{}{}", leader.addr.trim_end_matches('/'), PROPOSE_PATH);
        let response = self
            .http
            .post(url)
            .header(TOKEN_HEADER, self.secret())
            .timeout(FORWARD_TIMEOUT)
            .json(&entry)
            .send()
            .await
            .map_err(|_| CoordError::Unavailable)?;
        match response.status() {
            reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
                return Err(CoordError::Storage(
                    "the consul leader rejected the proposal token".to_owned(),
                ));
            }
            reqwest::StatusCode::SERVICE_UNAVAILABLE | reqwest::StatusCode::NOT_FOUND => {
                return Err(CoordError::Unavailable);
            }
            status if !status.is_success() => {
                return Err(CoordError::Storage(format!(
                    "the consul leader returned status {status}"
                )));
            }
            _ => {}
        }
        let reply: ProposeReply = response
            .json()
            .await
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        match reply {
            ProposeReply::Applied(applied) => {
                self.materialize_applied(&entry, &applied)?;
                Ok(applied)
            }
            ProposeReply::Rejected(reason) => Err(CoordError::Rejected(reason)),
        }
    }

    async fn apply_entry(&self, entry: Entry) -> Result<Applied, CoordError> {
        if !self.is_leader() {
            return Err(CoordError::Unavailable);
        }
        let current_term = self.term();
        if entry.term() != current_term {
            return Err(CoordError::StaleTerm {
                expected: current_term,
                actual: entry.term(),
            });
        }
        match entry {
            Entry::Commit { request, .. } => self.apply_commit(&request).await,
            Entry::Delete {
                bucket,
                key,
                owner,
                deleted_at,
                ..
            } => self.apply_delete(&bucket, &key, &owner, deleted_at).await,
            Entry::BucketConfig {
                bucket, request, ..
            } => self.apply_bucket_config(&bucket, request).await,
            Entry::Abort { .. } => Ok(Applied {
                index: self.applied.load(Ordering::Acquire),
                term: current_term,
                result: ApplyResult::Blank,
            }),
        }
    }

    async fn apply_commit(&self, request: &CommitRequest) -> Result<Applied, CoordError> {
        let key = metadata_key(&request.bucket, &request.key);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let current_entry = self.api.get(&key, true).await.map_err(map_consul)?;
            let current = current_entry
                .as_ref()
                .map(|entry| decode_metadata(&entry.value))
                .transpose()?;
            if condition_failed(
                current.as_ref(),
                request.if_none_match,
                request.if_match.as_deref(),
            ) {
                return Err(CoordError::Rejected(RejectReason::PreconditionFailed));
            }
            let cas = current_entry.as_ref().map_or(0, |entry| entry.modify_index);
            let metadata = commit_metadata(current.as_ref(), request);
            let value = serde_json::to_vec(&metadata)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            if !self
                .api
                .put_cas(&key, value, cas)
                .await
                .map_err(map_consul)?
            {
                continue;
            }
            let stored = self
                .api
                .get(&key, true)
                .await
                .map_err(map_consul)?
                .ok_or_else(|| {
                    CoordError::Storage("the committed key is not readable".to_owned())
                })?;
            let mut metadata = decode_metadata(&stored.value)?;
            metadata.version = stored.modify_index;
            self.store
                .materialize_metadata(&request.bucket, &request.key, &metadata)?;
            self.note_applied(stored.modify_index);
            return Ok(Applied {
                index: stored.modify_index,
                term: self.term(),
                result: ApplyResult::Commit(metadata),
            });
        }
        Err(CoordError::Rejected(RejectReason::Conflict))
    }

    async fn apply_delete(
        &self,
        bucket: &str,
        key: &str,
        owner: &str,
        deleted_at: i64,
    ) -> Result<Applied, CoordError> {
        let path = metadata_key(bucket, key);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let current_entry = self.api.get(&path, true).await.map_err(map_consul)?;
            let current = current_entry
                .as_ref()
                .map(|entry| decode_metadata(&entry.value))
                .transpose()?;
            let cas = current_entry.as_ref().map_or(0, |entry| entry.modify_index);
            let metadata = tombstone_metadata(current.as_ref(), owner, deleted_at);
            let value = serde_json::to_vec(&metadata)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            if !self
                .api
                .put_cas(&path, value, cas)
                .await
                .map_err(map_consul)?
            {
                continue;
            }
            let stored = self
                .api
                .get(&path, true)
                .await
                .map_err(map_consul)?
                .ok_or_else(|| CoordError::Storage("the deleted key is not readable".to_owned()))?;
            let mut metadata = decode_metadata(&stored.value)?;
            metadata.version = stored.modify_index;
            self.store.materialize_metadata(bucket, key, &metadata)?;
            self.note_applied(stored.modify_index);
            return Ok(Applied {
                index: stored.modify_index,
                term: self.term(),
                result: ApplyResult::Delete(metadata),
            });
        }
        Err(CoordError::Rejected(RejectReason::Conflict))
    }

    async fn apply_bucket_config(
        &self,
        bucket: &str,
        config: BucketConfigRecord,
    ) -> Result<Applied, CoordError> {
        let path = bucket_key(bucket);
        for _attempt in 0..MAX_CAS_ATTEMPTS {
            let current = self.api.get(&path, true).await.map_err(map_consul)?;
            let cas = current.as_ref().map_or(0, |entry| entry.modify_index);
            let value = serde_json::to_vec(&config)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            if !self
                .api
                .put_cas(&path, value, cas)
                .await
                .map_err(map_consul)?
            {
                continue;
            }
            self.store.materialize_bucket_config(bucket, &config)?;
            let index = self
                .api
                .get(&path, true)
                .await
                .map_err(map_consul)?
                .map_or(0, |entry| entry.modify_index);
            self.note_applied(index);
            return Ok(Applied {
                index,
                term: self.term(),
                result: ApplyResult::BucketConfig(config),
            });
        }
        Err(CoordError::Rejected(RejectReason::Conflict))
    }

    fn materialize_applied(&self, entry: &Entry, applied: &Applied) -> Result<(), CoordError> {
        match (entry, &applied.result) {
            (Entry::Commit { request, .. }, ApplyResult::Commit(metadata)) => self
                .store
                .materialize_metadata(&request.bucket, &request.key, metadata),
            (Entry::Delete { bucket, key, .. }, ApplyResult::Delete(metadata)) => {
                self.store.materialize_metadata(bucket, key, metadata)
            }
            (
                Entry::BucketConfig {
                    bucket, request, ..
                },
                ApplyResult::BucketConfig(stored),
            ) => {
                let _ = request;
                self.store.materialize_bucket_config(bucket, stored)
            }
            _ => Ok(()),
        }
    }

    fn note_applied(&self, index: u64) {
        self.applied.fetch_max(index, Ordering::AcqRel);
        self.metrics.set_cluster_state(
            index,
            self.term(),
            self.store.tombstone_count().unwrap_or(0),
        );
    }

    fn reconcile_all_metadata(&self, entries: &[KvEntry]) -> Result<(), CoordError> {
        let mut present = std::collections::HashSet::new();
        for entry in entries {
            if let Some((bucket, key, metadata)) = decode_metadata_entry(entry)? {
                present.insert((bucket, key));
                self.materialize_entry_with(entry, &metadata)?;
            }
        }
        for (bucket, key, metadata) in self.store.scan_all()? {
            if metadata.state == ObjectState::Pending {
                continue;
            }
            if !present.contains(&(bucket.clone(), key.clone())) {
                self.store.remove_materialized_metadata(&bucket, &key)?;
            }
        }
        Ok(())
    }

    fn reconcile_bucket(&self, bucket: &str, entries: &[KvEntry]) -> Result<(), CoordError> {
        let mut present = std::collections::HashSet::new();
        for entry in entries {
            if let Some((entry_bucket, key, metadata)) = decode_metadata_entry(entry)? {
                if entry_bucket != bucket {
                    continue;
                }
                present.insert(key);
                self.materialize_entry_with(entry, &metadata)?;
            }
        }
        for (key, metadata) in self.store.scan_bucket(bucket)? {
            if metadata.state == ObjectState::Pending {
                continue;
            }
            if !present.contains(&key) {
                self.store.remove_materialized_metadata(bucket, &key)?;
            }
        }
        Ok(())
    }

    fn reconcile_bucket_configs(&self, entries: &[KvEntry]) -> Result<(), CoordError> {
        for entry in entries {
            let Some(bucket) = decode_bucket_key(&entry.key) else {
                continue;
            };
            let config: BucketConfigRecord = serde_json::from_slice(&entry.value)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            self.store.materialize_bucket_config(&bucket, &config)?;
        }
        Ok(())
    }

    fn materialize_entry(&self, entry: &KvEntry) -> Result<(), CoordError> {
        let Some(metadata) = decode_metadata_entry_metadata(entry)? else {
            return Ok(());
        };
        self.materialize_entry_with(entry, &metadata)
    }

    fn materialize_entry_with(
        &self,
        entry: &KvEntry,
        metadata: &Metadata,
    ) -> Result<(), CoordError> {
        let Some((bucket, key, _)) = decode_metadata_entry(entry)? else {
            return Ok(());
        };
        let mut metadata = metadata.clone();
        metadata.version = entry.modify_index;
        self.store.materialize_metadata(&bucket, &key, &metadata)
    }
}

impl Coordinator for ConsulCoordinator {
    fn term(&self) -> u64 {
        self.snapshot().term
    }

    fn is_leader(&self) -> bool {
        self.snapshot().holds_leader
    }

    fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    fn propose(&self, entry: Entry) -> BoxFuture<'_, Result<Applied, CoordError>> {
        Box::pin(async move {
            if self.is_leader() {
                match self.apply_entry(entry.clone()).await {
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
        Box::pin(async move { Ok(self.applied.load(Ordering::Acquire)) })
    }

    fn refresh_read<'a>(
        &'a self,
        bucket: &'a str,
        key: Option<&'a str>,
    ) -> BoxFuture<'a, Result<(), CoordError>> {
        Box::pin(async move { ConsulCoordinator::refresh_read(self, bucket, key).await })
    }
}

fn spawn_loop<F, Fut>(weak: Weak<ConsulCoordinator>, tick: F)
where
    F: Fn(Arc<ConsulCoordinator>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            let Some(coordinator) = weak.upgrade() else {
                break;
            };
            tick(Arc::clone(&coordinator)).await;
            drop(coordinator);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });
}

async fn election_loop(coordinator: Arc<ConsulCoordinator>) {
    if let Err(error) = coordinator.election_tick().await {
        warn!(%error, "consul election attempt failed");
    }
    let session_ttl = coordinator.config.session_ttl.as_duration();
    let interval = (session_ttl / 4).clamp(Duration::from_millis(100), Duration::from_secs(1));
    tokio::time::sleep(interval).await;
}

async fn renewal_loop(coordinator: Arc<ConsulCoordinator>) {
    let session_ttl = coordinator.config.session_ttl.as_duration();
    let interval = (session_ttl / 2).clamp(Duration::from_millis(100), Duration::from_secs(10));
    tokio::time::sleep(interval).await;
    coordinator.renew_tick().await;
}

async fn watch_loop(coordinator: Arc<ConsulCoordinator>) {
    let mut index = 0;
    loop {
        let result = if index == 0 {
            coordinator.api.list(META_PREFIX, true).await
        } else {
            coordinator
                .api
                .watch_prefix(META_PREFIX, index, WATCH_WAIT)
                .await
        };
        match result {
            Ok(result) => {
                if let Ok(buckets) = coordinator.api.list(BUCKET_PREFIX, true).await
                    && let Err(error) = coordinator.reconcile_bucket_configs(&buckets.entries)
                {
                    warn!(%error, "consul bucket config sync failed");
                }
                match coordinator.reconcile_all_metadata(&result.entries) {
                    Ok(()) => coordinator.materialized.store(true, Ordering::Release),
                    Err(error) => warn!(%error, "consul metadata sync failed"),
                }
                coordinator.note_applied(result.index.max(index));
                if result.index > index {
                    index = result.index;
                } else {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
            Err(error) => {
                warn!(%error, "consul metadata watch failed");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
}

fn retag_entry(entry: Entry, term: u64) -> Entry {
    match entry {
        Entry::Commit { request, .. } => Entry::Commit { term, request },
        Entry::Abort { bucket, key, .. } => Entry::Abort { term, bucket, key },
        Entry::Delete {
            bucket,
            key,
            owner,
            deleted_at,
            ..
        } => Entry::Delete {
            term,
            bucket,
            key,
            owner,
            deleted_at,
        },
        Entry::BucketConfig {
            bucket, request, ..
        } => Entry::BucketConfig {
            term,
            bucket,
            request,
        },
    }
}

#[allow(clippy::needless_pass_by_value)]
fn map_consul(error: ConsulError) -> CoordError {
    if error.is_transport() {
        CoordError::Unavailable
    } else {
        CoordError::Storage(error.to_string())
    }
}

fn parse_leader(entry: &KvEntry) -> Result<LeaderRecord, CoordError> {
    serde_json::from_slice(&entry.value).map_err(|error| CoordError::Storage(error.to_string()))
}

fn decode_metadata(value: &[u8]) -> Result<Metadata, CoordError> {
    serde_json::from_slice(value).map_err(|error| CoordError::Storage(error.to_string()))
}

fn decode_metadata_entry(
    entry: &KvEntry,
) -> Result<Option<(String, String, Metadata)>, CoordError> {
    let Some((bucket, key)) = decode_metadata_key(&entry.key) else {
        return Ok(None);
    };
    let metadata = decode_metadata(&entry.value)?;
    Ok(Some((bucket, key, metadata)))
}

fn decode_metadata_entry_metadata(entry: &KvEntry) -> Result<Option<Metadata>, CoordError> {
    Ok(decode_metadata_entry(entry)?.map(|(_, _, metadata)| metadata))
}

fn condition_failed(
    current: Option<&Metadata>,
    if_none_match: bool,
    if_match: Option<&str>,
) -> bool {
    if if_none_match && current.is_some_and(Metadata::is_committed) {
        return true;
    }
    if let Some(expected) = if_match {
        let Some(metadata) = current else {
            return true;
        };
        if !metadata.is_committed() {
            return true;
        }
        if expected.trim() != "*" && metadata.etag != normalize_etag(expected) {
            return true;
        }
    }
    false
}

fn commit_metadata(current: Option<&Metadata>, request: &CommitRequest) -> Metadata {
    let created_at = current
        .as_ref()
        .map_or(request.committed_at, |metadata| metadata.created_at);
    Metadata {
        state: ObjectState::Committed,
        version: 0,
        blob: request.blob.clone(),
        size: request.size,
        checksum: request.checksum.clone(),
        content_type: request.content_type.clone(),
        etag: normalize_etag(&request.etag),
        user_metadata: request.user_metadata.clone(),
        owner: request.owner.clone(),
        replicas: request.replicas.clone(),
        created_at,
        committed_at: Some(request.committed_at),
        deleted_at: None,
        cache_control: request.cache_control.clone(),
        content_encoding: request.content_encoding.clone(),
        content_disposition: request.content_disposition.clone(),
    }
}

fn tombstone_metadata(current: Option<&Metadata>, owner: &str, deleted_at: i64) -> Metadata {
    Metadata {
        state: ObjectState::Tombstone,
        version: 0,
        blob: current.map_or_else(String::new, |metadata| metadata.blob.clone()),
        size: 0,
        checksum: String::new(),
        content_type: None,
        etag: String::new(),
        user_metadata: std::collections::BTreeMap::new(),
        owner: owner.to_owned(),
        replicas: Vec::new(),
        created_at: current.map_or(deleted_at, |metadata| metadata.created_at),
        committed_at: None,
        deleted_at: Some(deleted_at),
        cache_control: None,
        content_encoding: None,
        content_disposition: None,
    }
}

fn normalize_etag(value: &str) -> String {
    value.trim().trim_matches('"').to_owned()
}

/// Encode the Consul key for one metadata row.
#[must_use]
pub fn metadata_key(bucket: &str, key: &str) -> String {
    format!(
        "{META_PREFIX}{}/{}",
        hex::encode(bucket.as_bytes()),
        hex::encode(key.as_bytes())
    )
}

/// Encode the Consul prefix for one bucket.
#[must_use]
pub fn bucket_prefix(bucket: &str) -> String {
    format!("{META_PREFIX}{}/", hex::encode(bucket.as_bytes()))
}

/// Encode the Consul key for one bucket config.
#[must_use]
pub fn bucket_key(bucket: &str) -> String {
    format!("{BUCKET_PREFIX}{}", hex::encode(bucket.as_bytes()))
}

/// Decode the bucket and object key from one Consul metadata key.
#[must_use]
pub fn decode_metadata_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix(META_PREFIX)?;
    let (bucket, object) = rest.split_once('/')?;
    let bucket = decode_hex(bucket)?;
    let object = decode_hex(object)?;
    Some((bucket, object))
}

fn decode_bucket_key(key: &str) -> Option<String> {
    decode_hex(key.strip_prefix(BUCKET_PREFIX)?)
}

fn decode_hex(value: &str) -> Option<String> {
    let bytes = hex::decode(value).ok()?;
    String::from_utf8(bytes).ok()
}
