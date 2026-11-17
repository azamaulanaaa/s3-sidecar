//! Single-node cluster mode: staged writes, metadata reads, and garbage
//! collection.

pub mod blob;
pub mod consul;
pub mod coordinator;
pub mod raft;
pub mod replication;
pub mod state;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

use axum::body::Body;
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use futures_util::{StreamExt, TryStreamExt};
use md5::Md5;
use percent_encoding::{NON_ALPHANUMERIC, percent_decode_str, utf8_percent_encode};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::task::JoinHandle;
use tracing::{Instrument, info_span, warn};

use self::consul::api::{ConsulApi, HttpConsul};
use self::consul::coordinator::ConsulCoordinator;
use self::coordinator::{CommitRequest, CoordError, Coordinator, Entry, RejectReason, SingleNode};
use self::raft::RaftNode;
use self::raft::coordinator::RaftCoordinator;
use self::state::{ApplyResult, BucketConfigRecord, Metadata, MetadataStore};
use crate::auth::ClientCredential;
use crate::auth::body::{BodyFailure, BodyStream, failure};
use crate::config::{Config, ConsensusBackend, Mode, ReadMode, WriteAck};
use crate::errors::S3Error;
use crate::metrics::Metrics;
use crate::storage::{ForwardRequest, S3Client, StorageError};

/// A request body for one cluster write.
pub struct WriteBody {
    /// Decoded body stream.
    pub body: Option<BodyStream>,
    /// Decoded content length, when known.
    pub content_length: Option<u64>,
    /// Body verification failure flag.
    pub failure: Option<Arc<std::sync::atomic::AtomicU8>>,
}

/// Summary of one garbage collection pass.
#[derive(Debug, Clone, Copy, Default)]
pub struct GcReport {
    /// Uploads that exceeded `staging_ttl`.
    pub uploads_aborted: u64,
    /// Tombstones that exceeded `orphan_ttl`.
    pub tombstones_removed: u64,
    /// Orphan blobs that were deleted.
    pub orphan_blobs_removed: u64,
}

/// Cluster mode state.
pub struct Cluster {
    config: Arc<Config>,
    state: Arc<MetadataStore>,
    coord: Arc<dyn Coordinator>,
    single: Option<Arc<SingleNode>>,
    raft: Option<Arc<RaftCoordinator>>,
    consul: Option<Arc<ConsulCoordinator>>,
    client: S3Client,
    credential: Arc<ClientCredential>,
    http: reqwest::Client,
    node_id: String,
    metrics: Arc<Metrics>,
    reconcile_cursor: AtomicUsize,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Cluster {
    /// Build cluster mode state.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend is not supported or the metadata file
    /// cannot be opened.
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>) -> Result<Option<Arc<Self>>, S3Error> {
        Self::new_with_consul_api(config, metrics, None)
    }

    /// Build cluster mode state with an explicit Consul API.
    ///
    /// A `None` API uses the HTTP client. Tests pass an in-memory API.
    ///
    /// # Errors
    ///
    /// Returns an error when the backend is not supported or the metadata file
    /// cannot be opened.
    pub fn new_with_consul_api(
        config: Arc<Config>,
        metrics: Arc<Metrics>,
        consul_api: Option<Arc<dyn ConsulApi>>,
    ) -> Result<Option<Arc<Self>>, S3Error> {
        if config.mode != Mode::Cluster {
            return Ok(None);
        }
        let state = Arc::new(
            MetadataStore::open(Path::new(config.cluster.metadata_path.as_str())).map_err(
                |error| S3Error::internal().with_header("x-s3-sidecar-error", &error.to_string()),
            )?,
        );
        let mut single = None;
        let mut raft = None;
        let mut consul = None;
        let coord: Arc<dyn Coordinator> = match config.consensus.backend {
            ConsensusBackend::Raft => {
                let coordinator = Arc::new(RaftCoordinator::new(
                    Arc::clone(&state),
                    Arc::clone(&metrics),
                    config.consensus.raft.node_id.clone(),
                ));
                raft = Some(Arc::clone(&coordinator));
                coordinator
            }
            ConsensusBackend::Consul => {
                let api = match consul_api {
                    Some(api) => api,
                    None => {
                        Arc::new(HttpConsul::new(&config.consensus.consul).map_err(|error| {
                            S3Error::internal()
                                .with_header("x-s3-sidecar-error", &error.to_string())
                        })?)
                    }
                };
                let coordinator = Arc::new(ConsulCoordinator::new(
                    Arc::clone(&state),
                    Arc::clone(&metrics),
                    config.consensus.consul.clone(),
                    api,
                ));
                consul = Some(Arc::clone(&coordinator));
                coordinator
            }
            ConsensusBackend::Single => {
                let coordinator =
                    Arc::new(SingleNode::new(Arc::clone(&state), Arc::clone(&metrics)));
                single = Some(Arc::clone(&coordinator));
                coordinator
            }
        };
        let node_id = match config.consensus.backend {
            ConsensusBackend::Consul => config.consensus.consul.node_id.clone(),
            ConsensusBackend::Raft | ConsensusBackend::Single => {
                config.consensus.raft.node_id.clone()
            }
        };
        let client = S3Client::new_with_upstream(&config.backing, &config.upstream)?
            .with_record_object_keys(config.otel.record_object_keys);
        let credential = Arc::new(ClientCredential::new(
            config.backing.access_key_id.clone(),
            config.backing.secret_access_key.clone(),
            config.backing.session_token.clone(),
            Vec::new(),
        ));
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(10))
            .read_timeout(std::time::Duration::from_secs(300))
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .build()
            .map_err(|_| S3Error::internal())?;
        Ok(Some(Arc::new(Self {
            config,
            state,
            coord,
            single,
            raft,
            consul,
            client,
            credential,
            http,
            node_id,
            metrics,
            reconcile_cursor: AtomicUsize::new(0),
            tasks: Mutex::new(Vec::new()),
        })))
    }

    /// Seed bucket configs and start the garbage collection loop.
    ///
    /// # Errors
    ///
    /// Returns an error when a bucket config cannot be stored.
    pub async fn start(self: &Arc<Self>) -> Result<(), S3Error> {
        if let Some(coordinator) = &self.raft {
            let node = RaftNode::start(
                Arc::clone(&self.state),
                &self.config,
                Arc::clone(&self.metrics),
            )
            .await
            .map_err(map_coord_error)?;
            coordinator.attach(node).map_err(map_coord_error)?;
            let _ = self.wait_ready(std::time::Duration::from_secs(5)).await;
        }
        if let Some(coordinator) = &self.consul {
            coordinator.start().await;
            let _ = self.wait_ready(std::time::Duration::from_secs(5)).await;
        }
        self.seed_buckets().await?;
        self.refresh_metrics();
        let interval = self.config.cluster.gc_interval.as_duration();
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(cluster) = weak.upgrade() else {
                    break;
                };
                if let Err(error) = cluster.gc_once().await {
                    warn!(%error, "cluster garbage collection failed");
                }
            }
        });
        self.track(task);
        for _ in 0..self.config.cluster.replicate_workers.max(1) {
            let weak = Arc::downgrade(self);
            let task = tokio::spawn(async move {
                loop {
                    let Some(cluster) = weak.upgrade() else {
                        break;
                    };
                    let worked = cluster.replication_tick().await;
                    drop(cluster);
                    if !worked {
                        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                    }
                }
            });
            self.track(task);
        }
        let interval = self.config.cluster.reconcile_interval.as_duration();
        let weak = Arc::downgrade(self);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(cluster) = weak.upgrade() else {
                    break;
                };
                if let Err(error) = cluster.reconcile_once().await {
                    warn!(%error, "cluster reconciliation failed");
                }
            }
        });
        self.track(task);
        Ok(())
    }

    /// Stop background workers, close coordination sessions, and flush state.
    pub async fn shutdown(&self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
        if let Some(coordinator) = &self.raft
            && let Some(node) = coordinator.node()
            && let Err(error) = node.raft().shutdown().await
        {
            warn!(%error, "raft shutdown failed");
        }
        if let Some(coordinator) = &self.consul {
            coordinator.shutdown().await;
        }
        if let Err(error) = self.state.flush() {
            warn!(%error, "metadata flush failed");
        }
    }

    fn track(&self, task: JoinHandle<()>) {
        let mut tasks = lock(&self.tasks);
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    /// Return the coordinator.
    #[must_use]
    pub fn coordinator(&self) -> &Arc<dyn Coordinator> {
        &self.coord
    }

    /// Return the metadata state machine.
    #[must_use]
    pub fn state(&self) -> &Arc<MetadataStore> {
        &self.state
    }

    /// Return the running raft node, when raft mode is active.
    #[must_use]
    pub fn raft_node(&self) -> Option<&Arc<RaftNode>> {
        self.raft
            .as_ref()
            .and_then(|coordinator| coordinator.node())
    }

    /// Return the Consul coordinator, when Consul mode is active.
    #[must_use]
    pub fn consul(&self) -> Option<&Arc<ConsulCoordinator>> {
        self.consul.as_ref()
    }

    /// Return true when cluster mode can serve reads and writes.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        if let Some(coordinator) = &self.consul {
            return coordinator.is_ready();
        }
        match &self.raft {
            Some(coordinator) => coordinator.node().is_some_and(|node| node.is_ready()),
            None => true,
        }
    }

    /// Return the readiness reason.
    #[must_use]
    pub fn not_ready_reason(&self) -> Option<&'static str> {
        if let Some(coordinator) = &self.consul {
            return coordinator.not_ready_reason();
        }
        self.raft
            .as_ref()
            .and_then(|coordinator| coordinator.node().and_then(|node| node.not_ready_reason()))
    }

    /// Wait until the cluster has a leader and this node applied one entry.
    ///
    /// # Errors
    ///
    /// Returns a service error when the wait times out.
    pub async fn wait_ready(&self, timeout: std::time::Duration) -> Result<(), S3Error> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_ready() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(S3Error::service_unavailable());
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    async fn seed_buckets(&self) -> Result<(), S3Error> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let mut pending = false;
            for bucket in &self.config.buckets {
                if self
                    .state
                    .bucket_config(&bucket.name)
                    .map_err(map_coord_error)?
                    .is_some()
                {
                    continue;
                }
                let request = BucketConfigRecord::from_config(bucket);
                match self
                    .coord
                    .propose(Entry::BucketConfig {
                        term: self.coord.term(),
                        bucket: bucket.name.clone(),
                        request,
                    })
                    .await
                {
                    Ok(_) => {}
                    Err(CoordError::Unavailable | CoordError::Indeterminate(_))
                        if self.raft.is_some() || self.consul.is_some() =>
                    {
                        pending = true;
                    }
                    Err(error) => return Err(map_coord_error(error)),
                }
            }
            if !pending {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    fn blob_bucket(&self) -> &str {
        &self.config.backing.bucket
    }

    /// Refresh the applied index, term, and tombstone gauges.
    pub fn refresh_metrics(&self) {
        if let Some(coordinator) = &self.consul {
            coordinator.refresh_metrics();
        } else if let Some(coordinator) = &self.raft {
            coordinator.refresh_metrics();
        } else if let Some(single) = &self.single {
            single.refresh_metrics();
        }
        if let Ok((depth, age)) = self.state.replication_stats(state::unix_now()) {
            self.metrics.set_replication_queue(depth, age);
        }
    }

    /// Apply one object write.
    ///
    /// # Errors
    ///
    /// Returns an error when the blob cannot be staged, the proposal is
    /// rejected, or a condition fails.
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        headers: &HeaderMap,
        input: WriteBody,
    ) -> Result<Response, S3Error> {
        reject_encryption_headers(headers)?;
        let current = self
            .state
            .get_metadata(bucket, key)
            .map_err(map_coord_error)?;
        let if_none_match = header_is(headers, header::IF_NONE_MATCH, "*");
        let if_match = header_string(headers, header::IF_MATCH);
        if condition_failed(current.as_ref(), if_none_match, if_match.as_deref()) {
            return Err(precondition_failed());
        }
        let base_version = current.as_ref().map_or(0, |metadata| metadata.version);
        let blob = next_id();
        let bucket_config = self.bucket_config(bucket)?;
        let replicas = self.replicas_for(bucket, key, bucket_config.replicas);
        let fingerprint = Arc::new(Mutex::new(Fingerprint::new()));
        let stage_span = info_span!(
            "cluster.stage",
            "s3.bucket.name" = bucket,
            "cluster.node_id" = self.node_id.as_str(),
            "cluster.result" = tracing::field::Empty,
        );
        let stage_result = self
            .stage_blob(&blob, input, Arc::clone(&fingerprint))
            .instrument(stage_span.clone())
            .await;
        stage_span.record(
            "cluster.result",
            if stage_result.is_ok() { "ok" } else { "error" },
        );
        if let Err(error) = stage_result {
            self.metrics.aborted_write();
            return Err(error);
        }
        let (etag, checksum, size) = fingerprint_result(&fingerprint);
        let request = CommitRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            base_version,
            blob: blob.clone(),
            size,
            checksum,
            content_type: header_string(headers, header::CONTENT_TYPE),
            etag: etag.clone(),
            user_metadata: user_metadata(headers),
            owner: self.node_id.clone(),
            replicas,
            if_none_match,
            if_match,
            cache_control: header_string(headers, header::CACHE_CONTROL),
            content_encoding: header_string(headers, header::CONTENT_ENCODING),
            content_disposition: header_string(headers, header::CONTENT_DISPOSITION),
            content_language: header_string(headers, header::CONTENT_LANGUAGE),
            expires: header_string(headers, header::EXPIRES),
            storage_class: storage_class(headers),
            committed_at: state::unix_now(),
        };
        let metadata = self
            .commit_staged_write(bucket, key, &blob, request, &bucket_config)
            .await?;
        Ok(put_response(&metadata))
    }

    /// Replicate a staged blob as the bucket policy requires, then commit it.
    ///
    /// # Errors
    ///
    /// Returns `ServiceUnavailable` when the peer copies do not reach the
    /// required count, and the coordinator error when the commit fails.
    async fn commit_staged_write(
        &self,
        bucket: &str,
        key: &str,
        blob: &str,
        request: CommitRequest,
        bucket_config: &BucketConfigRecord,
    ) -> Result<Metadata, S3Error> {
        let span = info_span!(
            "cluster.commit",
            "s3.bucket.name" = bucket,
            "cluster.node_id" = self.node_id.as_str(),
            "cluster.write_ack" = tracing::field::Empty,
            "cluster.result" = tracing::field::Empty,
        );
        self.commit_staged_write_inner(bucket, key, blob, request, bucket_config)
            .instrument(span)
            .await
    }

    async fn commit_staged_write_inner(
        &self,
        bucket: &str,
        key: &str,
        blob: &str,
        mut request: CommitRequest,
        bucket_config: &BucketConfigRecord,
    ) -> Result<Metadata, S3Error> {
        let policy = bucket_config.write_ack;
        tracing::Span::current().record("cluster.write_ack", policy.to_string());
        self.metrics.write_ack(&policy.to_string());
        let intended: Vec<String> = request
            .replicas
            .iter()
            .filter(|peer| *peer != &self.node_id)
            .cloned()
            .collect();
        let required = match policy {
            WriteAck::Leader => 0,
            WriteAck::Quorum => {
                usize::try_from(bucket_config.min_replicas.saturating_sub(1)).unwrap_or(usize::MAX)
            }
            WriteAck::All => intended.len(),
        };
        let acked = if policy == WriteAck::Leader {
            Vec::new()
        } else {
            self.replicate_blob_sync(blob, &intended, required).await
        };
        if acked.len() < required {
            self.metrics.aborted_write();
            if policy == WriteAck::Quorum {
                self.metrics.quorum_failure();
            }
            self.discard_staged_blob(blob, &acked).await;
            tracing::Span::current().record("cluster.result", "aborted");
            return Err(S3Error::no_leader());
        }
        if policy != WriteAck::Leader {
            let mut actual = Vec::with_capacity(acked.len().saturating_add(1));
            actual.push(self.node_id.clone());
            actual.extend(acked.iter().cloned());
            request.replicas = actual;
        }
        let started = Instant::now();
        let applied = self
            .coord
            .propose(Entry::Commit {
                term: self.coord.term(),
                request: Box::new(request),
            })
            .await;
        match applied {
            Ok(applied) => {
                self.metrics.commit_latency(started.elapsed());
                self.metrics.committed_write();
                if policy == WriteAck::Quorum {
                    self.metrics.quorum_success();
                }
                self.refresh_metrics();
                let ApplyResult::Commit(metadata) = applied.result else {
                    return Err(S3Error::internal());
                };
                if policy == WriteAck::Leader {
                    self.enqueue_put_replication(bucket, key, &metadata);
                } else {
                    let remaining: Vec<String> = intended
                        .iter()
                        .filter(|peer| !acked.contains(peer))
                        .cloned()
                        .collect();
                    self.enqueue_put_targets(bucket, key, &metadata, remaining);
                }
                tracing::Span::current().record("cluster.result", "ok");
                Ok(metadata)
            }
            Err(error) => {
                self.metrics.aborted_write();
                if policy == WriteAck::Quorum {
                    self.metrics.quorum_failure();
                }
                self.discard_staged_blob(blob, &acked).await;
                tracing::Span::current().record("cluster.result", "error");
                Err(map_coord_error(error))
            }
        }
    }

    /// Copy one committed object to a new key.
    ///
    /// # Errors
    ///
    /// Returns `NoSuchKey` when the source is absent and a service error when
    /// the backing copy or the commit fails.
    #[allow(clippy::too_many_lines)]
    pub async fn copy_object(
        &self,
        bucket: &str,
        key: &str,
        headers: &HeaderMap,
    ) -> Result<Response, S3Error> {
        reject_encryption_headers(headers)?;
        let source_value = header_text(headers, "x-amz-copy-source")
            .ok_or_else(|| S3Error::invalid_request("The copy source header is required."))?;
        let (source_bucket, source_key) = parse_copy_source(&source_value)
            .ok_or_else(|| S3Error::invalid_request("The copy source is not valid."))?;
        let Some(source) = self
            .state
            .get_metadata(&source_bucket, &source_key)
            .map_err(map_coord_error)?
        else {
            return Err(no_such_key());
        };
        if !source.is_committed() {
            return Err(no_such_key().with_header("x-s3-sidecar-owner", &source.owner));
        }
        if self.head_blob(&source.blob).await.is_none() {
            self.metrics.missing_blob();
            return Err(no_such_key().with_header("x-s3-sidecar-owner", &source.owner));
        }
        let current = self
            .state
            .get_metadata(bucket, key)
            .map_err(map_coord_error)?;
        let if_none_match = header_is(headers, header::IF_NONE_MATCH, "*");
        let if_match = header_string(headers, header::IF_MATCH);
        if condition_failed(current.as_ref(), if_none_match, if_match.as_deref()) {
            return Err(precondition_failed());
        }
        let directive =
            header_text(headers, "x-amz-metadata-directive").unwrap_or_else(|| "COPY".to_owned());
        let replace = if directive.eq_ignore_ascii_case("COPY") {
            false
        } else if directive.eq_ignore_ascii_case("REPLACE") {
            true
        } else {
            return Err(S3Error::invalid_request(
                "The metadata directive must be COPY or REPLACE.",
            ));
        };
        let object = if replace {
            ObjectMetadata::from_headers(headers)
        } else {
            ObjectMetadata::from_metadata(&source)
        };
        let blob = next_id();
        let source_path = format!("/{}/objects/{}", self.blob_bucket(), source.blob);
        let mut copy_headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(&source_path) {
            copy_headers.insert("x-amz-copy-source", value);
        }
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::PUT,
                    path_and_query: format!("/{}/objects/{blob}", self.blob_bucket()),
                    headers: copy_headers,
                    body: None,
                    content_length: None,
                },
            )
            .await
            .map_err(|error| storage_error(error, None))?;
        if !response.status().is_success() {
            if response.status() == StatusCode::NOT_FOUND {
                self.metrics.missing_blob();
                return Err(no_such_key().with_header("x-s3-sidecar-owner", &source.owner));
            }
            return Err(S3Error::from_status(response.status()));
        }
        let bucket_config = self.bucket_config(bucket)?;
        let replicas = self.replicas_for(bucket, key, bucket_config.replicas);
        let request = CommitRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            base_version: current.as_ref().map_or(0, |metadata| metadata.version),
            blob: blob.clone(),
            size: source.size,
            checksum: source.checksum.clone(),
            content_type: object.content_type,
            etag: source.etag.clone(),
            user_metadata: object.user_metadata,
            owner: self.node_id.clone(),
            replicas,
            if_none_match,
            if_match,
            cache_control: object.cache_control,
            content_encoding: object.content_encoding,
            content_disposition: object.content_disposition,
            content_language: object.content_language,
            expires: object.expires,
            storage_class: object.storage_class,
            committed_at: state::unix_now(),
        };
        let metadata = self
            .commit_staged_write(bucket, key, &blob, request, &bucket_config)
            .await?;
        Ok(copy_response(&metadata))
    }

    async fn read_index_for(&self, bucket: &str, key: Option<&str>) -> Result<(), S3Error> {
        let span = info_span!(
            "cluster.read_index",
            "s3.bucket.name" = bucket,
            "cluster.node_id" = self.node_id.as_str(),
            "cluster.result" = tracing::field::Empty,
        );
        let result = async {
            self.coord.read_index().await.map_err(map_coord_error)?;
            self.coord
                .refresh_read(bucket, key)
                .await
                .map_err(map_coord_error)?;
            Ok(())
        }
        .instrument(span.clone())
        .await;
        span.record(
            "cluster.result",
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }

    /// Read one object or its metadata.
    ///
    /// # Errors
    ///
    /// Returns `NoSuchKey` when the key or blob is absent and a service error
    /// when the backing S3 request fails.
    pub async fn get(
        &self,
        method: &Method,
        bucket: &str,
        key: &str,
        headers: &HeaderMap,
    ) -> Result<Response, S3Error> {
        let config = self.bucket_config(bucket)?;
        if config.read == ReadMode::Leader {
            self.read_index_for(bucket, Some(key)).await?;
        }
        let Some(metadata) = self
            .state
            .get_metadata(bucket, key)
            .map_err(map_coord_error)?
        else {
            return Err(no_such_key());
        };
        if !metadata.is_committed() {
            return Err(no_such_key());
        }
        if config.read == ReadMode::Local {
            return if let Some(response) = self.local_blob(&metadata, method, headers).await? {
                Ok(response)
            } else {
                self.metrics.missing_blob();
                Err(no_such_key().with_header("x-s3-sidecar-owner", &metadata.owner))
            };
        }
        if let Some(response) = self.local_blob(&metadata, method, headers).await? {
            return Ok(response);
        }
        if let Some(response) = self.fetch_from_peers(&metadata, method, headers).await? {
            Ok(response)
        } else {
            self.metrics.missing_blob();
            Err(no_such_key().with_header("x-s3-sidecar-owner", &metadata.owner))
        }
    }

    async fn local_blob(
        &self,
        metadata: &Metadata,
        method: &Method,
        headers: &HeaderMap,
    ) -> Result<Option<Response>, S3Error> {
        let mut request_headers = HeaderMap::new();
        if let Some(range) = headers.get(header::RANGE) {
            request_headers.insert(header::RANGE, range.clone());
        }
        let request = ForwardRequest {
            method: method.clone(),
            path_and_query: format!("/{}/objects/{}", self.blob_bucket(), metadata.blob),
            headers: request_headers,
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(|error| storage_error(error, None))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        self.metrics.cluster_read_local();
        self.metrics.add_served_bytes(metadata.size);
        if method == Method::HEAD {
            drop(response);
            return Ok(Some(metadata_response(metadata)));
        }
        Ok(Some(stream_response(response, metadata)))
    }

    /// Delete one object. A missing key also returns `204`.
    ///
    /// # Errors
    ///
    /// Returns an error when the delete proposal fails.
    pub async fn delete(&self, bucket: &str, key: &str) -> Result<Response, S3Error> {
        let current = self
            .state
            .get_metadata(bucket, key)
            .map_err(map_coord_error)?;
        let blob = current
            .as_ref()
            .map(|metadata| metadata.blob.clone())
            .filter(|blob| !blob.is_empty());
        self.coord
            .propose(Entry::Delete {
                term: self.coord.term(),
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                owner: self.node_id.clone(),
                deleted_at: state::unix_now(),
            })
            .await
            .map_err(map_coord_error)?;
        self.refresh_metrics();
        if let Some(current) = &current {
            self.enqueue_delete_replication(bucket, key, current);
        }
        if let Some(blob) = blob {
            let client = self.client.clone();
            let credential = Arc::clone(&self.credential);
            let bucket = self.config.backing.bucket.clone();
            tokio::spawn(async move {
                delete_backing_blob(client, credential, &bucket, blob).await;
            });
        }
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        Ok(response)
    }

    /// List objects from committed metadata only.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid token or a metadata read failure.
    pub async fn list_objects(&self, bucket: &str, query: &str) -> Result<Response, S3Error> {
        let pairs = query_pairs(query);
        let version2 = query_value(&pairs, "list-type") == Some("2");
        if self.bucket_config(bucket)?.read == ReadMode::Leader {
            self.read_index_for(bucket, None).await?;
        }
        let prefix = query_value(&pairs, "prefix").unwrap_or_default();
        let delimiter = query_value(&pairs, "delimiter").filter(|value| !value.is_empty());
        let max_keys = query_value(&pairs, "max-keys")
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(1_000)
            .min(1_000);
        let marker = query_value(&pairs, "marker").unwrap_or_default();
        let start_after = if version2 {
            match query_value(&pairs, "continuation-token") {
                Some(token) => Some(decode_token(token)?),
                None => query_value(&pairs, "start-after").map(str::to_owned),
            }
        } else if marker.is_empty() {
            None
        } else {
            Some(marker.to_owned())
        };
        let rows = self.state.scan_bucket(bucket).map_err(map_coord_error)?;
        let mut contents = Vec::new();
        let mut prefixes: Vec<String> = Vec::new();
        let mut count = 0;
        let mut last = None;
        let mut truncated = false;
        for (key, metadata) in rows {
            if !metadata.is_committed() || !key.starts_with(prefix) {
                continue;
            }
            let rest = &key[prefix.len()..];
            if let Some(delimiter) = delimiter
                && let Some(index) = rest.find(delimiter)
            {
                let common = format!("{prefix}{}", &rest[..index + delimiter.len()]);
                if start_after
                    .as_ref()
                    .is_some_and(|after| common.as_str() <= after.as_str())
                {
                    continue;
                }
                if prefixes.last() == Some(&common) {
                    continue;
                }
                if count >= max_keys {
                    truncated = true;
                    break;
                }
                last = Some(common.clone());
                prefixes.push(common);
                count += 1;
                continue;
            }
            if start_after
                .as_ref()
                .is_some_and(|after| key.as_str() <= after.as_str())
            {
                continue;
            }
            if count >= max_keys {
                truncated = true;
                break;
            }
            last = Some(key.clone());
            contents.push((key, metadata));
            count += 1;
        }
        if version2 {
            let token = last.as_deref().map(encode_token);
            return Ok(list_response(
                bucket,
                prefix,
                max_keys,
                truncated,
                token.as_deref(),
                &contents,
                &prefixes,
            ));
        }
        let next_marker = if truncated { last.as_deref() } else { None };
        Ok(list_response_v1(
            bucket,
            prefix,
            marker,
            delimiter,
            max_keys,
            truncated,
            next_marker,
            &contents,
            &prefixes,
        ))
    }

    /// Start one multipart upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing upload cannot be created.
    pub async fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        headers: &HeaderMap,
    ) -> Result<Response, S3Error> {
        reject_encryption_headers(headers)?;
        let object = ObjectMetadata::from_headers(headers);
        let blob = next_id();
        let upload_id = format!("{}-{}", hex::encode(self.node_id.as_bytes()), next_id());
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::POST,
                    path_and_query: format!("/{}/objects/{blob}?uploads", self.blob_bucket()),
                    headers: HeaderMap::new(),
                    body: None,
                    content_length: Some(0),
                },
            )
            .await
            .map_err(|error| storage_error(error, None))?;
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| S3Error::service_unavailable())?;
        let text = String::from_utf8_lossy(&body);
        let backing_upload_id = extract_tag(&text, "UploadId")
            .ok_or_else(S3Error::internal)?
            .to_owned();
        let record = state::UploadRecord {
            upload_id: upload_id.clone(),
            backing_upload_id,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            node_id: self.node_id.clone(),
            blob: blob.clone(),
            content_type: object.content_type,
            user_metadata: object.user_metadata,
            cache_control: object.cache_control,
            content_encoding: object.content_encoding,
            content_disposition: object.content_disposition,
            content_language: object.content_language,
            expires: object.expires,
            storage_class: object.storage_class,
            created_at: state::unix_now(),
        };
        self.state.put_upload(&record).map_err(map_coord_error)?;
        self.state
            .set_pending(bucket, key, &blob, &self.node_id)
            .map_err(map_coord_error)?;
        Ok(xml_response(
            StatusCode::OK,
            format!(
                "<InitiateMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key>\
                 <UploadId>{}</UploadId></InitiateMultipartUploadResult>",
                xml_escape(bucket),
                xml_escape(key),
                xml_escape(&upload_id)
            ),
        ))
    }

    /// Upload one part to the owning node.
    ///
    /// # Errors
    ///
    /// Returns a clear error when the upload belongs to another node. Returns
    /// `NotImplemented` when the request carries an encryption header.
    pub async fn upload_part(
        &self,
        upload_id: &str,
        part_number: &str,
        headers: &HeaderMap,
        input: WriteBody,
    ) -> Result<Response, S3Error> {
        reject_encryption_headers(headers)?;
        let record = self.upload_for_node(upload_id)?;
        let path = format!(
            "/{}/objects/{}?partNumber={}&uploadId={}",
            self.blob_bucket(),
            record.blob,
            encode_query(part_number),
            encode_query(&record.backing_upload_id)
        );
        let request = ForwardRequest {
            method: Method::PUT,
            path_and_query: path,
            headers: HeaderMap::new(),
            body: input.body,
            content_length: input.content_length,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(|error| storage_error(error, input.failure.as_ref()))?;
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        let mut output = Response::new(Body::empty());
        *output.status_mut() = StatusCode::OK;
        if let Some(etag) = response.headers().get(header::ETAG) {
            output.headers_mut().insert(header::ETAG, etag.clone());
        }
        Ok(output)
    }

    /// Complete one multipart upload and commit its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing upload or the commit fails. Returns
    /// `NotImplemented` when the request carries an encryption header.
    #[allow(clippy::too_many_lines)]
    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        headers: &HeaderMap,
        input: WriteBody,
    ) -> Result<Response, S3Error> {
        reject_encryption_headers(headers)?;
        let record = self.upload_for_node(upload_id)?;
        let path = format!(
            "/{}/objects/{}?uploadId={}",
            self.blob_bucket(),
            record.blob,
            encode_query(&record.backing_upload_id)
        );
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::POST,
                    path_and_query: path,
                    headers: HeaderMap::new(),
                    body: input.body,
                    content_length: input.content_length,
                },
            )
            .await
            .map_err(|error| storage_error(error, input.failure.as_ref()))?;
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        let header_etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = response
            .bytes()
            .await
            .map_err(|_| S3Error::service_unavailable())?;
        let text = String::from_utf8_lossy(&body);
        let etag = header_etag
            .or_else(|| extract_tag(&text, "ETag").map(str::to_owned))
            .unwrap_or_default();
        let size = self.head_blob(&record.blob).await.unwrap_or(0);
        let current = self
            .state
            .get_metadata(&record.bucket, &record.key)
            .map_err(map_coord_error)?;
        let bucket_config = self.bucket_config(&record.bucket)?;
        let replicas = self.replicas_for(&record.bucket, &record.key, bucket_config.replicas);
        let request = CommitRequest {
            bucket: record.bucket.clone(),
            key: record.key.clone(),
            base_version: current.as_ref().map_or(0, |metadata| metadata.version),
            blob: record.blob.clone(),
            size,
            checksum: etag.clone(),
            content_type: record.content_type.clone(),
            etag: etag.clone(),
            user_metadata: record.user_metadata.clone(),
            owner: self.node_id.clone(),
            replicas,
            if_none_match: false,
            if_match: None,
            cache_control: record.cache_control.clone(),
            content_encoding: record.content_encoding.clone(),
            content_disposition: record.content_disposition.clone(),
            content_language: record.content_language.clone(),
            expires: record.expires.clone(),
            storage_class: record.storage_class.clone(),
            committed_at: state::unix_now(),
        };
        let result = self
            .commit_staged_write(
                &record.bucket,
                &record.key,
                &record.blob,
                request,
                &bucket_config,
            )
            .await;
        let metadata = match result {
            Ok(metadata) => metadata,
            Err(error) => {
                self.state
                    .remove_upload(&record.upload_id)
                    .map_err(map_coord_error)?;
                return Err(error);
            }
        };
        self.state
            .remove_upload(&record.upload_id)
            .map_err(map_coord_error)?;
        Ok(xml_response(
            StatusCode::OK,
            format!(
                "<CompleteMultipartUploadResult><Bucket>{}</Bucket><Key>{}</Key>\
                 <ETag>\"{}\"</ETag></CompleteMultipartUploadResult>",
                xml_escape(&record.bucket),
                xml_escape(&record.key),
                xml_escape(&metadata.etag)
            ),
        ))
    }

    /// Abort one multipart upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the backing abort fails.
    pub async fn abort_multipart(&self, upload_id: &str) -> Result<Response, S3Error> {
        let record = self.upload_for_node(upload_id)?;
        self.abort_backing(&record).await?;
        self.state
            .remove_upload(&record.upload_id)
            .map_err(map_coord_error)?;
        self.state
            .clear_pending(&record.bucket, &record.key)
            .map_err(map_coord_error)?;
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::NO_CONTENT;
        Ok(response)
    }

    /// List the parts of one multipart upload.
    ///
    /// # Errors
    ///
    /// Returns an error when the upload belongs to another node.
    pub async fn list_parts(&self, upload_id: &str, query: &str) -> Result<Response, S3Error> {
        let record = self.upload_for_node(upload_id)?;
        let pairs = query_pairs(query);
        let mut path = format!(
            "/{}/objects/{}?uploadId={}",
            self.blob_bucket(),
            record.blob,
            encode_query(&record.backing_upload_id)
        );
        for name in ["part-number-marker", "max-parts"] {
            if let Some(value) = query_value(&pairs, name) {
                path.push('&');
                path.push_str(name);
                path.push('=');
                path.push_str(&encode_query(value));
            }
        }
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::GET,
                    path_and_query: path,
                    headers: HeaderMap::new(),
                    body: None,
                    content_length: None,
                },
            )
            .await
            .map_err(|error| storage_error(error, None))?;
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        let status = response.status();
        let headers = response.headers().clone();
        let body = Body::from_stream(
            response
                .bytes_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
        );
        let mut output = Response::new(body);
        *output.status_mut() = status;
        copy_headers(&headers, output.headers_mut());
        Ok(output)
    }

    /// Run one garbage collection pass.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata or the backing listing cannot be read.
    #[tracing::instrument(name = "gc.cycle", skip_all)]
    pub async fn gc_once(&self) -> Result<GcReport, S3Error> {
        let now = state::unix_now();
        let staging = duration_seconds(self.config.cluster.staging_ttl.as_duration());
        let orphan = duration_seconds(self.config.cluster.orphan_ttl.as_duration());
        let mut report = GcReport::default();
        for upload in self.state.uploads().map_err(map_coord_error)? {
            if now.saturating_sub(upload.created_at) >= staging {
                self.abort_backing(&upload).await?;
                self.state
                    .remove_upload(&upload.upload_id)
                    .map_err(map_coord_error)?;
                self.state
                    .clear_pending(&upload.bucket, &upload.key)
                    .map_err(map_coord_error)?;
                report.uploads_aborted += 1;
            }
        }
        for (bucket, key, metadata) in self.state.tombstones().map_err(map_coord_error)? {
            if metadata
                .deleted_at
                .is_some_and(|deleted| now.saturating_sub(deleted) >= orphan)
            {
                if !metadata.blob.is_empty() {
                    delete_backing_blob(
                        self.client.clone(),
                        Arc::clone(&self.credential),
                        self.blob_bucket(),
                        metadata.blob.clone(),
                    )
                    .await;
                    report.orphan_blobs_removed += 1;
                    self.metrics.gc_deleted();
                }
                if self.consul.is_none() {
                    self.state
                        .remove_metadata(&bucket, &key)
                        .map_err(map_coord_error)?;
                }
                report.tombstones_removed += 1;
            }
        }
        if let Some(coordinator) = &self.consul {
            coordinator
                .expire_tombstones(self.config.cluster.orphan_ttl.as_duration())
                .await
                .map_err(map_coord_error)?;
        }
        let referenced = self.state.referenced_blobs().map_err(map_coord_error)?;
        for object in self.list_backing_objects().await? {
            if referenced.contains(&object.key) {
                continue;
            }
            if let Some(modified) = object.modified
                && now.saturating_sub(modified) >= staging
            {
                delete_backing_blob(
                    self.client.clone(),
                    Arc::clone(&self.credential),
                    self.blob_bucket(),
                    object.key,
                )
                .await;
                report.orphan_blobs_removed += 1;
                self.metrics.gc_deleted();
            }
        }
        self.refresh_metrics();
        Ok(report)
    }

    fn bucket_config(&self, bucket: &str) -> Result<BucketConfigRecord, S3Error> {
        if let Some(config) = self.state.bucket_config(bucket).map_err(map_coord_error)? {
            return Ok(config);
        }
        let from_file = self
            .config
            .buckets
            .iter()
            .find(|entry| entry.name == bucket)
            .map(BucketConfigRecord::from_config);
        Ok(from_file.unwrap_or(BucketConfigRecord {
            write_ack: crate::config::WriteAck::Leader,
            read: ReadMode::LocalThenPeer,
            replicas: 1,
            min_replicas: 1,
        }))
    }

    fn upload_for_node(&self, upload_id: &str) -> Result<state::UploadRecord, S3Error> {
        if let Some(owner) = upload_owner(upload_id)
            && owner != self.node_id
        {
            return Err(wrong_node(&owner));
        }
        let record = self
            .state
            .upload(upload_id)
            .map_err(map_coord_error)?
            .ok_or_else(no_such_upload)?;
        if record.node_id != self.node_id {
            return Err(wrong_node(&record.node_id));
        }
        Ok(record)
    }

    async fn stage_blob(
        &self,
        blob: &str,
        input: WriteBody,
        fingerprint: Arc<Mutex<Fingerprint>>,
    ) -> Result<(), S3Error> {
        let body = input
            .body
            .map(|body| fingerprint_stream(body, Arc::clone(&fingerprint)));
        let body = body.unwrap_or_else(|| {
            let stream: BodyStream = Box::pin(futures_util::stream::empty());
            stream
        });
        let response = crate::multipart::put_object(
            &self.client,
            &self.credential,
            self.blob_bucket(),
            &format!("objects/{blob}"),
            HeaderMap::new(),
            body,
            input.content_length,
            &self.config.multipart,
            &self.metrics,
        )
        .await
        .map_err(|error| storage_error(error, input.failure.as_ref()))?;
        if !response.status().is_success() {
            return Err(S3Error::from_status(response.status()));
        }
        if let Some(error) = body_failure_error(input.failure.as_ref()) {
            return Err(error);
        }
        Ok(())
    }

    async fn head_blob(&self, blob: &str) -> Option<u64> {
        let request = ForwardRequest {
            method: Method::HEAD,
            path_and_query: format!("/{}/objects/{blob}", self.blob_bucket()),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self.client.execute(&self.credential, request).await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
    }

    async fn abort_backing(&self, record: &state::UploadRecord) -> Result<(), S3Error> {
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::DELETE,
                    path_and_query: format!(
                        "/{}/objects/{}?uploadId={}",
                        self.blob_bucket(),
                        record.blob,
                        encode_query(&record.backing_upload_id)
                    ),
                    headers: HeaderMap::new(),
                    body: None,
                    content_length: None,
                },
            )
            .await
            .map_err(|error| storage_error(error, None))?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(S3Error::from_status(response.status()))
        }
    }

    async fn list_backing_objects(&self) -> Result<Vec<ListedObject>, S3Error> {
        let mut output = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/{}?list-type=2&prefix=", self.blob_bucket());
            path.push_str(&encode_query("objects/"));
            if let Some(token) = &token {
                path.push('&');
                path.push_str("continuation-token=");
                path.push_str(&encode_query(token));
            }
            let response = self
                .client
                .execute(
                    &self.credential,
                    ForwardRequest {
                        method: Method::GET,
                        path_and_query: path,
                        headers: HeaderMap::new(),
                        body: None,
                        content_length: None,
                    },
                )
                .await
                .map_err(|error| storage_error(error, None))?;
            if !response.status().is_success() {
                return Err(S3Error::from_status(response.status()));
            }
            let body = response
                .bytes()
                .await
                .map_err(|_| S3Error::service_unavailable())?;
            let text = String::from_utf8_lossy(&body);
            for mut object in parse_listing(&text) {
                if let Some(blob) = object.key.strip_prefix("objects/") {
                    let blob = blob.to_owned();
                    object.key = blob;
                    output.push(object);
                }
            }
            let truncated = extract_tag(&text, "IsTruncated")
                .is_some_and(|value| value.eq_ignore_ascii_case("true"));
            if !truncated {
                break;
            }
            token = extract_tag(&text, "NextContinuationToken").map(str::to_owned);
            if token.is_none() {
                break;
            }
        }
        Ok(output)
    }
}

struct ListedObject {
    key: String,
    modified: Option<i64>,
}

struct Fingerprint {
    md5: Md5,
    sha: Sha256,
    size: u64,
}

impl Fingerprint {
    fn new() -> Self {
        Self {
            md5: Md5::new(),
            sha: Sha256::new(),
            size: 0,
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.md5.update(bytes);
        self.sha.update(bytes);
        self.size = self
            .size
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
    }

    fn finish(self) -> (String, String, u64) {
        (
            hex::encode(self.md5.finalize()),
            hex::encode(self.sha.finalize()),
            self.size,
        )
    }
}

fn fingerprint_result(fingerprint: &Arc<Mutex<Fingerprint>>) -> (String, String, u64) {
    let mut guard = fingerprint.lock().unwrap_or_else(PoisonError::into_inner);
    let state = std::mem::replace(&mut *guard, Fingerprint::new());
    state.finish()
}

fn fingerprint_stream(body: BodyStream, fingerprint: Arc<Mutex<Fingerprint>>) -> BodyStream {
    let stream = async_stream::try_stream! {
        let mut body = body;
        while let Some(chunk) = body.next().await {
            let bytes = chunk?;
            {
                let mut guard = fingerprint
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                guard.update(&bytes);
            }
            yield bytes;
        }
    };
    Box::pin(stream)
}

fn put_response(metadata: &Metadata) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    set_metadata_headers(metadata, response.headers_mut());
    response
}

fn copy_response(metadata: &Metadata) -> Response {
    let last_modified = metadata.committed_at.map_or_default(unix_timestamp);
    xml_response(
        StatusCode::OK,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <ETag>\"{}\"</ETag><LastModified>{}</LastModified></CopyObjectResult>",
            xml_escape(&metadata.etag),
            xml_escape(&last_modified)
        ),
    )
}

fn metadata_response(metadata: &Metadata) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    set_metadata_headers(metadata, response.headers_mut());
    if let Ok(value) = HeaderValue::from_str(&metadata.size.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    response
}

fn stream_response(response: reqwest::Response, metadata: &Metadata) -> Response {
    let status = response.status();
    let headers = response.headers().clone();
    let body = Body::from_stream(
        response
            .bytes_stream()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
    );
    let mut output = Response::new(body);
    *output.status_mut() = status;
    copy_headers(&headers, output.headers_mut());
    set_metadata_headers(metadata, output.headers_mut());
    output
}

/// Headers that select server-side encryption for a request.
const ENCRYPTION_HEADERS: [&str; 7] = [
    "x-amz-server-side-encryption",
    "x-amz-server-side-encryption-aws-kms-key-id",
    "x-amz-server-side-encryption-context",
    "x-amz-server-side-encryption-bucket-key-enabled",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
];

/// Reject encryption headers that cluster mode cannot honor.
///
/// # Errors
///
/// Returns `NotImplemented` when the request carries an encryption header.
fn reject_encryption_headers(headers: &HeaderMap) -> Result<(), S3Error> {
    for name in ENCRYPTION_HEADERS {
        if headers.contains_key(name) {
            return Err(S3Error::not_implemented(
                "Cluster mode cannot honor server-side encryption headers. \
                 The backing bucket must use default encryption.",
            ));
        }
    }
    Ok(())
}

/// Object metadata values that a write can set.
struct ObjectMetadata {
    content_type: Option<String>,
    user_metadata: BTreeMap<String, String>,
    cache_control: Option<String>,
    content_encoding: Option<String>,
    content_disposition: Option<String>,
    content_language: Option<String>,
    expires: Option<String>,
    storage_class: Option<String>,
}

impl ObjectMetadata {
    fn from_headers(headers: &HeaderMap) -> Self {
        Self {
            content_type: header_string(headers, header::CONTENT_TYPE),
            user_metadata: user_metadata(headers),
            cache_control: header_string(headers, header::CACHE_CONTROL),
            content_encoding: header_string(headers, header::CONTENT_ENCODING),
            content_disposition: header_string(headers, header::CONTENT_DISPOSITION),
            content_language: header_string(headers, header::CONTENT_LANGUAGE),
            expires: header_string(headers, header::EXPIRES),
            storage_class: storage_class(headers),
        }
    }

    fn from_metadata(metadata: &Metadata) -> Self {
        Self {
            content_type: metadata.content_type.clone(),
            user_metadata: metadata.user_metadata.clone(),
            cache_control: metadata.cache_control.clone(),
            content_encoding: metadata.content_encoding.clone(),
            content_disposition: metadata.content_disposition.clone(),
            content_language: metadata.content_language.clone(),
            expires: metadata.expires.clone(),
            storage_class: metadata.storage_class.clone(),
        }
    }
}

fn set_metadata_headers(metadata: &Metadata, headers: &mut HeaderMap) {
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if !metadata.etag.is_empty()
        && let Ok(value) = HeaderValue::from_str(&format!("\"{}\"", metadata.etag))
    {
        headers.insert(header::ETAG, value);
    }
    for (name, value) in [
        (header::CONTENT_TYPE, metadata.content_type.as_ref()),
        (header::CONTENT_ENCODING, metadata.content_encoding.as_ref()),
        (header::CACHE_CONTROL, metadata.cache_control.as_ref()),
        (
            header::CONTENT_DISPOSITION,
            metadata.content_disposition.as_ref(),
        ),
        (header::CONTENT_LANGUAGE, metadata.content_language.as_ref()),
        (header::EXPIRES, metadata.expires.as_ref()),
    ] {
        if let Some(value) = value
            && let Ok(value) = HeaderValue::from_str(value)
        {
            headers.insert(name, value);
        }
    }
    if let Some(value) = &metadata.storage_class
        && !value.eq_ignore_ascii_case("STANDARD")
        && let Ok(value) = HeaderValue::from_str(value)
    {
        headers.insert(
            axum::http::HeaderName::from_static("x-amz-storage-class"),
            value,
        );
    }
    for (name, value) in &metadata.user_metadata {
        let name = format!("x-amz-meta-{name}");
        if let (Ok(name), Ok(value)) = (
            axum::http::HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn copy_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for (name, value) in source {
        if matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        ) {
            continue;
        }
        target.insert(name.clone(), value.clone());
    }
}

#[allow(clippy::too_many_arguments, clippy::format_push_string)]
fn list_response(
    bucket: &str,
    prefix: &str,
    max_keys: usize,
    truncated: bool,
    token: Option<&str>,
    contents: &[(String, Metadata)],
    prefixes: &[String],
) -> Response {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(prefix)));
    xml.push_str(&format!(
        "<KeyCount>{}</KeyCount>",
        contents.len() + prefixes.len()
    ));
    xml.push_str(&format!("<MaxKeys>{max_keys}</MaxKeys>"));
    xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if let Some(token) = token {
        xml.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(token)
        ));
    }
    for (key, metadata) in contents {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
        if let Some(committed) = metadata.committed_at {
            xml.push_str(&format!(
                "<LastModified>{}</LastModified>",
                unix_timestamp(committed)
            ));
        }
        xml.push_str(&format!("<ETag>\"{}\"</ETag>", xml_escape(&metadata.etag)));
        xml.push_str(&format!("<Size>{}</Size>", metadata.size));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }
    for prefix in prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml_response(StatusCode::OK, xml)
}

#[allow(clippy::too_many_arguments, clippy::format_push_string)]
fn list_response_v1(
    bucket: &str,
    prefix: &str,
    marker: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    truncated: bool,
    next_marker: Option<&str>,
    contents: &[(String, Metadata)],
    prefixes: &[String],
) -> Response {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    xml.push_str("<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
    xml.push_str(&format!("<Name>{}</Name>", xml_escape(bucket)));
    xml.push_str(&format!("<Prefix>{}</Prefix>", xml_escape(prefix)));
    xml.push_str(&format!("<Marker>{}</Marker>", xml_escape(marker)));
    xml.push_str(&format!("<MaxKeys>{max_keys}</MaxKeys>"));
    if let Some(delimiter) = delimiter {
        xml.push_str(&format!("<Delimiter>{}</Delimiter>", xml_escape(delimiter)));
    }
    xml.push_str(&format!("<IsTruncated>{truncated}</IsTruncated>"));
    if let Some(next_marker) = next_marker {
        xml.push_str(&format!(
            "<NextMarker>{}</NextMarker>",
            xml_escape(next_marker)
        ));
    }
    for (key, metadata) in contents {
        xml.push_str("<Contents>");
        xml.push_str(&format!("<Key>{}</Key>", xml_escape(key)));
        if let Some(committed) = metadata.committed_at {
            xml.push_str(&format!(
                "<LastModified>{}</LastModified>",
                unix_timestamp(committed)
            ));
        }
        xml.push_str(&format!("<ETag>\"{}\"</ETag>", xml_escape(&metadata.etag)));
        xml.push_str(&format!("<Size>{}</Size>", metadata.size));
        xml.push_str("<StorageClass>STANDARD</StorageClass>");
        xml.push_str("</Contents>");
    }
    for prefix in prefixes {
        xml.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(prefix)
        ));
    }
    xml.push_str("</ListBucketResult>");
    xml_response(StatusCode::OK, xml)
}

fn xml_response(status: StatusCode, xml: String) -> Response {
    let mut response = Response::new(Body::from(xml));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

fn unix_timestamp(seconds: i64) -> String {
    OffsetDateTime::from_unix_timestamp(seconds)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .unwrap_or_default()
}

fn xml_escape(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            other => output.push(other),
        }
    }
    output
}

fn header_string(headers: &HeaderMap, name: header::HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn storage_class(headers: &HeaderMap) -> Option<String> {
    header_text(headers, "x-amz-storage-class").filter(|value| !value.is_empty())
}

fn parse_copy_source(value: &str) -> Option<(String, String)> {
    let value = value.split('?').next().unwrap_or(value);
    let decoded = percent_decode_str(value).decode_utf8_lossy();
    let trimmed = decoded.trim_start_matches('/');
    let (bucket, key) = trimmed.split_once('/')?;
    if bucket.is_empty() || key.is_empty() {
        return None;
    }
    Some((bucket.to_owned(), key.to_owned()))
}

fn header_is(headers: &HeaderMap, name: header::HeaderName, expected: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim() == expected)
}

fn user_metadata(headers: &HeaderMap) -> BTreeMap<String, String> {
    let mut output = BTreeMap::new();
    for (name, value) in headers {
        let name = name.as_str();
        if let Some(suffix) = name.strip_prefix("x-amz-meta-")
            && let Ok(value) = value.to_str()
        {
            output.insert(suffix.to_owned(), value.to_owned());
        }
    }
    output
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
        if expected.trim() != "*" && metadata.etag != expected.trim().trim_matches('"') {
            return true;
        }
    }
    false
}

fn precondition_failed() -> S3Error {
    S3Error::new(
        StatusCode::PRECONDITION_FAILED,
        "PreconditionFailed",
        "A condition in the request failed.",
    )
}

fn no_such_key() -> S3Error {
    S3Error::new(
        StatusCode::NOT_FOUND,
        "NoSuchKey",
        "The key does not exist.",
    )
}

fn no_such_upload() -> S3Error {
    S3Error::new(
        StatusCode::NOT_FOUND,
        "NoSuchUpload",
        "The upload id does not exist.",
    )
}

fn wrong_node(owner: &str) -> S3Error {
    S3Error::new(
        StatusCode::BAD_REQUEST,
        "InvalidRequest",
        &format!("The upload id belongs to node \"{owner}\". Send the request to that node."),
    )
}

fn map_coord_error(error: CoordError) -> S3Error {
    match error {
        CoordError::Rejected(RejectReason::PreconditionFailed) => precondition_failed(),
        CoordError::Rejected(RejectReason::Conflict) => S3Error::new(
            StatusCode::CONFLICT,
            "Conflict",
            "The object changed during the write.",
        ),
        CoordError::StaleTerm { .. } | CoordError::Unavailable | CoordError::Indeterminate(_) => {
            S3Error::no_leader()
        }
        CoordError::Storage(message) => {
            warn!(error = %message, "cluster metadata operation failed");
            S3Error::internal()
        }
        CoordError::NotImplemented(message) => S3Error::not_implemented(message),
    }
}

fn storage_error(
    error: StorageError,
    failure_flag: Option<&Arc<std::sync::atomic::AtomicU8>>,
) -> S3Error {
    match error {
        StorageError::Sign(error) => error,
        StorageError::Status(status) => {
            body_failure_error(failure_flag).unwrap_or_else(|| S3Error::from_status(status))
        }
        StorageError::Body => body_failure_error(failure_flag)
            .unwrap_or_else(|| S3Error::invalid_request("The request body is not valid.")),
        StorageError::TooLarge => S3Error::entity_too_large(),
        StorageError::Timeout => {
            body_failure_error(failure_flag).unwrap_or_else(S3Error::request_timeout)
        }
        StorageError::Transport(error) if error.is_timeout() => {
            body_failure_error(failure_flag).unwrap_or_else(S3Error::request_timeout)
        }
        StorageError::Transport(_) => {
            body_failure_error(failure_flag).unwrap_or_else(S3Error::service_unavailable)
        }
    }
}

fn body_failure_error(failure_flag: Option<&Arc<std::sync::atomic::AtomicU8>>) -> Option<S3Error> {
    match failure_flag.map(|flag| failure(flag)) {
        Some(BodyFailure::PayloadHash) => Some(S3Error::payload_mismatch()),
        Some(BodyFailure::ChunkSignature) => Some(S3Error::signature_mismatch()),
        Some(BodyFailure::ChunkFormat | BodyFailure::DecodedLength) => {
            Some(S3Error::invalid_request("The request body is not valid."))
        }
        Some(BodyFailure::TooLarge) => Some(S3Error::entity_too_large()),
        _ => None,
    }
}

async fn delete_backing_blob(
    client: S3Client,
    credential: Arc<ClientCredential>,
    bucket: &str,
    blob: String,
) {
    let request = ForwardRequest {
        method: Method::DELETE,
        path_and_query: format!("/{bucket}/objects/{blob}"),
        headers: HeaderMap::new(),
        body: None,
        content_length: None,
    };
    if let Err(error) = client.execute(&credential, request).await {
        warn!(%error, "cluster blob delete failed");
    }
}

fn parse_listing(xml: &str) -> Vec<ListedObject> {
    let mut output = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find("<Contents>") {
        let after = &rest[start + "<Contents>".len()..];
        let Some(end) = after.find("</Contents>") else {
            break;
        };
        let block = &after[..end];
        if let Some(key) = extract_tag(block, "Key") {
            let modified = extract_tag(block, "LastModified")
                .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
                .map(OffsetDateTime::unix_timestamp);
            output.push(ListedObject {
                key: key.to_owned(),
                modified,
            });
        }
        rest = &after[end + "</Contents>".len()..];
    }
    output
}

fn extract_tag<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let start = xml.find(&open)?;
    let after = &xml[start + open.len()..];
    let end = after.find(&close)?;
    Some(&after[..end])
}

fn query_pairs(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            (decode_query(key), decode_query(value))
        })
        .collect()
}

fn query_value<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn decode_query(value: &str) -> String {
    percent_decode_str(&value.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

fn encode_query(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn encode_token(value: &str) -> String {
    hex::encode(value.as_bytes())
}

fn decode_token(value: &str) -> Result<String, S3Error> {
    let bytes = hex::decode(value).map_err(|_| {
        S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "The continuation token is not valid.",
        )
    })?;
    String::from_utf8(bytes).map_err(|_| {
        S3Error::new(
            StatusCode::BAD_REQUEST,
            "InvalidArgument",
            "The continuation token is not valid.",
        )
    })
}

fn upload_owner(upload_id: &str) -> Option<String> {
    let (node, _) = upload_id.split_once('-')?;
    let bytes = hex::decode(node).ok()?;
    String::from_utf8(bytes).ok()
}

fn duration_seconds(duration: std::time::Duration) -> i64 {
    i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn next_id() -> String {
    use std::sync::atomic::AtomicU64;
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{nanos:032x}{counter:016x}")
}

/// Send arbitrary text to the cluster XML parsers.
///
/// This function is an entry point for the fuzz target. It returns a small
/// count that the target can discard. It does not panic.
#[doc(hidden)]
#[must_use]
pub fn fuzz_s3_xml(input: &str) -> usize {
    let objects = parse_listing(input);
    let token = usize::from(extract_tag(input, "NextContinuationToken").is_some());
    let upload = usize::from(extract_tag(input, "UploadId").is_some());
    let etag = usize::from(extract_tag(input, "ETag").is_some());
    objects
        .len()
        .saturating_add(token)
        .saturating_add(upload)
        .saturating_add(etag)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::cluster::state::ObjectState;

    #[test]
    fn tokens_round_trip() {
        let token = encode_token("a/b c");
        assert_eq!(decode_token(&token).unwrap(), "a/b c");
        assert!(decode_token("zz").is_err());
    }

    #[test]
    fn listing_parses_contents_and_token() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated>\
                   <NextContinuationToken>next</NextContinuationToken>\
                   <Contents><Key>a</Key>\
                   <LastModified>2000-01-01T00:00:00Z</LastModified>\
                   </Contents></ListBucketResult>";
        let objects = parse_listing(xml);
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key, "a");
        assert!(objects[0].modified.is_some());
        assert_eq!(extract_tag(xml, "NextContinuationToken"), Some("next"));
    }

    #[test]
    fn upload_owner_reads_the_node_hex() {
        let id = format!("{}-{}", hex::encode("node-a").as_str(), "123");
        assert_eq!(upload_owner(&id).as_deref(), Some("node-a"));
    }

    #[test]
    fn a_missing_leader_returns_503_with_retry_after() {
        let response = map_coord_error(CoordError::Unavailable).into_response("request-id");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get("Retry-After").unwrap(), "1");
    }

    #[test]
    fn conditions_compare_etags() {
        let metadata = Metadata {
            state: ObjectState::Committed,
            version: 1,
            blob: "blob".to_owned(),
            size: 1,
            checksum: String::new(),
            content_type: None,
            etag: "abc".to_owned(),
            user_metadata: BTreeMap::new(),
            owner: "a".to_owned(),
            replicas: Vec::new(),
            created_at: 0,
            committed_at: Some(0),
            deleted_at: None,
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
            content_language: None,
            expires: None,
            storage_class: None,
        };
        assert!(condition_failed(Some(&metadata), true, None));
        assert!(!condition_failed(Some(&metadata), false, Some("\"abc\"")));
        assert!(condition_failed(Some(&metadata), false, Some("\"other\"")));
    }
}
