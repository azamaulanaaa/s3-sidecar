//! Read-through cache, write-back journal, and sync worker.

pub mod index;
pub mod keys;
pub mod meta;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use md5::Md5;
use sha2::{Digest, Sha256};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tracing::{Instrument, info, warn};

use self::index::{CacheIndex, IndexEntry};
use self::keys::{
    blob_key, journal_key, journal_prefix, meta_key, next_id, node_prefix, object_path,
};
use self::meta::{
    CacheMetadata, EntryState, JournalEntry, JournalKind, collect_headers, is_persisted_header,
    unix_now,
};
use crate::auth::body::BodyStream;
use crate::auth::{ClientCredential, now};
use crate::config::{CacheMode, Config, EndpointConfig, OnCacheError, WritePolicy};
use crate::errors::S3Error;
use crate::metrics::Metrics;
use crate::storage::{ForwardRequest, S3Client, StorageError};

pub use self::meta::CacheMetadata as Metadata;

/// Return the object key for a log field.
///
/// The key becomes `[redacted]` when `otel.record_object_keys` is false.
fn log_key<'a>(config: &Config, key: &'a str) -> &'a str {
    if config.otel.record_object_keys {
        key
    } else {
        "[redacted]"
    }
}

/// A problem in the cache data path.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// The cache endpoint could not be reached.
    #[error("the cache endpoint is not available")]
    Unavailable,
    /// The cache endpoint returned an unexpected status.
    #[error("the cache returned status {0}")]
    Status(u16),
    /// Object metadata is not valid.
    #[error("cache metadata is not valid")]
    Metadata,
    /// The LRU index failed.
    #[error("cache index failed: {0}")]
    Index(String),
    /// The body is larger than `max_object_bytes`.
    #[error("the object is too large for the cache")]
    TooLarge,
    /// The request body could not be read.
    #[error("the request body could not be read")]
    Body,
}

impl CacheError {
    /// Return true when a retry can succeed.
    #[must_use]
    pub fn is_unavailable(&self) -> bool {
        match self {
            Self::Unavailable | Self::Body => true,
            Self::Status(status) => *status >= 500 || *status == 408 || *status == 429,
            Self::Metadata | Self::Index(_) | Self::TooLarge => false,
        }
    }
}

/// One key in the origin namespace.
pub type CacheKey = (String, String);

/// A pending write in memory.
#[derive(Debug, Clone)]
struct PendingEntry {
    meta: CacheMetadata,
    journal_key: String,
}

/// State for one client-driven multipart upload that completes in the cache.
#[derive(Debug, Clone)]
struct ClientUpload {
    /// The multipart upload id at the cache endpoint.
    upload_id: String,
    /// The cache blob key for the completed object.
    cache_key: String,
    /// Origin bucket.
    bucket: String,
    /// Origin key.
    key: String,
    /// Content type from the client create request.
    content_type: Option<String>,
    /// Content encoding from the client create request.
    content_encoding: Option<String>,
    /// Object headers that the cache may store and replay.
    headers: BTreeMap<String, String>,
}

struct SyncJobs {
    latest: HashMap<CacheKey, JournalEntry>,
    running: HashSet<CacheKey>,
}

/// A cache lookup result.
pub enum Lookup {
    /// The cache has a usable entry.
    Hit(Box<CacheMetadata>),
    /// The cache does not have the key.
    Miss,
    /// A tombstone hides the key.
    Tombstone,
    /// The entry is quarantined.
    Failed,
}

/// The read-through cache and write-back journal.
pub struct Cache {
    config: Arc<Config>,
    client: S3Client,
    credential: Arc<ClientCredential>,
    origin: S3Client,
    origin_credential: Arc<ClientCredential>,
    index: Option<Mutex<CacheIndex>>,
    pending: Mutex<HashMap<CacheKey, PendingEntry>>,
    client_uploads: Mutex<HashMap<String, ClientUpload>>,
    tombstones: Mutex<HashMap<CacheKey, i64>>,
    jobs: Mutex<SyncJobs>,
    sync_slots: Arc<Semaphore>,
    metrics: Arc<Metrics>,
    started: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Cache {
    /// Build the cache when the mode is not `off`.
    ///
    /// # Errors
    ///
    /// Returns an error when an endpoint URL or the index file is not valid.
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>) -> Result<Option<Arc<Self>>, S3Error> {
        if config.cache.mode == CacheMode::Off {
            return Ok(None);
        }
        let cache_endpoint = EndpointConfig {
            endpoint: config.cache.endpoint.clone(),
            bucket: String::new(),
            region: config.cache.region.clone(),
            access_key_id: config.cache.access_key_id.clone(),
            secret_access_key: config.cache.secret_access_key.clone(),
            session_token: config.cache.session_token.clone(),
            force_path_style: config.cache.force_path_style,
        };
        let client = S3Client::new_with_upstream(&cache_endpoint, &config.upstream)?
            .with_record_object_keys(config.otel.record_object_keys);
        let credential = Arc::new(ClientCredential::new(
            config.cache.access_key_id.clone(),
            config.cache.secret_access_key.clone(),
            config.cache.session_token.clone(),
            Vec::new(),
        ));
        let origin = S3Client::new_with_upstream(&config.origin, &config.upstream)?
            .with_record_object_keys(config.otel.record_object_keys);
        let origin_credential = Arc::new(ClientCredential::new(
            config.origin.access_key_id.clone(),
            config.origin.secret_access_key.clone(),
            config.origin.session_token.clone(),
            Vec::new(),
        ));
        let index = match config.cache.max_bytes {
            Some(0) => {
                warn!("cache max_bytes is 0; the cache is unlimited and has no eviction loop");
                None
            }
            Some(_) => Some(Mutex::new(
                CacheIndex::open(Path::new(config.cache.index_path.as_str())).map_err(|error| {
                    S3Error::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "InternalError",
                        &format!("the cache index could not be opened: {error}"),
                    )
                })?,
            )),
            None => None,
        };

        let max_bytes = config.cache.max_bytes.unwrap_or(0);
        metrics.set_cache_limit(max_bytes, config.cache.max_bytes == Some(0));

        Ok(Some(Arc::new(Self {
            sync_slots: Arc::new(Semaphore::new(config.cache.sync_workers)),
            config,
            client,
            credential,
            origin,
            origin_credential,
            index,
            pending: Mutex::new(HashMap::new()),
            client_uploads: Mutex::new(HashMap::new()),
            tombstones: Mutex::new(HashMap::new()),
            jobs: Mutex::new(SyncJobs {
                latest: HashMap::new(),
                running: HashSet::new(),
            }),
            metrics,
            started: AtomicBool::new(false),
            tasks: Mutex::new(Vec::new()),
        })))
    }

    /// Return the cache bucket name.
    #[must_use]
    pub fn cache_bucket(&self) -> &str {
        &self.config.cache.bucket
    }

    /// Return the cache blob key for one origin object.
    fn cache_key(&self, bucket: &str, key: &str) -> String {
        blob_key(
            &self.config.cache.prefix,
            &self.config.cache.node_id,
            bucket,
            key,
        )
    }

    /// Return the configured cache mode.
    #[must_use]
    pub fn mode(&self) -> CacheMode {
        self.config.cache.mode
    }

    /// Return true when the mode has a read cache.
    #[must_use]
    pub fn reads_cache(&self) -> bool {
        matches!(self.mode(), CacheMode::Read | CacheMode::ReadWrite)
    }

    /// Return true when the mode accepts writes.
    #[must_use]
    pub fn writes_cache(&self) -> bool {
        matches!(self.mode(), CacheMode::Write | CacheMode::ReadWrite)
    }

    /// Return the maximum object size that may enter the cache.
    #[must_use]
    pub fn max_object_bytes(&self) -> u64 {
        self.config.cache.max_object_bytes
    }

    /// Return the fallback action for a cache failure.
    #[must_use]
    pub fn on_cache_error(&self) -> OnCacheError {
        self.config.cache.on_cache_error
    }

    /// Return true when a quarantined entry is served.
    #[must_use]
    pub fn serve_quarantined(&self) -> bool {
        self.config.cache.serve_quarantined
    }

    /// Return the TTL for one bucket.
    #[must_use]
    pub fn ttl_for(&self, bucket: &str) -> u64 {
        self.config
            .buckets
            .iter()
            .find(|entry| entry.name == bucket)
            .and_then(|entry| entry.ttl)
            .map_or(3_600, |ttl| ttl.as_duration().as_secs())
    }

    /// Return the write policy for one bucket.
    #[must_use]
    pub fn write_policy_for(&self, bucket: &str) -> WritePolicy {
        self.config
            .buckets
            .iter()
            .find(|entry| entry.name == bucket)
            .and_then(|entry| entry.cache_write_policy)
            .unwrap_or(self.config.cache.write_policy)
    }

    /// Return the tracked index size.
    #[must_use]
    pub fn tracked_bytes(&self) -> u64 {
        self.index
            .as_ref()
            .map_or(0, |index| lock(index).tracked_bytes())
    }

    /// Return the pending write count and the age of the oldest entry.
    #[must_use]
    pub fn pending_stats(&self) -> (u64, u64) {
        let now = unix_now();
        let pending = lock(&self.pending);
        let tombstones = lock(&self.tombstones);
        let count =
            u64::try_from(pending.len().saturating_add(tombstones.len())).unwrap_or(u64::MAX);
        let oldest = pending
            .values()
            .map(|entry| entry.meta.created_at)
            .chain(tombstones.values().copied())
            .min();
        let lag = oldest.map_or(0, |created| {
            u64::try_from(now.saturating_sub(created)).unwrap_or(u64::MAX)
        });
        (count, lag)
    }

    /// Refresh the cache size and journal gauges.
    pub fn refresh_metrics(&self) {
        self.metrics.set_tracked(self.tracked_bytes());
        let (pending, lag) = self.pending_stats();
        self.metrics.set_pending_stats(pending, lag);
    }

    /// Start recovery, the sync worker, and the garbage collector.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be read.
    pub async fn start(self: &Arc<Self>) -> Result<(), CacheError> {
        if self.started.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.ensure_index_loaded().await;
        self.recover().await?;
        self.spawn_gc();
        self.spawn_metrics();
        Ok(())
    }

    /// Stop the garbage collector and sync workers, then flush the index.
    pub fn shutdown(&self) {
        for task in lock(&self.tasks).drain(..) {
            task.abort();
        }
        if let Some(index) = &self.index
            && let Err(error) = lock(index).flush()
        {
            warn!(%error, "cache index flush failed");
        }
    }

    fn track(&self, task: JoinHandle<()>) {
        let mut tasks = lock(&self.tasks);
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    /// Return the metadata for a pending entry.
    #[must_use]
    pub fn pending_entry(&self, bucket: &str, key: &str) -> Option<CacheMetadata> {
        lock(&self.pending)
            .get(&(bucket.to_owned(), key.to_owned()))
            .map(|entry| entry.meta.clone())
    }

    /// Return true when a tombstone hides the key.
    #[must_use]
    pub fn is_tombstoned(&self, bucket: &str, key: &str) -> bool {
        lock(&self.tombstones).contains_key(&(bucket.to_owned(), key.to_owned()))
    }

    /// Read one cache entry from the metadata object.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache is not available or the metadata is not
    /// valid.
    pub async fn read_meta(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<CacheMetadata>, CacheError> {
        let cache_key = self.cache_key(bucket, key);
        let Some(response) = self
            .get_object(self.cache_bucket(), &meta_key(&cache_key), None)
            .await?
        else {
            return Ok(None);
        };
        let body = response.bytes().await.map_err(|_| CacheError::Body)?;
        let meta = serde_json::from_slice(&body).map_err(|_| CacheError::Metadata)?;
        Ok(Some(meta))
    }

    /// Read one blob from the cache. A range header is passed through.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache is not available.
    pub async fn get_blob(
        &self,
        bucket: &str,
        key: &str,
        range: Option<&str>,
    ) -> Result<Option<reqwest::Response>, CacheError> {
        let cache_key = self.cache_key(bucket, key);
        self.get_object(self.cache_bucket(), &cache_key, range)
            .await
    }

    /// Store updated metadata for an entry.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache is not available.
    pub async fn store_meta(
        &self,
        bucket: &str,
        key: &str,
        meta: &CacheMetadata,
    ) -> Result<(), CacheError> {
        let cache_key = self.cache_key(bucket, key);
        self.put_meta(self.cache_bucket(), &cache_key, meta).await
    }

    /// Update the LRU access time for an entry.
    pub fn touch(&self, bucket: &str, key: &str) {
        if let Some(index) = &self.index {
            let cache_key = self.cache_key(bucket, key);
            let index = lock(index);
            if let Err(error) = index.touch(&cache_key, unix_now()) {
                warn!(%error, "cache index touch failed");
            }
        }
    }

    /// Check that the cache endpoint accepts requests for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache endpoint is not available.
    pub async fn probe(&self) -> Result<(), CacheError> {
        let request = ForwardRequest {
            method: Method::HEAD,
            path_and_query: object_path(self.cache_bucket(), ""),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status().is_success() || response.status() == StatusCode::FORBIDDEN {
            return Ok(());
        }
        Err(CacheError::Status(response.status().as_u16()))
    }

    /// Build metadata from an origin response.
    #[must_use]
    pub fn metadata_from_response(
        bucket: &str,
        key: &str,
        response: &reqwest::Response,
    ) -> CacheMetadata {
        metadata_from_response(bucket, key, response)
    }

    /// Invalidate one cache entry. Errors are logged and ignored.
    pub async fn invalidate(&self, bucket: &str, key: &str) {
        let cache_key = self.cache_key(bucket, key);
        if let Err(error) = self
            .delete_object(self.cache_bucket(), &meta_key(&cache_key))
            .await
        {
            warn!(%error, "cache metadata delete failed");
        }
        if let Err(error) = self.delete_object(self.cache_bucket(), &cache_key).await {
            warn!(%error, "cache blob delete failed");
        }
        self.index_remove(&cache_key);
    }

    /// Write a blob and its journal record, then queue the sync.
    ///
    /// The caller must return success to the client only after this call
    /// returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache cannot store the blob, journal, or
    /// metadata.
    // The write path needs the body, its headers, and a fallback signal.
    #[allow(clippy::too_many_arguments)]
    pub async fn stage_put(
        self: &Arc<Self>,
        bucket: &str,
        key: &str,
        body: BodyStream,
        length: u64,
        content_type: Option<String>,
        content_encoding: Option<String>,
        headers: BTreeMap<String, String>,
        body_started: Arc<AtomicBool>,
    ) -> Result<CacheMetadata, CacheError> {
        let cache_key = self.cache_key(bucket, key);
        let mut meta = CacheMetadata::synced(
            bucket,
            key,
            String::new(),
            None,
            length,
            content_type,
            content_encoding,
            unix_now(),
        );
        meta.headers = headers;
        meta.state = EntryState::Pending;
        let hashes = self
            .put_blob_stream(
                self.cache_bucket(),
                &cache_key,
                body,
                Some(length),
                Some(Arc::clone(&body_started)),
            )
            .await?;
        meta.size = hashes.size;
        meta.checksum = hashes.sha256;
        meta.etag = format!("\"{}\"", hashes.md5);
        if let Err(error) = self
            .record_pending_put(bucket, key, &cache_key, &meta)
            .await
        {
            let _ = self.delete_object(self.cache_bucket(), &cache_key).await;
            return Err(error);
        }
        Ok(meta)
    }

    /// Remove a pending write and hide the key without an origin call.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be removed.
    pub async fn delete_pending(&self, bucket: &str, key: &str) -> Result<(), CacheError> {
        let cache_key = self.cache_key(bucket, key);
        let pending = lock(&self.pending).remove(&(bucket.to_owned(), key.to_owned()));
        if let Some(entry) = pending {
            self.delete_object(self.cache_bucket(), &entry.journal_key)
                .await?;
        }
        let _ = self
            .delete_object(self.cache_bucket(), &meta_key(&cache_key))
            .await;
        let _ = self.delete_object(self.cache_bucket(), &cache_key).await;
        self.index_remove(&cache_key);
        lock(&self.tombstones).insert((bucket.to_owned(), key.to_owned()), unix_now());
        Ok(())
    }

    /// Remove the cache entry, write a tombstone, and queue the origin delete.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be written.
    pub async fn stage_delete(self: &Arc<Self>, bucket: &str, key: &str) -> Result<(), CacheError> {
        let id = next_id();
        let cache_key = self.cache_key(bucket, key);
        let _ = self
            .delete_object(self.cache_bucket(), &meta_key(&cache_key))
            .await;
        let _ = self.delete_object(self.cache_bucket(), &cache_key).await;
        self.index_remove(&cache_key);
        lock(&self.pending).remove(&(bucket.to_owned(), key.to_owned()));
        let journal = JournalEntry::delete(id, bucket, key, cache_key, unix_now());
        let journal_key = journal_key(
            &self.config.cache.prefix,
            &self.config.cache.node_id,
            &journal.id,
        );
        self.put_journal(self.cache_bucket(), &journal_key, &journal)
            .await?;
        lock(&self.tombstones).insert((bucket.to_owned(), key.to_owned()), unix_now());
        self.enqueue((bucket.to_owned(), key.to_owned()), journal);
        Ok(())
    }

    /// Store an origin response while a client reads it.
    #[allow(clippy::needless_pass_by_value)]
    pub fn tee_response(
        self: &Arc<Self>,
        response: reqwest::Response,
        meta: CacheMetadata,
    ) -> BodyStream {
        let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(2);
        let cache = Arc::clone(self);
        let completed = Arc::new(AtomicBool::new(false));
        let completed_writer = Arc::clone(&completed);
        let max_object_bytes = self.max_object_bytes();
        let expected = meta.size;
        let meta_writer = meta.clone();
        let origin_bucket = meta.origin_bucket.clone();
        let origin_key = meta.origin_key.clone();
        let cache_key_value = self.cache_key(&origin_bucket, &origin_key);
        let tee_span = tracing::info_span!(
            "cache.tee",
            "s3.bucket.name" = meta.origin_bucket.as_str(),
            "cache.hit" = false,
            "cache.entry.state" = meta.state.as_str(),
        );
        tokio::spawn(
            async move {
                let body = receiver_stream(receiver);
                match cache
                    .put_blob_stream(
                        cache.cache_bucket(),
                        &cache_key_value,
                        body,
                        Some(meta_writer.size),
                        None,
                    )
                    .await
                {
                    Ok(hashes) => {
                        if !completed_writer.load(Ordering::Acquire) {
                            let _ = cache
                                .delete_object(cache.cache_bucket(), &cache_key_value)
                                .await;
                            return;
                        }
                        let mut stored = meta_writer;
                        stored.size = hashes.size;
                        stored.checksum = hashes.sha256;
                        stored.etag = format!("\"{}\"", hashes.md5);
                        if let Err(error) = cache
                            .put_meta(cache.cache_bucket(), &cache_key_value, &stored)
                            .await
                        {
                            warn!(%error, "cache metadata write failed");
                        }
                        if let Err(error) = cache.index_upsert(&cache_key_value, &stored).await {
                            warn!(%error, "cache index update failed");
                        }
                    }
                    Err(error) => {
                        warn!(%error, "cache tee write failed");
                    }
                }
            }
            .instrument(tee_span),
        );

        let stream = async_stream::try_stream! {
            let mut body = response.bytes_stream();
            let mut sender = Some(sender);
            let mut count = 0_u64;
            let mut complete = true;
            while let Some(chunk) = body.next().await {
                let bytes = chunk
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
                count = count.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
                if count <= max_object_bytes {
                    let keep = if let Some(sender) = sender.as_ref() {
                        sender.send(Ok(bytes.clone())).await.is_ok()
                    } else {
                        false
                    };
                    if !keep {
                        sender = None;
                        complete = false;
                    }
                } else {
                    sender = None;
                    complete = false;
                }
                if complete && count >= expected {
                    // The client can stop reading at Content-Length without
                    // polling the stream again. Mark the tee complete before
                    // the last chunk is yielded.
                    completed.store(true, Ordering::Release);
                }
                yield bytes;
            }
            if count < expected {
                completed.store(false, Ordering::Release);
            }
        };
        Box::pin(stream)
    }

    /// Run one garbage collection pass.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache listing fails.
    #[tracing::instrument(name = "gc.cycle", skip_all)]
    pub async fn gc_once(&self) -> Result<(), CacheError> {
        let bucket = self.cache_bucket().to_owned();
        self.gc_bucket(&bucket).await
    }

    async fn gc_bucket(&self, bucket: &str) -> Result<(), CacheError> {
        let prefix = node_prefix(&self.config.cache.prefix, &self.config.cache.node_id);
        let listing = self.list_keys(bucket, &prefix).await?;
        let journals = self.journal_entries(bucket).await?;
        let referenced: HashSet<String> = journals
            .values()
            .map(|entry| entry.cache_key.clone())
            .collect();
        let now = now();
        let staging = self.config.cache.staging_ttl.as_duration();
        let orphan = self.config.cache.orphan_ttl.as_duration();
        for key in listing {
            if is_meta_key(&key) {
                let blob = key.trim_end_matches(".meta").to_owned();
                if !self.head_object(bucket, &blob).await? {
                    let _ = self.delete_object(bucket, &key).await;
                }
                continue;
            }
            if referenced.contains(&key) {
                continue;
            }
            if self.head_object(bucket, &meta_key(&key)).await? {
                continue;
            }
            let Some(modified) = self.last_modified(bucket, &key).await? else {
                continue;
            };
            let age = now - modified;
            let age = age.whole_seconds();
            if age > i64::try_from(staging.as_secs()).unwrap_or(i64::MAX) {
                info!(
                    bucket,
                    key = log_key(&self.config, &key),
                    "removing orphan cache blob"
                );
                let _ = self.delete_object(bucket, &key).await;
            }
        }
        for entry in journals.values() {
            if entry.state != EntryState::Failed {
                continue;
            }
            let age = unix_now().saturating_sub(entry.created_at);
            if u64::try_from(age).unwrap_or(u64::MAX) > orphan.as_secs() {
                let journal = journal_key(
                    &self.config.cache.prefix,
                    &self.config.cache.node_id,
                    &entry.id,
                );
                let _ = self.delete_object(bucket, &journal).await;
            }
        }
        Ok(())
    }

    async fn journal_entries(
        &self,
        bucket: &str,
    ) -> Result<HashMap<CacheKey, JournalEntry>, CacheError> {
        let prefix = journal_prefix(&self.config.cache.prefix, &self.config.cache.node_id);
        let keys = self.list_keys(bucket, &prefix).await?;
        let mut entries = HashMap::new();
        for key in keys {
            let Some(response) = self.get_object(bucket, &key, None).await? else {
                continue;
            };
            let body = response.bytes().await.map_err(|_| CacheError::Body)?;
            let entry: JournalEntry =
                serde_json::from_slice(&body).map_err(|_| CacheError::Metadata)?;
            let cache_key = (entry.bucket.clone(), entry.key.clone());
            let replace = entries
                .get(&cache_key)
                .is_none_or(|current: &JournalEntry| {
                    (entry.created_at, &entry.id) > (current.created_at, &current.id)
                });
            if replace {
                entries.insert(cache_key, entry);
            }
        }
        Ok(entries)
    }

    async fn recover(self: &Arc<Self>) -> Result<(), CacheError> {
        let bucket = self.cache_bucket().to_owned();
        let prefix = journal_prefix(&self.config.cache.prefix, &self.config.cache.node_id);
        let keys = match self.list_keys(&bucket, &prefix).await {
            Ok(keys) => keys,
            Err(error) => {
                warn!(%error, "cannot scan the cache journal");
                return Ok(());
            }
        };
        let mut by_key: HashMap<CacheKey, JournalEntry> = HashMap::new();
        for object in &keys {
            let Some(response) = self.get_object(&bucket, object, None).await? else {
                continue;
            };
            let body = response.bytes().await.map_err(|_| CacheError::Body)?;
            let Ok(entry) = serde_json::from_slice::<JournalEntry>(&body) else {
                warn!(
                    key = %log_key(&self.config, object),
                    "skipping invalid journal record"
                );
                continue;
            };
            let cache_key = (entry.bucket.clone(), entry.key.clone());
            let replace = by_key.get(&cache_key).is_none_or(|current| {
                (entry.created_at, &entry.id) > (current.created_at, &current.id)
            });
            if replace {
                if let Some(previous) = by_key.insert(cache_key, entry) {
                    let stale = journal_key(
                        &self.config.cache.prefix,
                        &self.config.cache.node_id,
                        &previous.id,
                    );
                    let _ = self.delete_object(&bucket, &stale).await;
                }
            } else {
                let stale = journal_key(
                    &self.config.cache.prefix,
                    &self.config.cache.node_id,
                    &entry.id,
                );
                let _ = self.delete_object(&bucket, &stale).await;
            }
        }
        for (cache_key, entry) in by_key {
            self.restore_entry(&bucket, cache_key, entry).await;
        }
        Ok(())
    }

    async fn restore_entry(
        self: &Arc<Self>,
        bucket: &str,
        cache_key: CacheKey,
        entry: JournalEntry,
    ) {
        match entry.kind {
            JournalKind::Put => {
                if !self
                    .head_object(bucket, &entry.cache_key)
                    .await
                    .unwrap_or(false)
                {
                    warn!(
                        bucket = %entry.bucket,
                        key = %log_key(&self.config, &entry.key),
                        "journal blob is missing; the entry is quarantined"
                    );
                    let mut failed = entry.clone();
                    failed.state = EntryState::Failed;
                    let path = journal_key(
                        &self.config.cache.prefix,
                        &self.config.cache.node_id,
                        &entry.id,
                    );
                    let _ = self.put_journal(bucket, &path, &failed).await;
                    self.metrics.quarantined();
                    return;
                }
                let meta = metadata_from_journal(&entry);
                if entry.state == EntryState::Failed {
                    self.metrics.quarantined();
                } else {
                    lock(&self.pending).insert(
                        cache_key.clone(),
                        PendingEntry {
                            meta: meta.clone(),
                            journal_key: journal_key(
                                &self.config.cache.prefix,
                                &self.config.cache.node_id,
                                &entry.id,
                            ),
                        },
                    );
                    if self
                        .read_meta(&cache_key.0, &cache_key.1)
                        .await
                        .ok()
                        .flatten()
                        .is_none()
                    {
                        let path = self.cache_key(&cache_key.0, &cache_key.1);
                        let _ = self.put_meta_from_key(bucket, &path, &meta).await;
                    }
                }
            }
            JournalKind::Delete => {
                lock(&self.tombstones).insert(cache_key.clone(), entry.created_at);
            }
        }
        self.enqueue(cache_key, entry);
    }

    fn enqueue(self: &Arc<Self>, key: CacheKey, entry: JournalEntry) {
        let mut jobs = lock(&self.jobs);
        jobs.latest.insert(key.clone(), entry);
        if !jobs.running.insert(key.clone()) {
            return;
        }
        drop(jobs);
        let cache = Arc::clone(self);
        let task = tokio::spawn(async move {
            cache.sync_key_loop(key).await;
        });
        self.track(task);
    }

    async fn sync_key_loop(self: Arc<Self>, key: CacheKey) {
        loop {
            let entry = {
                let mut jobs = lock(&self.jobs);
                jobs.latest.remove(&key)
            };
            let Some(entry) = entry else {
                lock(&self.jobs).running.remove(&key);
                return;
            };
            let mut entry = entry;
            if self.config.cache.sync_debounce_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.config.cache.sync_debounce_ms)).await;
            }
            if let Some(newer) = lock(&self.jobs).latest.remove(&key) {
                entry = newer;
            }
            let Ok(_permit) = Arc::clone(&self.sync_slots).acquire_owned().await else {
                lock(&self.jobs).running.remove(&key);
                return;
            };
            self.sync_entry(&entry).await;
        }
    }

    async fn sync_entry(self: &Arc<Self>, entry: &JournalEntry) {
        let span = tracing::info_span!(
            "cache.sync",
            "s3.bucket.name" = entry.bucket.as_str(),
            "cache.entry.state" = entry.state.as_str(),
        );
        self.sync_entry_inner(entry).instrument(span).await;
    }

    async fn sync_entry_inner(self: &Arc<Self>, entry: &JournalEntry) {
        if entry.kind == JournalKind::Put && self.is_tombstoned(&entry.bucket, &entry.key) {
            let path = journal_key(
                &self.config.cache.prefix,
                &self.config.cache.node_id,
                &entry.id,
            );
            let _ = self.delete_object(self.cache_bucket(), &path).await;
            return;
        }
        let max_attempts = self.config.cache.sync_max_attempts;
        let mut current = entry.clone();
        loop {
            let result = match current.kind {
                JournalKind::Put => self.try_put_to_origin(&current).await,
                JournalKind::Delete => self.try_delete_at_origin(&current).await,
            };
            match result {
                Ok(etag) => {
                    self.finish_sync(&current, etag).await;
                    return;
                }
                Err(failure) => {
                    current.attempts = current.attempts.saturating_add(1);
                    self.metrics.sync_failed();
                    if failure.retryable && current.attempts < max_attempts {
                        let path = journal_key(
                            &self.config.cache.prefix,
                            &self.config.cache.node_id,
                            &current.id,
                        );
                        let _ = self.put_journal(self.cache_bucket(), &path, &current).await;
                        self.sleep_backoff(current.attempts).await;
                        continue;
                    }
                    warn!(
                        bucket = %current.bucket,
                        key = %log_key(&self.config, &current.key),
                        kind = ?current.kind,
                        reason = %failure.reason,
                        "sync quarantined"
                    );
                    self.quarantine(&current).await;
                    return;
                }
            }
        }
    }

    async fn finish_sync(self: &Arc<Self>, entry: &JournalEntry, etag: Option<String>) {
        let path = journal_key(
            &self.config.cache.prefix,
            &self.config.cache.node_id,
            &entry.id,
        );
        if entry.kind == JournalKind::Put {
            let bucket = &entry.bucket;
            let cache_key = self.cache_key(bucket, &entry.key);
            let mut meta = self
                .read_meta(bucket, &entry.key)
                .await
                .ok()
                .flatten()
                .unwrap_or_else(|| metadata_from_journal(entry));
            meta.mark_synced(etag.unwrap_or_else(|| entry.etag.clone()), unix_now());
            let _ = self.put_meta(self.cache_bucket(), &cache_key, &meta).await;
            let _ = self.index_upsert(&cache_key, &meta).await;
            let mut pending = lock(&self.pending);
            remove_pending_if_current(&mut pending, &(bucket.clone(), entry.key.clone()), &path);
        } else {
            lock(&self.tombstones).remove(&(entry.bucket.clone(), entry.key.clone()));
        }
        let _ = self.delete_object(self.cache_bucket(), &path).await;
        self.metrics.synced();
        info!(
            bucket = %entry.bucket,
            key = %log_key(&self.config, &entry.key),
            "sync completed"
        );
    }

    async fn quarantine(&self, entry: &JournalEntry) {
        let path = journal_key(
            &self.config.cache.prefix,
            &self.config.cache.node_id,
            &entry.id,
        );
        let mut failed = entry.clone();
        failed.state = EntryState::Failed;
        let _ = self.put_journal(self.cache_bucket(), &path, &failed).await;
        if entry.kind == JournalKind::Put {
            let cache_key = self.cache_key(&entry.bucket, &entry.key);
            if let Ok(Some(mut meta)) = self.read_meta(&entry.bucket, &entry.key).await {
                meta.mark_failed();
                let _ = self.put_meta(self.cache_bucket(), &cache_key, &meta).await;
                self.index_remove(&cache_key);
            }
            let mut pending = lock(&self.pending);
            remove_pending_if_current(
                &mut pending,
                &(entry.bucket.clone(), entry.key.clone()),
                &path,
            );
        } else {
            lock(&self.tombstones)
                .insert((entry.bucket.clone(), entry.key.clone()), entry.created_at);
        }
        self.metrics.quarantined();
    }

    async fn sleep_backoff(&self, attempt: u32) {
        let base = self.config.cache.sync_retry_base_ms.max(1);
        let exponent = attempt.saturating_sub(1).min(10);
        let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
        let delay = base.saturating_mul(factor).min(30_000);
        let seed = now().unix_timestamp_nanos().unsigned_abs();
        let jitter = u64::try_from(seed % u128::from(delay.max(1))).unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(delay.saturating_add(jitter))).await;
    }

    async fn try_put_to_origin(&self, entry: &JournalEntry) -> Result<Option<String>, SyncFailure> {
        let Some(response) = self
            .get_blob(&entry.bucket, &entry.key, None)
            .await
            .map_err(|error| SyncFailure::new(format!("cache read failed: {error}"), true))?
        else {
            return Err(SyncFailure::new("the cache blob is missing", false));
        };
        let body: BodyStream = Box::pin(
            response
                .bytes_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
        );
        let mut headers = HeaderMap::new();
        for (name, value) in &entry.headers {
            if !is_persisted_header(name) {
                continue;
            }
            let Ok(name) = axum::http::HeaderName::from_bytes(name.as_bytes()) else {
                continue;
            };
            let Ok(value) = HeaderValue::from_str(value) else {
                continue;
            };
            headers.insert(name, value);
        }
        if let Some(value) = &entry.content_type
            && let Ok(value) = HeaderValue::from_str(value)
        {
            headers.insert(header::CONTENT_TYPE, value);
        }
        if let Some(value) = &entry.content_encoding
            && let Ok(value) = HeaderValue::from_str(value)
        {
            headers.insert(header::CONTENT_ENCODING, value);
        }
        let response = crate::multipart::put_object(
            &self.origin,
            &self.origin_credential,
            &entry.bucket,
            &entry.key,
            headers,
            body,
            Some(entry.size),
            &self.config.multipart,
            &self.metrics,
        )
        .await
        .map_err(|error| SyncFailure::new(format!("origin request failed: {error}"), true))?;
        if response.status().is_success() {
            let etag = response
                .headers()
                .get(header::ETAG)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let _ = response.bytes().await;
            return Ok(etag);
        }
        let status = response.status();
        let retryable = is_retryable_status(status);
        Err(SyncFailure::new(
            format!("origin returned status {}", status.as_u16()),
            retryable,
        ))
    }

    async fn try_delete_at_origin(
        &self,
        entry: &JournalEntry,
    ) -> Result<Option<String>, SyncFailure> {
        let request = ForwardRequest {
            method: Method::DELETE,
            path_and_query: object_path(&entry.bucket, &entry.key),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .origin
            .execute(&self.origin_credential, request)
            .await
            .map_err(|error| SyncFailure::new(format!("origin request failed: {error}"), true))?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            let _ = response.bytes().await;
            return Ok(None);
        }
        let status = response.status();
        let retryable = is_retryable_status(status);
        Err(SyncFailure::new(
            format!("origin returned status {}", status.as_u16()),
            retryable,
        ))
    }

    /// Write a client body to the cache for a write-through request.
    ///
    /// The caller must call [`Self::commit_synced_blob`] only after origin
    /// accepts the object.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache write fails.
    pub async fn write_blob_from_receiver(
        &self,
        origin_bucket: &str,
        origin_key: &str,
        receiver: mpsc::Receiver<Result<Bytes, io::Error>>,
        length: Option<u64>,
    ) -> Result<StoredBlob, CacheError> {
        let cache_key = self.cache_key(origin_bucket, origin_key);
        let body = receiver_stream(receiver);
        let hashes = self
            .put_blob_stream(self.cache_bucket(), &cache_key, body, length, None)
            .await?;
        Ok(StoredBlob {
            cache_key,
            size: hashes.size,
            checksum: hashes.sha256,
            md5: hashes.md5,
        })
    }

    /// Mark a write-through blob as synced after origin accepted it.
    ///
    /// # Errors
    ///
    /// Returns an error when the metadata or index write fails.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_synced_blob(
        self: &Arc<Self>,
        origin_bucket: &str,
        origin_key: &str,
        blob: &StoredBlob,
        etag: Option<String>,
        content_type: Option<String>,
        content_encoding: Option<String>,
        headers: BTreeMap<String, String>,
    ) -> Result<(), CacheError> {
        let etag = etag.unwrap_or_else(|| format!("\"{}\"", blob.md5));
        let mut meta = CacheMetadata::synced(
            origin_bucket,
            origin_key,
            etag,
            None,
            blob.size,
            content_type,
            content_encoding,
            unix_now(),
        );
        meta.headers = headers;
        meta.checksum.clone_from(&blob.checksum);
        self.put_meta(self.cache_bucket(), &blob.cache_key, &meta)
            .await?;
        let key = (origin_bucket.to_owned(), origin_key.to_owned());
        let previous = {
            let mut pending = lock(&self.pending);
            pending.remove(&key)
        };
        if let Some(previous) = previous {
            let _ = self
                .delete_object(self.cache_bucket(), &previous.journal_key)
                .await;
        }
        lock(&self.tombstones).remove(&key);
        self.index_upsert(&blob.cache_key, &meta).await
    }

    /// Delete a best-effort cache blob for a failed write-through.
    pub async fn discard_blob(&self, origin_bucket: &str, origin_key: &str) {
        let cache_key = self.cache_key(origin_bucket, origin_key);
        let _ = self
            .delete_object(self.cache_bucket(), &meta_key(&cache_key))
            .await;
        let _ = self.delete_object(self.cache_bucket(), &cache_key).await;
        self.index_remove(&cache_key);
    }

    /// Return true when a client multipart upload completes in the cache.
    #[must_use]
    pub fn has_client_upload(&self, upload_id: &str) -> bool {
        lock(&self.client_uploads).contains_key(upload_id)
    }

    /// Start a client multipart upload at the cache endpoint.
    ///
    /// Returns the sidecar upload id for the client. The sidecar maps this id
    /// to the cache upload id.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache cannot start the upload or the response
    /// does not contain an upload id.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_client_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<String>,
        content_encoding: Option<String>,
        headers: BTreeMap<String, String>,
        request_headers: &HeaderMap,
    ) -> Result<String, CacheError> {
        let cache_key = self.cache_key(bucket, key);
        let request = ForwardRequest {
            method: Method::POST,
            path_and_query: format!("{}?uploads", object_path(self.cache_bucket(), &cache_key)),
            headers: request_headers.clone(),
            body: None,
            content_length: Some(0),
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if !response.status().is_success() {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        let body = response.bytes().await.map_err(|_| CacheError::Body)?;
        let text = String::from_utf8_lossy(&body);
        let cache_upload_id = extract_tag(&text, "UploadId").ok_or(CacheError::Metadata)?;
        let id = next_id();
        lock(&self.client_uploads).insert(
            id.clone(),
            ClientUpload {
                upload_id: cache_upload_id,
                cache_key,
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                content_type,
                content_encoding,
                headers,
            },
        );
        Ok(id)
    }

    /// Send one client part to the cache endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache does not know the upload or the cache
    /// request fails.
    pub async fn upload_client_part(
        &self,
        upload_id: &str,
        part_number: &str,
        headers: &HeaderMap,
        body: BodyStream,
        length: Option<u64>,
    ) -> Result<reqwest::Response, CacheError> {
        let upload = self.client_upload(upload_id)?;
        let request = ForwardRequest {
            method: Method::PUT,
            path_and_query: format!(
                "{}?partNumber={part_number}&uploadId={}",
                object_path(self.cache_bucket(), &upload.cache_key),
                encode_query_value(&upload.upload_id)
            ),
            headers: headers.clone(),
            body: Some(body),
            content_length: length,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status().is_success() {
            Ok(response)
        } else {
            Err(CacheError::Status(response.status().as_u16()))
        }
    }

    /// Complete a client multipart upload at the cache endpoint.
    ///
    /// The cache entry becomes pending. The origin sync starts after this call
    /// writes the journal and metadata.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache does not know the upload, the cache
    /// complete fails, or the cache cannot store the metadata.
    pub async fn complete_client_upload(
        self: &Arc<Self>,
        upload_id: &str,
        headers: &HeaderMap,
        body: Bytes,
    ) -> Result<ClientComplete, CacheError> {
        let upload = self.client_upload(upload_id)?;
        let length = u64::try_from(body.len()).map_err(|_| CacheError::Body)?;
        let stream: BodyStream = Box::pin(futures_util::stream::once(async move { Ok(body) }));
        let request = ForwardRequest {
            method: Method::POST,
            path_and_query: format!(
                "{}?uploadId={}",
                object_path(self.cache_bucket(), &upload.cache_key),
                encode_query_value(&upload.upload_id)
            ),
            headers: headers.clone(),
            body: Some(stream),
            content_length: Some(length),
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if !response.status().is_success() {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        let response_headers = response.headers().clone();
        let response_body = response.bytes().await.map_err(|_| CacheError::Body)?;
        let text = String::from_utf8_lossy(&response_body);
        let etag = response_headers
            .get(header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .or_else(|| extract_tag(&text, "ETag"))
            .unwrap_or_default();
        let size = self
            .head_content_length(self.cache_bucket(), &upload.cache_key)
            .await?;
        lock(&self.client_uploads).remove(upload_id);
        let mut meta = CacheMetadata::synced(
            &upload.bucket,
            &upload.key,
            etag,
            None,
            size,
            upload.content_type,
            upload.content_encoding,
            unix_now(),
        );
        meta.headers = upload.headers;
        meta.state = EntryState::Pending;
        self.record_pending_put(&upload.bucket, &upload.key, &upload.cache_key, &meta)
            .await?;
        Ok(ClientComplete {
            headers: response_headers,
            body: response_body,
        })
    }

    /// Abort a client multipart upload at the cache endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache does not know the upload or the cache
    /// abort fails.
    pub async fn abort_client_upload(
        &self,
        upload_id: &str,
        headers: &HeaderMap,
    ) -> Result<reqwest::Response, CacheError> {
        let upload = self.client_upload(upload_id)?;
        let request = ForwardRequest {
            method: Method::DELETE,
            path_and_query: format!(
                "{}?uploadId={}",
                object_path(self.cache_bucket(), &upload.cache_key),
                encode_query_value(&upload.upload_id)
            ),
            headers: headers.clone(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if !response.status().is_success() && response.status() != StatusCode::NOT_FOUND {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        lock(&self.client_uploads).remove(upload_id);
        Ok(response)
    }

    /// List the parts of a client multipart upload at the cache endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when the cache does not know the upload or the cache
    /// request fails.
    pub async fn list_client_parts(
        &self,
        upload_id: &str,
        query: &str,
        headers: &HeaderMap,
    ) -> Result<reqwest::Response, CacheError> {
        let upload = self.client_upload(upload_id)?;
        let query = replace_upload_id(query, &upload.upload_id);
        let request = ForwardRequest {
            method: Method::GET,
            path_and_query: format!(
                "{}?{query}",
                object_path(self.cache_bucket(), &upload.cache_key)
            ),
            headers: headers.clone(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status().is_success() {
            Ok(response)
        } else {
            Err(CacheError::Status(response.status().as_u16()))
        }
    }

    /// Fill the cache from an origin response.
    ///
    /// This call does not write a journal record. Origin already has the
    /// object.
    ///
    /// # Errors
    ///
    /// Returns an error when the body is too large or the cache write fails.
    pub async fn fill_from_response(
        self: &Arc<Self>,
        bucket: &str,
        key: &str,
        response: reqwest::Response,
        mut meta: CacheMetadata,
    ) -> Result<(), CacheError> {
        if meta.size > self.max_object_bytes() {
            return Err(CacheError::TooLarge);
        }
        let cache_key = self.cache_key(bucket, key);
        let body: BodyStream = Box::pin(
            response
                .bytes_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string())),
        );
        let hashes = self
            .put_blob_stream(self.cache_bucket(), &cache_key, body, Some(meta.size), None)
            .await?;
        meta.size = hashes.size;
        meta.checksum = hashes.sha256;
        if meta.etag.is_empty() {
            meta.etag = format!("\"{}\"", hashes.md5);
        }
        meta.state = EntryState::Synced;
        meta.validated_at = unix_now();
        self.put_meta(self.cache_bucket(), &cache_key, &meta)
            .await?;
        let pending = lock(&self.pending).remove(&(bucket.to_owned(), key.to_owned()));
        if let Some(entry) = pending {
            let _ = self
                .delete_object(self.cache_bucket(), &entry.journal_key)
                .await;
        }
        lock(&self.tombstones).remove(&(bucket.to_owned(), key.to_owned()));
        self.index_upsert(&cache_key, &meta).await
    }

    fn client_upload(&self, upload_id: &str) -> Result<ClientUpload, CacheError> {
        lock(&self.client_uploads)
            .get(upload_id)
            .cloned()
            .ok_or(CacheError::Status(StatusCode::NOT_FOUND.as_u16()))
    }

    /// Write the journal and metadata for a pending cache entry.
    async fn record_pending_put(
        self: &Arc<Self>,
        bucket: &str,
        key: &str,
        cache_key: &str,
        meta: &CacheMetadata,
    ) -> Result<(), CacheError> {
        let journal = JournalEntry::put(next_id(), cache_key.to_owned(), meta);
        let journal_key = journal_key(
            &self.config.cache.prefix,
            &self.config.cache.node_id,
            &journal.id,
        );
        self.put_journal(self.cache_bucket(), &journal_key, &journal)
            .await?;
        if let Err(error) = self.put_meta(self.cache_bucket(), cache_key, meta).await {
            let _ = self.delete_object(self.cache_bucket(), &journal_key).await;
            return Err(error);
        }
        let previous = lock(&self.pending).remove(&(bucket.to_owned(), key.to_owned()));
        if let Some(previous) = previous {
            let _ = self
                .delete_object(self.cache_bucket(), &previous.journal_key)
                .await;
        }
        lock(&self.pending).insert(
            (bucket.to_owned(), key.to_owned()),
            PendingEntry {
                meta: meta.clone(),
                journal_key,
            },
        );
        lock(&self.tombstones).remove(&(bucket.to_owned(), key.to_owned()));
        self.enqueue((bucket.to_owned(), key.to_owned()), journal);
        Ok(())
    }

    async fn head_content_length(&self, bucket: &str, key: &str) -> Result<u64, CacheError> {
        let request = ForwardRequest {
            method: Method::HEAD,
            path_and_query: object_path(bucket, key),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if !response.status().is_success() {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(CacheError::Metadata)
    }

    async fn put_journal(
        &self,
        bucket: &str,
        key: &str,
        entry: &JournalEntry,
    ) -> Result<(), CacheError> {
        let body = serde_json::to_vec(entry).map_err(|_| CacheError::Metadata)?;
        self.put_object(bucket, key, Bytes::from(body), "application/json")
            .await
    }

    async fn put_meta(
        &self,
        bucket: &str,
        cache_key: &str,
        meta: &CacheMetadata,
    ) -> Result<(), CacheError> {
        let body = serde_json::to_vec(meta).map_err(|_| CacheError::Metadata)?;
        self.put_object(
            bucket,
            &meta_key(cache_key),
            Bytes::from(body),
            "application/json",
        )
        .await
    }

    async fn put_meta_from_key(
        &self,
        bucket: &str,
        cache_key: &str,
        meta: &CacheMetadata,
    ) -> Result<(), CacheError> {
        self.put_meta(bucket, cache_key, meta).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: Bytes,
        content_type: &str,
    ) -> Result<(), CacheError> {
        let mut headers = HeaderMap::new();
        if let Ok(value) = HeaderValue::from_str(content_type) {
            headers.insert(header::CONTENT_TYPE, value);
        }
        let length = u64::try_from(body.len()).unwrap_or(u64::MAX);
        let stream: BodyStream = Box::pin(futures_util::stream::once(async move { Ok(body) }));
        let request = ForwardRequest {
            method: Method::PUT,
            path_and_query: object_path(bucket, key),
            headers,
            body: Some(stream),
            content_length: Some(length),
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status().is_success() {
            let _ = response.bytes().await;
            Ok(())
        } else {
            Err(CacheError::Status(response.status().as_u16()))
        }
    }

    async fn put_blob_stream(
        &self,
        bucket: &str,
        cache_key: &str,
        body: BodyStream,
        length: Option<u64>,
        body_started: Option<Arc<AtomicBool>>,
    ) -> Result<HashResult, CacheError> {
        let state = Arc::new(Mutex::new(HashState::new(self.max_object_bytes())));
        let started = body_started;
        let hashing = {
            let state = Arc::clone(&state);
            let mut body = body;
            async_stream::try_stream! {
                while let Some(chunk) = body.next().await {
                    let bytes = chunk?;
                    if let Some(started) = started.as_ref() {
                        started.store(true, Ordering::Release);
                    }
                    update_hash(&state, &bytes).map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidData, error.to_string())
                    })?;
                    yield bytes;
                }
            }
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        let response = crate::multipart::put_object(
            &self.client,
            &self.credential,
            bucket,
            cache_key,
            headers,
            Box::pin(hashing),
            length,
            &self.config.multipart,
            &self.metrics,
        )
        .await
        .map_err(storage_error)?;
        if !response.status().is_success() {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        let _ = response.bytes().await;
        let hasher = state.lock().unwrap_or_else(PoisonError::into_inner);
        Ok(hasher.finish())
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<&str>,
    ) -> Result<Option<reqwest::Response>, CacheError> {
        let mut headers = HeaderMap::new();
        if let Some(range) = range
            && let Ok(value) = HeaderValue::from_str(range)
        {
            headers.insert(header::RANGE, value);
        }
        let request = ForwardRequest {
            method: Method::GET,
            path_and_query: object_path(bucket, key),
            headers,
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status().is_success() {
            return Ok(Some(response));
        }
        Err(CacheError::Status(response.status().as_u16()))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<bool, CacheError> {
        let request = ForwardRequest {
            method: Method::HEAD,
            path_and_query: object_path(bucket, key),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if response.status().is_success() {
            return Ok(true);
        }
        Err(CacheError::Status(response.status().as_u16()))
    }

    async fn last_modified(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<time::OffsetDateTime>, CacheError> {
        let request = ForwardRequest {
            method: Method::HEAD,
            path_and_query: object_path(bucket, key),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if !response.status().is_success() {
            return Ok(None);
        }
        let Some(value) = response.headers().get(header::LAST_MODIFIED) else {
            return Ok(None);
        };
        let Ok(value) = value.to_str() else {
            return Ok(None);
        };
        Ok(time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc2822).ok())
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), CacheError> {
        let request = ForwardRequest {
            method: Method::DELETE,
            path_and_query: object_path(bucket, key),
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            let _ = response.bytes().await;
            return Ok(());
        }
        Err(CacheError::Status(response.status().as_u16()))
    }

    async fn list_keys(&self, bucket: &str, prefix: &str) -> Result<Vec<String>, CacheError> {
        let encoded =
            percent_encoding::utf8_percent_encode(prefix, percent_encoding::NON_ALPHANUMERIC);
        let path = format!("/{bucket}?list-type=2&prefix={encoded}");
        let request = ForwardRequest {
            method: Method::GET,
            path_and_query: path,
            headers: HeaderMap::new(),
            body: None,
            content_length: None,
        };
        let response = self
            .client
            .execute(&self.credential, request)
            .await
            .map_err(storage_error)?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }
        if !response.status().is_success() {
            return Err(CacheError::Status(response.status().as_u16()));
        }
        let body = response.text().await.map_err(|_| CacheError::Body)?;
        Ok(parse_keys(&body))
    }

    fn index_remove(&self, cache_key: &str) {
        if let Some(index) = &self.index {
            let index = lock(index);
            if let Err(error) = index.remove(cache_key) {
                warn!(%error, "cache index remove failed");
            }
            self.metrics.set_tracked(index.tracked_bytes());
        }
    }

    async fn index_upsert(
        self: &Arc<Self>,
        cache_key: &str,
        meta: &CacheMetadata,
    ) -> Result<(), CacheError> {
        let Some(index) = &self.index else {
            return Ok(());
        };
        let entry = IndexEntry {
            origin_bucket: meta.origin_bucket.clone(),
            origin_key: meta.origin_key.clone(),
            size: meta.size,
            last_access: unix_now(),
            validated_at: meta.validated_at,
            state: meta.state,
        };
        {
            let index = lock(index);
            index.insert(cache_key, &entry)?;
            self.metrics.set_tracked(index.tracked_bytes());
        }
        self.maybe_evict().await
    }

    async fn maybe_evict(self: &Arc<Self>) -> Result<(), CacheError> {
        let Some(max_bytes) = self.config.cache.max_bytes else {
            return Ok(());
        };
        if max_bytes == 0 {
            return Ok(());
        }
        let Some(index) = &self.index else {
            return Ok(());
        };
        let high = watermark(max_bytes, self.config.cache.high_watermark);
        let low = watermark(max_bytes, self.config.cache.low_watermark);
        let tracked = {
            let index = lock(index);
            index.tracked_bytes()
        };
        if tracked <= high {
            return Ok(());
        }
        let evicted = {
            let index = lock(index);
            index.evict_to(low)?
        };
        for (cache_key, entry) in evicted {
            let _ = self
                .delete_object(self.cache_bucket(), &meta_key(&cache_key))
                .await;
            let _ = self.delete_object(self.cache_bucket(), &cache_key).await;
            info!(
                bucket = %entry.origin_bucket,
                key = %log_key(&self.config, &entry.origin_key),
                "cache entry evicted"
            );
            self.metrics.evicted();
        }
        let tracked = {
            let index = lock(index);
            index.tracked_bytes()
        };
        self.metrics.set_tracked(tracked);
        Ok(())
    }

    async fn ensure_index_loaded(self: &Arc<Self>) {
        let Some(index) = &self.index else {
            return;
        };
        {
            let index = lock(index);
            if index.is_loaded() {
                self.metrics.set_tracked(index.tracked_bytes());
                return;
            }
        }
        let mut entries = Vec::new();
        let bucket = self.cache_bucket().to_owned();
        let prefix = node_prefix(&self.config.cache.prefix, &self.config.cache.node_id);
        let Ok(keys) = self.list_keys(&bucket, &prefix).await else {
            return;
        };
        for key in keys {
            if !is_meta_key(&key) {
                continue;
            }
            let blob = key.trim_end_matches(".meta");
            let Ok(Some(response)) = self.get_object(&bucket, &key, None).await else {
                continue;
            };
            let Ok(body) = response.bytes().await else {
                continue;
            };
            let Ok(meta) = serde_json::from_slice::<CacheMetadata>(&body) else {
                continue;
            };
            entries.push((
                blob.to_owned(),
                IndexEntry {
                    origin_bucket: meta.origin_bucket.clone(),
                    origin_key: meta.origin_key.clone(),
                    size: meta.size,
                    last_access: meta.validated_at,
                    validated_at: meta.validated_at,
                    state: meta.state,
                },
            ));
        }
        let index = lock(index);
        if let Err(error) = index.rebuild(entries) {
            warn!(%error, "cache index rebuild failed");
        }
        self.metrics.set_tracked(index.tracked_bytes());
    }

    fn spawn_gc(self: &Arc<Self>) {
        let cache = Arc::clone(self);
        let interval = self.config.cache.gc_interval.as_duration();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(error) = cache.gc_once().await {
                    warn!(%error, "cache garbage collection failed");
                }
            }
        });
        self.track(task);
    }

    fn spawn_metrics(self: &Arc<Self>) {
        let cache = Arc::clone(self);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                cache.refresh_metrics();
            }
        });
        self.track(task);
    }
}

struct SyncFailure {
    reason: String,
    retryable: bool,
}

impl SyncFailure {
    fn new(reason: impl Into<String>, retryable: bool) -> Self {
        Self {
            reason: reason.into(),
            retryable,
        }
    }
}

/// A blob written to the cache for a write-through request.
pub struct StoredBlob {
    cache_key: String,
    size: u64,
    checksum: String,
    md5: String,
}

/// A completed client multipart upload response from the cache.
pub struct ClientComplete {
    /// Response headers from the cache.
    pub headers: HeaderMap,
    /// Response body from the cache.
    pub body: Bytes,
}

struct HashResult {
    size: u64,
    sha256: String,
    md5: String,
}

struct HashState {
    max: u64,
    size: u64,
    sha: Sha256,
    md5: Md5,
}

impl HashState {
    fn new(max: u64) -> Self {
        Self {
            max,
            size: 0,
            sha: Sha256::new(),
            md5: Md5::new(),
        }
    }

    fn update(&mut self, bytes: &[u8]) -> Result<(), CacheError> {
        self.size = self
            .size
            .saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        if self.size > self.max {
            return Err(CacheError::TooLarge);
        }
        self.sha.update(bytes);
        self.md5.update(bytes);
        Ok(())
    }

    fn finish(&self) -> HashResult {
        HashResult {
            size: self.size,
            sha256: hex::encode(self.sha.clone().finalize()),
            md5: hex::encode(self.md5.clone().finalize()),
        }
    }
}

fn metadata_from_journal(entry: &JournalEntry) -> CacheMetadata {
    CacheMetadata {
        version: 1,
        origin_bucket: entry.bucket.clone(),
        origin_key: entry.key.clone(),
        etag: entry.etag.clone(),
        last_modified: None,
        validated_at: entry.created_at,
        size: entry.size,
        checksum: entry.checksum.clone(),
        content_type: entry.content_type.clone(),
        content_encoding: entry.content_encoding.clone(),
        headers: entry.headers.clone(),
        state: entry.state,
        created_at: entry.created_at,
    }
}

fn metadata_from_response(bucket: &str, key: &str, response: &reqwest::Response) -> CacheMetadata {
    let etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let last_modified = response
        .headers()
        .get(header::LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let size = response
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let content_encoding = response
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut meta = CacheMetadata::synced(
        bucket,
        key,
        etag,
        last_modified,
        size,
        content_type,
        content_encoding,
        unix_now(),
    );
    meta.headers = collect_headers(response.headers());
    meta
}

fn is_meta_key(key: &str) -> bool {
    std::path::Path::new(key)
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("meta"))
}

/// Remove the pending entry only when it still names the finished journal.
fn remove_pending_if_current(
    pending: &mut HashMap<CacheKey, PendingEntry>,
    key: &CacheKey,
    journal: &str,
) -> bool {
    let current = pending
        .get(key)
        .is_some_and(|entry| entry.journal_key == journal);
    if current {
        pending.remove(key);
    }
    current
}

fn update_hash(state: &Mutex<HashState>, bytes: &[u8]) -> Result<(), CacheError> {
    let mut hasher = lock(state);
    hasher.update(bytes)
}

fn receiver_stream(mut receiver: mpsc::Receiver<Result<Bytes, io::Error>>) -> BodyStream {
    let stream = async_stream::try_stream! {
        while let Some(item) = receiver.recv().await {
            yield item?;
        }
    };
    Box::pin(stream)
}

#[allow(clippy::needless_pass_by_value)]
fn storage_error(error: StorageError) -> CacheError {
    match error {
        StorageError::Sign(_) => CacheError::Metadata,
        StorageError::Status(status) => CacheError::Status(status.as_u16()),
        StorageError::Body => CacheError::Body,
        StorageError::TooLarge => CacheError::TooLarge,
        StorageError::Timeout | StorageError::Transport(_) => CacheError::Unavailable,
    }
}

fn is_retryable_status(status: StatusCode) -> bool {
    status.is_server_error()
        || status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
}

fn parse_keys(body: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + 5..];
        let Some(end) = rest.find("</Key>") else {
            break;
        };
        keys.push(unescape_xml(&rest[..end]));
        rest = &rest[end + 6..];
    }
    keys
}

/// Return one XML element value.
fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.saturating_add(open.len());
    let rest = xml.get(start..)?;
    let end = rest.find(&close)?;
    Some(unescape_xml(rest.get(..end)?))
}

/// Replace the upload id in one query string.
fn replace_upload_id(query: &str, upload_id: &str) -> String {
    let value = encode_query_value(upload_id);
    query
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let key = pair.split_once('=').map_or(pair, |(key, _)| key);
            if key == "uploadId" {
                format!("uploadId={value}")
            } else {
                pair.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

/// Percent-encode one query value.
fn encode_query_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(output, "%{byte:02X}");
        }
    }
    output
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn watermark(max_bytes: u64, fraction: f64) -> u64 {
    let clamped = fraction.clamp(0.0, 1.0);
    // The fraction is validated to the range 0.0..=1.0. Integer math keeps the
    // result exact enough for the threshold.
    let scaled = (clamped * 1_000_000.0).round() as u64;
    u64::try_from((u128::from(max_bytes) * u128::from(scaled)) / 1_000_000).unwrap_or(max_bytes)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Send arbitrary text to the cache key parser.
///
/// This function is an entry point for the fuzz target. It returns the number
/// of parsed keys. It does not panic.
#[doc(hidden)]
#[must_use]
pub fn fuzz_s3_xml(input: &str) -> usize {
    parse_keys(input).len()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn watermark_scales_the_limit() {
        assert_eq!(watermark(100, 0.9), 90);
        assert_eq!(watermark(100, 0.8), 80);
        assert_eq!(watermark(0, 0.9), 0);
    }

    #[test]
    fn log_key_redacts_when_object_keys_are_disabled() {
        let mut config = Config::default();
        config.otel.record_object_keys = false;
        assert_eq!(log_key(&config, "private-key"), "[redacted]");

        config.otel.record_object_keys = true;
        assert_eq!(log_key(&config, "private-key"), "private-key");
    }

    #[test]
    fn key_parser_handles_escaped_names() {
        let xml = "<ListBucketResult><Contents><Key>a&amp;b.txt</Key></Contents>\
                   <Contents><Key>plain</Key></Contents></ListBucketResult>";
        assert_eq!(
            parse_keys(xml),
            vec!["a&b.txt".to_owned(), "plain".to_owned()]
        );
    }

    #[test]
    fn retryable_status_includes_throttle_and_5xx() {
        assert!(is_retryable_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!is_retryable_status(StatusCode::FORBIDDEN));
        assert!(!is_retryable_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn pending_removal_keeps_a_newer_journal() {
        let meta = CacheMetadata::synced(
            "media",
            "a.txt",
            "\"etag\"".to_owned(),
            None,
            1,
            None,
            None,
            1,
        );
        let key = ("media".to_owned(), "a.txt".to_owned());
        let mut pending = HashMap::new();
        pending.insert(
            key.clone(),
            PendingEntry {
                meta: meta.clone(),
                journal_key: "old".to_owned(),
            },
        );
        assert!(!remove_pending_if_current(&mut pending, &key, "new"));
        assert!(pending.contains_key(&key));
        pending.insert(
            key.clone(),
            PendingEntry {
                meta,
                journal_key: "new".to_owned(),
            },
        );
        assert!(!remove_pending_if_current(&mut pending, &key, "old"));
        assert!(pending.contains_key(&key));
        assert!(remove_pending_if_current(&mut pending, &key, "new"));
        assert!(!pending.contains_key(&key));
    }
}
