//! Deterministic metadata store for cluster mode.
//!
//! The store holds object metadata, bucket consistency settings, in-flight
//! multipart uploads, the raft log, and raft bookkeeping in one `redb`
//! database. Blob bytes never enter this database. A metadata key encodes
//! `(bucket, key)` with an order that matches the raw byte order. A prefix scan
//! therefore returns keys in lexicographic order. The encoding escapes `0x00`
//! bytes, so control characters and long keys are safe.
//!
//! Every applied log entry writes its metadata and the applied log index in one
//! transaction. Apply is deterministic: the entry carries the commit time, and
//! the store assigns `version = log index`.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::coordinator::{CommitRequest, CoordError, Entry, RejectReason};
use super::raft::{LogIdOf, StoredMembershipOf};
use crate::config::{BucketConfig, ReadMode, WriteAck};

const METADATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");
const BUCKET_CONFIG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bucket_config");
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
const SYSTEM: TableDefinition<&str, u64> = TableDefinition::new("system");
const RAFT_LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const RAFT_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");
const SNAPSHOT: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshot");

const APPLIED_KEY: &str = "applied";
const LAST_APPLIED_KEY: &str = "last_applied";
const MEMBERSHIP_KEY: &str = "membership";
const SNAPSHOT_KEY: &str = "latest";

/// Lifecycle state of one object key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectState {
    /// An in-flight multipart upload holds this key.
    Pending,
    /// The key is visible and the blob is durable.
    Committed,
    /// A delete is in progress.
    Deleting,
    /// The key is deleted and not visible.
    Tombstone,
}

/// Metadata for one object key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    /// Lifecycle state.
    pub state: ObjectState,
    /// Version for this key. The store assigns the raft log index.
    pub version: u64,
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
    /// Creation time in Unix seconds.
    pub created_at: i64,
    /// Commit time in Unix seconds.
    pub committed_at: Option<i64>,
    /// Delete time in Unix seconds.
    pub deleted_at: Option<i64>,
    /// Cache-Control value.
    pub cache_control: Option<String>,
    /// Content-Encoding value.
    pub content_encoding: Option<String>,
    /// Content-Disposition value.
    pub content_disposition: Option<String>,
}

impl Metadata {
    /// Return true when the key is visible to clients.
    #[must_use]
    pub fn is_committed(&self) -> bool {
        self.state == ObjectState::Committed
    }
}

/// Stored consistency settings for one bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketConfigRecord {
    /// Write acknowledgement level.
    pub write_ack: WriteAck,
    /// Read routing mode.
    pub read: ReadMode,
    /// Target replica count.
    pub replicas: u32,
    /// Minimum replica count.
    pub min_replicas: u32,
}

impl BucketConfigRecord {
    /// Build a record from a file bucket entry.
    ///
    /// The single-node backend uses one replica when the file does not set a
    /// count.
    #[must_use]
    pub fn from_config(bucket: &BucketConfig) -> Self {
        Self {
            write_ack: bucket.write_ack.unwrap_or(WriteAck::Leader),
            read: bucket.read.unwrap_or(ReadMode::LocalThenPeer),
            replicas: bucket.replicas.unwrap_or(1),
            min_replicas: bucket.min_replicas.unwrap_or(1),
        }
    }
}

/// One in-flight multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadRecord {
    /// Upload identifier that the sidecar returns to the client.
    pub upload_id: String,
    /// Upload identifier from the backing S3 service.
    pub backing_upload_id: String,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Node that owns the upload.
    pub node_id: String,
    /// Backing blob identifier.
    pub blob: String,
    /// Content type for the final object.
    pub content_type: Option<String>,
    /// User metadata for the final object.
    pub user_metadata: BTreeMap<String, String>,
    /// Cache-Control for the final object.
    #[serde(default)]
    pub cache_control: Option<String>,
    /// Content-Encoding for the final object.
    #[serde(default)]
    pub content_encoding: Option<String>,
    /// Content-Disposition for the final object.
    #[serde(default)]
    pub content_disposition: Option<String>,
    /// Creation time in Unix seconds.
    pub created_at: i64,
}

/// Result of one applied entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApplyResult {
    /// A commit became visible.
    Commit(Metadata),
    /// A pending multipart key was removed.
    Abort,
    /// A tombstone replaced the key.
    Delete(Metadata),
    /// A bucket config was stored.
    BucketConfig(BucketConfigRecord),
    /// A log entry carried no application data.
    Blank,
}

/// Outcome of one applied log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum ApplyOutcome {
    /// The entry changed the state.
    Applied(ApplyResult),
    /// The state machine rejected the entry.
    Rejected(RejectReason),
    /// The entry was already applied.
    Duplicate,
}

/// One applied entry and its index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Applied {
    /// Applied log index.
    pub index: u64,
    /// Coordinator term.
    pub term: u64,
    /// Apply result.
    pub result: ApplyResult,
}

/// A durable metadata state machine.
pub struct MetadataStore {
    db: Arc<Database>,
    applied: AtomicU64,
    write_lock: Mutex<()>,
}

#[derive(Serialize, Deserialize)]
struct ObjectRecord {
    bucket: String,
    key: String,
    metadata: Metadata,
}

#[derive(Serialize, Deserialize)]
struct BucketRecord {
    bucket: String,
    config: BucketConfigRecord,
}

#[derive(Serialize, Deserialize)]
struct StateExport {
    objects: Vec<ObjectRecord>,
    buckets: Vec<BucketRecord>,
    uploads: Vec<UploadRecord>,
    last_applied: Option<LogIdOf>,
    membership: Option<StoredMembershipOf>,
}

impl MetadataStore {
    /// Open or create the metadata database.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be opened or read.
    pub fn open(path: &Path) -> Result<Self, CoordError> {
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        let db = Database::create(path).map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let write = db
                .begin_write()
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            {
                write
                    .open_table(METADATA)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                write
                    .open_table(BUCKET_CONFIG)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                write
                    .open_table(UPLOADS)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                write
                    .open_table(RAFT_LOG)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                write
                    .open_table(RAFT_META)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                write
                    .open_table(SNAPSHOT)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            {
                let mut system = write
                    .open_table(SYSTEM)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                if system
                    .get(APPLIED_KEY)
                    .map_err(|error| CoordError::Storage(error.to_string()))?
                    .is_none()
                {
                    system
                        .insert(APPLIED_KEY, 0)
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                }
            }
            write
                .commit()
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        let applied = {
            let read = db
                .begin_read()
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let system = read
                .open_table(SYSTEM)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            system
                .get(APPLIED_KEY)
                .map_err(|error| CoordError::Storage(error.to_string()))?
                .map_or(0, |value| value.value())
        };
        Ok(Self {
            db: Arc::new(db),
            applied: AtomicU64::new(applied),
            write_lock: Mutex::new(()),
        })
    }

    /// Return the shared database handle.
    #[must_use]
    pub fn database(&self) -> Arc<Database> {
        Arc::clone(&self.db)
    }

    /// Return the last applied index.
    #[must_use]
    pub fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// Return the last applied log id.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be read.
    pub fn last_applied_log(&self) -> Result<Option<LogIdOf>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(RAFT_META)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(LAST_APPLIED_KEY)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let Some(value) = value else {
            return Ok(None);
        };
        let log_id = serde_json::from_slice(value.value())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(Some(log_id))
    }

    /// Return the last applied membership.
    ///
    /// # Errors
    ///
    /// Returns an error when the value cannot be read.
    pub fn stored_membership(&self) -> Result<StoredMembershipOf, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(RAFT_META)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(MEMBERSHIP_KEY)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let Some(value) = value else {
            return Ok(StoredMembershipOf::default());
        };
        let membership = serde_json::from_slice(value.value())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(membership)
    }

    /// Apply one log entry.
    ///
    /// The metadata write and the applied log id are one transaction. The
    /// version is the log index, so replay produces the same metadata.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_entry(&self, log_id: &LogIdOf, entry: &Entry) -> Result<ApplyOutcome, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self
            .last_applied_log()?
            .is_some_and(|applied| log_id.index <= applied.index)
        {
            return Ok(ApplyOutcome::Duplicate);
        }
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let outcome = match entry {
            Entry::Commit { request, .. } => self.write_commit(&write, log_id, request)?,
            Entry::Abort { bucket, key, .. } => {
                self.write_abort(&write, bucket, key)?;
                ApplyOutcome::Applied(ApplyResult::Abort)
            }
            Entry::Delete {
                bucket,
                key,
                owner,
                deleted_at,
                ..
            } => self.write_delete(&write, log_id, bucket, key, owner, *deleted_at)?,
            Entry::BucketConfig {
                bucket, request, ..
            } => self.write_bucket_config(&write, bucket, *request)?,
        };
        self.write_applied(&write, log_id)?;
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.applied.store(log_id.index, Ordering::Release);
        Ok(outcome)
    }

    /// Apply one membership entry.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_membership(
        &self,
        log_id: &LogIdOf,
        membership: &StoredMembershipOf,
    ) -> Result<ApplyOutcome, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self
            .last_applied_log()?
            .is_some_and(|applied| log_id.index <= applied.index)
        {
            return Ok(ApplyOutcome::Duplicate);
        }
        let value = serde_json::to_vec(membership)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(RAFT_META)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(MEMBERSHIP_KEY, value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        self.write_applied(&write, log_id)?;
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.applied.store(log_id.index, Ordering::Release);
        Ok(ApplyOutcome::Applied(ApplyResult::Blank))
    }

    /// Apply one blank entry.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_blank(&self, log_id: &LogIdOf) -> Result<ApplyOutcome, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self
            .last_applied_log()?
            .is_some_and(|applied| log_id.index <= applied.index)
        {
            return Ok(ApplyOutcome::Duplicate);
        }
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.write_applied(&write, log_id)?;
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.applied.store(log_id.index, Ordering::Release);
        Ok(ApplyOutcome::Applied(ApplyResult::Blank))
    }

    #[allow(clippy::unused_self)]
    fn write_commit(
        &self,
        write: &redb::WriteTransaction,
        log_id: &LogIdOf,
        request: &CommitRequest,
    ) -> Result<ApplyOutcome, CoordError> {
        let key = encode_metadata_key(&request.bucket, &request.key);
        let current = read_metadata_in_write(write, &key)?;
        if request.if_none_match && current.as_ref().is_some_and(Metadata::is_committed) {
            return Ok(ApplyOutcome::Rejected(RejectReason::PreconditionFailed));
        }
        if let Some(expected) = &request.if_match {
            let matches = current.as_ref().is_some_and(|metadata| {
                metadata.is_committed() && etag_matches(&metadata.etag, expected)
            });
            if !matches {
                return Ok(ApplyOutcome::Rejected(RejectReason::PreconditionFailed));
            }
        }
        let created_at = current
            .as_ref()
            .map_or(request.committed_at, |metadata| metadata.created_at);
        let metadata = Metadata {
            state: ObjectState::Committed,
            version: log_id.index,
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
        };
        write_metadata(write, &key, &metadata)?;
        Ok(ApplyOutcome::Applied(ApplyResult::Commit(metadata)))
    }

    #[allow(clippy::unused_self, clippy::too_many_arguments)]
    fn write_delete(
        &self,
        write: &redb::WriteTransaction,
        log_id: &LogIdOf,
        bucket: &str,
        key: &str,
        owner: &str,
        deleted_at: i64,
    ) -> Result<ApplyOutcome, CoordError> {
        let encoded = encode_metadata_key(bucket, key);
        let current = read_metadata_in_write(write, &encoded)?;
        let metadata = Metadata {
            state: ObjectState::Tombstone,
            version: log_id.index,
            blob: current
                .as_ref()
                .map_or_else(String::new, |metadata| metadata.blob.clone()),
            size: 0,
            checksum: String::new(),
            content_type: None,
            etag: String::new(),
            user_metadata: BTreeMap::new(),
            owner: owner.to_owned(),
            replicas: Vec::new(),
            created_at: current
                .as_ref()
                .map_or(deleted_at, |metadata| metadata.created_at),
            committed_at: None,
            deleted_at: Some(deleted_at),
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
        };
        write_metadata(write, &encoded, &metadata)?;
        Ok(ApplyOutcome::Applied(ApplyResult::Delete(metadata)))
    }

    #[allow(clippy::unused_self)]
    fn write_abort(
        &self,
        write: &redb::WriteTransaction,
        bucket: &str,
        key: &str,
    ) -> Result<(), CoordError> {
        let encoded = encode_metadata_key(bucket, key);
        if read_metadata_in_write(write, &encoded)?
            .is_some_and(|metadata| metadata.state == ObjectState::Pending)
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(encoded.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    #[allow(clippy::unused_self)]
    fn write_bucket_config(
        &self,
        write: &redb::WriteTransaction,
        bucket: &str,
        config: BucketConfigRecord,
    ) -> Result<ApplyOutcome, CoordError> {
        let key = encode_component(bucket);
        let value =
            serde_json::to_vec(&config).map_err(|error| CoordError::Storage(error.to_string()))?;
        let mut table = write
            .open_table(BUCKET_CONFIG)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        table
            .insert(key.as_slice(), value.as_slice())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(ApplyOutcome::Applied(ApplyResult::BucketConfig(config)))
    }

    #[allow(clippy::unused_self)]
    fn write_applied(
        &self,
        write: &redb::WriteTransaction,
        log_id: &LogIdOf,
    ) -> Result<(), CoordError> {
        let value =
            serde_json::to_vec(log_id).map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(RAFT_META)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(LAST_APPLIED_KEY, value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        {
            let mut system = write
                .open_table(SYSTEM)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            system
                .insert(APPLIED_KEY, log_id.index)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        Ok(())
    }

    /// Return one metadata row.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn get_metadata(&self, bucket: &str, key: &str) -> Result<Option<Metadata>, CoordError> {
        let encoded = encode_metadata_key(bucket, key);
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(METADATA)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(encoded.as_slice())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let Some(value) = value else {
            return Ok(None);
        };
        let metadata = serde_json::from_slice(value.value())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(Some(metadata))
    }

    /// Return all metadata rows in one bucket, ordered by key.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn scan_bucket(&self, bucket: &str) -> Result<Vec<(String, Metadata)>, CoordError> {
        let start = bucket_prefix(bucket);
        let end = bucket_prefix_end(bucket);
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(METADATA)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let range = table
            .range::<&[u8]>(start.as_slice()..end.as_slice())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let mut output = Vec::new();
        for item in range {
            let (key, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
            let Some((_, object_key)) = decode_metadata_key(key.value()) else {
                continue;
            };
            let metadata = serde_json::from_slice(value.value())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            output.push((object_key, metadata));
        }
        Ok(output)
    }

    /// Return every metadata row.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn scan_all(&self) -> Result<Vec<(String, String, Metadata)>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(METADATA)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let range = table
            .range::<&[u8]>(..)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let mut output = Vec::new();
        for item in range {
            let (key, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
            let Some((bucket, object_key)) = decode_metadata_key(key.value()) else {
                continue;
            };
            let metadata = serde_json::from_slice(value.value())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            output.push((bucket, object_key, metadata));
        }
        Ok(output)
    }

    /// Return one stored bucket config.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn bucket_config(&self, bucket: &str) -> Result<Option<BucketConfigRecord>, CoordError> {
        let key = encode_component(bucket);
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(BUCKET_CONFIG)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(key.as_slice())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let Some(value) = value else {
            return Ok(None);
        };
        let record = serde_json::from_slice(value.value())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(Some(record))
    }

    /// Persist one multipart upload record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn put_upload(&self, record: &UploadRecord) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let value =
            serde_json::to_vec(record).map_err(|error| CoordError::Storage(error.to_string()))?;
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(UPLOADS)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(record.upload_id.as_bytes(), value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Return one multipart upload record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn upload(&self, upload_id: &str) -> Result<Option<UploadRecord>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(UPLOADS)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(upload_id.as_bytes())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let Some(value) = value else {
            return Ok(None);
        };
        let record = serde_json::from_slice(value.value())
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(Some(record))
    }

    /// Remove one multipart upload record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn remove_upload(&self, upload_id: &str) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(UPLOADS)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(upload_id.as_bytes())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Return every multipart upload record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn uploads(&self) -> Result<Vec<UploadRecord>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(UPLOADS)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let range = table
            .range::<&[u8]>(..)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let mut output = Vec::new();
        for item in range {
            let (_, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
            let record = serde_json::from_slice(value.value())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            output.push(record);
        }
        Ok(output)
    }

    /// Return the set of blobs that any metadata row references.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn referenced_blobs(&self) -> Result<HashSet<String>, CoordError> {
        let mut output = HashSet::new();
        for (_, _, metadata) in self.scan_all()? {
            if !metadata.blob.is_empty() {
                output.insert(metadata.blob);
            }
        }
        Ok(output)
    }

    /// Return every tombstone row.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn tombstones(&self) -> Result<Vec<(String, String, Metadata)>, CoordError> {
        let mut output = Vec::new();
        for (bucket, key, metadata) in self.scan_all()? {
            if metadata.state == ObjectState::Tombstone {
                output.push((bucket, key, metadata));
            }
        }
        Ok(output)
    }

    /// Return the tombstone count.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn tombstone_count(&self) -> Result<u64, CoordError> {
        Ok(u64::try_from(self.tombstones()?.len()).unwrap_or(u64::MAX))
    }

    /// Remove one metadata row.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn remove_metadata(&self, bucket: &str, key: &str) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(encoded.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Store one metadata row from a remote coordinator.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn materialize_metadata(
        &self,
        bucket: &str,
        key: &str,
        metadata: &Metadata,
    ) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        write_metadata(&write, &encoded, metadata)?;
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Remove one metadata row that the remote coordinator no longer holds.
    ///
    /// A local `Pending` row is kept. It belongs to an in-flight multipart
    /// upload on this node.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn remove_materialized_metadata(&self, bucket: &str, key: &str) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        if read_metadata_in_write(&write, &encoded)?
            .is_some_and(|metadata| metadata.state != ObjectState::Pending)
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(encoded.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Store one bucket config from a remote coordinator.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn materialize_bucket_config(
        &self,
        bucket: &str,
        config: &BucketConfigRecord,
    ) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let key = encode_component(bucket);
        let value =
            serde_json::to_vec(config).map_err(|error| CoordError::Storage(error.to_string()))?;
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(BUCKET_CONFIG)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(key.as_slice(), value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Add a pending row for an in-flight multipart upload.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn set_pending(
        &self,
        bucket: &str,
        key: &str,
        blob: &str,
        owner: &str,
    ) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        if read_metadata_in_write(&write, &encoded)?.is_some() {
            return Ok(());
        }
        let now = unix_now();
        let metadata = Metadata {
            state: ObjectState::Pending,
            version: 1,
            blob: blob.to_owned(),
            size: 0,
            checksum: String::new(),
            content_type: None,
            etag: String::new(),
            user_metadata: BTreeMap::new(),
            owner: owner.to_owned(),
            replicas: Vec::new(),
            created_at: now,
            committed_at: None,
            deleted_at: None,
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
        };
        write_metadata(&write, &encoded, &metadata)?;
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Remove a pending row for one key.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn clear_pending(&self, bucket: &str, key: &str) -> Result<(), CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        if read_metadata_in_write(&write, &encoded)?
            .is_some_and(|metadata| metadata.state == ObjectState::Pending)
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(encoded.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Export the applied state as JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn export_state(&self) -> Result<Vec<u8>, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let mut objects = Vec::new();
        {
            let table = read
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let range = table
                .range::<&[u8]>(..)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            for item in range {
                let (key, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
                let Some((bucket, object_key)) = decode_metadata_key(key.value()) else {
                    continue;
                };
                let metadata = serde_json::from_slice(value.value())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                objects.push(ObjectRecord {
                    bucket,
                    key: object_key,
                    metadata,
                });
            }
        }
        let mut buckets = Vec::new();
        {
            let table = read
                .open_table(BUCKET_CONFIG)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let range = table
                .range::<&[u8]>(..)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            for item in range {
                let (key, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
                let bucket = decode_component(key.value());
                let config = serde_json::from_slice(value.value())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                buckets.push(BucketRecord { bucket, config });
            }
        }
        let uploads = {
            let table = read
                .open_table(UPLOADS)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let range = table
                .range::<&[u8]>(..)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let mut output = Vec::new();
            for item in range {
                let (_, value) = item.map_err(|error| CoordError::Storage(error.to_string()))?;
                let record = serde_json::from_slice(value.value())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                output.push(record);
            }
            output
        };
        let last_applied = {
            let table = read
                .open_table(RAFT_META)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let value = table
                .get(LAST_APPLIED_KEY)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            value
                .map(|value| {
                    serde_json::from_slice(value.value())
                        .map_err(|error| CoordError::Storage(error.to_string()))
                })
                .transpose()?
        };
        let membership = {
            let table = read
                .open_table(RAFT_META)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let value = table
                .get(MEMBERSHIP_KEY)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            value
                .map(|value| {
                    serde_json::from_slice(value.value())
                        .map_err(|error| CoordError::Storage(error.to_string()))
                })
                .transpose()?
        };
        let export = StateExport {
            objects,
            buckets,
            uploads,
            last_applied,
            membership,
        };
        serde_json::to_vec(&export).map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Replace the applied state from snapshot JSON bytes.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the snapshot cannot be decoded.
    pub fn install_state(&self, bytes: &[u8]) -> Result<(), CoordError> {
        self.install_state_record(bytes, None)
    }

    /// Replace the applied state and store one snapshot record.
    ///
    /// The state tables and the snapshot record change in one transaction.
    ///
    /// # Errors
    ///
    /// Returns a storage error when the snapshot cannot be decoded.
    pub fn install_snapshot(&self, bytes: &[u8], record: &[u8]) -> Result<(), CoordError> {
        self.install_state_record(bytes, Some(record))
    }

    #[allow(clippy::too_many_lines)]
    fn install_state_record(&self, bytes: &[u8], record: Option<&[u8]>) -> Result<(), CoordError> {
        let export: StateExport = serde_json::from_slice(bytes)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            for object in &export.objects {
                let key = encode_metadata_key(&object.bucket, &object.key);
                let value = serde_json::to_vec(&object.metadata)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
        }
        {
            let mut table = write
                .open_table(BUCKET_CONFIG)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            for bucket in &export.buckets {
                let key = encode_component(&bucket.bucket);
                let value = serde_json::to_vec(&bucket.config)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                table
                    .insert(key.as_slice(), value.as_slice())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
        }
        {
            let mut table = write
                .open_table(UPLOADS)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .retain(|_, _| false)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            for upload in &export.uploads {
                let value = serde_json::to_vec(upload)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                table
                    .insert(upload.upload_id.as_bytes(), value.as_slice())
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
        }
        {
            let mut table = write
                .open_table(RAFT_META)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            match &export.last_applied {
                Some(log_id) => {
                    let value = serde_json::to_vec(log_id)
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                    table
                        .insert(LAST_APPLIED_KEY, value.as_slice())
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                }
                None => {
                    table
                        .remove(LAST_APPLIED_KEY)
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                }
            }
            match &export.membership {
                Some(membership) => {
                    let value = serde_json::to_vec(membership)
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                    table
                        .insert(MEMBERSHIP_KEY, value.as_slice())
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                }
                None => {
                    table
                        .remove(MEMBERSHIP_KEY)
                        .map_err(|error| CoordError::Storage(error.to_string()))?;
                }
            }
        }
        {
            let mut system = write
                .open_table(SYSTEM)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            let index = export
                .last_applied
                .as_ref()
                .map_or(0, |log_id| log_id.index);
            system
                .insert(APPLIED_KEY, index)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        if let Some(record) = record {
            let mut table = write
                .open_table(SNAPSHOT)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(SNAPSHOT_KEY, record)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let index = export
            .last_applied
            .as_ref()
            .map_or(0, |log_id| log_id.index);
        self.applied.store(index, Ordering::Release);
        Ok(())
    }

    /// Store one snapshot record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn put_snapshot(&self, bytes: &[u8]) -> Result<(), CoordError> {
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(SNAPSHOT)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(SNAPSHOT_KEY, bytes)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    /// Return the latest snapshot record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn snapshot(&self) -> Result<Option<Vec<u8>>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
            .open_table(SNAPSHOT)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let value = table
            .get(SNAPSHOT_KEY)
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        Ok(value.map(|value| value.value().to_vec()))
    }
}

fn read_metadata_in_write(
    write: &redb::WriteTransaction,
    key: &[u8],
) -> Result<Option<Metadata>, CoordError> {
    let table = write
        .open_table(METADATA)
        .map_err(|error| CoordError::Storage(error.to_string()))?;
    let value = table
        .get(key)
        .map_err(|error| CoordError::Storage(error.to_string()))?;
    let Some(value) = value else {
        return Ok(None);
    };
    let metadata = serde_json::from_slice(value.value())
        .map_err(|error| CoordError::Storage(error.to_string()))?;
    Ok(Some(metadata))
}

fn write_metadata(
    write: &redb::WriteTransaction,
    key: &[u8],
    metadata: &Metadata,
) -> Result<(), CoordError> {
    let value =
        serde_json::to_vec(metadata).map_err(|error| CoordError::Storage(error.to_string()))?;
    let mut table = write
        .open_table(METADATA)
        .map_err(|error| CoordError::Storage(error.to_string()))?;
    table
        .insert(key, value.as_slice())
        .map_err(|error| CoordError::Storage(error.to_string()))?;
    Ok(())
}

/// Encode one metadata key.
#[must_use]
pub fn encode_metadata_key(bucket: &str, key: &str) -> Vec<u8> {
    let mut output = encode_component(bucket);
    output.push(0);
    output.push(0);
    output.extend(encode_component(key));
    output
}

fn bucket_prefix(bucket: &str) -> Vec<u8> {
    let mut output = encode_component(bucket);
    output.push(0);
    output.push(0);
    output
}

fn bucket_prefix_end(bucket: &str) -> Vec<u8> {
    let mut output = encode_component(bucket);
    output.push(0);
    output.push(1);
    output
}

fn encode_component(value: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(value.len());
    for byte in value.as_bytes() {
        if *byte == 0 {
            output.push(0);
            output.push(0xFF);
        } else {
            output.push(*byte);
        }
    }
    output
}

fn decode_component(mut value: &[u8]) -> String {
    let mut output = Vec::with_capacity(value.len());
    while let Some((&byte, rest)) = value.split_first() {
        if byte == 0 && rest.first() == Some(&0xFF) {
            output.push(0);
            value = &rest[1..];
        } else {
            output.push(byte);
            value = rest;
        }
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn decode_metadata_key(value: &[u8]) -> Option<(String, String)> {
    let mut index = 0;
    while index + 1 < value.len() {
        if value[index] == 0 && value[index + 1] == 0 {
            let bucket = decode_component(&value[..index]);
            let key = decode_component(&value[index + 2..]);
            return Some((bucket, key));
        }
        if value[index] == 0 && value[index + 1] == 0xFF {
            index += 2;
        } else {
            index += 1;
        }
    }
    None
}

fn normalize_etag(value: &str) -> String {
    value.trim().trim_matches('"').to_owned()
}

fn etag_matches(stored: &str, expected: &str) -> bool {
    if expected.trim() == "*" {
        return true;
    }
    stored == normalize_etag(expected)
}

/// Return the current time in Unix seconds.
#[must_use]
pub fn unix_now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use crate::cluster::raft::CommittedLeaderIdOf;
    use openraft::LogId;
    use openraft::vote::RaftLeaderId;
    use tempfile::tempdir;

    use super::*;

    fn log_id(term: u64, index: u64) -> LogIdOf {
        LogId::new(CommittedLeaderIdOf::new(term, "a".to_owned()), index)
    }

    fn request(key: &str) -> CommitRequest {
        CommitRequest {
            bucket: "media".to_owned(),
            key: key.to_owned(),
            base_version: 0,
            blob: format!("blob-{key}"),
            size: 1,
            checksum: "checksum".to_owned(),
            content_type: None,
            etag: "etag".to_owned(),
            user_metadata: BTreeMap::new(),
            owner: "a".to_owned(),
            replicas: vec!["a".to_owned()],
            if_none_match: false,
            if_match: None,
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
            committed_at: 10,
        }
    }

    #[test]
    fn key_encoding_preserves_byte_order() {
        let keys = ["a", "a/b", "a\u{0}b", "a!b", "b"];
        let mut encoded: Vec<Vec<u8>> = keys
            .iter()
            .map(|key| encode_metadata_key("bucket", key))
            .collect();
        encoded.sort();
        let decoded: Vec<String> = encoded
            .iter()
            .filter_map(|value| decode_metadata_key(value))
            .map(|(_, key)| key)
            .collect();
        let mut expected: Vec<String> = keys.iter().map(|key| (*key).to_owned()).collect();
        expected.sort();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn bucket_scan_is_ordered_and_scoped() {
        let dir = tempdir().unwrap();
        let state = MetadataStore::open(&dir.path().join("meta.redb")).unwrap();
        let config = BucketConfigRecord {
            write_ack: WriteAck::Leader,
            read: ReadMode::Local,
            replicas: 1,
            min_replicas: 1,
        };
        let mut index = 1;
        state
            .apply_entry(
                &log_id(1, index),
                &Entry::BucketConfig {
                    term: 1,
                    bucket: "media".to_owned(),
                    request: config,
                },
            )
            .unwrap();
        for key in ["b", "a", "c"] {
            index += 1;
            state
                .apply_entry(
                    &log_id(1, index),
                    &Entry::Commit {
                        term: 1,
                        request: Box::new(request(key)),
                    },
                )
                .unwrap();
        }
        let keys: Vec<String> = state
            .scan_bucket("media")
            .unwrap()
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn version_is_the_log_index() {
        let dir = tempdir().unwrap();
        let state = MetadataStore::open(&dir.path().join("meta.redb")).unwrap();
        let first = state
            .apply_entry(
                &log_id(1, 4),
                &Entry::Commit {
                    term: 1,
                    request: Box::new(request("a")),
                },
            )
            .unwrap();
        let ApplyOutcome::Applied(ApplyResult::Commit(first_metadata)) = first else {
            panic!("expected a commit");
        };
        assert_eq!(first_metadata.version, 4);
        let second = state
            .apply_entry(
                &log_id(1, 9),
                &Entry::Commit {
                    term: 1,
                    request: Box::new(request("a")),
                },
            )
            .unwrap();
        let ApplyOutcome::Applied(ApplyResult::Commit(second_metadata)) = second else {
            panic!("expected a commit");
        };
        assert_eq!(second_metadata.version, 9);
    }

    #[test]
    fn replay_does_not_change_the_state() {
        let dir = tempdir().unwrap();
        let state = MetadataStore::open(&dir.path().join("meta.redb")).unwrap();
        let first = Entry::Commit {
            term: 1,
            request: Box::new(request("a")),
        };
        state.apply_entry(&log_id(1, 1), &first).unwrap();
        let second = Entry::Commit {
            term: 1,
            request: Box::new(request("a")),
        };
        state.apply_entry(&log_id(1, 2), &second).unwrap();
        let before = state.get_metadata("media", "a").unwrap().unwrap();
        let replay = state.apply_entry(&log_id(1, 1), &first).unwrap();
        assert!(matches!(replay, ApplyOutcome::Duplicate));
        let after = state.get_metadata("media", "a").unwrap().unwrap();
        assert_eq!(before.version, after.version);
        assert_eq!(before.blob, after.blob);
    }

    #[test]
    fn conditional_rejection_is_deterministic() {
        let dir = tempdir().unwrap();
        let state = MetadataStore::open(&dir.path().join("meta.redb")).unwrap();
        state
            .apply_entry(
                &log_id(1, 1),
                &Entry::Commit {
                    term: 1,
                    request: Box::new(request("a")),
                },
            )
            .unwrap();
        let mut conditional = request("a");
        conditional.if_none_match = true;
        let outcome = state
            .apply_entry(
                &log_id(1, 2),
                &Entry::Commit {
                    term: 1,
                    request: Box::new(conditional),
                },
            )
            .unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Rejected(RejectReason::PreconditionFailed)
        ));
        let metadata = state.get_metadata("media", "a").unwrap().unwrap();
        assert_eq!(metadata.version, 1);
    }

    #[test]
    fn snapshot_round_trip_replaces_the_state() {
        let source_dir = tempdir().unwrap();
        let source = MetadataStore::open(&source_dir.path().join("meta.redb")).unwrap();
        source
            .apply_entry(
                &log_id(1, 3),
                &Entry::Commit {
                    term: 1,
                    request: Box::new(request("a")),
                },
            )
            .unwrap();
        let bytes = source.export_state().unwrap();
        let target_dir = tempdir().unwrap();
        let target = MetadataStore::open(&target_dir.path().join("meta.redb")).unwrap();
        target.install_state(&bytes).unwrap();
        assert_eq!(target.applied_index(), 3);
        let metadata = target.get_metadata("media", "a").unwrap().unwrap();
        assert_eq!(metadata.blob, "blob-a");
        assert_eq!(metadata.version, 3);
    }
}
