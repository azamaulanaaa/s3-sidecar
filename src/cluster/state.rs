//! Metadata state machine for cluster mode.
//!
//! The state machine stores object metadata, bucket consistency settings, and
//! in-flight multipart uploads in one `redb` database. Blob bytes never enter
//! this database. A metadata key encodes `(bucket, key)` with an order that
//! matches the raw byte order. A prefix scan therefore returns keys in
//! lexicographic order. The encoding escapes `0x00` bytes, so control
//! characters and long keys are safe.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::coordinator::{CommitRequest, CoordError};
use crate::config::{BucketConfig, ReadMode, WriteAck};

const METADATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("metadata");
const BUCKET_CONFIG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bucket_config");
const UPLOADS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("uploads");
const SYSTEM: TableDefinition<&str, u64> = TableDefinition::new("system");
const APPLIED_KEY: &str = "applied";

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
    /// Monotonic version for this key.
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
    /// Creation time in Unix seconds.
    pub created_at: i64,
}

/// Result of one applied entry.
#[derive(Debug, Clone)]
pub enum ApplyResult {
    /// A commit became visible.
    Commit(Metadata),
    /// A pending multipart key was removed.
    Abort,
    /// A tombstone replaced the key.
    Delete(Metadata),
    /// A bucket config was stored.
    BucketConfig(BucketConfigRecord),
}

/// One applied entry and its index.
#[derive(Debug, Clone)]
pub struct Applied {
    /// Applied log index.
    pub index: u64,
    /// Coordinator term.
    pub term: u64,
    /// Apply result.
    pub result: ApplyResult,
}

/// A durable metadata state machine.
pub struct StateMachine {
    db: Database,
    applied: AtomicU64,
    write_lock: Mutex<()>,
}

impl StateMachine {
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
            db,
            applied: AtomicU64::new(applied),
            write_lock: Mutex::new(()),
        })
    }

    /// Return the last applied index.
    #[must_use]
    pub fn applied_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire)
    }

    /// Apply one commit entry.
    ///
    /// # Errors
    ///
    /// Returns a rejection or a storage error.
    pub fn apply_commit(&self, term: u64, request: &CommitRequest) -> Result<Applied, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let key = encode_metadata_key(&request.bucket, &request.key);
        let current = self.read_metadata_locked(&key)?;
        let previous_version = current.as_ref().map_or(0, |metadata| metadata.version);
        if request.base_version != previous_version {
            return Err(CoordError::Rejected(
                super::coordinator::RejectReason::Conflict,
            ));
        }
        let committed = current
            .as_ref()
            .is_some_and(|metadata| metadata.state == ObjectState::Committed);
        if request.if_none_match && committed {
            return Err(CoordError::Rejected(
                super::coordinator::RejectReason::PreconditionFailed,
            ));
        }
        if let Some(expected) = &request.if_match {
            let matches = current.as_ref().is_some_and(|metadata| {
                metadata.state == ObjectState::Committed && etag_matches(&metadata.etag, expected)
            });
            if !matches {
                return Err(CoordError::Rejected(
                    super::coordinator::RejectReason::PreconditionFailed,
                ));
            }
        }
        let now = unix_now();
        let metadata = Metadata {
            state: ObjectState::Committed,
            version: previous_version.saturating_add(1),
            blob: request.blob.clone(),
            size: request.size,
            checksum: request.checksum.clone(),
            content_type: request.content_type.clone(),
            etag: normalize_etag(&request.etag),
            user_metadata: request.user_metadata.clone(),
            owner: request.owner.clone(),
            replicas: request.replicas.clone(),
            created_at: now,
            committed_at: Some(now),
            deleted_at: None,
            cache_control: request.cache_control.clone(),
            content_encoding: request.content_encoding.clone(),
            content_disposition: request.content_disposition.clone(),
        };
        let index = self.write_metadata_and_index(&key, &metadata)?;
        Ok(Applied {
            index,
            term,
            result: ApplyResult::Commit(metadata),
        })
    }

    /// Apply one delete entry. A missing key becomes an empty tombstone.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_delete(
        &self,
        term: u64,
        bucket: &str,
        key: &str,
        owner: &str,
    ) -> Result<Applied, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let current = self.read_metadata_locked(&encoded)?;
        let previous_version = current.as_ref().map_or(0, |metadata| metadata.version);
        let now = unix_now();
        let metadata = Metadata {
            state: ObjectState::Tombstone,
            version: previous_version.saturating_add(1),
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
            created_at: current.as_ref().map_or(now, |metadata| metadata.created_at),
            committed_at: None,
            deleted_at: Some(now),
            cache_control: None,
            content_encoding: None,
            content_disposition: None,
        };
        let index = self.write_metadata_and_index(&encoded, &metadata)?;
        Ok(Applied {
            index,
            term,
            result: ApplyResult::Delete(metadata),
        })
    }

    /// Apply one abort entry. It removes a pending row only.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_abort(&self, term: u64, bucket: &str, key: &str) -> Result<Applied, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let encoded = encode_metadata_key(bucket, key);
        let current = self.read_metadata_locked(&encoded)?;
        if current
            .as_ref()
            .is_some_and(|metadata| metadata.state == ObjectState::Pending)
        {
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
            let index = self.next_index();
            {
                let mut system = write
                    .open_table(SYSTEM)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
                system
                    .insert(APPLIED_KEY, index)
                    .map_err(|error| CoordError::Storage(error.to_string()))?;
            }
            write
                .commit()
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            self.applied.store(index, Ordering::Release);
            return Ok(Applied {
                index,
                term,
                result: ApplyResult::Abort,
            });
        }
        Ok(Applied {
            index: self.applied_index(),
            term,
            result: ApplyResult::Abort,
        })
    }

    /// Store one bucket config record.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn apply_bucket_config(
        &self,
        term: u64,
        bucket: &str,
        config: BucketConfigRecord,
    ) -> Result<Applied, CoordError> {
        let _guard = self
            .write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let key = encode_component(bucket);
        let value =
            serde_json::to_vec(&config).map_err(|error| CoordError::Storage(error.to_string()))?;
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
        let index = self.next_index();
        {
            let mut system = write
                .open_table(SYSTEM)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            system
                .insert(APPLIED_KEY, index)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.applied.store(index, Ordering::Release);
        Ok(Applied {
            index,
            term,
            result: ApplyResult::BucketConfig(config),
        })
    }

    fn next_index(&self) -> u64 {
        self.applied.load(Ordering::Acquire).saturating_add(1)
    }

    fn write_metadata_and_index(&self, key: &[u8], metadata: &Metadata) -> Result<u64, CoordError> {
        let value =
            serde_json::to_vec(metadata).map_err(|error| CoordError::Storage(error.to_string()))?;
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(key, value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        let index = self.next_index();
        {
            let mut system = write
                .open_table(SYSTEM)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            system
                .insert(APPLIED_KEY, index)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        self.applied.store(index, Ordering::Release);
        Ok(index)
    }

    fn read_metadata_locked(&self, key: &[u8]) -> Result<Option<Metadata>, CoordError> {
        let read = self
            .db
            .begin_read()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        let table = read
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

    /// Return one metadata row.
    ///
    /// # Errors
    ///
    /// Returns a storage error.
    pub fn get_metadata(&self, bucket: &str, key: &str) -> Result<Option<Metadata>, CoordError> {
        let encoded = encode_metadata_key(bucket, key);
        self.read_metadata_locked(&encoded)
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

    /// Add a pending row for an in-flight multipart upload.
    ///
    /// A single PUT never calls this method. The row exists only when no other
    /// metadata row holds the key.
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
        if self.read_metadata_locked(&encoded)?.is_some() {
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
        self.write_metadata_only(&encoded, &metadata)
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
        if !self
            .read_metadata_locked(&encoded)?
            .is_some_and(|metadata| metadata.state == ObjectState::Pending)
        {
            return Ok(());
        }
        self.remove_metadata_locked(&encoded)
    }

    fn write_metadata_only(&self, key: &[u8], metadata: &Metadata) -> Result<(), CoordError> {
        let value =
            serde_json::to_vec(metadata).map_err(|error| CoordError::Storage(error.to_string()))?;
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .insert(key, value.as_slice())
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }

    fn remove_metadata_locked(&self, key: &[u8]) -> Result<(), CoordError> {
        let write = self
            .db
            .begin_write()
            .map_err(|error| CoordError::Storage(error.to_string()))?;
        {
            let mut table = write
                .open_table(METADATA)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
            table
                .remove(key)
                .map_err(|error| CoordError::Storage(error.to_string()))?;
        }
        write
            .commit()
            .map_err(|error| CoordError::Storage(error.to_string()))
    }
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

    use tempfile::tempdir;

    use super::*;

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
        let state = StateMachine::open(&dir.path().join("meta.redb")).unwrap();
        let config = BucketConfigRecord {
            write_ack: WriteAck::Leader,
            read: ReadMode::Local,
            replicas: 1,
            min_replicas: 1,
        };
        state.apply_bucket_config(1, "media", config).unwrap();
        for key in ["b", "a", "c"] {
            let request = CommitRequest {
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
            };
            state.apply_commit(1, &request).unwrap();
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
    fn version_increases_at_commit() {
        let dir = tempdir().unwrap();
        let state = StateMachine::open(&dir.path().join("meta.redb")).unwrap();
        let mut request = CommitRequest {
            bucket: "media".to_owned(),
            key: "a".to_owned(),
            base_version: 0,
            blob: "one".to_owned(),
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
        };
        let first = state.apply_commit(1, &request).unwrap();
        let ApplyResult::Commit(first_metadata) = first.result else {
            panic!("expected a commit");
        };
        assert_eq!(first_metadata.version, 1);
        request.base_version = 1;
        request.blob = "two".to_owned();
        let second = state.apply_commit(1, &request).unwrap();
        let ApplyResult::Commit(second_metadata) = second.result else {
            panic!("expected a commit");
        };
        assert_eq!(second_metadata.version, 2);
    }
}
