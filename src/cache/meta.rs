//! Cache entry metadata and journal records.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a cache entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryState {
    /// The entry is in the cache but origin does not have it yet.
    Pending,
    /// Origin has the entry.
    Synced,
    /// Origin rejected the entry permanently.
    Failed,
}

impl EntryState {
    /// Return the stable name used in telemetry attributes.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Synced => "synced",
            Self::Failed => "failed",
        }
    }
}

/// Metadata stored next to a cache blob.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheMetadata {
    /// Metadata format version.
    pub version: u32,
    /// Origin bucket.
    pub origin_bucket: String,
    /// Origin object key.
    pub origin_key: String,
    /// Origin entity tag.
    pub etag: String,
    /// Origin last-modified value.
    pub last_modified: Option<String>,
    /// Time of the last origin validation, in Unix seconds.
    pub validated_at: i64,
    /// Object size in bytes.
    pub size: u64,
    /// SHA-256 checksum of the body.
    pub checksum: String,
    /// Content type.
    pub content_type: Option<String>,
    /// Content encoding.
    pub content_encoding: Option<String>,
    /// Entry state.
    pub state: EntryState,
    /// Time the entry was created, in Unix seconds.
    pub created_at: i64,
}

impl CacheMetadata {
    /// Build metadata for an entry that origin already has.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn synced(
        origin_bucket: &str,
        origin_key: &str,
        etag: String,
        last_modified: Option<String>,
        size: u64,
        content_type: Option<String>,
        content_encoding: Option<String>,
        now: i64,
    ) -> Self {
        Self {
            version: 1,
            origin_bucket: origin_bucket.to_owned(),
            origin_key: origin_key.to_owned(),
            etag,
            last_modified,
            validated_at: now,
            size,
            checksum: String::new(),
            content_type,
            content_encoding,
            state: EntryState::Synced,
            created_at: now,
        }
    }

    /// Return true when the entry is fresh for a time-to-live.
    #[must_use]
    pub fn is_fresh(&self, now: i64, ttl_seconds: u64) -> bool {
        if self.state == EntryState::Pending {
            return true;
        }
        let age = now.saturating_sub(self.validated_at);
        u64::try_from(age).unwrap_or(u64::MAX) < ttl_seconds
    }

    /// Mark the entry as validated at the given time.
    pub fn refresh(&mut self, now: i64) {
        self.validated_at = now;
    }

    /// Mark the entry as synced to origin.
    pub fn mark_synced(&mut self, etag: String, now: i64) {
        self.etag = etag;
        self.validated_at = now;
        self.state = EntryState::Synced;
    }

    /// Mark the entry as quarantined.
    pub fn mark_failed(&mut self) {
        self.state = EntryState::Failed;
    }
}

/// Journal record kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JournalKind {
    /// A pending object write.
    Put,
    /// A pending object delete.
    Delete,
}

/// One journal record. The blob is immutable; this record names it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    /// Journal format version.
    pub version: u32,
    /// Journal identifier. It is also the object name.
    pub id: String,
    /// Record kind.
    pub kind: JournalKind,
    /// Origin bucket.
    pub bucket: String,
    /// Origin key.
    pub key: String,
    /// Cache blob key.
    pub cache_key: String,
    /// Entity tag for the cached body.
    pub etag: String,
    /// Object size.
    pub size: u64,
    /// SHA-256 checksum.
    pub checksum: String,
    /// Content type.
    pub content_type: Option<String>,
    /// Content encoding.
    pub content_encoding: Option<String>,
    /// Creation time in Unix seconds.
    pub created_at: i64,
    /// Number of sync attempts that failed.
    pub attempts: u32,
    /// Entry state.
    pub state: EntryState,
}

impl JournalEntry {
    /// Build a put record from metadata.
    #[must_use]
    pub fn put(id: String, cache_key: String, meta: &CacheMetadata) -> Self {
        Self {
            version: 1,
            id,
            kind: JournalKind::Put,
            bucket: meta.origin_bucket.clone(),
            key: meta.origin_key.clone(),
            cache_key,
            etag: meta.etag.clone(),
            size: meta.size,
            checksum: meta.checksum.clone(),
            content_type: meta.content_type.clone(),
            content_encoding: meta.content_encoding.clone(),
            created_at: meta.created_at,
            attempts: 0,
            state: EntryState::Pending,
        }
    }

    /// Build a delete record.
    #[must_use]
    pub fn delete(id: String, bucket: &str, key: &str, cache_key: String, now: i64) -> Self {
        Self {
            version: 1,
            id,
            kind: JournalKind::Delete,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            cache_key,
            etag: String::new(),
            size: 0,
            checksum: String::new(),
            content_type: None,
            content_encoding: None,
            created_at: now,
            attempts: 0,
            state: EntryState::Pending,
        }
    }
}

/// Return the current time in Unix seconds.
#[must_use]
pub fn unix_now() -> i64 {
    time::OffsetDateTime::now_utc().unix_timestamp()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn meta() -> CacheMetadata {
        CacheMetadata::synced(
            "media",
            "a.txt",
            "\"etag\"".to_owned(),
            None,
            5,
            Some("text/plain".to_owned()),
            None,
            100,
        )
    }

    #[test]
    fn ttl_uses_validated_at() {
        let mut entry = meta();
        assert!(entry.is_fresh(100, 10));
        assert!(entry.is_fresh(109, 10));
        assert!(!entry.is_fresh(110, 10));
        entry.refresh(200);
        assert!(entry.is_fresh(205, 10));
    }

    #[test]
    fn pending_entries_are_always_fresh() {
        let mut entry = meta();
        entry.state = EntryState::Pending;
        assert!(entry.is_fresh(10_000, 1));
    }

    #[test]
    fn state_transitions_are_explicit() {
        let mut entry = meta();
        entry.mark_failed();
        assert_eq!(entry.state, EntryState::Failed);
        entry.mark_synced("\"new\"".to_owned(), 300);
        assert_eq!(entry.state, EntryState::Synced);
        assert_eq!(entry.validated_at, 300);
        assert_eq!(entry.etag, "\"new\"");
    }

    #[test]
    fn journal_round_trips_through_json() {
        let entry = JournalEntry::put("id".to_owned(), "cache/key".to_owned(), &meta());
        let text = serde_json::to_string(&entry).unwrap();
        let decoded: JournalEntry = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded.kind, JournalKind::Put);
        assert_eq!(decoded.bucket, "media");
        assert_eq!(decoded.key, "a.txt");
        assert_eq!(decoded.state, EntryState::Pending);
    }

    #[test]
    fn delete_journal_has_no_body() {
        let entry =
            JournalEntry::delete("id".to_owned(), "media", "a.txt", "cache/key".to_owned(), 5);
        assert_eq!(entry.kind, JournalKind::Delete);
        assert_eq!(entry.size, 0);
        assert_eq!(entry.attempts, 0);
    }
}
