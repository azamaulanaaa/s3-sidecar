//! Cache key derivation.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sha2::{Digest, Sha256};

/// Characters that are not safe in one object path segment.
const SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}');

/// Return a lowercase SHA-256 hex digest.
#[must_use]
pub fn sha256_hex(value: &str) -> String {
    hex::encode(Sha256::digest(value.as_bytes()))
}

/// Return the bucket hash that scopes cache keys.
#[must_use]
pub fn bucket_hash(bucket: &str) -> String {
    sha256_hex(bucket)
}

/// Return the cache blob key for one origin object.
#[must_use]
pub fn blob_key(prefix: &str, bucket: &str, key: &str) -> String {
    format!(
        "{}/{}/{}",
        prefix.trim_matches('/'),
        bucket_hash(bucket),
        sha256_hex(key)
    )
}

/// Return the metadata key that belongs to a blob key.
#[must_use]
pub fn meta_key(blob: &str) -> String {
    format!("{blob}.meta")
}

/// Return the journal prefix.
#[must_use]
pub fn journal_prefix(prefix: &str) -> String {
    format!("{}/journal/", prefix.trim_matches('/'))
}

/// Return the journal key for one record.
#[must_use]
pub fn journal_key(prefix: &str, id: &str) -> String {
    format!("{}{}.json", journal_prefix(prefix), id)
}

/// Return the cache blob prefix.
#[must_use]
pub fn blob_prefix(prefix: &str) -> String {
    format!("{}/", prefix.trim_matches('/'))
}

/// Percent-encode one object key for a path. Slashes stay as separators.
#[must_use]
pub fn encode_key_path(key: &str) -> String {
    key.split('/')
        .map(|segment| utf8_percent_encode(segment, SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Return the path and query for one object.
#[must_use]
pub fn object_path(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("/{bucket}")
    } else {
        format!("/{bucket}/{}", encode_key_path(key))
    }
}

/// Return a fresh journal identifier from the clock and a process counter.
#[must_use]
pub fn next_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{nanos:032x}{counter:016x}")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn cache_key_uses_bucket_hash_and_key_hash() {
        let key = blob_key("cache", "media", "dir/file.txt");
        assert_eq!(
            key,
            format!(
                "cache/{}/{}",
                bucket_hash("media"),
                sha256_hex("dir/file.txt")
            )
        );
        assert!(!key.contains("file.txt"));
    }

    #[test]
    fn different_buckets_have_different_hashes() {
        assert_ne!(bucket_hash("media"), bucket_hash("assets"));
    }

    #[test]
    fn journal_keys_use_the_prefix() {
        assert_eq!(journal_key("cache", "id"), "cache/journal/id.json");
        assert_eq!(journal_prefix("cache/"), "cache/journal/");
    }

    #[test]
    fn key_helpers_trim_prefix_slashes() {
        let key = blob_key("/cache/", "media", "item");
        assert_eq!(
            key,
            format!("cache/{}/{}", bucket_hash("media"), sha256_hex("item"))
        );
        assert_eq!(journal_prefix("/cache/"), "cache/journal/");
        assert_eq!(blob_prefix("/cache/"), "cache/");
    }

    #[test]
    fn object_path_keeps_slashes_and_encodes_spaces() {
        assert_eq!(object_path("media", "a b/c.txt"), "/media/a%20b/c.txt");
        assert_eq!(object_path("media", ""), "/media");
    }

    #[test]
    fn ids_are_unique() {
        assert_ne!(next_id(), next_id());
    }
}
