//! Advisory LRU index for the cache.

use std::path::Path;
use std::sync::{Mutex, PoisonError};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::CacheError;
use super::meta::EntryState;

const TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("cache_lru");

/// One index record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    /// Origin bucket for eviction.
    pub origin_bucket: String,
    /// Origin key for eviction.
    pub origin_key: String,
    /// Body size in bytes.
    pub size: u64,
    /// Last access time in Unix seconds.
    pub last_access: i64,
    /// Last origin validation time in Unix seconds.
    pub validated_at: i64,
    /// Entry state. Only synced entries are evictable.
    pub state: EntryState,
}

#[derive(Debug)]
struct IndexState {
    total: u64,
    loaded: bool,
}

/// A `redb` backed LRU index. The index is advisory.
#[derive(Debug)]
pub struct CacheIndex {
    db: Database,
    state: Mutex<IndexState>,
}

impl CacheIndex {
    /// Open or create the index file.
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or read.
    pub fn open(path: &Path) -> Result<Self, CacheError> {
        let db = Database::create(path).map_err(CacheError::from)?;
        // Creating the table on open makes later read transactions valid even
        // when the index is new and empty.
        {
            let write = db.begin_write().map_err(CacheError::from)?;
            write.open_table(TABLE).map_err(CacheError::from)?;
            write.commit().map_err(CacheError::from)?;
        }
        let index = Self {
            db,
            state: Mutex::new(IndexState {
                total: 0,
                loaded: false,
            }),
        };
        let total = index.read_total()?;
        {
            let mut state = index.lock();
            state.total = total;
        }
        Ok(index)
    }

    /// Commit an empty write so the index is durable.
    ///
    /// # Errors
    ///
    /// Returns an error when the database commit fails.
    pub fn flush(&self) -> Result<(), CacheError> {
        let write = self.db.begin_write().map_err(CacheError::from)?;
        {
            write.open_table(TABLE).map_err(CacheError::from)?;
        }
        write.commit().map_err(CacheError::from)
    }

    /// Return the tracked byte count.
    #[must_use]
    pub fn tracked_bytes(&self) -> u64 {
        self.lock().total
    }

    /// Return true when the index was loaded from the database.
    #[must_use]
    pub fn is_loaded(&self) -> bool {
        self.lock().loaded
    }

    /// Mark the index as loaded.
    pub fn mark_loaded(&self) {
        self.lock().loaded = true;
    }

    /// Insert or replace one record.
    ///
    /// # Errors
    ///
    /// Returns an error when the database write fails.
    pub fn insert(&self, key: &str, entry: &IndexEntry) -> Result<(), CacheError> {
        let mut state = self.lock();
        let previous = self.get_locked(key)?;
        let write = self.db.begin_write().map_err(CacheError::from)?;
        {
            let mut table = write.open_table(TABLE).map_err(CacheError::from)?;
            let bytes = serde_json::to_vec(entry).map_err(CacheError::from)?;
            table
                .insert(key, bytes.as_slice())
                .map_err(CacheError::from)?;
        }
        write.commit().map_err(CacheError::from)?;
        if let Some(previous) = previous {
            state.total = state.total.saturating_sub(previous.size);
        }
        state.total = state.total.saturating_add(entry.size);
        Ok(())
    }

    /// Remove one record and return true when it was present.
    ///
    /// # Errors
    ///
    /// Returns an error when the database write fails.
    pub fn remove(&self, key: &str) -> Result<bool, CacheError> {
        let mut state = self.lock();
        let previous = self.get_locked(key)?;
        if previous.is_none() {
            return Ok(false);
        }
        let write = self.db.begin_write().map_err(CacheError::from)?;
        {
            let mut table = write.open_table(TABLE).map_err(CacheError::from)?;
            table.remove(key).map_err(CacheError::from)?;
        }
        write.commit().map_err(CacheError::from)?;
        if let Some(previous) = previous {
            state.total = state.total.saturating_sub(previous.size);
        }
        Ok(true)
    }

    /// Update the last access time of one record.
    ///
    /// # Errors
    ///
    /// Returns an error when the database write fails.
    pub fn touch(&self, key: &str, now: i64) -> Result<(), CacheError> {
        let Some(mut entry) = self.get(key)? else {
            return Ok(());
        };
        entry.last_access = now;
        self.insert(key, &entry)
    }

    /// Return one record.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read fails.
    pub fn get(&self, key: &str) -> Result<Option<IndexEntry>, CacheError> {
        let _state = self.lock();
        self.get_locked(key)
    }

    /// Return every record, newest last.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read fails.
    pub fn entries(&self) -> Result<Vec<(String, IndexEntry)>, CacheError> {
        let _state = self.lock();
        self.entries_locked()
    }

    /// Remove the least recently used synced entries until the tracked size is
    /// at or below the target. Returns the removed records.
    ///
    /// # Errors
    ///
    /// Returns an error when the database read or write fails.
    pub fn evict_to(&self, target: u64) -> Result<Vec<(String, IndexEntry)>, CacheError> {
        let mut state = self.lock();
        let mut entries = self.entries_locked()?;
        entries.sort_by_key(|(_, entry)| (entry.last_access, entry.validated_at));
        let mut total = state.total;
        let mut evicted = Vec::new();
        let write = self.db.begin_write().map_err(CacheError::from)?;
        {
            let mut table = write.open_table(TABLE).map_err(CacheError::from)?;
            for (key, entry) in entries {
                if total <= target {
                    break;
                }
                if entry.state != EntryState::Synced {
                    continue;
                }
                table.remove(key.as_str()).map_err(CacheError::from)?;
                total = total.saturating_sub(entry.size);
                evicted.push((key, entry));
            }
        }
        write.commit().map_err(CacheError::from)?;
        state.total = total;
        Ok(evicted)
    }

    /// Replace the index with a rebuild from the cache listing.
    ///
    /// # Errors
    ///
    /// Returns an error when the database write fails.
    pub fn rebuild(&self, entries: Vec<(String, IndexEntry)>) -> Result<(), CacheError> {
        let mut state = self.lock();
        let write = self.db.begin_write().map_err(CacheError::from)?;
        let mut total = 0_u64;
        {
            let mut table = write.open_table(TABLE).map_err(CacheError::from)?;
            table.retain(|_, _| false).map_err(CacheError::from)?;
            for (key, entry) in entries {
                let bytes = serde_json::to_vec(&entry).map_err(CacheError::from)?;
                table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(CacheError::from)?;
                total = total.saturating_add(entry.size);
            }
        }
        write.commit().map_err(CacheError::from)?;
        state.total = total;
        state.loaded = true;
        Ok(())
    }

    fn read_total(&self) -> Result<u64, CacheError> {
        let entries = self.entries()?;
        Ok(entries.iter().map(|(_, entry)| entry.size).sum())
    }

    fn entries_locked(&self) -> Result<Vec<(String, IndexEntry)>, CacheError> {
        let read = self.db.begin_read().map_err(CacheError::from)?;
        let table = read.open_table(TABLE).map_err(CacheError::from)?;
        let iter = table.iter().map_err(CacheError::from)?;
        let mut output = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(CacheError::from)?;
            let entry: IndexEntry =
                serde_json::from_slice(value.value()).map_err(CacheError::from)?;
            output.push((key.value().to_owned(), entry));
        }
        Ok(output)
    }

    fn get_locked(&self, key: &str) -> Result<Option<IndexEntry>, CacheError> {
        let read = self.db.begin_read().map_err(CacheError::from)?;
        let table = read.open_table(TABLE).map_err(CacheError::from)?;
        let value = table.get(key).map_err(CacheError::from)?;
        let Some(value) = value else {
            return Ok(None);
        };
        let entry = serde_json::from_slice(value.value()).map_err(CacheError::from)?;
        Ok(Some(entry))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, IndexState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    #![expect(clippy::unwrap_used, reason = "test code uses unwrap for fixtures")]

    use tempfile::tempdir;

    use super::*;

    fn entry(size: u64, access: i64) -> IndexEntry {
        IndexEntry {
            origin_bucket: "media".to_owned(),
            origin_key: format!("key-{access}"),
            size,
            last_access: access,
            validated_at: access,
            state: EntryState::Synced,
        }
    }

    #[test]
    fn tracks_total_size_and_removes_entries() {
        let dir = tempdir().unwrap();
        let index = CacheIndex::open(&dir.path().join("index.redb")).unwrap();
        index.insert("a", &entry(10, 1)).unwrap();
        index.insert("b", &entry(20, 2)).unwrap();
        assert_eq!(index.tracked_bytes(), 30);
        index.insert("a", &entry(15, 3)).unwrap();
        assert_eq!(index.tracked_bytes(), 35);
        assert!(index.remove("a").unwrap());
        assert_eq!(index.tracked_bytes(), 20);
        assert!(!index.remove("a").unwrap());
    }

    #[test]
    fn evicts_in_least_recently_used_order() {
        let dir = tempdir().unwrap();
        let index = CacheIndex::open(&dir.path().join("index.redb")).unwrap();
        index.insert("a", &entry(50, 1)).unwrap();
        index.insert("b", &entry(50, 2)).unwrap();
        index.insert("c", &entry(50, 3)).unwrap();
        let evicted = index.evict_to(80).unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(index.tracked_bytes(), 50);
        let evicted = index.evict_to(80).unwrap();
        assert!(evicted.is_empty());
        assert_eq!(index.tracked_bytes(), 50);
    }

    #[test]
    fn pinned_entries_are_not_evicted() {
        let dir = tempdir().unwrap();
        let index = CacheIndex::open(&dir.path().join("index.redb")).unwrap();
        index
            .insert(
                "pending",
                &IndexEntry {
                    origin_bucket: "media".to_owned(),
                    origin_key: "pending".to_owned(),
                    size: 100,
                    last_access: 1,
                    validated_at: 1,
                    state: EntryState::Pending,
                },
            )
            .unwrap();
        index.insert("synced", &entry(100, 2)).unwrap();
        let evicted = index.evict_to(0).unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["synced"]
        );
        assert_eq!(index.tracked_bytes(), 100);
    }

    #[test]
    fn rebuild_replaces_the_index() {
        let dir = tempdir().unwrap();
        let index = CacheIndex::open(&dir.path().join("index.redb")).unwrap();
        index.insert("old", &entry(10, 1)).unwrap();
        index
            .rebuild(vec![("new".to_owned(), entry(7, 2))])
            .unwrap();
        assert_eq!(index.tracked_bytes(), 7);
        assert!(index.get("old").unwrap().is_none());
        assert!(index.is_loaded());
    }

    #[test]
    fn touch_updates_the_recency_order() {
        let dir = tempdir().unwrap();
        let index = CacheIndex::open(&dir.path().join("index.redb")).unwrap();
        index.insert("a", &entry(50, 1)).unwrap();
        index.insert("b", &entry(50, 2)).unwrap();
        index.touch("a", 3).unwrap();
        let evicted = index.evict_to(80).unwrap();
        assert_eq!(
            evicted
                .iter()
                .map(|(key, _)| key.as_str())
                .collect::<Vec<_>>(),
            vec!["b"]
        );
    }
}
