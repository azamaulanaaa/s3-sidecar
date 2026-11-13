//! `redb` storage for the raft log and the raft state machine adapter.

#![allow(clippy::unused_async_trait_impl)]

use std::fmt::Debug;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use futures_util::StreamExt;
use openraft::storage::{
    EntryResponder, IOFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot,
};
use openraft::{EntryPayload, RaftSnapshotBuilder, SnapshotMeta};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use super::{EntryOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf, TypeConfig, VoteOf};
use crate::cluster::state::MetadataStore;

const RAFT_LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
const RAFT_META: TableDefinition<&str, &[u8]> = TableDefinition::new("raft_meta");

const VOTE_KEY: &str = "vote";
const COMMITTED_KEY: &str = "committed";
const LAST_PURGED_KEY: &str = "last_purged";

/// Durable raft log storage.
#[derive(Clone)]
pub struct RaftLogStore {
    db: Arc<Database>,
}

impl RaftLogStore {
    /// Build a log store on one database.
    #[must_use]
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    fn read_meta<T>(&self, key: &str) -> Result<Option<T>, io::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        let read = self.db.begin_read().map_err(io_error)?;
        let table = read.open_table(RAFT_META).map_err(io_error)?;
        let value = table.get(key).map_err(io_error)?;
        let Some(value) = value else {
            return Ok(None);
        };
        serde_json::from_slice(value.value())
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn write_meta<T>(&self, key: &str, value: &T) -> Result<(), io::Error>
    where
        T: Serialize,
    {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let write = self.db.begin_write().map_err(io_error)?;
        {
            let mut table = write.open_table(RAFT_META).map_err(io_error)?;
            table.insert(key, bytes.as_slice()).map_err(io_error)?;
        }
        write.commit().map_err(io_error)
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl RaftLogReader<TypeConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf>, io::Error> {
        let (start, end) = bounds(&range);
        let read = self.db.begin_read().map_err(io_error)?;
        let table = read.open_table(RAFT_LOG).map_err(io_error)?;
        let entries = table.range(start..end).map_err(io_error)?;
        let mut output = Vec::new();
        for item in entries {
            let (_, value) = item.map_err(io_error)?;
            let entry = serde_json::from_slice(value.value())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            output.push(entry);
        }
        Ok(output)
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf>, io::Error> {
        self.read_meta(VOTE_KEY)
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl RaftLogStorage<TypeConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged = self.read_meta::<LogIdOf>(LAST_PURGED_KEY)?;
        let read = self.db.begin_read().map_err(io_error)?;
        let table = read.open_table(RAFT_LOG).map_err(io_error)?;
        let last = table.last().map_err(io_error)?;
        let last_log_id = match last {
            Some((_, value)) => {
                let entry: EntryOf = serde_json::from_slice(value.value())
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                Some(entry.log_id)
            }
            None => last_purged.clone(),
        };
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &VoteOf) -> Result<(), io::Error> {
        self.write_meta(VOTE_KEY, vote)
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf>) -> Result<(), io::Error> {
        self.write_meta(COMMITTED_KEY, &committed)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf>, io::Error> {
        self.read_meta(COMMITTED_KEY)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf> + Send,
        I::IntoIter: Send,
    {
        let result = self.append_inner(entries);
        match result {
            Ok(()) => {
                callback.io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.io_completed(Err(io::Error::new(error.kind(), error.to_string())));
                Err(error)
            }
        }
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf>) -> Result<(), io::Error> {
        let write = self.db.begin_write().map_err(io_error)?;
        {
            let mut table = write.open_table(RAFT_LOG).map_err(io_error)?;
            let mut keys = Vec::new();
            if let Some(log_id) = last_log_id {
                let range = table
                    .range(log_id.index.saturating_add(1)..)
                    .map_err(io_error)?;
                for item in range {
                    let (key, _) = item.map_err(io_error)?;
                    keys.push(key.value());
                }
            } else {
                let range = table.iter().map_err(io_error)?;
                for item in range {
                    let (key, _) = item.map_err(io_error)?;
                    keys.push(key.value());
                }
            }
            for key in keys {
                table.remove(key).map_err(io_error)?;
            }
        }
        write.commit().map_err(io_error)
    }

    async fn purge(&mut self, log_id: LogIdOf) -> Result<(), io::Error> {
        let current = self.read_meta::<LogIdOf>(LAST_PURGED_KEY)?;
        if current
            .as_ref()
            .is_some_and(|value| value.index >= log_id.index)
        {
            return Ok(());
        }
        let write = self.db.begin_write().map_err(io_error)?;
        {
            let mut table = write.open_table(RAFT_LOG).map_err(io_error)?;
            let keys: Vec<u64> = {
                let range = table.range(..=log_id.index).map_err(io_error)?;
                let mut keys = Vec::new();
                for item in range {
                    let (key, _) = item.map_err(io_error)?;
                    keys.push(key.value());
                }
                keys
            };
            for key in keys {
                table.remove(key).map_err(io_error)?;
            }
        }
        {
            let bytes = serde_json::to_vec(&log_id)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let mut table = write.open_table(RAFT_META).map_err(io_error)?;
            table
                .insert(LAST_PURGED_KEY, bytes.as_slice())
                .map_err(io_error)?;
        }
        write.commit().map_err(io_error)
    }
}

impl RaftLogStore {
    fn append_inner<I>(&self, entries: I) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf>,
    {
        let write = self.db.begin_write().map_err(io_error)?;
        {
            let mut table = write.open_table(RAFT_LOG).map_err(io_error)?;
            for entry in entries {
                let index = entry.log_id.index;
                let value = serde_json::to_vec(&entry)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                table.insert(index, value.as_slice()).map_err(io_error)?;
            }
        }
        write.commit().map_err(io_error)
    }
}

/// Snapshot data for the sidecar state machine.
pub type SnapshotData = Vec<u8>;

/// The raft state machine adapter.
#[derive(Clone)]
pub struct RaftStore {
    store: Arc<MetadataStore>,
}

impl RaftStore {
    /// Build one state machine adapter.
    #[must_use]
    pub fn new(store: Arc<MetadataStore>) -> Self {
        Self { store }
    }
}

/// Snapshot builder for the sidecar state machine.
pub struct SnapshotBuilder {
    store: Arc<MetadataStore>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotRecord {
    meta: SnapshotMetaOf,
    data: Vec<u8>,
}

#[allow(clippy::unused_async_trait_impl)]
impl RaftStateMachine<TypeConfig> for RaftStore {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = SnapshotBuilder;

    async fn applied_state(&mut self) -> Result<(Option<LogIdOf>, StoredMembershipOf), io::Error> {
        let log_id = self.store.last_applied_log().map_err(coord_error)?;
        let membership = self.store.stored_membership().map_err(coord_error)?;
        Ok((log_id, membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures_util::Stream<Item = Result<EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;
            let log_id = entry.log_id.clone();
            let outcome = match &entry.payload {
                EntryPayload::Blank => self.store.apply_blank(&log_id).map_err(coord_error)?,
                EntryPayload::Normal(data) => {
                    self.store.apply_entry(&log_id, data).map_err(coord_error)?
                }
                EntryPayload::Membership(membership) => {
                    let stored = StoredMembershipOf::new(Some(log_id.clone()), membership.clone());
                    self.store
                        .apply_membership(&log_id, &stored)
                        .map_err(coord_error)?
                }
            };
            if let Some(responder) = responder {
                responder.send(outcome);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SnapshotBuilder {
            store: Arc::clone(&self.store),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf,
        snapshot: Self::SnapshotData,
    ) -> Result<(), io::Error> {
        let record = SnapshotRecord {
            meta: meta.clone(),
            data: snapshot.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.store
            .install_snapshot(&snapshot, &bytes)
            .map_err(coord_error)
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<SnapshotOf>, io::Error> {
        let Some(bytes) = self.store.snapshot().map_err(coord_error)? else {
            return Ok(None);
        };
        let record: SnapshotRecord = serde_json::from_slice(&bytes)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        Ok(Some(Snapshot {
            meta: record.meta,
            snapshot: record.data,
        }))
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl RaftSnapshotBuilder<TypeConfig> for SnapshotBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf, io::Error> {
        let data = self.store.export_state().map_err(coord_error)?;
        let log_id = self.store.last_applied_log().map_err(coord_error)?;
        let membership = self.store.stored_membership().map_err(coord_error)?;
        let meta = SnapshotMeta {
            last_log_id: log_id,
            last_membership: membership,
        };
        let record = SnapshotRecord {
            meta: meta.clone(),
            data: data.clone(),
        };
        let bytes = serde_json::to_vec(&record)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.store.put_snapshot(&bytes).map_err(coord_error)?;
        Ok(Snapshot {
            meta,
            snapshot: data,
        })
    }
}

fn bounds<RB: RangeBounds<u64>>(range: &RB) -> (u64, u64) {
    let start = match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.saturating_add(1),
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(value) => value.saturating_add(1),
        Bound::Excluded(value) => *value,
        Bound::Unbounded => u64::MAX,
    };
    (start, end)
}

fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

#[allow(clippy::needless_pass_by_value)]
fn coord_error(error: crate::cluster::coordinator::CoordError) -> io::Error {
    io::Error::other(error.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use openraft::vote::RaftLeaderId;
    use openraft::{LogId, RaftSnapshotBuilder};
    use tempfile::tempdir;

    use super::*;
    use crate::cluster::raft::CommittedLeaderIdOf;

    fn log_id(term: u64, index: u64) -> LogIdOf {
        LogId::new(CommittedLeaderIdOf::new(term, "a".to_owned()), index)
    }

    #[tokio::test]
    async fn snapshot_builder_round_trips_through_install() {
        let source_dir = tempdir().unwrap();
        let source = Arc::new(MetadataStore::open(&source_dir.path().join("meta.redb")).unwrap());
        source.apply_blank(&log_id(1, 7)).unwrap();
        let mut machine = RaftStore::new(Arc::clone(&source));
        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        assert_eq!(
            snapshot.meta.last_log_id.as_ref().map(|log| log.index),
            Some(7)
        );

        let target_dir = tempdir().unwrap();
        let target = Arc::new(MetadataStore::open(&target_dir.path().join("meta.redb")).unwrap());
        let mut target_machine = RaftStore::new(Arc::clone(&target));
        target_machine
            .install_snapshot(&snapshot.meta, snapshot.snapshot.clone())
            .await
            .unwrap();
        let current = target_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(current.meta, snapshot.meta);
        assert_eq!(current.snapshot, snapshot.snapshot);
        let (applied, _) = target_machine.applied_state().await.unwrap();
        assert_eq!(applied.as_ref().map(|log| log.index), Some(7));
    }
}
