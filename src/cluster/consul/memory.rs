//! In-memory Consul API for tests.
//!
//! The model keeps sessions, KV values, modify indexes, compare-and-swap
//! checks, and blocking queries.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::sync::Notify;

use super::api::{ConsulApi, ConsulError, KvEntry, PrefixResult};

#[derive(Debug)]
struct Session {
    keys: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct ValueEntry {
    value: Vec<u8>,
    modify_index: u64,
    session: Option<String>,
}

#[derive(Debug, Default)]
struct Inner {
    sessions: BTreeMap<String, Session>,
    values: BTreeMap<String, ValueEntry>,
    index: u64,
    counter: u64,
}

/// One in-memory Consul.
#[derive(Debug, Clone, Default)]
pub struct MemoryConsul {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
}

impl MemoryConsul {
    /// Build one empty in-memory Consul.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the current global index.
    #[must_use]
    pub fn index(&self) -> u64 {
        lock(&self.inner).index
    }

    /// Return the number of active sessions.
    #[must_use]
    pub fn session_count(&self) -> usize {
        lock(&self.inner).sessions.len()
    }

    /// Return one stored value.
    #[must_use]
    pub fn value(&self, key: &str) -> Option<Vec<u8>> {
        lock(&self.inner)
            .values
            .get(key)
            .map(|entry| entry.value.clone())
    }

    /// Return the number of stored values.
    #[must_use]
    pub fn key_count(&self) -> usize {
        lock(&self.inner).values.len()
    }

    fn next_index(inner: &mut Inner) -> u64 {
        inner.index = inner.index.saturating_add(1);
        inner.index
    }

    fn snapshot(inner: &Inner, prefix: &str) -> PrefixResult {
        let entries = inner
            .values
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, entry)| KvEntry {
                key: key.clone(),
                value: entry.value.clone(),
                modify_index: entry.modify_index,
                session: entry.session.clone(),
            })
            .collect();
        PrefixResult {
            index: inner.index,
            entries,
        }
    }
}

impl ConsulApi for MemoryConsul {
    fn create_session(&self, _ttl: Duration) -> BoxFuture<'_, Result<String, ConsulError>> {
        Box::pin(async move {
            let mut inner = lock(&self.inner);
            inner.counter = inner.counter.saturating_add(1);
            let id = format!("session-{}", inner.counter);
            inner.sessions.insert(
                id.clone(),
                Session {
                    keys: BTreeSet::new(),
                },
            );
            Ok(id)
        })
    }

    fn renew_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move { Ok(lock(&self.inner).sessions.contains_key(id)) })
    }

    fn destroy_session<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), ConsulError>> {
        Box::pin(async move {
            let removed = {
                let mut inner = lock(&self.inner);
                let session = inner.sessions.remove(id);
                let mut removed = false;
                if let Some(session) = session {
                    for key in session.keys {
                        removed |= inner.values.remove(&key).is_some();
                    }
                }
                removed
            };
            if removed {
                self.notify.notify_waiters();
            }
            Ok(())
        })
    }

    fn acquire<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        session: &'a str,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let mut inner = lock(&self.inner);
            if !inner.sessions.contains_key(session) {
                return Ok(false);
            }
            if let Some(current) = inner.values.get(key)
                && current
                    .session
                    .as_ref()
                    .is_some_and(|holder| holder != session)
            {
                return Ok(false);
            }
            let modify_index = Self::next_index(&mut inner);
            inner.values.insert(
                key.to_owned(),
                ValueEntry {
                    value,
                    modify_index,
                    session: Some(session.to_owned()),
                },
            );
            if let Some(entry) = inner.sessions.get_mut(session) {
                entry.keys.insert(key.to_owned());
            }
            drop(inner);
            self.notify.notify_waiters();
            Ok(true)
        })
    }

    fn get<'a>(
        &'a self,
        key: &'a str,
        _consistent: bool,
    ) -> BoxFuture<'a, Result<Option<KvEntry>, ConsulError>> {
        Box::pin(async move {
            let inner = lock(&self.inner);
            Ok(inner.values.get(key).map(|entry| KvEntry {
                key: key.to_owned(),
                value: entry.value.clone(),
                modify_index: entry.modify_index,
                session: entry.session.clone(),
            }))
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a str,
        _consistent: bool,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        Box::pin(async move { Ok(Self::snapshot(&lock(&self.inner), prefix)) })
    }

    fn put_cas<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let mut inner = lock(&self.inner);
            let current = inner.values.get(key).map(|entry| entry.modify_index);
            let matches = if cas == 0 {
                current.is_none()
            } else {
                current == Some(cas)
            };
            if !matches {
                return Ok(false);
            }
            let modify_index = Self::next_index(&mut inner);
            inner.values.insert(
                key.to_owned(),
                ValueEntry {
                    value,
                    modify_index,
                    session: None,
                },
            );
            drop(inner);
            self.notify.notify_waiters();
            Ok(true)
        })
    }

    fn delete_cas<'a>(
        &'a self,
        key: &'a str,
        cas: u64,
    ) -> BoxFuture<'a, Result<bool, ConsulError>> {
        Box::pin(async move {
            let mut inner = lock(&self.inner);
            let Some(current) = inner.values.get(key) else {
                return Ok(false);
            };
            if current.modify_index != cas {
                return Ok(false);
            }
            inner.values.remove(key);
            drop(inner);
            self.notify.notify_waiters();
            Ok(true)
        })
    }

    fn watch_prefix<'a>(
        &'a self,
        prefix: &'a str,
        index: u64,
        wait: Duration,
    ) -> BoxFuture<'a, Result<PrefixResult, ConsulError>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let notified = self.notify.notified();
                {
                    let inner = lock(&self.inner);
                    if inner.index > index {
                        return Ok(Self::snapshot(&inner, prefix));
                    }
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    return Ok(Self::snapshot(&lock(&self.inner), prefix));
                }
                tokio::select! {
                    () = notified => {}
                    () = tokio::time::sleep(remaining) => {
                        return Ok(Self::snapshot(&lock(&self.inner), prefix));
                    }
                }
            }
        })
    }
}

fn lock(inner: &Arc<Mutex<Inner>>) -> std::sync::MutexGuard<'_, Inner> {
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}
