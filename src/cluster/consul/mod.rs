//! Consul consensus backend.

pub mod api;
pub mod coordinator;
pub mod http;
pub mod memory;

pub use api::{ConsulApi, ConsulError, HttpConsul, KvEntry, PrefixResult};
pub use coordinator::{ConsulCoordinator, LeaderRecord, metadata_key};
pub use memory::MemoryConsul;
