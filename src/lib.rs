//! An S3 sidecar proxy.
//!
//! The `multiple_crate_versions` lint is allowed here because the Rust TLS
//! and HTTP dependency tree has duplicate transitive versions. The
//! `cargo-deny` policy in `deny.toml` checks duplicates and documents each
//! exception.

#![expect(
    clippy::multiple_crate_versions,
    reason = "the TLS and HTTP dependency tree has duplicate transitive versions"
)]
//!
//! This build implements the proxy core: layered configuration, `SigV4`
//! authentication, and byte-preserving pass-through to one origin endpoint.

pub mod auth;
pub mod cache;
pub mod cli;
pub mod cluster;
pub mod config;
pub mod errors;
#[doc(hidden)]
pub mod fuzz_support;
pub mod metrics;
pub mod multipart;
pub mod observability;
pub mod server;
pub mod storage;
pub mod telemetry;
