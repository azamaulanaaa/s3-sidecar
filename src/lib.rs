//! An S3 sidecar proxy.
//!
//! The `multiple_crate_versions` lint is allowed here because the Rust TLS
//! and HTTP dependency tree has duplicate transitive versions. The
//! `cargo-deny` policy in `deny.toml` checks duplicates and documents each
//! exception.

#![allow(clippy::multiple_crate_versions)]
//!
//! This build implements the proxy core: layered configuration, `SigV4`
//! authentication, and byte-preserving pass-through to one origin endpoint.

pub mod auth;
pub mod cache;
pub mod cli;
pub mod cluster;
pub mod config;
pub mod errors;
pub mod metrics;
pub mod observability;
pub mod server;
pub mod storage;
