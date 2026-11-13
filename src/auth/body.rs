//! Streaming payload verification.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use axum::body::BodyDataStream;
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// A boxed request body stream with an I/O error type.
pub type BodyStream = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;

/// Failure reason for a request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BodyFailure {
    /// No failure.
    None = 0,
    /// The payload hash does not match.
    PayloadHash = 1,
    /// A chunk signature does not match.
    ChunkSignature = 2,
    /// The chunked framing is not valid.
    ChunkFormat = 3,
    /// The decoded length does not match.
    DecodedLength = 4,
}

impl BodyFailure {
    /// Convert a stored code to a failure value.
    #[must_use]
    pub fn from_code(code: u8) -> Self {
        match code {
            1 => Self::PayloadHash,
            2 => Self::ChunkSignature,
            3 => Self::ChunkFormat,
            4 => Self::DecodedLength,
            _ => Self::None,
        }
    }

    /// Return a short message for the failure.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            Self::None => "no failure",
            Self::PayloadHash => "the payload hash does not match",
            Self::ChunkSignature => "a chunk signature does not match",
            Self::ChunkFormat => "the chunked body is not valid",
            Self::DecodedLength => "the decoded content length does not match",
        }
    }
}

/// Create a failure flag.
#[must_use]
pub fn failure_flag() -> Arc<AtomicU8> {
    Arc::new(AtomicU8::new(BodyFailure::None as u8))
}

/// Record a body failure and build the stream error.
#[must_use]
pub fn fail(flag: &AtomicU8, failure: BodyFailure) -> io::Error {
    flag.store(failure as u8, Ordering::Release);
    io::Error::new(io::ErrorKind::InvalidData, failure.message())
}

/// Read the recorded body failure.
#[must_use]
pub fn failure(flag: &AtomicU8) -> BodyFailure {
    BodyFailure::from_code(flag.load(Ordering::Acquire))
}

/// Verify a SHA-256 payload hash while the body streams.
pub fn verify_hash(
    inner: BodyDataStream,
    expected: [u8; 32],
    failure_flag: Arc<AtomicU8>,
) -> BodyStream {
    let stream = async_stream::try_stream! {
        let mut inner = inner;
        let mut hasher = Sha256::new();
        while let Some(chunk) = inner.next().await {
            let bytes = chunk
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
            hasher.update(&bytes);
            yield bytes;
        }
        let actual = hasher.finalize();
        if !bool::from(actual.as_slice().ct_eq(&expected)) {
            Err(fail(&failure_flag, BodyFailure::PayloadHash))?;
        }
    };
    Box::pin(stream)
}
