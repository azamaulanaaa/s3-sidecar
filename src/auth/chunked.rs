//! Decode and verify `aws-chunked` request bodies.

use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use axum::body::BodyDataStream;
use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;

use super::body::{BodyFailure, BodyStream, fail, failure_flag};
use super::{EMPTY_SHA256, StreamingContext, constant_time_eq, hmac_sha256, sha256_hex};

const MAX_CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const MAX_LINE_SIZE: usize = 8 * 1024;
const MAX_TRAILERS: usize = 32;

/// Chunk signature mode.
#[derive(Debug)]
pub enum ChunkMode {
    /// Verify each chunk signature.
    Signed(StreamingContext),
    /// Decode the framing and ignore signatures.
    UnsignedTrailer,
}

/// Decode an `aws-chunked` body.
pub fn decode(inner: BodyDataStream, mode: ChunkMode) -> (BodyStream, Arc<AtomicU8>) {
    let failure = failure_flag();
    let failure_inner = Arc::clone(&failure);
    let stream = async_stream::try_stream! {
        let mut inner = inner;
        let mut buffer = BytesMut::new();
        let mut decoded_length = 0_u64;
        let (signing, mut previous_signature) = match &mode {
            ChunkMode::Signed(context) => (Some(context), context.seed_signature.clone()),
            ChunkMode::UnsignedTrailer => (None, String::new()),
        };

        loop {
            let line = read_line(&mut inner, &mut buffer)
                .await
                .map_err(|_error| fail(&failure_inner, BodyFailure::ChunkFormat))?;
            let header = parse_chunk_header(&line, signing.is_some())
                .map_err(|_error| fail(&failure_inner, BodyFailure::ChunkFormat))?;

            if header.size == 0 {
                if let Some(context) = signing {
                    let signature = header
                        .signature
                        .as_deref()
                        .ok_or(())
                        .map_err(|()| fail(&failure_inner, BodyFailure::ChunkFormat))?;
                    verify_chunk(context, &previous_signature, signature, &[])
                        .map_err(|()| fail(&failure_inner, BodyFailure::ChunkSignature))?;
                    previous_signature = signature.to_owned();
                }
                let trailers = read_trailers(&mut inner, &mut buffer)
                    .await
                    .map_err(|_error| fail(&failure_inner, BodyFailure::ChunkFormat))?;
                if let Some(context) = signing
                    && context.trailer
                {
                    verify_trailer(context, &previous_signature, &trailers)
                        .map_err(|()| fail(&failure_inner, BodyFailure::ChunkSignature))?;
                }
                break;
            }

            if header.size > MAX_CHUNK_SIZE {
                Err(fail(&failure_inner, BodyFailure::ChunkFormat))?;
            }
            let data = read_exact(&mut inner, &mut buffer, header.size)
                .await
                .map_err(|_error| fail(&failure_inner, BodyFailure::ChunkFormat))?;
            if let Some(context) = signing {
                let signature = header
                    .signature
                    .as_deref()
                    .ok_or(())
                    .map_err(|()| fail(&failure_inner, BodyFailure::ChunkFormat))?;
                verify_chunk(context, &previous_signature, signature, &data)
                    .map_err(|()| fail(&failure_inner, BodyFailure::ChunkSignature))?;
                previous_signature = signature.to_owned();
            }
            read_crlf(&mut inner, &mut buffer)
                .await
                .map_err(|_error| fail(&failure_inner, BodyFailure::ChunkFormat))?;
            decoded_length = decoded_length.saturating_add(data.len() as u64);
            yield Bytes::from(data);
        }

        if let Some(context) = signing
            && let Some(expected) = context.decoded_length
            && decoded_length != expected
        {
            Err(fail(&failure_inner, BodyFailure::DecodedLength))?;
        }
    };
    (Box::pin(stream), failure)
}

struct ChunkHeader {
    size: u64,
    signature: Option<String>,
}

fn parse_chunk_header(line: &[u8], signed: bool) -> Result<ChunkHeader, ()> {
    let text = std::str::from_utf8(line).map_err(|_| ())?;
    let (size_text, extensions) = text.split_once(';').unwrap_or((text, ""));
    let size = u64::from_str_radix(size_text.trim(), 16).map_err(|_| ())?;
    let mut signature = None;
    if signed {
        for extension in extensions.split(';') {
            let Some((name, value)) = extension.split_once('=') else {
                continue;
            };
            if name.trim() == "chunk-signature" {
                let value = value.trim();
                if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(());
                }
                signature = Some(value.to_owned());
            }
        }
        if signature.is_none() {
            return Err(());
        }
    }
    Ok(ChunkHeader { size, signature })
}

fn verify_chunk(
    context: &StreamingContext,
    previous: &str,
    signature: &str,
    data: &[u8],
) -> Result<(), ()> {
    let string = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        context.amz_date,
        context.scope,
        previous,
        EMPTY_SHA256,
        sha256_hex(data)
    );
    let expected = hex::encode(hmac_sha256(
        context.signing_key.as_bytes(),
        string.as_bytes(),
    ));
    if constant_time_eq(&expected, signature) {
        Ok(())
    } else {
        Err(())
    }
}

fn verify_trailer(
    context: &StreamingContext,
    previous: &str,
    trailers: &[(String, String)],
) -> Result<(), ()> {
    let signature = trailers
        .iter()
        .find(|(name, _)| name == "x-amz-trailer-signature")
        .map(|(_, value)| value.clone())
        .ok_or(())?;
    let mut canonical = String::new();
    let mut fields: Vec<&(String, String)> = trailers
        .iter()
        .filter(|(name, _)| name != "x-amz-trailer-signature")
        .collect();
    fields.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, value) in fields {
        canonical.push_str(name);
        canonical.push(':');
        canonical.push_str(value);
        canonical.push('\n');
    }
    let string = format!(
        "AWS4-HMAC-SHA256-TRAILER\n{}\n{}\n{}\n{}",
        context.amz_date,
        context.scope,
        previous,
        sha256_hex(canonical.as_bytes())
    );
    let expected = hex::encode(hmac_sha256(
        context.signing_key.as_bytes(),
        string.as_bytes(),
    ));
    if !constant_time_eq(&expected, &signature) {
        return Err(());
    }
    Ok(())
}

async fn ensure(
    inner: &mut BodyDataStream,
    buffer: &mut BytesMut,
    needed: usize,
) -> Result<(), ()> {
    while buffer.len() < needed {
        match inner.next().await {
            Some(Ok(bytes)) => buffer.extend_from_slice(&bytes),
            Some(Err(_)) | None => return Err(()),
        }
    }
    Ok(())
}

async fn read_line(inner: &mut BodyDataStream, buffer: &mut BytesMut) -> Result<Vec<u8>, ()> {
    loop {
        if let Some(position) = find_crlf(buffer) {
            let line = buffer.split_to(position + 2);
            return Ok(line[..position].to_vec());
        }
        if buffer.len() > MAX_LINE_SIZE {
            return Err(());
        }
        match inner.next().await {
            Some(Ok(bytes)) => buffer.extend_from_slice(&bytes),
            Some(Err(_)) | None => return Err(()),
        }
    }
}

async fn read_exact(
    inner: &mut BodyDataStream,
    buffer: &mut BytesMut,
    length: u64,
) -> Result<Vec<u8>, ()> {
    let length = usize::try_from(length).map_err(|_| ())?;
    ensure(inner, buffer, length).await?;
    Ok(buffer.split_to(length).to_vec())
}

async fn read_crlf(inner: &mut BodyDataStream, buffer: &mut BytesMut) -> Result<(), ()> {
    ensure(inner, buffer, 2).await?;
    let marker = buffer.split_to(2);
    if marker.as_ref() == b"\r\n" {
        Ok(())
    } else {
        Err(())
    }
}

async fn read_trailers(
    inner: &mut BodyDataStream,
    buffer: &mut BytesMut,
) -> Result<Vec<(String, String)>, ()> {
    let mut trailers = Vec::new();
    loop {
        let line = read_line(inner, buffer).await?;
        if line.is_empty() {
            return Ok(trailers);
        }
        if trailers.len() >= MAX_TRAILERS {
            return Err(());
        }
        let text = std::str::from_utf8(&line).map_err(|_| ())?;
        let (name, value) = text.split_once(':').ok_or(())?;
        trailers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
    }
}

fn find_crlf(buffer: &BytesMut) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\r\n")
}

#[cfg(test)]
mod tests {

    use axum::body::Body;
    use futures_util::StreamExt;

    use super::{ChunkMode, decode};
    use crate::auth::body::{BodyFailure, failure};
    use crate::auth::{EMPTY_SHA256, SigningKey, StreamingContext, hmac_sha256, sha256_hex};

    const DATE: &str = "20130524T000000Z";
    const SCOPE: &str = "20130524/us-east-1/s3/aws4_request";

    fn context(key: [u8; 32], seed: &str, decoded_length: Option<u64>) -> StreamingContext {
        StreamingContext {
            signing_key: SigningKey::new(key),
            seed_signature: seed.to_owned(),
            amz_date: DATE.to_owned(),
            scope: SCOPE.to_owned(),
            decoded_length,
            trailer: false,
        }
    }

    fn chunk_signature(key: &[u8; 32], previous: &str, data: &[u8]) -> String {
        let string = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{DATE}\n{SCOPE}\n{previous}\n{EMPTY_SHA256}\n{}",
            sha256_hex(data)
        );
        hex::encode(hmac_sha256(key, string.as_bytes()))
    }

    fn signed_body(key: &[u8; 32], seed: &str, tamper: bool) -> String {
        let first = chunk_signature(key, seed, b"hello ");
        let second = chunk_signature(key, &first, b"world");
        let last = chunk_signature(key, &second, b"");
        let first = if tamper {
            format!("0{}", &first[1..])
        } else {
            first
        };
        format!(
            "6;chunk-signature={first}\r\nhello \r\n\
             5;chunk-signature={second}\r\nworld\r\n\
             0;chunk-signature={last}\r\n\r\n"
        )
    }

    async fn collect(mut stream: crate::auth::body::BodyStream) -> (Vec<u8>, bool) {
        let mut output = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => output.extend_from_slice(&bytes),
                Err(_) => return (output, false),
            }
        }
        (output, true)
    }

    #[tokio::test]
    async fn signed_chunks_are_decoded() {
        let key = [7_u8; 32];
        let seed = chunk_signature(&key, "seed", b"");
        let body = signed_body(&key, &seed, false);
        let (stream, flag) = decode(
            Body::from(body).into_data_stream(),
            ChunkMode::Signed(context(key, &seed, Some(11))),
        );
        let (output, result) = collect(stream).await;
        assert_eq!(output, b"hello world");
        assert!(result);
        assert_eq!(failure(&flag), BodyFailure::None);
    }

    #[tokio::test]
    async fn tampered_chunk_signature_is_rejected() {
        let key = [7_u8; 32];
        let seed = chunk_signature(&key, "seed", b"");
        let body = signed_body(&key, &seed, true);
        let (stream, flag) = decode(
            Body::from(body).into_data_stream(),
            ChunkMode::Signed(context(key, &seed, Some(11))),
        );
        let (output, result) = collect(stream).await;
        assert_eq!(output, Vec::<u8>::new());
        assert!(!result);
        assert_eq!(failure(&flag), BodyFailure::ChunkSignature);
    }

    #[tokio::test]
    async fn decoded_length_mismatch_is_rejected() {
        let key = [7_u8; 32];
        let seed = chunk_signature(&key, "seed", b"");
        let body = signed_body(&key, &seed, false);
        let (stream, flag) = decode(
            Body::from(body).into_data_stream(),
            ChunkMode::Signed(context(key, &seed, Some(12))),
        );
        let (output, result) = collect(stream).await;
        assert_eq!(output, b"hello world");
        assert!(!result);
        assert_eq!(failure(&flag), BodyFailure::DecodedLength);
    }

    #[tokio::test]
    async fn unsigned_trailer_chunks_are_decoded() {
        let body = "6\r\nhello \r\n5\r\nworld\r\n0\r\n\r\n";
        let (stream, flag) = decode(
            Body::from(body).into_data_stream(),
            ChunkMode::UnsignedTrailer,
        );
        let (output, result) = collect(stream).await;
        assert_eq!(output, b"hello world");
        assert!(result);
        assert_eq!(failure(&flag), BodyFailure::None);
    }
}
