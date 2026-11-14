//! Multipart object writes for sidecar-owned data.
//!
//! Client-driven multipart requests still pass through unchanged. This module
//! gives the cache, cluster, and replication paths one writer that keeps large
//! objects below the provider single-`PUT` limits.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::time::Instant;

use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tracing::warn;

use crate::auth::ClientCredential;
use crate::auth::body::BodyStream;
use crate::cache::keys::object_path;
use crate::config::MultipartConfig;
use crate::metrics::Metrics;
use crate::storage::{ForwardRequest, S3Client, StorageError};

/// Largest part size that S3 accepts.
const MAX_PART_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024;
/// One mebibyte.
const MIB: u64 = 1_048_576;

/// One failed multipart step.
struct Failure {
    error: StorageError,
    reason: &'static str,
    upload_id: Option<String>,
}

impl Failure {
    fn before_start(error: StorageError, reason: &'static str) -> Self {
        Self {
            error,
            reason,
            upload_id: None,
        }
    }

    fn after_start(error: StorageError, reason: &'static str, upload_id: &str) -> Self {
        Self {
            error,
            reason,
            upload_id: Some(upload_id.to_owned()),
        }
    }
}

/// Write one object with one `PUT` or one multipart upload.
///
/// A body with an unknown length, a disabled writer, or a body below the
/// threshold uses one `PUT`. Every other body uses multipart.
///
/// # Errors
///
/// Returns a signing, status, body, size, timeout, or transport error. A
/// multipart failure aborts the upload before it returns.
#[allow(clippy::too_many_arguments)]
pub async fn put_object(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: BodyStream,
    content_length: Option<u64>,
    config: &MultipartConfig,
    metrics: &Metrics,
) -> Result<reqwest::Response, StorageError> {
    let Some(size) = content_length else {
        return put_single(client, credential, bucket, key, headers, body, None).await;
    };
    if !config.enabled || size == 0 || size < config.threshold.as_bytes() {
        return put_single(client, credential, bucket, key, headers, body, Some(size)).await;
    }
    match put_multipart(
        client, credential, bucket, key, headers, body, size, config, metrics,
    )
    .await
    {
        Ok(response) => Ok(response),
        Err(failure) => {
            metrics.multipart_failed(failure.reason);
            if let Some(upload_id) = &failure.upload_id {
                abort(client, credential, bucket, key, upload_id, metrics).await;
            }
            Err(failure.error)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn put_single(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: BodyStream,
    content_length: Option<u64>,
) -> Result<reqwest::Response, StorageError> {
    client
        .execute(
            credential,
            ForwardRequest {
                method: Method::PUT,
                path_and_query: object_path(bucket, key),
                headers,
                body: Some(body),
                content_length,
            },
        )
        .await
}

#[allow(clippy::too_many_arguments, clippy::result_large_err)]
async fn put_multipart(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    headers: HeaderMap,
    body: BodyStream,
    size: u64,
    config: &MultipartConfig,
    metrics: &Metrics,
) -> Result<reqwest::Response, Failure> {
    let part_size =
        part_size_for(size, config).map_err(|error| Failure::before_start(error, "part_size"))?;
    let start_path = format!("{}?uploads", object_path(bucket, key));
    let response = client
        .execute(
            credential,
            ForwardRequest {
                method: Method::POST,
                path_and_query: start_path,
                headers,
                body: None,
                content_length: Some(0),
            },
        )
        .await
        .map_err(|error| Failure::before_start(error, "start"))?;
    if !response.status().is_success() {
        return Err(Failure::before_start(
            StorageError::Status(response.status()),
            "start",
        ));
    }
    metrics.multipart_started();
    let bytes = response
        .bytes()
        .await
        .map_err(|error| Failure::before_start(StorageError::Transport(error), "start_body"))?;
    let text = String::from_utf8_lossy(&bytes);
    let upload_id = extract_tag(&text, "UploadId")
        .ok_or_else(|| Failure::before_start(StorageError::Body, "start_body"))?;

    let etags = run_upload(
        client, credential, bucket, key, body, size, part_size, config, &upload_id, metrics,
    )
    .await
    .map_err(|(error, reason)| Failure::after_start(error, reason, &upload_id))?;

    complete(client, credential, bucket, key, &upload_id, &etags)
        .await
        .map_err(|(error, reason)| Failure::after_start(error, reason, &upload_id))
}

#[allow(clippy::too_many_arguments)]
async fn run_upload(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    body: BodyStream,
    size: u64,
    part_size: u64,
    config: &MultipartConfig,
    upload_id: &str,
    metrics: &Metrics,
) -> Result<Vec<String>, (StorageError, &'static str)> {
    let part_count = size.div_ceil(part_size);
    let count = usize::try_from(part_count).map_err(|_| (StorageError::TooLarge, "part_size"))?;
    let mut reader = PartReader::new(body, part_size, size);
    let mut etags: Vec<Option<String>> = vec![None; count];
    let mut in_flight = FuturesUnordered::new();
    let mut next_number = 1_u64;
    let concurrency = config.concurrency.max(1);

    loop {
        while in_flight.len() < concurrency && next_number <= part_count {
            let part = reader.next_part().await.map_err(|error| (error, "body"))?;
            let number = next_number;
            let length = u64::try_from(part.len()).map_err(|_| (StorageError::Body, "body"))?;
            let index = usize::try_from(number.saturating_sub(1))
                .map_err(|_| (StorageError::TooLarge, "part_size"))?;
            let path = format!(
                "{}?partNumber={number}&uploadId={}",
                object_path(bucket, key),
                encode_query_value(upload_id)
            );
            let client_ref = client;
            let credential_ref = credential;
            let metrics_ref = metrics;
            in_flight.push(async move {
                let started = Instant::now();
                let stream: BodyStream =
                    Box::pin(futures_util::stream::once(async move { Ok(part) }));
                let result = client_ref
                    .execute(
                        credential_ref,
                        ForwardRequest {
                            method: Method::PUT,
                            path_and_query: path,
                            headers: HeaderMap::new(),
                            body: Some(stream),
                            content_length: Some(length),
                        },
                    )
                    .await;
                metrics_ref.multipart_part_latency(started.elapsed());
                match result {
                    Ok(response) if response.status().is_success() => {
                        let etag = response
                            .headers()
                            .get(header::ETAG)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        match etag {
                            Some(etag) => {
                                metrics_ref.multipart_part();
                                Ok((index, etag))
                            }
                            None => Err((StorageError::Body, "part_etag")),
                        }
                    }
                    Ok(response) => Err((StorageError::Status(response.status()), "part")),
                    Err(error) => Err((error, "part")),
                }
            });
            next_number = next_number.saturating_add(1);
        }
        let Some(result) = in_flight.next().await else {
            break;
        };
        match result {
            Ok((index, etag)) => {
                if let Some(slot) = etags.get_mut(index) {
                    *slot = Some(etag);
                } else {
                    return Err((StorageError::Body, "part"));
                }
            }
            Err(failure) => return Err(failure),
        }
    }
    reader.finish().await.map_err(|error| (error, "body"))?;
    etags
        .into_iter()
        .map(|etag| etag.ok_or((StorageError::Body, "part")))
        .collect()
}

async fn complete(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    upload_id: &str,
    etags: &[String],
) -> Result<reqwest::Response, (StorageError, &'static str)> {
    let body = complete_body(etags);
    let length = u64::try_from(body.len()).map_err(|_| (StorageError::Body, "complete"))?;
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    let stream: BodyStream = Box::pin(futures_util::stream::once(async move { Ok(body) }));
    let response = client
        .execute(
            credential,
            ForwardRequest {
                method: Method::POST,
                path_and_query: format!(
                    "{}?uploadId={}",
                    object_path(bucket, key),
                    encode_query_value(upload_id)
                ),
                headers,
                body: Some(stream),
                content_length: Some(length),
            },
        )
        .await
        .map_err(|error| (error, "complete"))?;
    if response.status().is_success() {
        Ok(complete_response_with_etag(response).await)
    } else {
        Err((StorageError::Status(response.status()), "complete"))
    }
}

/// Read the small complete response body and keep its `ETag` header.
///
/// A real S3 server can send the `ETag` only in the XML body. The caller then
/// sees the same status and headers, plus an `ETag` header when the body has
/// one.
async fn complete_response_with_etag(response: reqwest::Response) -> reqwest::Response {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.ok();
    let mut output: axum::http::Response<reqwest::Body> =
        axum::http::Response::new(reqwest::Body::from(Vec::new()));
    *output.status_mut() = status;
    *output.headers_mut() = headers;
    if let Some(body) = body {
        let text = String::from_utf8_lossy(&body);
        if output.headers().get(header::ETAG).is_none()
            && let Some(etag) = extract_tag(&text, "ETag")
            && let Ok(value) = HeaderValue::from_str(&etag)
        {
            output.headers_mut().insert(header::ETAG, value);
        }
    }
    output.into()
}

async fn abort(
    client: &S3Client,
    credential: &ClientCredential,
    bucket: &str,
    key: &str,
    upload_id: &str,
    metrics: &Metrics,
) {
    let request = ForwardRequest {
        method: Method::DELETE,
        path_and_query: format!(
            "{}?uploadId={}",
            object_path(bucket, key),
            encode_query_value(upload_id)
        ),
        headers: HeaderMap::new(),
        body: None,
        content_length: None,
    };
    match client.execute(credential, request).await {
        Ok(response)
            if response.status().is_success() || response.status() == StatusCode::NOT_FOUND =>
        {
            metrics.multipart_aborted();
        }
        Ok(response) => {
            warn!(
                status = response.status().as_u16(),
                "multipart abort failed"
            );
        }
        Err(error) => {
            warn!(%error, "multipart abort failed");
        }
    }
}

fn part_size_for(size: u64, config: &MultipartConfig) -> Result<u64, StorageError> {
    let mut part_size = config.chunk_size.as_bytes().max(1);
    let max_parts = u64::from(config.max_parts.max(1));
    if size.div_ceil(part_size) > max_parts {
        part_size = round_up_mib(size.div_ceil(max_parts));
    }
    if part_size > MAX_PART_BYTES {
        return Err(StorageError::TooLarge);
    }
    Ok(part_size)
}

fn round_up_mib(bytes: u64) -> u64 {
    bytes.div_ceil(MIB).saturating_mul(MIB)
}

fn complete_body(etags: &[String]) -> Bytes {
    let mut xml = String::from("<CompleteMultipartUpload>");
    for (index, etag) in etags.iter().enumerate() {
        let number = index.saturating_add(1);
        let _ = write!(
            xml,
            "<Part><PartNumber>{number}</PartNumber><ETag>{}</ETag></Part>",
            escape_xml(etag)
        );
    }
    xml.push_str("</CompleteMultipartUpload>");
    Bytes::from(xml)
}

fn extract_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)?.saturating_add(open.len());
    let rest = xml.get(start..)?;
    let end = rest.find(&close)?;
    Some(unescape_xml(rest.get(..end)?))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn unescape_xml(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn encode_query_value(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(char::from(byte));
        } else {
            let _ = write!(output, "%{byte:02X}");
        }
    }
    output
}

/// Read one body into parts of a fixed size.
struct PartReader {
    body: BodyStream,
    pending: VecDeque<Bytes>,
    part_size: u64,
    remaining: u64,
}

impl PartReader {
    fn new(body: BodyStream, part_size: u64, size: u64) -> Self {
        Self {
            body,
            pending: VecDeque::new(),
            part_size,
            remaining: size,
        }
    }

    async fn next_part(&mut self) -> Result<Bytes, StorageError> {
        let wanted = self.part_size.min(self.remaining);
        if wanted == 0 {
            return Err(StorageError::Body);
        }
        let mut buffer = Vec::with_capacity(usize::try_from(wanted).unwrap_or(0));
        loop {
            let filled = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
            if filled >= wanted {
                break;
            }
            if let Some(front) = self.pending.front() {
                let need = wanted.saturating_sub(filled);
                let available = u64::try_from(front.len()).unwrap_or(u64::MAX);
                let take = usize::try_from(need.min(available)).unwrap_or(front.len());
                buffer.extend_from_slice(&front[..take]);
                if take == front.len() {
                    self.pending.pop_front();
                } else {
                    let rest = front.slice(take..);
                    self.pending.pop_front();
                    self.pending.push_front(rest);
                }
                continue;
            }
            match self.body.next().await {
                Some(Ok(chunk)) if !chunk.is_empty() => self.pending.push_back(chunk),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return Err(StorageError::Body),
            }
        }
        self.remaining = self.remaining.saturating_sub(wanted);
        Ok(Bytes::from(buffer))
    }

    async fn finish(&mut self) -> Result<(), StorageError> {
        if self.pending.iter().any(|chunk| !chunk.is_empty()) {
            return Err(StorageError::Body);
        }
        while let Some(item) = self.body.next().await {
            match item {
                Ok(chunk) if !chunk.is_empty() => return Err(StorageError::Body),
                Ok(_) => {}
                Err(_) => return Err(StorageError::Body),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::config::size::Size;

    #[test]
    fn complete_body_escapes_etags() {
        let body = complete_body(&["\"a&b\"".to_owned(), "\"<c>\"".to_owned()]);
        let text = String::from_utf8_lossy(&body);
        assert!(text.contains("<PartNumber>1</PartNumber>"));
        assert!(text.contains("<PartNumber>2</PartNumber>"));
        assert!(text.contains("&amp;"));
        assert!(text.contains("&lt;c&gt;"));
    }

    #[test]
    fn extract_tag_removes_xml_escapes() {
        let xml = "<Result><UploadId>a&amp;b</UploadId></Result>";
        assert_eq!(extract_tag(xml, "UploadId").as_deref(), Some("a&b"));
    }

    #[test]
    fn query_values_escape_reserved_bytes() {
        assert_eq!(encode_query_value("a+b/c="), "a%2Bb%2Fc%3D");
        assert_eq!(encode_query_value("plain-id_1.2~"), "plain-id_1.2~");
    }

    #[test]
    fn part_size_rises_for_max_parts() {
        let config = MultipartConfig {
            chunk_size: Size::new(MIB),
            max_parts: 4,
            ..MultipartConfig::default()
        };
        let size = 4 * MIB + 1;
        assert_eq!(part_size_for(size, &config).unwrap(), 2 * MIB);
    }

    #[test]
    fn part_size_keeps_the_chunk_size_below_max_parts() {
        let config = MultipartConfig {
            chunk_size: Size::new(2 * MIB),
            max_parts: 4,
            ..MultipartConfig::default()
        };
        assert_eq!(part_size_for(8 * MIB, &config).unwrap(), 2 * MIB);
    }

    #[test]
    fn part_size_rejects_above_the_s3_limit() {
        let config = MultipartConfig {
            chunk_size: Size::new(MIB),
            max_parts: 1,
            ..MultipartConfig::default()
        };
        assert!(matches!(
            part_size_for(6 * 1_024 * MIB, &config),
            Err(StorageError::TooLarge)
        ));
    }
}
