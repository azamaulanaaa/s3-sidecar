//! Internal blob transport endpoints.
//!
//! These routes bypass S3 client authentication. Every request must carry the
//! cluster token in `x-s3-sidecar-cluster-token`.

use std::io;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::response::Response;
use futures_util::TryStreamExt;
use subtle::ConstantTimeEq;
use tracing::warn;

use super::copy_headers;
use crate::server::AppState;
use crate::storage::ForwardRequest;

/// Header that carries the cluster blob transport token.
pub const CLUSTER_TOKEN_HEADER: &str = "x-s3-sidecar-cluster-token";

/// Stream one blob to a peer.
///
/// # Errors
///
/// Returns `401` for a missing token and `404` when the blob is absent.
pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let cluster = authorize(&state, &headers)?;
    let mut request_headers = HeaderMap::new();
    if let Some(range) = headers.get(header::RANGE) {
        request_headers.insert(header::RANGE, range.clone());
    }
    let response = cluster
        .client
        .execute(
            &cluster.credential,
            ForwardRequest {
                method: Method::GET,
                path_and_query: format!("/{}/objects/{uuid}", cluster.blob_bucket()),
                headers: request_headers,
                body: None,
                content_length: None,
            },
        )
        .await
        .map_err(|error| {
            warn!(%error, "blob transport read failed");
            StatusCode::BAD_GATEWAY
        })?;
    if response.status() == StatusCode::NOT_FOUND {
        return Err(StatusCode::NOT_FOUND);
    }
    if !response.status().is_success() {
        return Err(response.status());
    }
    let status = response.status();
    let response_headers = response.headers().clone();
    let body = Body::from_stream(
        response
            .bytes_stream()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
    );
    let mut output = Response::new(body);
    *output.status_mut() = status;
    copy_headers(&response_headers, output.headers_mut());
    Ok(output)
}

/// Report one blob length.
///
/// # Errors
///
/// Returns `401` for a missing token and `404` when the blob is absent.
pub async fn head(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
    headers: HeaderMap,
) -> Result<Response, StatusCode> {
    let cluster = authorize(&state, &headers)?;
    let Some(length) = cluster.head_blob(&uuid).await else {
        return Err(StatusCode::NOT_FOUND);
    };
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::OK;
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(value) = HeaderValue::from_str(&length.to_string()) {
        response.headers_mut().insert(header::CONTENT_LENGTH, value);
    }
    Ok(response)
}

/// Store one blob from a peer.
///
/// # Errors
///
/// Returns `401` for a missing token and `502` when the backing write fails.
pub async fn put(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, StatusCode> {
    let cluster = authorize(&state, &headers)?;
    let length = parse_length(&headers)?;
    let stream = Box::pin(
        body.into_data_stream()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
    );
    let response = crate::multipart::put_object(
        &cluster.client,
        &cluster.credential,
        cluster.blob_bucket(),
        &format!("objects/{uuid}"),
        HeaderMap::new(),
        stream,
        length,
        &cluster.config.multipart,
        &cluster.metrics,
    )
    .await
    .map_err(|error| {
        warn!(%error, "blob transport write failed");
        StatusCode::BAD_GATEWAY
    })?;
    if response.status().is_success() {
        Ok(StatusCode::OK)
    } else {
        Err(response.status())
    }
}

/// Delete one blob when no committed metadata references it.
///
/// # Errors
///
/// Returns `401` for a missing token, `409` when metadata references the blob,
/// and `502` when the backing delete fails.
pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(uuid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, StatusCode> {
    let cluster = authorize(&state, &headers)?;
    let referenced = cluster
        .state
        .has_committed_blob_reference(&uuid)
        .map_err(|error| {
            warn!(%error, "blob reference check failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    if referenced {
        return Err(StatusCode::CONFLICT);
    }
    let response = cluster
        .client
        .execute(
            &cluster.credential,
            ForwardRequest {
                method: Method::DELETE,
                path_and_query: format!("/{}/objects/{uuid}", cluster.blob_bucket()),
                headers: HeaderMap::new(),
                body: None,
                content_length: None,
            },
        )
        .await
        .map_err(|error| {
            warn!(%error, "blob transport delete failed");
            StatusCode::BAD_GATEWAY
        })?;
    if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(response.status())
    }
}

fn authorize<'a>(
    state: &'a AppState,
    headers: &HeaderMap,
) -> Result<&'a Arc<super::Cluster>, StatusCode> {
    let cluster = state.cluster().ok_or(StatusCode::NOT_FOUND)?;
    let secret = state.config().cluster.secret.expose_secret();
    if secret.is_empty() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let provided = headers
        .get(CLUSTER_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if provided.len() != secret.len() || !bool::from(provided.as_bytes().ct_eq(secret.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(cluster)
}

fn parse_length(headers: &HeaderMap) -> Result<Option<u64>, StatusCode> {
    let Some(value) = headers.get(header::CONTENT_LENGTH) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| StatusCode::BAD_REQUEST)?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| StatusCode::BAD_REQUEST)
}
