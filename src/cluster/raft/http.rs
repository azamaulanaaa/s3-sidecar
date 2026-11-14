//! HTTP endpoints for internal raft messages.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{ReadPolicy, Snapshot};
use serde::de::DeserializeOwned;
use subtle::ConstantTimeEq;

use super::coordinator::{
    PROPOSE_ERROR_HEADER, PROPOSE_ERROR_INDETERMINATE, PROPOSE_ERROR_UNAVAILABLE, ProposeReply,
    ReadIndexReply,
};
use super::network::{SnapshotRequest, TOKEN_HEADER};
use super::{RaftNode, TypeConfig};
use crate::cluster::coordinator::{CoordError, Entry};
use crate::server::AppState;

fn raft_node(state: &AppState, headers: &HeaderMap) -> Result<Arc<RaftNode>, StatusCode> {
    let node = state
        .cluster()
        .and_then(|cluster| cluster.raft_node().cloned())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let provided = headers
        .get(TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(provided.as_bytes().ct_eq(node.secret().as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(node)
}

fn parse<T: DeserializeOwned>(body: &Bytes) -> Result<T, StatusCode> {
    serde_json::from_slice(body).map_err(|_| StatusCode::BAD_REQUEST)
}

/// Handle one vote request.
///
/// # Errors
///
/// Returns an error status when the token is wrong or the raft call fails.
pub async fn vote(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<VoteResponse<TypeConfig>>, StatusCode> {
    let node = raft_node(&state, &headers)?;
    let request: VoteRequest<TypeConfig> = parse(&body)?;
    node.raft()
        .vote(request)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Handle one append request.
///
/// # Errors
///
/// Returns an error status when the token is wrong or the raft call fails.
pub async fn append(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<AppendEntriesResponse<TypeConfig>>, StatusCode> {
    let node = raft_node(&state, &headers)?;
    let request: AppendEntriesRequest<TypeConfig> = parse(&body)?;
    let result = node
        .raft()
        .append_entries(request)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(AppendEntriesResponse::from(result)))
}

/// Handle one full snapshot install.
///
/// # Errors
///
/// Returns an error status when the token is wrong or the raft call fails.
pub async fn snapshot(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SnapshotResponse<TypeConfig>>, StatusCode> {
    let node = raft_node(&state, &headers)?;
    let request: SnapshotRequest = parse(&body)?;
    let snapshot = Snapshot {
        meta: request.meta,
        snapshot: request.data,
    };
    node.raft()
        .install_full_snapshot(request.vote, snapshot)
        .await
        .map(Json)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)
}

/// Handle one forwarded client proposal.
///
/// # Errors
///
/// Returns an error status when the token is wrong, the node has no leader,
/// or the proposal result is unknown.
pub async fn propose(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let node = match raft_node(&state, &headers) {
        Ok(node) => node,
        Err(status) => return status.into_response(),
    };
    let entry: Entry = match parse(&body) {
        Ok(entry) => entry,
        Err(status) => return status.into_response(),
    };
    match node.client_write(entry).await {
        Ok(response) => Json(ProposeReply {
            log_id: response.log_id,
            outcome: response.data,
        })
        .into_response(),
        Err(CoordError::Unavailable) => {
            propose_error(StatusCode::SERVICE_UNAVAILABLE, PROPOSE_ERROR_UNAVAILABLE)
        }
        Err(CoordError::Indeterminate(_)) => {
            propose_error(StatusCode::SERVICE_UNAVAILABLE, PROPOSE_ERROR_INDETERMINATE)
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn propose_error(status: StatusCode, reason: &'static str) -> Response {
    let mut response = status.into_response();
    if let Ok(value) = HeaderValue::from_str(reason) {
        response
            .headers_mut()
            .insert(HeaderName::from_static(PROPOSE_ERROR_HEADER), value);
    }
    response
}

/// Handle one forwarded read index request.
///
/// # Errors
///
/// Returns an error status when the token is wrong or the raft call fails.
pub async fn read_index(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<ReadIndexReply>, StatusCode> {
    let node = raft_node(&state, &headers)?;
    let index = node
        .raft()
        .ensure_linearizable(ReadPolicy::ReadIndex)
        .await
        .map(|read_log_id| read_log_id.index())
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(ReadIndexReply { index }))
}
