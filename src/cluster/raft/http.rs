//! HTTP endpoints for internal raft messages.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{ReadPolicy, Snapshot};
use serde::de::DeserializeOwned;
use subtle::ConstantTimeEq;

use super::coordinator::{ProposeReply, ReadIndexReply};
use super::network::{SnapshotRequest, TOKEN_HEADER};
use super::{RaftNode, TypeConfig};
use crate::cluster::coordinator::Entry;
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
/// Returns an error status when the token is wrong or the raft call fails.
pub async fn propose(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ProposeReply>, StatusCode> {
    let node = raft_node(&state, &headers)?;
    let entry: Entry = parse(&body)?;
    let response = node
        .client_write(entry)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    Ok(Json(ProposeReply {
        log_id: response.log_id,
        outcome: response.data,
    }))
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
