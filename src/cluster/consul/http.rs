//! HTTP endpoint for internal Consul proposals.

use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;
use tracing::warn;

use super::coordinator::{ConsulCoordinator, ProposeReply, TOKEN_HEADER};
use crate::cluster::coordinator::{CoordError, Entry};
use crate::server::AppState;

fn coordinator(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<Arc<ConsulCoordinator>, StatusCode> {
    let coordinator = state
        .cluster()
        .and_then(|cluster| cluster.consul().cloned())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let provided = headers
        .get(TOKEN_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if !bool::from(provided.as_bytes().ct_eq(coordinator.secret().as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(coordinator)
}

/// Handle one forwarded proposal.
///
/// # Errors
///
/// Returns an error status when the token is wrong or the leader cannot apply
/// the entry.
pub async fn propose(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Ok(coordinator) = coordinator(&state, &headers) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let entry: Entry = match serde_json::from_slice(&body) {
        Ok(entry) => entry,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    match coordinator.apply_forwarded(entry).await {
        Ok(applied) => Json(ProposeReply::Applied(applied)).into_response(),
        Err(CoordError::Rejected(reason)) => Json(ProposeReply::Rejected(reason)).into_response(),
        Err(
            CoordError::StaleTerm { .. } | CoordError::Unavailable | CoordError::Indeterminate(_),
        ) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(
            error @ (CoordError::Storage(_)
            | CoordError::Codec(_)
            | CoordError::Io(_)
            | CoordError::Backend(_)
            | CoordError::Message(_)),
        ) => {
            warn!(error = %error, "consul proposal failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(CoordError::NotImplemented(message)) => {
            (StatusCode::NOT_IMPLEMENTED, message).into_response()
        }
    }
}
