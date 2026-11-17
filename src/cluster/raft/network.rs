//! HTTP transport for internal raft messages.

#![allow(clippy::unused_async_trait_impl)]

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::BasicNode;
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use serde::{Deserialize, Serialize};

use super::{SnapshotMetaOf, SnapshotOf, TypeConfig, VoteOf};
use crate::cluster::coordinator::CoordError;
use crate::config::RaftConfig;

/// Header that carries the shared raft token.
pub const TOKEN_HEADER: &str = "x-s3-sidecar-raft-token";

/// One full snapshot request.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// Leader vote.
    pub vote: VoteOf,
    /// Snapshot metadata.
    pub meta: SnapshotMetaOf,
    /// Snapshot data.
    pub data: Vec<u8>,
}

/// Build an HTTP network factory from the raft configuration.
#[derive(Clone)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
    token: String,
    timeout: Duration,
    addresses: BTreeMap<String, String>,
}

impl HttpNetworkFactory {
    /// Build one factory.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(config: &RaftConfig) -> Result<Self, CoordError> {
        let timeout = config.rpc_timeout.as_duration();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(timeout)
            .read_timeout(timeout)
            .build()
            .map_err(CoordError::backend)?;
        let addresses = config
            .nodes
            .iter()
            .map(|node| (node.id.clone(), node.addr.trim_end_matches('/').to_owned()))
            .collect();
        Ok(Self {
            client,
            token: config.secret.expose_secret().to_owned(),
            timeout,
            addresses,
        })
    }
}

#[allow(clippy::unused_async_trait_impl)]
impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: String, node: &BasicNode) -> Self::Network {
        let address = self
            .addresses
            .get(&target)
            .cloned()
            .unwrap_or_else(|| node.addr.trim_end_matches('/').to_owned());
        HttpNetwork {
            client: self.client.clone(),
            address,
            token: self.token.clone(),
            timeout: self.timeout,
        }
    }
}

/// One HTTP raft network client for a single target node.
#[derive(Clone)]
pub struct HttpNetwork {
    client: reqwest::Client,
    address: String,
    token: String,
    timeout: Duration,
}

impl HttpNetwork {
    async fn post<B, R>(&self, path: &str, body: &B) -> Result<R, RPCError<TypeConfig>>
    where
        B: Serialize + ?Sized,
        R: for<'de> Deserialize<'de>,
    {
        let url = format!("{}{}", self.address, path);
        let response = self
            .client
            .post(url)
            .header(TOKEN_HEADER, &self.token)
            .headers(crate::telemetry::injected_headers(
                axum::http::HeaderMap::new(),
            ))
            .timeout(self.timeout)
            .json(body)
            .send()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))?;
        if !response.status().is_success() {
            return Err(RPCError::Unreachable(Unreachable::from_string(format!(
                "raft peer returned status {}",
                response.status()
            ))));
        }
        response
            .json()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::from_string(error.to_string())))
    }
}

impl RaftNetworkV2<TypeConfig> for HttpNetwork {
    type SnapshotData = Vec<u8>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendResult<TypeConfig>, RPCError<TypeConfig>> {
        let prev_log_id = rpc.prev_log_id.clone();
        let last_log_id = rpc
            .entries
            .last()
            .map(|entry| entry.log_id.clone())
            .or_else(|| prev_log_id.clone());
        let response: AppendEntriesResponse<TypeConfig> = self.post("/_raft/append", &rpc).await?;
        Ok(response.into_append_result(prev_log_id, last_log_id))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        self.post("/_raft/vote", &rpc).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf,
        snapshot: SnapshotOf,
        cancel: impl std::future::Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let request = SnapshotRequest {
            vote: vote.clone(),
            meta: snapshot.meta,
            data: snapshot.snapshot,
        };
        let mut task = Box::pin(
            self.post::<SnapshotRequest, SnapshotResponse<TypeConfig>>("/_raft/snapshot", &request),
        );
        let mut cancel = Box::pin(cancel);
        tokio::select! {
            result = &mut task => result.map_err(StreamingError::from),
            reason = &mut cancel => Err(StreamingError::Closed(reason)),
        }
    }
}
