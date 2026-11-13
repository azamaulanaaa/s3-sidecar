//! Deterministic replica placement, asynchronous blob replication, peer fetch,
//! read repair, and replica reconciliation.

use std::collections::BTreeSet;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::Response;
use futures_util::{StreamExt, TryStreamExt};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tracing::warn;

use super::blob::CLUSTER_TOKEN_HEADER;
use super::state::{Metadata, ReplicationJob, ReplicationOp, unix_now};
use super::{Cluster, map_coord_error, metadata_response, set_metadata_headers};
use crate::errors::S3Error;
use crate::storage::{ForwardRequest, StorageError};

const CLAIM_LEASE_SECONDS: i64 = 5;
const RECONCILE_BATCH: usize = 256;
const OWNER_REPAIR_BATCH: usize = 16;
const MAX_RETRY_SECONDS: u64 = 60;

/// Return the owner and its deterministic peer replica set.
///
/// The node list is sorted by id. The first eight bytes of the SHA-256 of
/// `bucket/key` select a start position in the sorted list. The owner is always
/// in the set. If the replica count is larger than the node count, every node is
/// used. A count of one or an empty node list returns only the owner.
#[must_use]
pub fn replica_set(
    nodes: &[String],
    owner: &str,
    replicas: u32,
    bucket: &str,
    key: &str,
) -> Vec<String> {
    let mut sorted = nodes.to_vec();
    sorted.sort();
    sorted.dedup();
    if replicas <= 1 || sorted.is_empty() || !sorted.iter().any(|node| node == owner) {
        return vec![owner.to_owned()];
    }
    let target = usize::try_from(replicas)
        .unwrap_or(usize::MAX)
        .min(sorted.len());
    let digest = Sha256::digest(format!("{bucket}/{key}").as_bytes());
    let mut seed = [0_u8; 8];
    seed.copy_from_slice(&digest[..8]);
    let start = usize::try_from(u64::from_le_bytes(seed)).unwrap_or(usize::MAX) % sorted.len();
    let mut output = vec![owner.to_owned()];
    for step in 0..sorted.len() {
        if output.len() >= target {
            break;
        }
        let candidate = &sorted[(start + step) % sorted.len()];
        if candidate != owner && !output.iter().any(|node| node == candidate) {
            output.push(candidate.clone());
        }
    }
    output
}

impl Cluster {
    fn nodes(&self) -> Vec<String> {
        self.config
            .cluster
            .nodes
            .iter()
            .map(|node| node.id.clone())
            .collect()
    }

    pub(crate) fn replicas_for(&self, bucket: &str, key: &str, replicas: u32) -> Vec<String> {
        replica_set(&self.nodes(), &self.node_id, replicas, bucket, key)
    }

    /// Queue one put job for a newly committed blob.
    pub(crate) fn enqueue_put_replication(&self, bucket: &str, key: &str, metadata: &Metadata) {
        if !metadata.is_committed() || metadata.blob.is_empty() {
            return;
        }
        let targets: Vec<String> = metadata
            .replicas
            .iter()
            .filter(|target| *target != &self.node_id)
            .cloned()
            .collect();
        if targets.is_empty() {
            return;
        }
        let now = unix_now();
        let job = ReplicationJob {
            uuid: metadata.blob.clone(),
            op: ReplicationOp::Put,
            targets,
            done: Vec::new(),
            attempts: 0,
            next_at: now,
            created_at: now,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        };
        if let Err(error) = self.state.enqueue_replication_job(&job) {
            warn!(%error, "cannot queue blob replication");
        }
    }

    /// Queue one delete job for the old replicas of a deleted key.
    pub(crate) fn enqueue_delete_replication(&self, bucket: &str, key: &str, metadata: &Metadata) {
        if metadata.blob.is_empty() {
            return;
        }
        let mut targets = BTreeSet::new();
        for replica in &metadata.replicas {
            if replica != &self.node_id {
                targets.insert(replica.clone());
            }
        }
        if metadata.owner != self.node_id {
            targets.insert(metadata.owner.clone());
        }
        if targets.is_empty() {
            return;
        }
        let now = unix_now();
        let job = ReplicationJob {
            uuid: metadata.blob.clone(),
            op: ReplicationOp::Delete,
            targets: targets.into_iter().collect(),
            done: Vec::new(),
            attempts: 0,
            next_at: now,
            created_at: now,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
        };
        if let Err(error) = self.state.enqueue_replication_job(&job) {
            warn!(%error, "cannot queue blob delete");
        }
    }

    /// Process at most one due replication job. Returns true when a job ran.
    pub(crate) async fn replication_tick(&self) -> bool {
        let job = match self
            .state
            .claim_due_replication_job(unix_now(), CLAIM_LEASE_SECONDS)
        {
            Ok(job) => job,
            Err(error) => {
                warn!(%error, "cannot claim a replication job");
                return false;
            }
        };
        let Some(job) = job else {
            return false;
        };
        self.process_replication_job(&job).await;
        true
    }

    async fn process_replication_job(&self, job: &ReplicationJob) {
        if job.targets.is_empty() {
            let _ = self.state.remove_replication_job(&job.uuid);
            return;
        }
        let mut retry = Vec::new();
        for target in job.targets.clone() {
            if target == self.node_id {
                let _ = self.state.complete_replication_target(&job.uuid, &target);
                continue;
            }
            let Some(address) = self.config.cluster.node_addr(&target).map(str::to_owned) else {
                warn!(node = %target, "replication target is not in cluster.nodes");
                let _ = self.state.complete_replication_target(&job.uuid, &target);
                continue;
            };
            let result = match job.op {
                ReplicationOp::Put => self.send_blob(&address, &job.uuid).await,
                ReplicationOp::Delete => self.send_delete(&address, &job.uuid).await,
            };
            match result {
                Ok(status) if operation_succeeded(job.op, status) => {
                    self.metrics.replication_success();
                    let _ = self.state.complete_replication_target(&job.uuid, &target);
                }
                Ok(status) => {
                    self.metrics
                        .replication_failure(&status.as_u16().to_string());
                    if transient_status(status) {
                        retry.push(target);
                    } else {
                        let _ = self.state.complete_replication_target(&job.uuid, &target);
                    }
                }
                Err(error) => {
                    warn!(%error, node = %target, "replication request failed");
                    self.metrics.replication_failure("transport");
                    retry.push(target);
                }
            }
        }
        if !retry.is_empty() {
            let attempts = job.attempts.saturating_add(1);
            let next_at = next_retry_at(attempts, unix_now());
            if let Err(error) = self
                .state
                .reschedule_replication_job(&job.uuid, attempts, next_at)
            {
                warn!(%error, "cannot reschedule a replication job");
            }
        }
    }

    async fn send_blob(&self, address: &str, uuid: &str) -> Result<StatusCode, BlobError> {
        let response = self
            .client
            .execute(
                &self.credential,
                ForwardRequest {
                    method: Method::GET,
                    path_and_query: format!("/{}/objects/{uuid}", self.blob_bucket()),
                    headers: HeaderMap::new(),
                    body: None,
                    content_length: None,
                },
            )
            .await
            .map_err(BlobError::Local)?;
        if !response.status().is_success() {
            return Ok(response.status());
        }
        let length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let url = format!("{}/_blob/{uuid}", address.trim_end_matches('/'));
        let mut builder = self.http.put(url).header(
            CLUSTER_TOKEN_HEADER,
            self.config.cluster.secret.expose_secret(),
        );
        if let Some(length) = length {
            builder = builder.header(header::CONTENT_LENGTH, length);
        }
        let sent = builder
            .body(reqwest::Body::wrap_stream(response.bytes_stream()))
            .send()
            .await
            .map_err(|_| BlobError::Http)?;
        Ok(sent.status())
    }

    async fn send_delete(&self, address: &str, uuid: &str) -> Result<StatusCode, BlobError> {
        let url = format!("{}/_blob/{uuid}", address.trim_end_matches('/'));
        let response = self
            .http
            .delete(url)
            .header(
                CLUSTER_TOKEN_HEADER,
                self.config.cluster.secret.expose_secret(),
            )
            .send()
            .await
            .map_err(|_| BlobError::Http)?;
        Ok(response.status())
    }

    async fn peer_head(&self, address: &str, uuid: &str) -> bool {
        let url = format!("{}/_blob/{uuid}", address.trim_end_matches('/'));
        self.http
            .head(url)
            .header(
                CLUSTER_TOKEN_HEADER,
                self.config.cluster.secret.expose_secret(),
            )
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }

    /// Restore a missing owner blob from one reachable replica.
    async fn repair_owner_blob(&self, metadata: &Metadata) -> bool {
        for node in &metadata.replicas {
            if node == &self.node_id {
                continue;
            }
            let Some(address) = self.config.cluster.node_addr(node).map(str::to_owned) else {
                continue;
            };
            if !self.peer_head(&address, &metadata.blob).await {
                continue;
            }
            if self.copy_peer_blob(&address, metadata).await {
                return true;
            }
        }
        false
    }

    async fn copy_peer_blob(&self, address: &str, metadata: &Metadata) -> bool {
        let url = format!("{}/_blob/{}", address.trim_end_matches('/'), metadata.blob);
        let response = match self
            .http
            .get(url)
            .header(
                CLUSTER_TOKEN_HEADER,
                self.config.cluster.secret.expose_secret(),
            )
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => response,
            Ok(_) => return false,
            Err(error) => {
                warn!(%error, "owner blob repair fetch failed");
                return false;
            }
        };
        let length = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(metadata.size);
        let stream: crate::auth::body::BodyStream = Box::pin(
            response
                .bytes_stream()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        );
        let request = ForwardRequest {
            method: Method::PUT,
            path_and_query: format!("/{}/objects/{}", self.blob_bucket(), metadata.blob),
            headers: HeaderMap::new(),
            body: Some(stream),
            content_length: Some(length),
        };
        match self.client.execute(&self.credential, request).await {
            Ok(response) => response.status().is_success(),
            Err(error) => {
                warn!(%error, "owner blob repair write failed");
                false
            }
        }
    }

    /// Fetch one blob from the replica set, then from the owner.
    ///
    /// The caller already tried the local backing store.
    pub(crate) async fn fetch_from_peers(
        &self,
        metadata: &Metadata,
        method: &Method,
        headers: &HeaderMap,
    ) -> Result<Option<Response>, S3Error> {
        let mut candidates = metadata.replicas.clone();
        if !candidates.iter().any(|node| node == &metadata.owner) {
            candidates.push(metadata.owner.clone());
        }
        let mut tried = BTreeSet::new();
        for node in candidates {
            if node == self.node_id || !tried.insert(node.clone()) {
                continue;
            }
            let Some(address) = self.config.cluster.node_addr(&node).map(str::to_owned) else {
                continue;
            };
            if !self.peer_head(&address, &metadata.blob).await {
                continue;
            }
            if method == Method::HEAD {
                self.metrics.peer_fetch();
                return Ok(Some(metadata_response(metadata)));
            }
            let url = format!("{}/_blob/{}", address.trim_end_matches('/'), metadata.blob);
            let mut request = self.http.get(url).header(
                CLUSTER_TOKEN_HEADER,
                self.config.cluster.secret.expose_secret(),
            );
            if let Some(range) = headers.get(header::RANGE) {
                request = request.header(header::RANGE, range.clone());
            }
            let response = request.send().await.map_err(|error| {
                warn!(%error, "peer blob fetch failed");
                S3Error::service_unavailable()
            })?;
            if !response.status().is_success() {
                continue;
            }
            self.metrics.peer_fetch();
            let status = response.status();
            let response_headers = response.headers().clone();
            let repair = self.config.cluster.read_repair
                && headers.get(header::RANGE).is_none()
                && status == StatusCode::OK;
            let body = if repair {
                self.metrics.read_repaired();
                self.repair_body(response, metadata)
            } else {
                Body::from_stream(
                    response
                        .bytes_stream()
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
                )
            };
            let mut output = Response::new(body);
            *output.status_mut() = status;
            for (name, value) in &response_headers {
                if skip_header(name.as_str()) {
                    continue;
                }
                output.headers_mut().insert(name.clone(), value.clone());
            }
            set_metadata_headers(metadata, output.headers_mut());
            return Ok(Some(output));
        }
        Ok(None)
    }

    fn repair_body(&self, response: reqwest::Response, metadata: &Metadata) -> Body {
        let (sender, mut receiver) = mpsc::channel::<Result<bytes::Bytes, io::Error>>(2);
        let client = self.client.clone();
        let credential = std::sync::Arc::clone(&self.credential);
        let bucket = self.blob_bucket().to_owned();
        let blob = metadata.blob.clone();
        let length = metadata.size;
        tokio::spawn(async move {
            let stream = async_stream::try_stream! {
                while let Some(item) = receiver.recv().await {
                    yield item?;
                }
            };
            let request = ForwardRequest {
                method: Method::PUT,
                path_and_query: format!("/{bucket}/objects/{blob}"),
                headers: HeaderMap::new(),
                body: Some(Box::pin(stream)),
                content_length: Some(length),
            };
            if let Err(error) = client.execute(&credential, request).await {
                warn!(%error, "read repair write failed");
            }
        });
        let stream: crate::auth::body::BodyStream = Box::pin(async_stream::try_stream! {
            let mut body = response.bytes_stream();
            let mut sender = Some(sender);
            while let Some(chunk) = body.next().await {
                let bytes = chunk.map_err(|error| {
                    io::Error::new(io::ErrorKind::InvalidData, error)
                })?;
                if let Some(channel) = sender.as_ref()
                    && channel.send(Ok(bytes.clone())).await.is_err()
                {
                    sender = None;
                }
                yield bytes;
            }
        });
        Body::from_stream(stream)
    }

    /// Run one bounded replica reconciliation pass.
    ///
    /// # Errors
    ///
    /// Returns an error when metadata cannot be read.
    pub async fn reconcile_once(&self) -> Result<(), S3Error> {
        let rows = self.state.scan_all().map_err(map_coord_error)?;
        let committed: Vec<_> = rows
            .into_iter()
            .filter(|(_, _, metadata)| metadata.is_committed() && !metadata.blob.is_empty())
            .collect();
        let total = committed.len();
        if total == 0 {
            self.metrics.set_reconcile(0, 0, 0);
            return Ok(());
        }
        let start = self.reconcile_cursor.load(Ordering::Relaxed) % total;
        let end = start.saturating_add(RECONCILE_BATCH).min(total);
        let mut checked = 0_u64;
        let mut present = 0_u64;
        let mut missing = 0_u64;
        let mut repairs = 0_usize;
        for (bucket, key, metadata) in &committed[start..end] {
            let intended = if metadata.replicas.is_empty() {
                vec![metadata.owner.clone()]
            } else {
                metadata.replicas.clone()
            };
            for target in intended {
                checked = checked.saturating_add(1);
                let found = if target == self.node_id {
                    self.head_blob(&metadata.blob).await.is_some()
                } else if let Some(address) = self.config.cluster.node_addr(&target) {
                    self.peer_head(address, &metadata.blob).await
                } else {
                    false
                };
                if found {
                    present = present.saturating_add(1);
                    continue;
                }
                missing = missing.saturating_add(1);
                if target == self.node_id {
                    if metadata.owner == self.node_id
                        && repairs < OWNER_REPAIR_BATCH
                        && self.repair_owner_blob(metadata).await
                    {
                        repairs = repairs.saturating_add(1);
                        self.metrics.owner_blob_repaired();
                        present = present.saturating_add(1);
                        missing = missing.saturating_sub(1);
                    } else if metadata.owner == self.node_id && repairs < OWNER_REPAIR_BATCH {
                        repairs = repairs.saturating_add(1);
                        self.metrics.owner_blob_unrecoverable();
                    }
                    continue;
                }
                let now = unix_now();
                let job = ReplicationJob {
                    uuid: metadata.blob.clone(),
                    op: ReplicationOp::Put,
                    targets: vec![target],
                    done: Vec::new(),
                    attempts: 0,
                    next_at: now,
                    created_at: now,
                    bucket: bucket.clone(),
                    key: key.clone(),
                };
                if let Err(error) = self.state.enqueue_replication_job(&job) {
                    warn!(%error, "cannot queue reconciliation replication");
                }
            }
        }
        self.reconcile_cursor.store(end % total, Ordering::Relaxed);
        self.metrics.set_reconcile(checked, present, missing);
        Ok(())
    }
}

enum BlobError {
    Local(StorageError),
    Http,
}

impl std::fmt::Display for BlobError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(error) => write!(formatter, "{error}"),
            Self::Http => formatter.write_str("http transport error"),
        }
    }
}

fn operation_succeeded(op: ReplicationOp, status: StatusCode) -> bool {
    status.is_success() || (op == ReplicationOp::Delete && status == StatusCode::NOT_FOUND)
}

fn transient_status(status: StatusCode) -> bool {
    status.is_server_error()
        || matches!(
            status,
            StatusCode::REQUEST_TIMEOUT
                | StatusCode::CONFLICT
                | StatusCode::TOO_EARLY
                | StatusCode::TOO_MANY_REQUESTS
        )
}

fn next_retry_at(attempts: u32, now: i64) -> i64 {
    let shift = attempts.min(6);
    let base = 1_u64 << shift;
    let base = base.min(MAX_RETRY_SECONDS);
    let jitter = pseudo_random() % (base / 2 + 1);
    now.saturating_add(i64::try_from(base + jitter).unwrap_or(i64::MAX))
}

fn pseudo_random() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let seed = u64::try_from(nanos).unwrap_or(u64::MAX) ^ COUNTER.fetch_add(1, Ordering::Relaxed);
    seed.wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407)
}

fn skip_header(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}
