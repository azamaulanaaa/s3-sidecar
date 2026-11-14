# Runbook

Use this guide when a sidecar deployment has a problem. Each section gives the
symptom, the check, and the action.

Use the `x-s3-sidecar-request-id` response header to find the matching log
entry.

## No leader or no metadata quorum

**Symptom**

- Writes return `503 ServiceUnavailable` with `Retry-After: 1`.
- `GET /readyz` returns `503`.
- Reads may continue on a node with applied metadata.

**Check**

1. Read the body of `GET /readyz`.
2. For raft, check `s3_sidecar_raft_leader` and `s3_sidecar_raft_term`.
3. For Consul, check `s3_sidecar_consul_leader` and
   `s3_sidecar_consul_materialized`.
4. Check the logs for `leader`, `quorum`, and `election`.
5. Check the network path to each voter or to Consul.

**Action**

1. Start or restart the failed voters.
2. For raft, confirm that one node has `bootstrap = true`.
3. For raft, confirm that every `[[consensus.raft.nodes]]` entry has the right
   address.
4. For Consul, confirm that the Consul cluster has a quorum.
5. For Consul, confirm that the ACL token can read and write under
   `s3-sidecar/` and can create, renew, and destroy sessions.
6. Do not delete `cluster.metadata_path` to force an election.
7. If the quorum cannot return, restore metadata from a backup.

## Replication backlog or high sync lag

**Symptom**

- `s3_sidecar_cache_sync_lag_seconds` grows.
- `s3_sidecar_cache_pending_writes` grows.
- `s3_sidecar_cluster_replication_queue_depth` or
  `s3_sidecar_cluster_replication_oldest_age_seconds` grows.
- `s3_sidecar_cluster_replication_failures_total` increases.

**Check**

1. Check `s3_sidecar_cache_sync_failure_total` and
   `s3_sidecar_cluster_replica_ack_failures_total`.
2. Check the logs for `sync`, `replication`, and `blob`.
3. Check the origin or the peer for `5xx`, `429`, and timeouts.
4. Check the credentials for the origin service account.
5. Check `cache.sync_workers` and `cluster.replicate_workers`.
6. Check `cluster.replicate_timeout`.

**Action**

1. Fix the origin or peer error before you change the queue settings.
2. Raise `cache.sync_workers` when one key is not the bottleneck.
3. Raise `cluster.replicate_workers` when the queue has many jobs.
4. Do not delete journal records to clear the queue.
5. Do not delete a blob that metadata still references.
6. A permanent target failure leaves a job in the queue. Remove the target from
   `cluster.nodes` only after you confirm that the node is gone.

## Quarantined cache writes

**Symptom**

- `s3_sidecar_cache_quarantine_total` increases.
- A read of the key returns `404`.
- The log says `sync quarantined`.

**Check**

1. Read the `reason` field in the log line.
2. Check the origin status code in the log.
3. Check the origin bucket name and region.
4. Check the origin service credential and its bucket policy.
5. Check whether `cache.serve_quarantined` is true.

**Action**

1. A `400`, `403`, or bucket `404` is permanent. Fix the bucket or credential.
2. A `5xx`, timeout, or throttle is transient. Fix the origin and let the
   retries run.
3. After you fix a permanent error, restart the sidecar to retry the failed
   journal entries.
4. Keep the blob and the journal record until the sync succeeds.
5. Enable `cache.serve_quarantined` only when a stale body is better than a
   `404`.

## `412 PreconditionFailed` on a cache write

**Symptom**

- A write-back `PUT` or `DELETE` returns `412 PreconditionFailed`.
- The request has `If-Match` or `If-None-Match`.
- The origin receives no request when the cache has an entry.

**Check**

1. Check whether the cache has an entry for the key.
2. Compare the condition with the cached ETag.
3. Use `x-s3-sidecar-request-id` to find the request log entry.
4. Enable `otel.record_object_keys` when you need the object key in the log.
5. Check the bucket `write_policy` value.

**Action**

1. Read the key and use its current ETag in `If-Match`.
2. Remove `If-None-Match: *` when the key already exists in the cache.
3. If the cache has no entry, origin checks the condition. Check the origin
   status.
4. Do not retry a failed condition with the same ETag.
5. Set `otel.record_object_keys = false` after the diagnosis.

## Cache bucket full or eviction storms

**Symptom**

- `s3_sidecar_cache_evictions_total` grows fast.
- The cache hit ratio falls.
- The cache provider reports a full bucket or a quota error.
- `s3_sidecar_cache_tracked_bytes` stays near `s3_sidecar_cache_max_bytes`.

**Check**

1. Compare `s3_sidecar_cache_tracked_bytes` with
   `s3_sidecar_cache_max_bytes`.
2. Check `s3_sidecar_cache_unlimited`.
3. Check `s3_sidecar_cache_pending_writes`.
4. Check the cache provider bucket size and quota.
5. Check the object size distribution against `cache.max_object_bytes`.

**Action**

1. Raise `cache.max_bytes` when the working set is larger than the limit.
2. Add capacity to the cache bucket.
3. Confirm that the S3 lifecycle rule targets only the cache prefix.
4. Reduce the TTL when the objects change often.
5. Do not set `cache.max_bytes` below the expected concurrent object size.
6. Eviction skips pending and quarantined entries. Clear the sync backlog
   before you expect eviction to free space.

## Consul session loss

**Symptom**

- `s3_sidecar_consul_session_losses_total` increases.
- `s3_sidecar_consul_leader_changes_total` increases.
- Writes return `503` during the election.
- The log says `session` or `leader`.

**Check**

1. Check the Consul server health and quorum.
2. Check the network path from the sidecar to `consensus.consul.address`.
3. Check the Consul ACL token.
4. Check `consensus.consul.session_ttl`.
5. Check the system clock on the sidecar and the Consul servers.

**Action**

1. Restore the Consul quorum first.
2. Fix the ACL token or the network path.
3. Keep `session_ttl` at `10s` or more.
4. Let the sidecar acquire a new session and leader key.
5. Check `s3_sidecar_consul_materialized` before you send writes.
6. Do not edit the `s3-sidecar/leader` key by hand.

## Raft snapshot failure or log growth

**Symptom**

- `s3_sidecar_raft_last_log_index` grows while
  `s3_sidecar_raft_snapshot_index` stays the same.
- The log says `snapshot` and an error.
- Disk use for `cluster.metadata_path` grows.

**Check**

1. Subtract `s3_sidecar_raft_snapshot_index` from
   `s3_sidecar_raft_last_log_index`.
2. Check free disk space for `cluster.metadata_path`.
3. Check file permissions on `cluster.metadata_path`.
4. Check the log for a snapshot install or export error.
5. Check `s3_sidecar_raft_last_applied`.

**Action**

1. Free disk space before you restart the node.
2. Keep `cluster.metadata_path` on durable local storage.
3. Restart the node after you fix the disk or permission problem.
4. Do not delete the raft log or the metadata file by hand.
5. The snapshot threshold is fixed at 64 log entries. Do not expect a config
   knob.
6. If the snapshot keeps failing, collect the logs and the file system state
   before you restart the whole cluster.

## TLS certificate expiry or startup failure

**Symptom**

- The process exits at startup with a TLS error.
- Clients report a certificate error or an expired certificate.
- `GET /readyz` fails over HTTPS.

**Check**

1. Check the certificate end date:
   `openssl x509 -enddate -noout -in cert.pem`.
2. Check that `server.tls.cert_file` and `server.tls.key_file` exist and are
   readable by the process user.
3. Check that the certificate chain is in PEM form.
4. Check that the private key matches the certificate.
5. Check the startup log for the file path and the parse error.

**Action**

1. Replace the certificate and key with a valid pair.
2. Restart the sidecar. The sidecar does not reload a certificate at runtime.
3. Check `GET /readyz` over HTTPS.
4. Check a client `HEAD` request through the listener.
5. Do not disable TLS to work around an expired certificate.

## `501 NotImplemented` with an encryption header

**Symptom**

- A write, copy, multipart start, part upload, or multipart complete returns
  `501 NotImplemented`.
- The S3 XML code is `NotImplemented`.
- The request has an `x-amz-server-side-encryption` header.

**Check**

1. Read the request headers for `x-amz-server-side-encryption` and
   `x-amz-server-side-encryption-customer-*`.
2. Check `cache.mode` or `mode = "cluster"`.
3. For cluster mode, check the default encryption setting on the backing
   bucket.
4. Read the S3 error message for `default encryption`.

**Action**

1. In cache `write` or `read_write`, remove the SSE-C headers.
2. Use `cache.mode = "read"` for SSE-C.
3. In cluster mode, remove the encryption headers from the write and copy
   requests.
4. In cluster mode, remove the encryption headers from `UploadPart` and
   `CompleteMultipartUpload`.
5. In cluster mode, enable default encryption on the backing bucket.

## `403 InvalidToken`

**Symptom**

- A request returns `403 Forbidden`.
- The S3 XML code is `InvalidToken`.

**Check**

1. Find the `[[clients]]` entry for the request access key.
2. Check whether `session_token` is set on the entry.
3. For header authentication, check `x-amz-security-token`.
4. For presigned authentication, check `X-Amz-Security-Token` in the query.
5. Use `x-s3-sidecar-request-id` to match the request with the client
   application.

**Action**

1. Send the same token that the client entry configures.
2. Update `clients[].session_token` when the token rotates.
3. Remove `session_token` from the entry only when the client sends no token.
4. Do not put a session token in `argv`.

## Read 404 with x-s3-sidecar-owner

**Symptom**

- A `GET` or `HEAD` returns `404 NoSuchKey`.
- The response has an `x-s3-sidecar-owner` header.
- `s3_sidecar_cluster_missing_blob_reads_total` increases.

**Check**

1. Read the `x-s3-sidecar-owner` value.
2. Find the request id in the sidecar log.
3. Check `s3_sidecar_cluster_peer_fetches_total` and
   `s3_sidecar_cluster_owner_blobs_unrecoverable_total`.
4. Check `s3_sidecar_cluster_replication_queue_depth` and
   `s3_sidecar_cluster_reconcile_missing_total`.
5. Check the health of the owner node and the replica nodes.
6. Check the `read` mode for the bucket.

**Action**

1. This `404` means the metadata committed but no reachable node has the blob.
2. Do not delete the metadata entry.
3. Restore the owner node or a replica node.
4. Let reconciliation copy a missing blob from a reachable replica.
5. If no replica has the blob, restore the blob and the metadata from the same
   backup point.
6. A `read = local` miss also returns the owner header. Confirm that the key
   exists on another node before you treat the object as lost.

## `400 Bad Request` on `/_blob/`

**Symptom**

- A peer request to `/_blob/<id>` returns `400 Bad Request`.
- The request has a valid cluster token.

**Check**

1. Read the id in the request path.
2. Confirm that the id has 48 characters.
3. Confirm that the id uses lowercase hexadecimal characters only.
4. Confirm that `x-s3-sidecar-cluster-token` matches `cluster.secret`.
5. Confirm that the sender uses a blob id from metadata or a replication job.
6. Confirm that no proxy rewrites the path.

**Action**

1. Use the blob id from the replication job or the metadata entry.
2. Do not edit, shorten, or change the case of a blob id.
3. Check the sidecar version on the sender and the receiver.
4. A missing or wrong cluster token returns `401`, not `400`.
5. A bad id is a sender defect. Do not retry the same id.

## Graceful shutdown timeout

**Symptom**

- The process does not exit after `SIGTERM` or `SIGINT`.
- The exit status is non-zero.
- The log contains `graceful shutdown timed out`.

**Check**

1. Check `server.shutdown_timeout`.
2. Check for a long upload or download in the access log.
3. Check for a slow client.
4. Check the upstream for a stalled response.
5. Check `s3_sidecar_cache_pending_writes`.

**Action**

1. Raise `server.shutdown_timeout` when the workload has long transfers.
2. Fix the slow client or upstream.
3. Wait for the drain to finish.
4. If the process is still stuck, collect the log and send `SIGKILL`.
5. After a forced stop, check the cache journal and the `redb` files at the next
   start.
6. Check that pending journal entries resume after the restart.
