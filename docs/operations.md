# Operations

This guide covers deployment, consistency, metrics, sizing, security, backup,
and upgrade for s3-sidecar.

## Deployment topologies

### Cache mode

One sidecar sits between the client and two S3 endpoints.

```
client -> sidecar -> cache S3
                  -> origin S3
```

- The cache is a second S3 endpoint. Use a different endpoint, region, or
  provider from origin.
- The client sees one S3 endpoint.
- A read is served from the cache when the entry is fresh.
- A write is accepted by the cache and synced to origin.
- Set `cache.mode = "read"` for a pure read cache.
- Set `cache.mode = "write"` for a write buffer.
- Set `cache.mode = "read_write"` for both.
- Set `cache.mode = "off"` for a pure proxy.
- The cache needs its own bucket. Set `cache.bucket`.
- The journal is under the cache bucket. All sidecars that share a cache bucket
  must use a different `cache.node_id`.

### Single-node cluster

One sidecar with `consensus.backend = "single"`.

- The node holds the only metadata copy.
- The node is the leader.
- The mode is for development and for a single-node deployment with no
  failover.
- A lost `cluster.metadata_path` file loses the namespace.
- Set `replicas = 1` and `min_replicas = 1`.

### Raft cluster

Three or five sidecars with `consensus.backend = "raft"`.

```
client -> sidecar A -> backing S3 A
       -> sidecar B -> backing S3 B
       -> sidecar C -> backing S3 C
              raft metadata
```

- Each sidecar is the only client of its backing S3.
- The raft log and the metadata state machine live in
  `cluster.metadata_path`.
- Set `bootstrap = true` on one node only.
- List every node in `[[consensus.raft.nodes]]`.
- List every sidecar in `[[cluster.nodes]]` for blob replication.
- Use an odd voter count. Three voters survive one failure. Five voters survive
  two failures.
- Blob bytes never enter the raft log.

### Consul cluster

Two or more sidecars with `consensus.backend = "consul"`.

- Consul KV is the metadata authority.
- Each sidecar keeps a materialized `redb` cache in `cluster.metadata_path`.
- The leader holds a Consul session and the `s3-sidecar/leader` key.
- A follower forwards a proposal to the leader.
- The sidecar does not create Consul ACL policy. Grant KV write access under
  `s3-sidecar/` and session create, renew, and destroy access.
- The watcher reconciles the full metadata prefix on each change. Use this
  backend for moderate namespaces.

## Consistency

The knobs are per bucket. They are stored in the metadata backend and seeded
from the config file. A restart does not overwrite them.

The file default is `write_ack = "leader"`, `read = "local_then_peer"`,
`replicas = 1`, and `min_replicas = 1`. A replicated cluster usually sets
`replicas = 3` and `min_replicas = 2`.

### Definitions

- A **commit** is a metadata entry applied by the leader.
- The **owner** is the node that accepted the write.
- **min_replicas** is the number of copies that must hold a blob before a
  quorum write is acknowledged. The owner counts as one copy.
- **replicas** is the number of intended copies. The owner counts as one copy.
- A **read index** is a barrier that makes this node apply every commit that
  completed before the read.
- The **loss window** is the period or failure in which an acknowledged write
  can disappear.

All rows below assume that the metadata backend keeps a quorum. Loss of the
metadata quorum makes metadata unavailable. A restore can also lose the newest
entries. This risk applies whatever the write acknowledgement.

### Matrix

| `write_ack` | `read` | Guarantee | Loss or staleness window |
|---|---|---|---|
| `leader` | `local` | `200` means the leader committed the metadata. The read serves a local blob only. | The owner can die before it copies the blob to a peer, so the write can be lost. A node can serve stale metadata until it applies the newest entry. A committed key with no local blob returns `404` with `x-s3-sidecar-owner`. |
| `leader` | `local_then_peer` | `200` means the leader committed the metadata. The read fetches a missing local blob from a replica or the owner. | The owner can die before it copies the blob to a peer, so the write can be lost. A node can serve stale metadata until it applies the newest entry. If no replica holds the blob, the read returns `404` with `x-s3-sidecar-owner`. |
| `leader` | `leader` | The read index runs first. The read sees every commit that completed before the read. The body comes from a local or peer blob. | The metadata view is current. The body can still be absent if the owner died before replication, so the read returns `404`. The write loss window lasts until the first peer copy completes. |
| `quorum` | `local` | `200` means the metadata committed and at least `min_replicas` nodes hold the blob. The read serves a local blob only. | Loss requires the loss of all `min_replicas` copies before asynchronous repair. A node can serve stale metadata until it applies the newest entry. A missing local blob returns `404`. |
| `quorum` | `local_then_peer` | `200` means the metadata committed and at least `min_replicas` nodes hold the blob. The read fetches a missing local blob from a replica or the owner. | Loss requires the loss of all `min_replicas` copies before asynchronous repair. A node can serve stale metadata until it applies the newest entry. |
| `quorum` | `leader` | The read index runs first. The read sees every commit that completed before the read. The body comes from any of the `min_replicas` copies. | The metadata view is current. Loss requires the loss of all `min_replicas` copies. |
| `all` | `local` | `200` means every intended replica holds the blob. The read serves a local blob only. | Loss requires the loss of every intended replica. A node can serve stale metadata until it applies the newest entry. A missing local blob returns `404`. |
| `all` | `local_then_peer` | `200` means every intended replica holds the blob. The read fetches a missing local blob from a replica or the owner. | Loss requires the loss of every intended replica. A node can serve stale metadata until it applies the newest entry. |
| `all` | `leader` | The read index runs first. The read sees every commit that completed before the read. The body comes from any intended replica. | Loss requires the loss of every intended replica. The metadata view is current. |

`write_ack = quorum` needs `min_replicas - 1` peer acknowledgements. A quorum
or all failure deletes the staged blob and returns `503 ServiceUnavailable`.

A read that finds committed metadata but no reachable copy returns `404
NoSuchKey` with `x-s3-sidecar-owner`. Do not treat this `404` as proof that the
key never existed.

## Metrics

The sidecar exports metrics with OpenTelemetry over OTLP. Export is off by
default. There is no local metrics endpoint. `GET /healthz` reports the process.
`GET /readyz` reports readiness.

Set `otel.enabled = true` to start export.

```toml
[otel]
enabled = true
service_name = "s3-sidecar"
service_version = "0.1.0"
endpoint = "http://127.0.0.1:4317"
protocol = "grpc"                # grpc | http/protobuf
headers = { "x-api-key" = "..." }
timeout = "10s"
export_interval = "60s"
export_timeout = "10s"
resource_attributes = { "deployment.environment.name" = "production" }
```

| Setting | Default | Meaning |
|---|---|---|
| `otel.enabled` | `false` | Send metrics over OTLP. |
| `otel.service_name` | `s3-sidecar` | The `service.name` resource attribute. |
| `otel.service_version` | `0.1.0` | The `service.version` resource attribute. |
| `otel.endpoint` | unset | Collector URL. |
| `otel.protocol` | `grpc` | `grpc` or `http/protobuf`. |
| `otel.headers` | empty | OTLP request headers. Values are secret. |
| `otel.headers_file` | unset | File with a JSON object of OTLP headers. |
| `otel.timeout` | `10s` | Timeout for one export request. |
| `otel.export_interval` | `60s` | Interval between exports. |
| `otel.export_timeout` | `10s` | Bound for the final flush at shutdown. |
| `otel.resource_attributes` | empty | Extra resource attributes. |

Environment fallbacks apply when a setting is unset:

| Setting | Fallback |
|---|---|
| `otel.endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` |
| `otel.headers` | `OTEL_EXPORTER_OTLP_HEADERS` |
| `otel.service_name` | `OTEL_SERVICE_NAME`, then `s3-sidecar` |
| `otel.protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL`, then `grpc` |
| `otel.export_interval` | `OTEL_METRIC_EXPORT_INTERVAL`, then `60s` |

The exporter adds `service.name`, `service.version`, and `service.instance.id`
to the resource. `service.instance.id` is the cluster node id when present,
otherwise the hostname. The SDK adds the `telemetry.sdk.*` attributes.

For HTTP, use the HTTP collector port. The sidecar appends `/v1/metrics` to the
configured endpoint when the path is not already present.

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
s3-sidecar serve --otel-enabled
```

All instrument names start with `s3_sidecar.` and use dots. The tables below
use that prefix once per table.

### Cache

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `cache.hits` | counter | `1` | | Cache hits. |
| `cache.misses` | counter | `1` | | Cache misses. |
| `cache.bytes_served` | counter | `By` | | Bytes sent to clients from the cache. |
| `cache.origin_bytes` | counter | `By` | | Bytes fetched from origin. |
| `cache.revalidations` | counter | `1` | | Conditional origin requests that returned `304`. |
| `cache.sync.success` | counter | `1` | | Syncs that reached origin. |
| `cache.sync.failure` | counter | `1` | | Sync attempts that failed. |
| `cache.quarantine` | counter | `1` | | Entries moved to quarantine. |
| `cache.evictions` | counter | `1` | | Cache entries removed by eviction. |
| `cache.fallbacks` | counter | `1` | | Cache failures that fell back to origin. |
| `cache.pending_writes` | gauge | `1` | | Pending write and delete journal entries. |
| `cache.sync_lag` | gauge | `s` | | Age of the oldest pending journal entry. |
| `cache.tracked_bytes` | gauge | `By` | | Bytes tracked by the LRU index. |
| `cache.max_bytes` | gauge | `By` | | Configured cache limit. Zero means unlimited. |
| `cache.unlimited` | gauge | `1` | | One when the cache has no size limit. |

### Cluster data path

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `cluster.committed_writes` | counter | `1` | | Cluster writes that reached the committed state. |
| `cluster.aborted_writes` | counter | `1` | | Cluster writes that were rejected and removed. |
| `cluster.commit.duration` | histogram | `ms` | | Duration of one cluster commit. |
| `cluster.read_local_hits` | counter | `1` | | Reads served from local metadata and a local blob. |
| `cluster.missing_blob_reads` | counter | `1` | | Reads where the blob was missing. |
| `cluster.tombstones` | gauge | `1` | | Current cluster tombstone count. |
| `cluster.gc_deletions` | counter | `1` | | Blobs removed by cluster garbage collection. |
| `cluster.applied_index` | gauge | `1` | | Applied metadata index. |
| `cluster.term` | gauge | `1` | | Current coordinator term. |
| `cluster.read_index_calls` | counter | `1` | | Calls to the coordinator read index. |
| `cluster.peer_fetches` | counter | `1` | | Reads served by a peer blob fetch. |
| `cluster.read_repairs` | counter | `1` | | Peer reads that stored the blob locally. |
| `cluster.replication.success` | counter | `1` | | Replication operations that reached a target. |
| `cluster.replication.failures` | counter | `1` | `status` | Replication failures by status or transport class. |
| `cluster.replication.queue_depth` | gauge | `1` | | Pending replication jobs. |
| `cluster.replication.oldest_age` | gauge | `s` | | Age of the oldest pending replication job. |
| `cluster.reconcile.checked` | counter | `1` | | Replica targets checked by reconciliation. |
| `cluster.reconcile.present` | counter | `1` | | Replica targets present during reconciliation. |
| `cluster.reconcile.missing` | counter | `1` | | Replica targets missing during reconciliation. |
| `cluster.owner_blobs.repaired` | counter | `1` | | Owner blobs restored by reconciliation. |
| `cluster.owner_blobs.unrecoverable` | counter | `1` | | Owner blobs that no replica could supply. |
| `cluster.write_ack` | counter | `1` | `policy` | Accepted writes by acknowledgement policy. |
| `cluster.quorum.success` | counter | `1` | | Quorum writes that reached the required acknowledgements. |
| `cluster.quorum.failure` | counter | `1` | | Quorum writes that did not reach the required acknowledgements. |
| `cluster.sync_replication.duration` | histogram | `ms` | | Duration of one synchronous peer copy. |
| `cluster.replica_acks` | counter | `1` | | Peer copies that acknowledged the blob. |
| `cluster.replica_ack_failures` | counter | `1` | | Peer copies that failed or timed out. |

### Raft

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `raft.term` | gauge | `1` | | Current raft term. |
| `raft.leader` | gauge | `1` | `id` | One for the current leader. Zero with `id=""` when there is no leader. |
| `raft.last_log_index` | gauge | `1` | | Last raft log index. |
| `raft.last_applied` | gauge | `1` | | Last applied raft log index. |
| `raft.snapshot_index` | gauge | `1` | | Last snapshot raft log index. |
| `raft.elections` | counter | `1` | | Observed raft leader elections. |

### Consul

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `consul.leader` | gauge | `1` | `id` | One for the current leader. Zero with `id=""` when there is no leader. |
| `consul.materialized` | gauge | `1` | | One when the first Consul reconcile completed. |
| `consul.session.renewals` | counter | `1` | | Successful Consul session renewals. |
| `consul.session.losses` | counter | `1` | | Consul sessions that ended or failed to renew. |
| `consul.leader.changes` | counter | `1` | | Consul leadership acquisitions. |

The exporter logs a warning when an export fails. Export failure does not stop
the sidecar. Shutdown flushes pending metrics before the process exits.

## Sizing

| Setting | Default | Effect |
|---|---|---|
| `cache.max_bytes` | required when cache mode is not `off` | Cache byte limit. `0` means unlimited. A finite value enables the LRU index and eviction. |
| `cache.max_object_bytes` | 1 GiB | Bodies larger than this value do not enter the cache. Range reads are not cached. |
| `cache.high_watermark` | 0.9 | Eviction starts above this fraction of `max_bytes`. |
| `cache.low_watermark` | 0.8 | Eviction stops at or below this fraction. |
| `cache.sync_workers` | 4 | Concurrent key syncs to origin. |
| `cache.sync_max_attempts` | 5 | Retries before a sync is quarantined. |
| `replicas` | 1 | Intended blob copies, including the owner. Must not exceed `cluster.nodes.len()`. |
| `min_replicas` | 1 | Copies that must hold a blob before a quorum write is acknowledged. Must be at least 1 and at most `replicas`. |
| `cluster.replicate_workers` | 4 | Concurrent asynchronous replication jobs per node. |
| `cluster.replicate_timeout` | 10s | Bound for one synchronous peer copy. |
| `server.max_connections` | 1024 | Concurrent S3 requests. An excess request returns `503 SlowDown` with `Retry-After: 1`. |
| `server.max_body_bytes` | 5 GiB | Largest request body. A larger body returns `413 EntityTooLarge`. |
| `upstream.retry_attempts` | 2 | Retries after the first attempt for an idempotent request. |
| `upstream.connect_timeout` | 10s | Bound for the upstream connection phase. |
| `upstream.request_timeout` | 5m | Bound for upstream response headers on a control request. |

Set `cache.max_bytes` to the working set plus headroom. Do not set it below
`cache.max_object_bytes` times the expected concurrent object count.

`replicas` and `min_replicas` trade availability against write latency. A
`write_ack = quorum` write waits for `min_replicas - 1` peer copies. A
`write_ack = all` write waits for every intended copy.

Set `server.max_connections` to a value that the upstream and the backing S3
can serve. Each in-flight request holds one upstream connection. Each
replication worker holds one connection per target.

## Security

### TLS

Set `[server.tls]` with `cert_file` and `key_file` to serve HTTPS.

- The sidecar reads and parses both files at startup.
- A missing, unreadable, or invalid file stops startup.
- Client certificate authentication is not implemented.
- Automatic certificate reload is not implemented. Restart the process after a
  certificate change.
- Plain HTTP is the default when the section is absent. Use TLS for any client
  that sends a secret or an unsigned trailer body.

### Internal tokens

| Setting | Header | Protects |
|---|---|---|
| `cluster.secret` | `x-s3-sidecar-cluster-token` | The `/_blob/` routes. |
| `consensus.raft.secret` | `x-s3-sidecar-raft-token` | The `/_raft/` routes. |
| `consensus.consul.secret` | `x-s3-sidecar-consul-token` | The `/_coord/propose` route. |
| `consensus.consul.token` | `X-Consul-Token` | Calls from the sidecar to the Consul HTTP API. |

- Set a different value for each token.
- Do not put a token in `argv`. Use the `_FILE` environment variable, the
  secret-file flag, or the stdin flag.
- Keep the internal routes on a private network. The tokens are shared bearer
  secrets, not per-node identities.
- An internal route with a missing or wrong token returns `401`.

### Client credentials

The `[[clients]]` map holds each client access key, secret, optional session
token, and bucket allow-list.

- The sidecar verifies the client signature locally.
- The sidecar re-signs each synchronous upstream request with the client
  secret.
- The sidecar checks the bucket allow-list before the upstream call.
- An asynchronous cache sync uses the stored origin service credential, not the
  client credential.
- Origin does not see the client identity for an asynchronously synced write.
- The sidecar stores client secrets in memory and in the config source. This is
  an accepted risk for v1. The bucket allow-list is the mitigation.
- The later option is one service credential with mutual TLS or a secret
  manager.

## Backup and restore

Stop the process before you copy a `redb` file. A running process can hold a
write transaction, and a raw copy can be inconsistent.

### What is node-local and what is replicated

| Data | Location | Replicated | Backup |
|---|---|---|---|
| Cache blobs, metadata, journal | Cache S3 bucket | By the cache provider | Provider snapshot or bucket copy. |
| Cache LRU index | `cache.index_path` `redb` file | No. It is advisory. | Not required. The index rebuilds lazily. |
| Cluster blobs | `backing.bucket` on each node's S3 | No. Copied by replication. | Back up each backing bucket. |
| Single-node metadata | `cluster.metadata_path` `redb` file | No. It is the only copy. | Stop the process. Copy the file. |
| Raft metadata | `cluster.metadata_path` `redb` file | Yes, through the raft log and snapshots. | Raft snapshot, or stop each node and copy the file. |
| Consul metadata | Consul KV | Yes, through the Consul quorum. | Consul snapshot. |
| Consul materialized cache | `cluster.metadata_path` `redb` file | No. It is a cache. | Not required. It rebuilds from Consul. |

### Cache mode

- The origin owns the data. Do not use the cache as the origin backup.
- The cache bucket holds blobs, metadata, and journal records.
- If the cache is lost before a sync, the write is lost. A cache backup can
  restore a pending write.
- Copy the cache bucket with a provider snapshot, or stop the sidecar and copy
  the bucket.
- The LRU index is not part of the backup.

### Raft

- A raft snapshot holds the metadata, bucket config, uploads, last applied log
  id, and membership.
- Copy the snapshot with the provider or file copy method for your deployment.
- The snapshot does not contain blob bytes. Back up each `backing.bucket`.
- To restore, stop every node. Restore one metadata generation on every node.
  Do not mix log or snapshot generations.
- If you restore only the metadata and the blobs are gone, reads return `404`
  with `x-s3-sidecar-owner`.

### Consul

- Take a Consul snapshot of the KV namespace under `s3-sidecar/`.
- The `cluster.metadata_path` files are caches. Delete them after a restore and
  let each sidecar rematerialize from Consul.
- Back up each `backing.bucket` for blob bytes.
- A Consul restore and a blob restore must use the same point in time.

### Single node

- Stop the process.
- Copy `cluster.metadata_path`.
- Copy `backing.bucket`.
- Restore both files and buckets together.

## Upgrade

Do not change a bucket consistency knob and the binary in the same step.

### Cache mode

1. Stop new traffic to one sidecar.
2. Let in-flight requests finish, or wait for `server.shutdown_timeout`.
3. Replace the binary.
4. Start the sidecar with the same config.
5. Check `GET /readyz`.
6. Check `s3_sidecar_cache_pending_writes` and the journal log.
7. Move to the next sidecar.

The journal resumes from the cache bucket. A crash before a sync is safe when
the journal record exists.

### Single-node cluster

1. Stop the process.
2. Back up `cluster.metadata_path` and the backing bucket.
3. Replace the binary.
4. Start the sidecar.
5. Check `GET /readyz` and `s3_sidecar_cluster_applied_index`.
6. Run a read and a write on a test key.

### Raft cluster

1. Upgrade one follower at a time.
2. Stop the follower.
3. Replace the binary.
4. Start the follower.
5. Check `GET /readyz`, `s3_sidecar_raft_leader`, and
   `s3_sidecar_raft_last_applied`.
6. Wait until the follower applied the current index.
7. Repeat for the other followers.
8. Upgrade the leader last. Or move leadership first with a controlled step
   down.
9. Check that `s3_sidecar_raft_elections_total` does not grow after the
   upgrade.

Do not delete `cluster.metadata_path` on a raft node. The log is part of the
replicated state.

### Consul cluster

1. Upgrade one follower at a time.
2. Stop the follower.
3. Replace the binary.
4. Start the follower.
5. Check `GET /readyz` and `s3_sidecar_consul_materialized`.
6. Wait for the first full reconcile.
7. Repeat for the other followers.
8. Upgrade the leader last. The session ends and a new leader is elected.
9. Check `s3_sidecar_consul_session_losses_total` and
   `s3_sidecar_consul_leader_changes_total`.

## Benchmark prerequisites

The benchmark script needs a local S3 server and an S3 client.

- Put `rustfs` or `minio` on `PATH`, or set `RUSTFS_BIN` or `MINIO_BIN`.
- Put `aws` or `mc` on `PATH`, or set `AWS_BIN` or `MC_BIN`.
- Install `curl`.

The script checks for the S3 client at startup. If neither client exists, it
prints an error and exits non-zero. If a test bucket already exists, the script
skips creation.

Benchmarks were not run for this repository snapshot because no S3 binary or
S3 client was available.

## Known limits

- No S3 versioning.
- No ACLs, bucket policies, or object tagging.
- No `Select`.
- Multipart checksum headers are dropped. A client that sends part checksums
  in `CompleteMultipartUpload` can fail.
- No negative cache. A missing key returns to origin on each read.
- Multipart upload records are node-local. A request must reach the same node
  for the whole upload.
- A forwarded cluster proposal has no idempotency key. A commit followed by a
  lost response can repeat a non-idempotent operation on a client retry.
- The raft snapshot threshold is fixed at 64 log entries.
- The Consul watcher scans the full metadata prefix on each change.
- Garbage collection deletes one object at a time. A large cleanup takes
  several passes.
- The real-service end-to-end tests are ignored by default. They run only when
  the S3 binaries and the AWS CLI are present.
- `ListBuckets` and bucket operations that need a real bucket view return
  `NotImplemented` in cluster mode.
- Path-style bucket addressing only.
- One origin endpoint and one cache endpoint.
- Replication retries have no maximum attempt count. A permanent target outage
  keeps a job in the queue with a bounded retry interval.
- Read repair does not change metadata. The replica list is the intended set,
  not a live inventory.
- A TLS listener serves one certificate. Client certificate authentication and
  automatic certificate reload are not implemented.
