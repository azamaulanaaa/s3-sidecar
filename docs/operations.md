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
- The cache persists and replays a fixed set of object headers.
- A cached read evaluates the `If-Match`, `If-None-Match`,
  `If-Modified-Since`, and `If-Unmodified-Since` headers.
- A write-back `PUT` and `DELETE` check their conditions against the cached
  entry.
- Set `cache.mode = "read"` for a pure read cache.
- Set `cache.mode = "write"` for a write buffer.
- Set `cache.mode = "read_write"` for both.
- Set `cache.mode = "off"` for a pure proxy.
- Cache `write` and `read_write` reject SSE-C with `501 NotImplemented`.
- Cache `read` forwards SSE-C to origin and keeps the cache out of the path.
- Pure proxy mode is the supported mode for SSE-C.
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

### Cache mode

The cluster matrix does not apply to cache mode. A cache entry is two S3
objects: a blob and a metadata object. The sidecar writes the blob, then the
journal record, then the metadata object. There is no atomic switch between the
blob and the metadata object.

A different sidecar that reads the shared cache bucket during a write can
observe old metadata with new bytes. The accepting sidecar hides the window
after it installs the pending entry. A read before that install can still
observe the mix. Another sidecar sees the write after the sync completes.

A cached `GET` or `HEAD` evaluates conditional headers against the cached entry.
The cache does not evaluate `If-Range`. A conditional range request can return
`206 PartialContent` when the validator does not match.

## OpenTelemetry

The sidecar exports metrics, traces, and logs with OpenTelemetry over OTLP.
Export is off by default. There is no local metrics endpoint. `GET /healthz`
reports the process. `GET /readyz` reports readiness.

`otel.enabled` is the master switch. `otel.metrics` defaults to `true`, so an
existing metrics configuration keeps working. `otel.traces` and `otel.logs`
default to `false`.

```toml
[otel]
enabled = true
metrics = true
traces = true
logs = true
trace_sample_ratio = 1.0
trace_endpoint = "http://127.0.0.1:4317"
logs_endpoint = "http://127.0.0.1:4317"
trace_schedule_delay = "5s"
log_schedule_delay = "5s"
record_object_keys = false
service_name = "s3-sidecar"
service_version = "0.2.0"
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
| `otel.enabled` | `false` | Master switch for all OTLP export. |
| `otel.metrics` | `true` | Export metrics over OTLP. |
| `otel.traces` | `false` | Export traces over OTLP. |
| `otel.logs` | `false` | Export logs over OTLP. |
| `otel.service_name` | `s3-sidecar` | The `service.name` resource attribute. |
| `otel.service_version` | `crate version` | The `service.version` resource attribute. |
| `otel.endpoint` | unset | Collector URL for every enabled signal. |
| `otel.trace_endpoint` | unset | Trace collector URL. Falls back to `otel.endpoint`. |
| `otel.logs_endpoint` | unset | Log collector URL. Falls back to `otel.endpoint`. |
| `otel.trace_sample_ratio` | `1.0` | Fraction of root traces to sample, from `0.0` to `1.0`. |
| `otel.protocol` | `grpc` | `grpc` or `http/protobuf`. |
| `otel.headers` | empty | OTLP request headers. Values are secret. |
| `otel.headers_file` | unset | File with a JSON object of OTLP headers. |
| `otel.timeout` | `10s` | Timeout for one export request. |
| `otel.export_interval` | `60s` | Interval between metric exports. |
| `otel.trace_schedule_delay` | `5s` | Delay between trace exports. |
| `otel.log_schedule_delay` | `5s` | Delay between log exports. |
| `otel.export_timeout` | `10s` | Bound for the final flush at shutdown. |
| `otel.resource_attributes` | empty | Extra resource attributes. |
| `otel.record_object_keys` | `false` | Add `s3.object.key` and keep the object key in `url.path`. |

Environment fallbacks apply when a setting is unset:

| Setting | Fallback |
|---|---|
| `otel.endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` |
| `otel.trace_endpoint` | `otel.endpoint`, then `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` |
| `otel.logs_endpoint` | `otel.endpoint`, then `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` |
| `otel.headers` | `OTEL_EXPORTER_OTLP_HEADERS` |
| `otel.service_name` | `OTEL_SERVICE_NAME`, then `s3-sidecar` |
| `otel.protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL`, then `grpc` |
| `otel.export_interval` | `OTEL_METRIC_EXPORT_INTERVAL`, then `60s` |

The exporter adds `service.name`, `service.version`, and `service.instance.id`
to the resource. `service.instance.id` is the cluster node id when present,
otherwise the hostname. The SDK adds the `telemetry.sdk.*` attributes. Metrics,
traces, and logs use the same resource.

For HTTP, use the HTTP collector port. The sidecar appends `/v1/metrics`,
`/v1/traces`, or `/v1/logs` when the path is not already present.

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
s3-sidecar serve --otel-enabled --otel-traces --otel-logs
```

### Traces

A trace starts at `s3.request`. The sidecar extracts the W3C `traceparent` and
`tracestate` headers from an incoming S3 request and sets the server span
parent. It injects the current W3C context into every upstream S3 request,
cache request, backing S3 request, peer blob request, raft RPC, and Consul RPC.
The origin receives `traceparent`.

Sampling uses `ParentBased(TraceIdRatioBased(ratio))`. A ratio of `0.0` turns
root spans off. A ratio of `1.0` samples every root span. A child follows the
sampling decision of its parent.

| Span | Kind | Attributes |
|---|---|---|
| `s3.request` | server | `http.request.method`, `url.path`, `s3.bucket.name`, `s3.operation`, `http.response.status_code`, `otel.kind`, and `s3.object.key` when enabled. |
| `s3.upstream` | client | `http.request.method`, `server.address`, `url.path`, `http.response.status_code`, `otel.kind`. |
| `cache.lookup` | internal | `cache.hit`, `s3.bucket.name`, `cache.entry.state`. |
| `cache.revalidate` | internal | `cache.hit`, `s3.bucket.name`, `cache.entry.state`. |
| `cache.tee` | internal | `cache.hit`, `s3.bucket.name`, `cache.entry.state`. |
| `cache.sync` | internal | `s3.bucket.name`, `cache.entry.state`. |
| `cluster.stage` | internal | `s3.bucket.name`, `cluster.node_id`, `cluster.result`. |
| `cluster.commit` | internal | `s3.bucket.name`, `cluster.node_id`, `cluster.write_ack`, `cluster.result`. |
| `cluster.read_index` | internal | `s3.bucket.name`, `cluster.node_id`, `cluster.result`. |
| `cluster.replicate` | internal | `cluster.node_id`, `cluster.peer_id`, `cluster.result`. |
| `cluster.peer_fetch` | internal | `cluster.node_id`, `cluster.peer_id`, `cluster.result`. |
| `replication.copy` | internal | `cluster.node_id`, `cluster.peer_id`, `cluster.result`. |
| `gc.cycle` | internal | None. |
| `consul.reconcile` | internal | None. |
| `raft.apply` | internal | None. |

Span names are fixed. Object keys, UUIDs, and free-form error strings are never
part of a span name. Bounded values belong in attributes.

`record_object_keys = false` is the default. Object keys can identify private
data. The flag gates both `s3.object.key` and the object part of `url.path`.
When it is false, an object path uses `/{bucket}/[redacted]` on the server span
and on every upstream span. The bucket name stays because it has low
cardinality. Bucket-level paths stay unchanged. Control paths such as
`/healthz`, `/readyz`, `/_raft/...`, `/_coord/...`, and `/_blob/...` stay
unchanged in both modes. When it is true, the full path and `s3.object.key` are
kept. Treat exported traces as sensitive when keys are private.

### Logs

When `otel.logs = true`, the sidecar exports `tracing` events through the
OpenTelemetry log appender. It keeps stderr output. Every record carries the
trace id and span id of the current span. The appender maps `tracing` levels to
OpenTelemetry severity: `TRACE`, `DEBUG`, `INFO`, `WARN`, and `ERROR`.

`check` does not start OpenTelemetry and does not export any signal.

### Metrics

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

### Multipart writes

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `multipart.uploads` | counter | `1` | | Multipart uploads that started. |
| `multipart.parts` | counter | `1` | | Parts uploaded by a multipart write. |
| `multipart.aborts` | counter | `1` | | Multipart uploads that were aborted. |
| `multipart.failures` | counter | `1` | `reason` | Multipart writes that failed. |
| `multipart.part.duration` | histogram | `ms` | | Duration of one part upload. |

The `reason` attribute is one of `start`, `start_body`, `part_size`, `body`,
`part`, `part_etag`, or `complete`.

### Raft

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `raft.term` | gauge | `1` | | Current raft term. || `raft.leader` | gauge | `1` | `id` | One for the current leader. Zero with `id=""` when there is no leader. |
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

## Multipart writes

The sidecar writes its own large objects with multipart. This applies to cache
fills, cache write-through, cache write-back sync, cluster staging, replication
receives, and read repair. Small metadata, journal, manifest, and bucket-config
objects still use one `PUT`.

```toml
[multipart]
enabled = true
threshold = "64MiB"
chunk_size = "16MiB"
min_chunk_size = "5MiB"
max_parts = 10000
concurrency = 4
```

- `threshold` is the smallest object size that uses multipart. A smaller object
  uses one `PUT`. `0` makes every non-empty object use multipart.
- `chunk_size` is the target part size. The sidecar reads the body one part at a
  time. The last part can be smaller.
- `min_chunk_size` is the smallest part size. S3 requires at least 5 MiB for
  every part except the last part. The default is the S3 minimum.
- `max_parts` is the largest part count. S3 accepts at most 10,000 parts. When
  an object needs more parts, the sidecar raises the part size to the next whole
  MiB. If that part size is above 5 GiB, the write fails.
- `concurrency` is the largest number of part uploads in flight. A higher value
  can increase throughput. It also increases memory use.

Memory use is bounded by `concurrency * chunk_size` plus one part. The sidecar
does not load the full object into memory. A larger part size lowers the part
count and the number of requests. A smaller part size lowers the peak memory.
Do not set `chunk_size` so high that `concurrency * chunk_size` is above the
memory you can reserve.

Every failed multipart write aborts the upload before it returns the error. A
failed abort logs a warning. The original error is the error that the caller
sees.

Client-driven multipart requests are not affected. A client that sends
`CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload` still
reaches the upstream server as before. The `[multipart]` values apply only to
writes that the sidecar starts.

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
| `multipart.enabled` | true | Use multipart for sidecar-owned object writes. |
| `multipart.threshold` | 64MiB | Smallest object that uses multipart. `0` always uses multipart. |
| `multipart.chunk_size` | 16MiB | Target part size. |
| `multipart.min_chunk_size` | 5MiB | Smallest part size, the S3 minimum. Must be at most `chunk_size`. |
| `multipart.max_parts` | 10000 | Largest part count, the S3 maximum. A higher need raises the part size. |
| `multipart.concurrency` | 4 | Parallel part uploads. Memory is about `concurrency * chunk_size`. |

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
- A client entry with `session_token` requires a matching token on each request.
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

### Encryption headers

- Cache `write` and `read_write` return `501 NotImplemented` for SSE-C.
- Cache `read` forwards SSE-C to origin and bypasses the cache.
- Cache `off` and pure proxy mode forward SSE-C unchanged.
- Cluster mode returns `501 NotImplemented` for SSE-C and SSE-S3 or SSE-KMS
  headers on `PutObject`, `CopyObject`, `CreateMultipartUpload`, `UploadPart`,
  and `CompleteMultipartUpload`.
- Cluster mode needs default encryption on the backing bucket.
- A client that needs client-supplied encryption keys must use pure proxy mode
  or cache mode `read`.

Do not add an encryption header to a cache write. The sidecar cannot store the
customer key safely.

### Session token errors

A missing or wrong session token returns `403 Forbidden` with code
`InvalidToken`.

- Header authentication reads `x-amz-security-token`.
- Presigned authentication reads `X-Amz-Security-Token` in the query.
- A client entry with no configured token ignores a supplied token.
- Rotate `clients[].session_token` and the client token together.
- Do not put a session token in `argv`.

### Object key privacy

`otel.record_object_keys = false` redacts the object part of `url.path` on the
server and upstream spans. It also omits the `s3.object.key` span attribute. The
setting redacts the `path` field in the request log and the object key fields in
the cache sync, orphan, eviction, journal, and quarantine logs.

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

- Object tagging, versioning, ACLs, bucket policies, and `Select` are not
  supported.
- A cache entry is two S3 objects: a blob and a metadata object. There is no
  atomic switch between them. A different sidecar that reads the shared cache
  bucket during a write can observe old metadata with new bytes. The accepting
  sidecar hides the window after it installs the pending entry. A read before
  that install can still observe the mix.
- Cache mode does not evaluate `If-Range`. A conditional range request can
  return `206 PartialContent` when the validator does not match.
- The cache header allowlist is fixed. The cache does not preserve object tags,
  object lock headers, website redirect headers, or encryption headers.
- SSE-C works only in pure proxy mode, or in cache mode `read` where the cache
  is bypassed. Cache write modes return `501 NotImplemented`.
- Cluster mode requires default encryption on the backing bucket. Client
  encryption headers are not supported.
- Cluster mode rejects encryption headers on `PutObject`, `CopyObject`,
  `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload`.
- There is no local metrics endpoint. Metrics export over OTLP only.
- A slow client can hold a connection permit for a long time. The idle timeout
  bounds only the gap between chunks. There is no total response deadline.
- Multipart checksum headers are dropped. A client that sends part checksums
  in `CompleteMultipartUpload` can fail.
- Sidecar-owned multipart writes do not send per-part checksum headers.
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
