# s3-sidecar

An S3 sidecar proxy in Rust.

This build implements milestones M0 (proxy core), M1 (cache), and the
single-node foundation of M2 (coordination):

- Layered configuration: defaults, TOML file, environment, and flags.
- SigV4 authentication: header auth and presigned query auth.
- Byte-preserving pass-through to one origin S3 endpoint.
- Read-through cache with TTL and `If-None-Match` revalidation.
- Write-back journal with async origin sync, deletes, and tombstones.
- Quarantine, retry with backoff, garbage collection, and LRU eviction.
- Cluster mode with `consensus.backend = "single"`, `"raft"`, or `"consul"`:
  staged writes, a `redb` metadata state machine, metadata-backed reads and
  listing, multipart, tombstones, garbage collection, and metrics.
- Raft mode with an `openraft` leader, a `redb` log store, snapshots, and an
  HTTP transport for internal RPCs.
- Consul mode with a session-based leader, KV compare-and-swap writes, a
  materialized `redb` cache, and a blocking-query watcher.
- Asynchronous blob replication to a deterministic replica set, peer blob
  fetch, read repair, delete propagation, and replica reconciliation.
- Health and readiness endpoints.
- OpenTelemetry metrics, traces, and logs over OTLP, with W3C context
  propagation.
- `s3-sidecar check --show-config` with redacted secrets and field sources.

Raft and Consul replicate metadata. Each node also copies blobs to its intended
replicas through the internal `/_blob/` endpoints. `write_ack` selects when the
client receives `200`: after the leader commit, after a quorum of replicas, or
after all replicas. `read = local_then_peer` serves a local blob or fetches it
from a replica.

## Operations

Read [docs/operations.md](docs/operations.md) for deployment topologies, the
consistency matrix, the metrics reference, sizing, security, backup, and
upgrade. Read [docs/runbook.md](docs/runbook.md) for the symptom, check, and
action for common failures.

## Build and test

This project uses [mise](https://mise.jdx.dev/) to manage tools and tasks.

```bash
mise install
mise run build
mise run test
mise run fmt
mise run lint
mise run bench
mise run loadgen
mise run fuzz
```

`mise run bench` and `mise run fuzz` need the extra tools in the sections below.

The full local gates are:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo doc --no-deps
cargo deny check
```

## Run

Write a configuration file. See [`sidecar.example.toml`](sidecar.example.toml)
for every field.

```toml
mode = "cache"

[server]
bind = "127.0.0.1:9002"

[origin]
endpoint = "http://127.0.0.1:9001"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"

[cache]
endpoint = "http://127.0.0.1:9002"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
bucket = "s3-sidecar-cache"
prefix = "cache"
mode = "read_write"
max_bytes = 0
max_object_bytes = 1073741824
write_policy = "write_back"
on_cache_error = "origin"

[[clients]]
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
buckets = ["media"]
```

Cache mode always uses a cache. `cache.endpoint`, `cache.bucket`, and
`cache.max_bytes` are required. Set `cache.max_bytes` to a byte count, or `0`
for unlimited. Cluster mode has no cache.

Check the merged configuration:

```bash
s3-sidecar check --config sidecar.toml --show-config
```

Start the proxy:

```bash
s3-sidecar serve --config sidecar.toml
```

Point an S3 client at the sidecar. The client access key must be in the
`[[clients]]` list, and the bucket must be in that client's `buckets` list.

```bash
aws --endpoint-url http://127.0.0.1:9002 s3 ls s3://media/
mc alias set sidecar http://127.0.0.1:9002 minioadmin minioadmin
mc ls sidecar/media
```

### TLS listener

Add `[server.tls]` to serve HTTPS with a PEM certificate chain and private key:

```toml
[server]
bind = "0.0.0.0:9000"

[server.tls]
cert_file = "/etc/s3-sidecar/tls/cert.pem"
key_file = "/etc/s3-sidecar/tls/key.pem"
```

The sidecar reads and parses both files at startup. A missing, unreadable, or
invalid file stops startup with an error. Plain HTTP remains the default when
the section is absent. Client certificate authentication is not implemented.

```bash
aws --endpoint-url https://127.0.0.1:9000 s3 ls s3://media/
```

### Graceful shutdown

`SIGTERM` and `SIGINT` start a graceful shutdown. The sidecar stops accepting
new connections, lets in-flight requests finish, stops the cache and cluster
workers, destroys the Consul session, shuts down the raft node, and commits the
`redb` files. Draining is bounded by `server.shutdown_timeout` (default `30s`).
A timeout exits with a non-zero status. A fatal listener error also exits
non-zero; a normal drain exits zero.

### Limits and timeouts

```toml
[server]
max_body_bytes = 5368709120     # 5 GiB
request_timeout = "5m"
idle_timeout = "60s"
max_connections = 1024
shutdown_timeout = "30s"

[upstream]
connect_timeout = "10s"
request_timeout = "5m"
retry_attempts = 2
```

- A body larger than `server.max_body_bytes` returns `413 EntityTooLarge` in S3
  XML. The `Content-Length` header is checked first. A chunked body is also
  counted while it streams.
- `server.request_timeout` applies to non-streaming control requests. It does
  not cut off a long upload or download. `server.idle_timeout` bounds the gap
  between stream chunks instead.
- `server.max_connections` bounds concurrent S3 requests. A request above the
  bound returns `503 SlowDown` with `Retry-After: 1`.
- `upstream.request_timeout` bounds the wait for upstream response headers.
- An upstream timeout returns `504 RequestTimeout` in S3 XML.
- A client disconnect cancels the request task and the matching upstream
  request.
- The sidecar streams a request body and does not buffer it. A body with no
  `Content-Length` and no `Transfer-Encoding` is forwarded. This applies to
  HTTP/2 requests.
- A write-back `PUT` with no `Content-Length` goes to origin, because the sync
  worker cannot replay the body.

### Retry policy and request identifiers

The sidecar retries `GET`, `HEAD`, `DELETE`, and list requests on a connection
error, a `429`, or a `5xx`. It retries `upstream.retry_attempts` times after the
first attempt. It uses exponential backoff with jitter and honors a numeric
`Retry-After` header.

A request with a body is never retried after the body starts streaming, so a
`PUT` or `POST` body reaches the origin at most one time. Do not retry a
non-idempotent request at the client unless the operation is safe to repeat.

Every response has `x-s3-sidecar-request-id`. The sidecar also uses this value
in its request log. A client value is propagated upstream; otherwise the
sidecar generates one. The identifier stays the same across internal retries.

A `503` with `Retry-After: 1` means no leader or no metadata quorum is
available. Clients should retry those responses. Forwarded cluster proposals
have no idempotency key, so a client retry can create a second proposal. A
commit followed by a lost response can therefore repeat an operation that is
not naturally idempotent.

## Configuration

Precedence from highest to lowest:

1. CLI flag
2. Environment variable
3. TOML configuration file
4. Built-in default

Environment names use the `S3_SIDECAR_` prefix and `__` between levels.
Names are case-insensitive.

```bash
S3_SIDECAR_SERVER__BIND=0.0.0.0:9001
S3_SIDECAR_BUCKETS='[{"name":"media","ttl":"1h"}]'
```

Flags use the TOML path in kebab-case. Arrays and tables take JSON.

```bash
s3-sidecar serve --server-bind 0.0.0.0:9001 \
  --origin-endpoint http://minio:9000 \
  --buckets-json '[{"name":"media","ttl":"1h"}]'
```

The convenience flag `--bucket media` adds a bucket name. Repeat it to add
more buckets.

### OpenTelemetry metrics, traces, and logs

Export is off by default. Set `otel.enabled = true` to send signals to an OTLP
collector. Metrics are on by default when OpenTelemetry is enabled. Traces and
logs are opt-in.

```toml
[otel]
enabled = true
metrics = true
traces = true
logs = true
trace_sample_ratio = 1.0        # 0.0 disables root traces.
record_object_keys = false      # Redact object keys in spans and logs. Off for privacy.
service_name = "s3-sidecar"
service_version = "0.2.0"
endpoint = "http://127.0.0.1:4317"
trace_endpoint = "http://127.0.0.1:4317"
logs_endpoint = "http://127.0.0.1:4317"
protocol = "grpc"                # grpc | http/protobuf
timeout = "10s"
export_interval = "60s"
trace_schedule_delay = "5s"
log_schedule_delay = "5s"
export_timeout = "10s"
resource_attributes = { "deployment.environment.name" = "production" }
```

For HTTP, set `protocol = "http/protobuf"` and use the HTTP collector port.

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 \
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf \
s3-sidecar serve --otel-enabled --otel-traces --otel-logs --config sidecar.toml
```

An unset `otel.endpoint` uses `OTEL_EXPORTER_OTLP_ENDPOINT`. An unset
`otel.trace_endpoint` uses `otel.endpoint`, then
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`. An unset `otel.logs_endpoint` uses
`otel.endpoint`, then `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`. An unset
`otel.protocol` uses `OTEL_EXPORTER_OTLP_PROTOCOL`, then `grpc`. An unset
`otel.export_interval` uses `OTEL_METRIC_EXPORT_INTERVAL`, then `60s`.

A trace starts at `s3.request`. The sidecar extracts W3C `traceparent` and
`tracestate` from incoming requests and injects the context into every upstream
S3, cache, peer, raft, and Consul request. Log records carry the trace and span
ids of the current span. `record_object_keys = false` redacts the object part
of `url.path` as `/{bucket}/[redacted]` on server and upstream spans and omits
`s3.object.key`. Control paths are unchanged. The setting also redacts the
`path` field in the request log and the object key fields in the cache sync,
orphan, eviction, journal, and quarantine logs. See the operations guide for the
span and attribute list and the object-key privacy note.

Use `otel.headers` or `otel.headers_file` for collector authentication. The
headers file contains a JSON object. `check --show-config` redacts header values.

```toml
[otel.headers]
"x-api-key" = "..."
```

### Secrets

Do not put a secret in `argv`. Use one of these forms:

- Environment: `S3_SIDECAR_ORIGIN__SECRET_ACCESS_KEY`
- Config file: `secret_access_key = "..."`
- Secret file from the environment: `S3_SIDECAR_ORIGIN__SECRET_ACCESS_KEY_FILE=/run/secrets/origin`
- Secret file from a flag: `--origin-secret-access-key-file /run/secrets/origin`
- Standard input: `--origin-secret-access-key-stdin`

`check --show-config` prints `****` for each set secret. The sidecar never logs
`Authorization` headers or query strings.

### File discovery

The sidecar reads the first file that exists:

1. `--config <path>`
2. `S3_SIDECAR_CONFIG`
3. `./sidecar.toml`
4. `/etc/s3-sidecar/config.toml`

A missing explicit file is an error. A missing default file is allowed.

## Auth

The sidecar verifies SigV4 header auth and presigned query auth. It checks the
canonical request, the signed headers, the payload hash, the region, the clock
skew (15 minutes), and the presigned lifetime (1 to 604800 seconds).

The sidecar re-signs each upstream request with the client's secret. The
bucket allow-list is enforced before the upstream call. A copy request must
name an allowed source bucket.

`aws-chunked` bodies are decoded and verified:
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD`,
`STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`, and
`STREAMING-UNSIGNED-PAYLOAD-TRAILER` are accepted. The signed modes verify
each chunk signature. The unsigned trailer mode does not verify the payload.
Do not use it without TLS.

If a `[[clients]]` entry has `session_token`, the request must send the same
token. Header authentication reads `x-amz-security-token`. Presigned
authentication reads `X-Amz-Security-Token` from the query. A missing or wrong
token returns `403 Forbidden` with code `InvalidToken`. A client entry with no
configured token ignores a supplied token.

## Cache mode

The cache is a second S3 endpoint. Use a different endpoint, region, or
provider from origin. Set `cache.bucket` to one bucket on the cache endpoint.
All cache blobs, metadata, and journal entries live in that bucket. The
`<bucket-hash>` path segment scopes origin buckets, so one cache bucket serves
every origin bucket. The sidecar stores cache objects under this path:

```
<prefix>/<bucket-hash>/<sha256(key)>
<prefix>/<bucket-hash>/<sha256(key)>.meta
<prefix>/journal/<uuid>.json
```

`cache.bucket` is required in cache mode.
`cache.prefix` defaults to `cache`. Set `cache.prefix` to a stable namespace
for each sidecar. The prefix is part of the key and the journal path. Sidecars
do not process each other's journals.

### Modes

| Mode | GET and HEAD | PUT and DELETE |
|---|---|---|
| `read` | cache, then revalidate | origin, then invalidate |
| `write` | pending entry, else origin | cache first, sync origin later |
| `read_write` | pending entry, else cache, then revalidate | cache first, sync origin later |

`read` mode is a pure read cache. `write` mode is a write buffer.
`read_write` does both.

### Server-side encryption with customer keys

Cache mode `write` and `read_write` reject a request that carries SSE-C headers
with `501 NotImplemented`. The sidecar cannot store the customer key safely.
Cache mode `read` is the supported mode for SSE-C. It keeps the cache out of
the path and forwards the request to origin.

### Read path

A fresh entry is served without an origin request. A stale entry sends a
conditional `GET` or `HEAD` with `If-None-Match`:

- `304` refreshes the validation time and serves the cache body.
- `200` streams to the client and writes the cache in parallel.
- `404` drops the cache entry.

The sidecar does not cache a body larger than `cache.max_object_bytes`. It
serves a range from the cache only when a complete cached body exists.
Otherwise the range goes to origin and the partial body is not cached.

The cache write is best effort. A cache failure does not fail the client
response. A client disconnect cancels the cache write. A bounded channel
applies backpressure from the cache to the client stream.

`HEAD` serves metadata only. It never fills a cache body.

### Object headers

The cache persists and replays a fixed allowlist of object headers:
`x-amz-meta-*`, `Content-Type`, `Content-Encoding`, `Content-Disposition`,
`Cache-Control`, `Content-Language`, `Expires`, and `x-amz-storage-class`.

The headers survive a cache fill, a cached `GET` or `HEAD`, a write-back sync,
and a write-through write. The cache does not store object tags, object lock
headers, website redirect headers, or encryption headers.

### Conditional requests

A cached `GET` or `HEAD` evaluates client conditions against the cached entry.
`If-None-Match` and `If-Modified-Since` return `304 Not Modified` when the
condition says not modified. `If-Match` and `If-Unmodified-Since` return
`412 PreconditionFailed` when the condition fails. A stale entry with a client
condition goes to origin. Revalidation adds `If-None-Match` only when the client
did not send one.

A write-back `PUT` checks `If-None-Match` and `If-Match` against the cached
entry. If the cache has no entry, the sidecar sends the request to origin so
origin checks the condition. A write-back `DELETE` checks `If-Match` against the
cached entry. If the cache has no entry, the sidecar sends the request to
origin.

### Write-back contract

A `200` response means "durable in the cache and queued for origin". It does
not mean "stored in origin". These properties follow:

1. The cache is the write target and origin catches up.
2. Async sync uses the stored origin service credential, not the client
   credential. The client can be gone and its presigned URL can be expired.
3. The bucket allow-list is checked when the sidecar accepts the write. Origin
   does not see the client identity for an asynchronous sync.
4. Durability depends on the cache provider. If the cache loses the object
   before sync, the write is lost.

The write protocol is:

1. Write the blob to the cache.
2. Write a journal record.
3. Return `200` to the client.
4. A worker signs with the origin service credential and sends the object to
   origin.
5. The worker marks the entry synced and deletes the journal record.

Startup scans the journal and resumes. A crash between the blob write and the
journal write leaves an orphan blob. The garbage collector removes it after
`cache.staging_ttl`.

Syncs for one key are serialized. Two queued writes to one key coalesce to the
latest write. Deleting a pending write removes the blob and journal record and
makes no origin call.

Pending writes and tombstones are visible only on the sidecar that accepted
them. Another sidecar sees the write after sync completes. This is a property
of write-back mode.

### Write-through

`write_policy = "write_through"` sends the client body to origin and the cache
at the same time. The sidecar reads the body one time. No second origin `GET`
is used.

The cache entry becomes synced only after origin returns success. If origin
fails, the sidecar deletes the cache object. If the cache write fails, origin
still receives the object and the client still receives success. The sidecar
deletes a partial cache object.

### Delete

A successful delete returns `204`. In `write` and `read_write` modes the
sidecar removes the cache object and writes a tombstone journal record before
it returns. A delete of a pending write makes no origin call. A tombstone hides
the key from reads. In `read` mode the delete goes to origin first.

### Failure policy

The sync worker retries `5xx`, timeouts, and throttling with exponential
backoff and jitter. It stops after `cache.sync_max_attempts`.

A permanent error (`400`, `403`, or bucket `404`) is quarantined. The worker
keeps the blob and journal record, marks the entry failed, emits a metric, and
logs a warning. A read of a quarantined entry returns `404` unless
`cache.serve_quarantined = true`.

The default conflict policy is last-write-wins. Origin receives the latest
body. There is no conflict knob in M1. This is a documented limitation.

If the cache is down, `cache.on_cache_error = "origin"` sends the write to
origin and returns success. `"fail"` returns an error.

### Sizing and lifecycle

`cache.max_bytes = 0` means unlimited. The sidecar does not open the LRU index
and does not run an eviction loop. It logs a startup warning. Orphan garbage
collection and journal cleanup still run.

`cache.max_bytes > 0` opens a `redb` LRU index at `cache.index_path`. The index
tracks size, last access, and validation time. When tracked bytes exceed
`cache.high_watermark`, the sidecar evicts least recently used synced entries
until tracked bytes are at or below `cache.low_watermark`. Pending and
quarantined entries are not evicted. The index is advisory: a bucket miss drops
the entry, and a lost index rebuilds by listing the cache prefix.

Add an S3 lifecycle rule on the cache prefix as a backstop. The lifecycle rule
removes stale objects when the index or the sidecar is unavailable. The
sidecar never deletes a blob that a journal record or committed metadata still
references.

### Metrics

Metrics export over OTLP when `otel.enabled = true`. The sidecar sends the
`cache.*`, `cluster.*`, `raft.*`, and `consul.*` instruments that
[docs/operations.md](docs/operations.md) lists. There is no local metrics
endpoint.

Cache metrics cover:

- cache hits, misses, bytes served, and origin bytes
- revalidations and cache fallbacks
- pending writes and sync lag in seconds
- sync success and failure counts
- quarantine count
- evictions and tracked bytes
- cache size limit and unlimited state

## Cluster mode

Cluster mode stores object bytes as immutable blobs on the local backing S3
endpoint. Metadata is the namespace. The backing S3 key space is not visible to
clients.

```toml
mode = "cluster"

[consensus]
backend = "single"

[consensus.raft]
node_id = "a"
peers = ["a"]

[backing]
bucket = "s3-sidecar-data"
endpoint = "http://127.0.0.1:9002"
region = "us-east-1"
access_key_id = "..."
secret_access_key = "..."

[cluster]
staging_ttl = "1h"
orphan_ttl = "24h"
gc_interval = "60s"
metadata_path = "./s3-sidecar-meta.redb"
# Prefer S3_SIDECAR_CLUSTER__SECRET_FILE or --cluster-secret-file.
secret = "replace-me"
read_repair = true
replicate_workers = 4
replicate_timeout = "10s"
reconcile_interval = "5m"

[[cluster.nodes]]
id = "a"
addr = "http://127.0.0.1:9000"

[[cluster.nodes]]
id = "b"
addr = "http://127.0.0.1:9100"

[[cluster.nodes]]
id = "c"
addr = "http://127.0.0.1:9200"

[[buckets]]
name = "media"
write_ack = "leader"
read = "local_then_peer"
replicas = 3
min_replicas = 2
```

`[[cluster.nodes]]` is the full node directory. It is required when any bucket
has `replicas > 1`. The directory must contain the local raft or Consul node id.
`cluster.secret` is required when a bucket uses `replicas > 1`. The sidecar
sends it as `x-s3-sidecar-cluster-token` on internal blob requests. It accepts
`S3_SIDECAR_CLUSTER__SECRET_FILE`, `--cluster-secret-file`, and
`--cluster-secret-stdin`.

### Raft mode

Set `consensus.backend = "raft"` and list every node. The list includes this
node. Each node name is unique, and each address is the base URL of that node's
sidecar listener.

```toml
[consensus]
backend = "raft"

[consensus.raft]
node_id = "a"
bootstrap = false
# Prefer S3_SIDECAR_CONSENSUS__RAFT__SECRET_FILE or
# --consensus-raft-secret-file in production.
secret = "replace-me"
rpc_timeout = "5s"

[[consensus.raft.nodes]]
id = "a"
addr = "http://127.0.0.1:9000"

[[consensus.raft.nodes]]
id = "b"
addr = "http://127.0.0.1:9100"

[[consensus.raft.nodes]]
id = "c"
addr = "http://127.0.0.1:9200"
```

Rules:

- `nodes` is required with `raft`. `peers` is not valid with `raft`. Use
  `peers` with the `single` backend.
- `consensus.raft.secret` is required. Internal `/_raft/` requests must send
  `x-s3-sidecar-raft-token` with this value. A missing or wrong token returns
  `401`.
- Set `bootstrap = true` on one node. That node forms the cluster with every
  entry in `nodes`. A node with `bootstrap = false` does not initialize a
  competing cluster. A single-node cluster bootstraps without the flag.
- The raft log, vote, committed index, applied index, membership, and snapshot
  share `cluster.metadata_path` with the metadata tables.
- This node uses the address from the `[[consensus.raft.nodes]]` entry whose
  `id` equals `node_id`. Every node address must be an absolute `http://` or
  `https://` URL.
- `consensus.raft.rpc_timeout` bounds each internal HTTP request.
- `consensus.raft.secret` also accepts
  `S3_SIDECAR_CONSENSUS__RAFT__SECRET_FILE` and
  `S3_SIDECAR_CONSENSUS__RAFT__SECRET`, or `--consensus-raft-secret-file` and
  `--consensus-raft-secret-stdin`.

A write on a follower writes the blob to the local backing S3 endpoint, then
forwards the proposal to the leader. The leader appends the entry to the raft
log. The follower returns `200` after the entry commits. The owner then copies
the blob to the other nodes in the committed replica set. A read on a node with
committed metadata but no local blob fetches the blob from a replica. If no
replica has the blob, the read returns `404` with `x-s3-sidecar-owner`.

The RAFT log uses `snapshot_policy = LogsSinceLast(64)`. A snapshot is a JSON
export of the metadata, bucket config, uploads, last applied log id, and
membership. Install replaces the state tables and the snapshot record in one
transaction.

Raft metrics include `s3_sidecar_raft_term`, `s3_sidecar_raft_leader`,
`s3_sidecar_raft_last_log_index`, `s3_sidecar_raft_last_applied`,
`s3_sidecar_raft_snapshot_index`, and `s3_sidecar_raft_elections_total`.
`/readyz` returns `503` with a reason until the node has a leader and has
applied at least one entry.

#### Raft limits

- `write_ack = "leader"` acknowledges after metadata commit. The owner can
  acknowledge a write that is lost if it dies before replication.
- Multipart upload records and pending rows are node-local.
- Tombstone removal is local. Visibility is unchanged because tombstones are
  hidden.
- The snapshot threshold is fixed at 64 log entries.
- A forwarded proposal has no idempotency key. A commit-then-timeout retry can
  return an error while the write is visible.
- Metadata replicates through raft. Blobs replicate synchronously or
  asynchronously according to `write_ack`.

### Consul mode

Set `consensus.backend = "consul"`. Consul KV is the metadata authority. Each
node keeps `cluster.metadata_path` as a materialized cache so reads, listing,
multipart, and GC use the same local code path.

```toml
[consensus]
backend = "consul"

[consensus.consul]
node_id = "a"
address = "http://127.0.0.1:8500"
advertise_addr = "http://127.0.0.1:9000"
# Prefer S3_SIDECAR_CONSENSUS__CONSUL__TOKEN_FILE or
# --consensus-consul-token-file in production.
token = ""
session_ttl = "15s"
# Prefer S3_SIDECAR_CONSENSUS__CONSUL__SECRET_FILE or
# --consensus-consul-secret-file in production.
secret = "replace-me"
```

Rules:

- `address`, `advertise_addr`, `node_id`, and `secret` are required.
- `address` and `advertise_addr` must be absolute `http://` or `https://` URLs.
- `session_ttl` must be at least `10s`, the Consul minimum.
- `token` is optional. The sidecar sends it as `X-Consul-Token` when it is set.
- `secret` is the proposal token. Internal `/_coord/propose` requests must send
  `x-s3-sidecar-consul-token` with this value. A missing or wrong token returns
  `401`.
- Both `token` and `secret` accept `_FILE` environment variables, secret-file
  flags, and the stdin flags. Do not put either value in `argv`.

Consul keys:

```
s3-sidecar/leader                      {"id":"a","addr":"http://..."}
s3-sidecar/meta/<hex(bucket)>/<hex(key)>   metadata JSON
s3-sidecar/bucket/<hex(bucket)>            bucket consistency JSON
```

Hex encoding keeps keys URL-safe and keeps one prefix scan per bucket.

At startup, the sidecar fetches `s3-sidecar/meta/?recurse` and applies every
value to `redb`. A blocking query on the same prefix then applies changes.
Each full prefix response reconciles deletions: a local row that Consul no
longer has is removed. A local `Pending` multipart row is kept. This costs one
full namespace scan per change. Use it for moderate namespaces, not millions
of keys.

The first node that acquires `s3-sidecar/leader` with its session is the
leader. The leader renews the session at about half the TTL. A failed renewal
or a lost leader key causes a step-down. A follower forwards a proposal to
`POST <leader.addr>/_coord/propose`. If no leader exists, writes return `503`.

The leader reads the current key from Consul, applies `If-None-Match` and
`If-Match` against that value, then writes with `cas=<ModifyIndex>`. A new key
uses `cas=0`. The leader retries a bounded number of compare-and-swap
conflicts. A condition failure returns `412`. A conflict that persists returns
`409`. On success, `version` is the new `ModifyIndex`, and the value is applied
to local `redb`. A rejected proposal deletes the staged blob.

`read = "local"` serves the materialized store. `read = "leader"` reads the
key or bucket prefix from Consul with `?consistent`, updates `redb`, then
serves. DELETE writes a tombstone with compare-and-swap. The leader removes
tombstone keys older than `cluster.orphan_ttl`.

Readiness is false until the first full reconcile completes and a leader
exists.

Consul metrics include `s3_sidecar_consul_leader`,
`s3_sidecar_consul_materialized`,
`s3_sidecar_consul_session_renewals_total`,
`s3_sidecar_consul_session_losses_total`, and
`s3_sidecar_consul_leader_changes_total`.

#### Consul limits

- `write_ack = "leader"` acknowledges after metadata commit. The owner can
  acknowledge a write that is lost if it dies before replication.
- Multipart upload records and pending rows are node-local.
- The watcher reconciles the full metadata prefix. Large namespaces pay one
  full scan per change.
- A forwarded proposal has no idempotency key. A commit-then-timeout retry can
  return an error while the write is visible.
- Metadata replicates through Consul. Blobs replicate synchronously or
  asynchronously according to `write_ack`.
- Consul ACL policy is not created by the sidecar. Grant KV write access under
  `s3-sidecar/` and session create, renew, and destroy access.

Blobs live in `backing.bucket` under `objects/`:

```
<backing.bucket>/objects/<uuid>
```

`backing.bucket` is required when `mode = "cluster"`. It holds all blobs,
multipart uploads, and the GC listing. `cluster.metadata_path` holds the `redb`
state machine. Keep this file on durable local storage.

### Write path

A PUT streams the body to `objects/<uuid>`, computes the MD5 ETag and SHA-256
checksum, then applies the bucket `write_ack` policy. The client receives `200`
only after the policy is satisfied and the state machine applies the entry. A
rejected proposal deletes the staged blob and any peer copy. A single PUT does
not create a `Pending` metadata row. `Pending` is reserved for in-flight
multipart uploads. The upload record is the durable staging reference.

The sidecar honors `If-None-Match: *` and `If-Match`. A failed condition returns
`412 PreconditionFailed`, and no staged blob remains.

`write_ack` has three values:

| Value | Acknowledge after | Trade-off |
|---|---|---|
| `leader` | The leader commits metadata. Blobs replicate asynchronously. | Fast. The owner can acknowledge a write that is lost if it dies before replication. |
| `quorum` | The owner and at least `min_replicas - 1` peers hold the blob, then the leader commits. | Balanced. The write survives a minority peer failure. |
| `all` | The owner and every intended replica hold the blob, then the leader commits. | Strongest. Any intended peer failure returns `503`. |

The owner counts toward `replicas` and `min_replicas`. Quorum needs
`min_replicas - 1` peer acknowledgements. The sidecar validates
`1 <= min_replicas <= replicas <= cluster.nodes.len()` at startup. It copies
blobs before the metadata commit. On a quorum or all failure, it deletes the
staged blob on the owner and on the peers that accepted it, then returns `503
ServiceUnavailable`. A metadata commit failure after the peer copies also
deletes every staged blob and returns the commit error.

The commit stores the actual replica set: the owner plus the peers that
acknowledged. For `quorum` and `all`, the asynchronous queue holds only the
intended replicas that do not have the blob. Each synchronous peer copy is
aborted after `cluster.replicate_timeout`.

`read = "leader"` calls `read_index` before the metadata lookup.
`read = "local_then_peer"` does not call `read_index`.

The replica set starts with the owner. The other members are chosen from
`cluster.nodes` by a hash of `bucket/key`. The same key always selects the same
peers.

### Object headers

Cluster mode preserves `Content-Language`, `Expires`, and `x-amz-storage-class`
on `PutObject`, on `CopyObject` with `COPY` or `REPLACE`, and on multipart
upload. `GetObject` and `HeadObject` return the headers. The sidecar omits
`x-amz-storage-class` for `STANDARD`.

### Encryption headers

Cluster mode rejects a server-side encryption request header with
`501 NotImplemented`. This covers SSE-C and SSE-S3 or SSE-KMS headers. The
rejection applies to `PutObject`, `CopyObject`, `CreateMultipartUpload`,
`UploadPart`, and `CompleteMultipartUpload`. The backing bucket must use default
encryption.

### Read and list path

GET and HEAD resolve metadata from the applied state. A committed key with a
local blob streams from the backing S3 endpoint and passes `Range` through.
HEAD returns stored metadata and does not read the body.

`read = "local"` serves only a local blob. A missing blob returns `404
NoSuchKey`. `read = "local_then_peer"` tries the local blob, then each node in
`metadata.replicas`, then the owner. It sends `HEAD` first and then `GET`. It
forwards `Range` and the response headers. `read = "leader"` does the metadata
read-index first, then behaves like `local_then_peer`.

If `cluster.read_repair = true`, a peer read also stores the fetched body in the
local backing S3 endpoint. The client response does not wait for that write. A
range read is not stored. Read repair does not change metadata. The replica list
in metadata is the intended set.

A committed key with no reachable replica returns `404 NoSuchKey` with an
`x-s3-sidecar-owner` header and a metric. A tombstone or absent key returns `404
NoSuchKey`.

ListObjectsV2 reads committed metadata only. It supports prefix, delimiter,
continuation token, and max-keys. It never reads the raw backing key space.
Staging blobs, upload records, tombstones, and non-committed rows are hidden.
`ListBuckets` and bucket operations that need a real bucket view return
`NotImplemented` in cluster mode.

CopyObject requires a committed source on this node. It copies the backing blob,
then commits the destination key. `x-amz-metadata-directive = "COPY"` keeps the
source metadata, and `"REPLACE"` uses the request headers. Destination
`If-None-Match` and `If-Match` conditions are enforced. The client bucket
allow-list applies to the source and destination buckets.

### Replication and read repair

The internal blob transport uses these routes:

```
GET    /_blob/<uuid>   stream a blob. Range is supported.
PUT    /_blob/<uuid>   store a blob at <backing.bucket>/objects/<uuid>.
HEAD   /_blob/<uuid>   report existence and length.
DELETE /_blob/<uuid>   delete a blob with no committed metadata reference.
```

These routes bypass S3 client authentication. Each request must send
`x-s3-sidecar-cluster-token` with `cluster.secret`. A missing or wrong token
returns `401`. A delete of a referenced blob returns `409`. The routes stream
bodies and do not buffer a whole object.

With `write_ack = "leader"`, the sidecar returns `200` after the metadata
commit, then queues one durable replication job in `redb`. A job holds the blob
id, the target nodes, the completed nodes, the attempt count, the next attempt
time, and the bucket and key. `cluster.replicate_workers` workers read due jobs.
Each worker streams the blob from the local backing S3 endpoint to each target.
It saves progress after each target. A restart resumes the remaining work. A
transient failure uses exponential backoff with jitter. A permanent failure
removes that target from the job.

With `write_ack = "quorum"` or `"all"`, the sidecar copies the blob to the
required peers before the commit. It queues only the remaining intended peers
for asynchronous replication. The commit metadata lists the owner and the peers
that acknowledged.

After a delete commits, the sidecar queues delete jobs for the old replica set
and the owner. A receiving node deletes the blob only when its local metadata
has no committed reference to it. A `409` is a transient result, so the sender
retries after the tombstone reaches the target.

Every `cluster.reconcile_interval`, the sidecar checks a bounded batch of
committed metadata. It sends `HEAD /_blob/<uuid>` to each intended replica. It
queues a replication job for each missing blob. If the local owner lost its
blob, the owner fetches it from one reachable replica and stores it locally.
This repair does not change the metadata version or the replica list. If no
replica has the blob, the entry stays in place for the next cycle. The sidecar
does not remove a node from the intended set.

Replication metrics include queue depth, oldest job age, success count, and
failure count by status or transport class. Synchronous replication metrics
include latency, copy attempts, replica acknowledgements, and replica
acknowledgement failures. Write metrics include per-policy counts and quorum
success and failure counts. Reconciliation metrics include the number of checked,
present, and missing replica targets, owner blobs repaired, and owner blobs that
no replica could supply. Read metrics include peer fetches and read repairs.

### Multipart, delete, and garbage collection

Create, upload, complete, and abort use the backing S3 multipart API. The
sidecar upload id encodes the owner node. An upload id on the wrong node returns
an error that names the owner node. Complete returns `200` only after the
commit applies.

DELETE proposes a tombstone. The key is invisible after apply. A missing key
also returns `204`. The local blob delete is asynchronous. The sidecar also
queues delete jobs for the old replica set and the owner.

The GC loop removes:

- orphan `objects/<uuid>` blobs after `staging_ttl`
- tombstones and their blobs after `orphan_ttl`
- stuck multipart uploads after `staging_ttl`

The GC loop compares the backing `objects/` listing with metadata references.
It does not delete a blob that metadata still references.

Cluster metrics include committed writes, aborted writes, commit latency,
local read hits, missing-blob reads, tombstone count, GC deletions, applied
index, term, and read-index calls.

## Benchmarks

`examples/loadgen.rs` is a small load generator. It signs requests with the
crate `SigV4` code and reports count, errors, throughput, p50, p95, and p99
latency. It is an example, so the release artifacts do not include it.

```bash
cargo run --release --example loadgen -- \
  --endpoint http://127.0.0.1:9000 \
  --bucket media \
  --key bench/object \
  --requests 1000 \
  --concurrency 16 \
  --size 4096 \
  --mode mixed \
  --access-key minioadmin \
  --secret-key minioadmin \
  --region us-east-1
```

`--mode` takes `put`, `get`, or `mixed`. For `get`, seed the same key prefix
first. The request index is appended to `--key`.

`scripts/bench.sh` builds the release binary and runs the repeatable scenarios.
It starts a local origin S3 and a local cache S3, then runs:

- `cache-miss` then `cache-hit` with `cache.mode = "read_write"`.
- `cluster-leader` on a single-node cluster with `write_ack = "leader"`.
- `cluster-quorum` on a three-node raft cluster with `write_ack = "quorum"`.

Run it with rustfs or MinIO:

```bash
RUSTFS_BIN=/path/to/rustfs mise run bench
MINIO_BIN=/path/to/minio mise run bench
```

### Benchmark prerequisites

The script needs a local S3 server and an S3 client.

- Put `rustfs` or `minio` on `PATH`, or set `RUSTFS_BIN` or `MINIO_BIN`.
- Put `aws` or `mc` on `PATH`, or set `AWS_BIN` or `MC_BIN`.
- Install `curl`.

The script checks for the S3 client at startup. If neither client exists, it
prints an error and exits non-zero. If a test bucket already exists, the script
skips creation.

Benchmarks were not run for this repository snapshot because no S3 binary or
S3 client was available.

Set `BENCH_REQUESTS` to change the request count. Set `BENCH_RESULTS` to change
the output directory.

The script writes the measured output to `bench-results/`. Git ignores that
directory. Each file names one scenario. The fields are:

| Field | Meaning |
|---|---|
| `completed` | Requests that returned a response. |
| `errors` | Requests with a transport error or a non-2xx status. |
| `duration_seconds` | Wall time for the run. |
| `throughput_ops_per_second` | Successful requests divided by wall time. |
| `p50_ms`, `p95_ms`, `p99_ms` | Latency percentiles in milliseconds. |

The repository does not store benchmark numbers. Compare two runs on the same
host with the same S3 binary and the same request count.

## Fuzzing

The `fuzz/` crate holds `cargo-fuzz` targets for the `SigV4` parser and the S3
XML parsers. The crate is isolated from the root workspace.

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
mise run fuzz
```

The task runs both targets for 60 seconds each. See
[`fuzz/README.md`](fuzz/README.md) for the individual commands and the steps to
reproduce a finding. A panic is a finding. The fuzz run is a scheduled workflow
and is not part of the pull request gate.

## Limitations

- Path-style bucket addressing only.
- One origin endpoint and one cache endpoint.
- Cluster mode supports `consensus.backend = "single"`, `"raft"`, and
  `"consul"`. Cluster mode requires `backing.bucket`.
- Blob replication with `write_ack = "leader"` is asynchronous. The owner can
  acknowledge a write that is lost if the owner dies before replication. A read
  on a node that lacks a replica fetches from the intended replicas, then the
  owner. If no replica has the blob, the read returns `404` with
  `x-s3-sidecar-owner`.
- `cluster.secret` is required only when a bucket has `replicas > 1`.
- Read repair does not change metadata. The replica list is the intended set,
  not a live inventory.
- Read repair skips `Range` responses. Storing a partial body would corrupt the
  full local blob.
- Replication retries have no maximum attempt count. A permanent target outage
  keeps a job in the queue with a bounded retry interval.
- `x-s3-sidecar-owner` is returned on a `read = "local"` miss as well as on a
  no-replica miss.
- Reconciliation restores a missing owner blob from one reachable replica. If
  no replica has it, the entry stays in place for the next cycle.
- Consul mode materializes the full metadata prefix on each change. The cost
  grows with the namespace size.
- `bootstrap = true` belongs on one node. A restart of that node keeps the
  existing cluster. A fresh second node with `bootstrap = false` waits for the
  leader and does not form a cluster alone.
- In cache mode, multipart and CopyObject pass through to origin. The sidecar
  does not cache multipart parts.
- The TLS listener serves one certificate. Client certificate authentication and
  automatic certificate reload are not implemented.
- The sidecar retries only idempotent requests. It never replays a request body.
  A non-idempotent client retry can repeat a cluster proposal because forwarded
  proposals have no idempotency key.
- The sidecar decodes `aws-chunked` bodies. The HTTP client cannot send trailer
  checksums, so the sidecar drops `x-amz-checksum-algorithm` on
  `CreateMultipartUpload` and removes trailer checksum headers. The object
  bytes are unchanged. A client that sends part checksums in
  `CompleteMultipartUpload` can fail, because the origin did not receive the
  part checksums.
- A plain payload hash is checked while the body streams. A mismatch aborts
  the upstream request, but the upstream can receive the full body first.
- The readiness endpoint reports the sidecar process, not the origin.
- `ListBuckets` is not supported.
- A cache entry is two S3 objects: a blob and a metadata object. There is no
  atomic switch between them. A different sidecar that reads the shared cache
  bucket during a write can observe old metadata with new bytes. The accepting
  sidecar hides the window after it installs the pending entry. A read before
  that install can still observe the mix.
- Cache mode does not evaluate `If-Range`. A conditional range request can
  return `206 PartialContent` when the validator does not match.
- The cache header allowlist is fixed. The cache does not preserve object tags,
  object lock headers, website redirect headers, or encryption headers.
- SSE-C works only in cache mode `read` where the cache is bypassed. Cache
  write modes return `501 NotImplemented`.
- Cluster mode requires default encryption on the backing bucket. Client
  encryption headers are not supported.
- Cluster mode rejects encryption headers on `PutObject`, `CopyObject`,
  `CreateMultipartUpload`, `UploadPart`, and `CompleteMultipartUpload`.
- There is no local metrics endpoint. Metrics export over OTLP only.
- A slow client can hold a connection permit for a long time. The idle timeout
  bounds only the gap between chunks. There is no total response deadline.
- Object tagging, versioning, ACLs, bucket policies, and `Select` are not
  supported.

## Design choices

- The upstream client is `reqwest` plus the local `SigV4` implementation.
- The sidecar streams request and response bodies, passes XML through byte
  for byte, and must verify `SigV4` locally in any case.
- `aws-sdk-s3` and `s3s` were considered. `s3s` would replace the pass-through
  request and response path, so it was not adopted.

## End-to-end tests

`tests/proxy.rs` runs the full proxy path against an in-process fake origin.
It covers streaming, ranges, list, multipart, copy, presigned URLs, invalid
credentials, the bucket allow-list, and `.` and `..` segments in object keys.

`tests/runtime.rs` covers the TLS handshake with a self-signed certificate,
TLS startup rejection, graceful drain and shutdown timeout, `413` from a
`Content-Length` and from a chunked body, `504` from an upstream timeout, GET
retry after `503`, no retry for a streamed PUT body, and request identifiers.

`tests/cache.rs` runs cache mode against an in-process fake origin and fake
cache. It covers repeat reads, TTL revalidation, pending read-your-write,
restart recovery, retry, quarantine, delete of a pending write, coalescing,
cache-outage fallback, eviction, the metrics endpoint, object header
persistence, conditional requests, and SSE-C rejection and bypass.

`tests/cluster.rs` runs single-node cluster mode against an in-process fake
backing S3. It covers PUT, GET, HEAD, DELETE, ListObjectsV2, conditional writes,
CopyObject, object headers, encryption rejection, tombstones, restart
persistence, multipart, wrong-node upload ids, leader reads, bucket config
seeding, GC, and startup validation.

`tests/replication.rs` runs a three-node raft cluster and a two-node single
backend. It covers replica placement, blob copy, local and peer reads, read
repair on and off, a read with no replica, delete propagation, restart resume,
the blob transport token, invalid blob identifiers, the `409` reference check,
`Range`, the replication queue, quorum writes after raft leader loss, and a
write without a metadata quorum.

`tests/consistency.rs` runs Consul clusters with an in-memory authority. It
covers the write acknowledgement matrix: quorum with one peer down, quorum and
all with too few peers, a commit rejection after peer replication, a leader
write read from a replica, a stale replica after a delete, restart after a
failed quorum write, and the write policy metrics.

`scripts/e2e-rustfs.sh` runs a real origin and the AWS CLI. It needs `rustfs`
and the AWS CLI v2. The MinIO client `mc` is optional.

```bash
RUSTFS_BIN=/path/to/rustfs \
AWS_BIN=/path/to/aws \
MC_BIN=/path/to/mc \
  scripts/e2e-rustfs.sh
```

The script runs `aws s3 cp`, `aws s3 sync`, `aws s3 ls`, multipart upload,
presigned GET, invalid credentials, and `mc` when `mc` is available. It also
runs the ignored test in `tests/e2e_rustfs.rs`.

## Dependency policy

`deny.toml` allows the permissive licenses in the HTTP and TLS tree:
ISC, BSD-3-Clause, Unicode-3.0, and CDLA-Permissive-2.0. It also skips each
transitive duplicate version with a reason. The TLS listener uses
`axum-server` with the rustls ring provider, which reuses the `rustls`,
`tokio-rustls`, and `rustls-pki-types` versions from `reqwest`. The project
does not use the AWS SDK, so no AWS crate exceptions are needed. The cache LRU
index uses `redb`, which is MIT or Apache-2.0 and adds no duplicate versions.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
