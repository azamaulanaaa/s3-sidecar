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
- Cluster mode with `consensus.backend = "single"` or `"raft"`: staged writes,
  a `redb` metadata state machine, metadata-backed reads and listing, multipart,
  tombstones, garbage collection, and metrics.
- Raft mode with an `openraft` leader, a `redb` log store, snapshots, and an
  HTTP transport for internal RPCs.
- Health, readiness, and Prometheus metrics endpoints.
- `s3-sidecar check --show-config` with redacted secrets and field sources.

Raft mode replicates metadata. Blob bytes stay on the node that accepted the
write. Peer blob replication, `write_ack = quorum|all`, and
`read = local_then_peer` peer fetch are later milestones. The `consul` backend
returns `NotImplemented`.

## Build and test

This project uses [mise](https://mise.jdx.dev/) to manage tools and tasks.

```bash
mise install
mise run build
mise run test
mise run fmt
mise run lint
```

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
node_id = "node-a"
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

Set `cache.mode = "off"` to use the sidecar as a pure proxy. When
`cache.mode` is not `off`, `cache.max_bytes` is required: a byte count, or `0`
for unlimited.

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

## Cache mode

The cache is a second S3 endpoint. Use a different endpoint, region, or
provider from origin. Set `cache.bucket` to one bucket on the cache endpoint.
All cache blobs, metadata, and journal entries live in that bucket. The
`<bucket-hash>` path segment scopes origin buckets, so one cache bucket serves
every origin bucket. The sidecar stores cache objects under this path:

```
<prefix>/<node-id>/<bucket-hash>/<sha256(key)>
<prefix>/<node-id>/<bucket-hash>/<sha256(key)>.meta
<prefix>/journal/<node-id>/<uuid>.json
```

`cache.bucket` is required when `cache.mode` is not `off`.
`cache.prefix` defaults to `cache`. Set `cache.node_id` to a stable name for
each sidecar. The node id is part of the key and the journal path. Sidecars do
not process each other's journals.

### Modes

| Mode | GET and HEAD | PUT and DELETE |
|---|---|---|
| `off` | origin | origin |
| `read` | cache, then revalidate | origin, then invalidate |
| `write` | pending entry, else origin | cache first, sync origin later |
| `read_write` | pending entry, else cache, then revalidate | cache first, sync origin later |

`read` mode is a pure read cache. `write` mode is a write buffer.
`read_write` does both.

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
the key from reads. In `read` mode and `off` mode the delete goes to origin
first.

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

`GET /metrics` returns Prometheus text. It exports:

- cache hits, misses, and hit ratio
- bytes served from cache and bytes fetched from origin
- revalidations and cache fallbacks
- pending writes and sync lag in seconds
- sync success and failure counts
- quarantine count
- evictions and tracked bytes
- `cache_max_bytes` (`0` means unlimited) and `cache_unlimited`

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

[[buckets]]
name = "media"
write_ack = "leader"
read = "local"
replicas = 1
min_replicas = 1
```

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
secret = "replace-me"
addr = "http://127.0.0.1:9000"
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
- `consensus.raft.addr` must be an `http://` or `https://` URL.
- `consensus.raft.rpc_timeout` bounds each internal HTTP request.

A write on a follower writes the blob to the local backing S3 endpoint, then
forwards the proposal to the leader. The leader appends the entry to the raft
log. The follower returns `200` after the entry commits. A read on a node with
committed metadata but no local blob returns `404` with `x-s3-sidecar-owner`.

The RAFT log uses `snapshot_policy = LogsSinceLast(64)`. A snapshot is a JSON
export of the metadata, bucket config, uploads, last applied log id, and
membership. Install replaces the state tables and the snapshot record in one
transaction.

Raft metrics include `s3_sidecar_raft_term`, `s3_sidecar_raft_leader`,
`s3_sidecar_raft_last_log_index`, `s3_sidecar_raft_last_applied`,
`s3_sidecar_raft_snapshot_index`, and `s3_sidecar_raft_elections_total`.
`/readyz` returns `503` with a reason until the node has a leader and has
applied at least one entry.

Blobs live in `backing.bucket` under `objects/`:

```
<backing.bucket>/objects/<uuid>
```

`backing.bucket` is required when `mode = "cluster"`. It holds all blobs,
multipart uploads, and the GC listing. `cluster.metadata_path` holds the `redb`
state machine. Keep this file on durable local storage.

### Write path

A PUT streams the body to `objects/<uuid>`, computes the MD5 ETag and SHA-256
checksum, then proposes a `Commit` entry. The client receives `200` only after
the state machine applies the entry. A rejected proposal deletes the staged
blob. A single PUT does not create a `Pending` metadata row. `Pending` is
reserved for in-flight multipart uploads. The upload record is the durable
staging reference.

The sidecar honors `If-None-Match: *` and `If-Match`. A failed condition returns
`412 PreconditionFailed`, and no staged blob remains.

`write_ack` accepts only `leader` in this build. `quorum` and `all` are
validation errors. `read = "local_then_peer"` behaves as `local` until peer
fetch exists. `read = "leader"` calls `read_index` before the metadata lookup.

### Read and list path

GET and HEAD resolve metadata from the applied state. A committed key with a
local blob streams from the backing S3 endpoint and passes `Range` through.
HEAD returns stored metadata and does not read the body.

A committed key with a missing blob returns `404 NoSuchKey` with an
`x-s3-sidecar-owner` header and a metric. Peer fetch is a later milestone. A
tombstone or absent key returns `404 NoSuchKey`.

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

### Multipart, delete, and garbage collection

Create, upload, complete, and abort use the backing S3 multipart API. The
sidecar upload id encodes the owner node. An upload id on the wrong node returns
an error that names the owner node. Complete returns `200` only after the
commit applies.

DELETE proposes a tombstone. The key is invisible after apply. A missing key
also returns `204`. The blob delete is asynchronous.

The GC loop removes:

- orphan `objects/<uuid>` blobs after `staging_ttl`
- tombstones and their blobs after `orphan_ttl`
- stuck multipart uploads after `staging_ttl`

The GC loop compares the backing `objects/` listing with metadata references.
It does not delete a blob that metadata still references.

Cluster metrics include committed writes, aborted writes, commit latency,
local read hits, missing-blob reads, tombstone count, GC deletions, applied
index, term, and read-index calls.

## Limitations

- Path-style bucket addressing only.
- One origin endpoint and one cache endpoint.
- Cluster mode supports `consensus.backend = "single"` and `"raft"`. The
  `consul` backend, peer blob replication, quorum acknowledgements
  (`write_ack = "quorum"|"all"`), and `local_then_peer` peer fetch are not
  implemented. Cluster mode requires `backing.bucket`.
- Raft mode replicates metadata only. A committed blob is available only on the
  node that accepted the write until peer replication exists. A read on another
  node returns `404` with `x-s3-sidecar-owner`.
- `bootstrap = true` belongs on one node. A restart of that node keeps the
  existing cluster. A fresh second node with `bootstrap = false` waits for the
  leader and does not form a cluster alone.
- Multipart and CopyObject pass through to origin. The sidecar does not cache
  multipart parts.
- No TLS listener. Put the sidecar behind a TLS terminator.
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

## Design choices

- The upstream client is `reqwest` plus the local `SigV4` implementation.
- The sidecar streams request and response bodies, passes XML through byte
  for byte, and must verify `SigV4` locally in any case.
- `aws-sdk-s3` and `s3s` were considered. `s3s` would replace the pass-through
  request and response path, so it was not adopted.

## End-to-end tests

`tests/proxy.rs` runs the full proxy path against an in-process fake origin.
It covers streaming, ranges, list, multipart, copy, presigned URLs, invalid
credentials, and the bucket allow-list.

`tests/cache.rs` runs cache mode against an in-process fake origin and fake
cache. It covers repeat reads, TTL revalidation, pending read-your-write,
restart recovery, retry, quarantine, delete of a pending write, coalescing,
cache-outage fallback, eviction, and the metrics endpoint.

`tests/cluster.rs` runs single-node cluster mode against an in-process fake
backing S3. It covers PUT, GET, HEAD, DELETE, ListObjectsV2, conditional writes,
CopyObject, tombstones, restart persistence, multipart, wrong-node upload ids,
leader reads, bucket config seeding, GC, and startup validation.

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
ISC, BSD-3-Clause, Unicode-3.0, and CDLA-Permissive-2.0. It also skips three
transitive duplicate versions (`base64`, `syn`, and `windows-sys`) with a
reason for each. The project does not use the AWS SDK, so no AWS crate
exceptions are needed. The cache LRU index uses `redb`, which is MIT or
Apache-2.0 and adds no duplicate versions.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
