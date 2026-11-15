# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog. This project uses Semantic Versioning.

## [0.3.0] - 2026-11-15

### Added

- Cache mode supports client-driven multipart uploads. In `read` mode the
  upload completes at origin and the sidecar fills the cache. With
  `write_policy = "write_back"` the upload completes in the cache and syncs to
  origin. With `write_policy = "write_through"` the upload completes at origin
  and fills the cache.
- `ListBuckets` answers from the client allow list in cache mode and cluster
  mode. It does not read the origin or the backing bucket.
- Cluster mode answers `HeadBucket`, `ListObjects` V1, and `ListObjects` V2 from
  committed metadata. `CreateBucket` and `DeleteBucket` return `501
  NotImplemented` because the bucket set comes from configuration.
- Cache mode passes bucket operations to origin. This covers `CreateBucket`,
  `DeleteBucket`, `HeadBucket`, `ListObjects` V1, `ListObjects` V2, and bucket
  sub-resources.

### Changed

- **Breaking:** `cache.mode = "off"` is removed. Cache mode always uses a cache,
  and the default mode is `read_write`. `cache.endpoint`, `cache.bucket`,
  `cache.max_bytes`, `cache.prefix`, and `cache.index_path` are required in
  cache mode.
- **Breaking:** `cache.node_id` is removed. `cache.prefix` is the only cache
  namespace.
- **Breaking:** The cache key layout changed from `<prefix>/<node_id>/...` to
  `<prefix>/...`. The journal path changed from
  `<prefix>/journal/<node_id>/` to `<prefix>/journal/`. A cache from 0.2.0
  starts cold after the upgrade.
- A write-back multipart complete response now reports the origin bucket, the
  origin key, and the stored ETag.
- A write-back `ListParts` response now reports the origin bucket, the origin
  key, and the sidecar upload id.
- A cache `DeleteObjects` request now reads the key list from the request body.
  The sidecar invalidates the deleted keys after origin accepts the request.
- The sidecar now reads multipart query parameters by exact name. A request
  path that contains `uploads` or `uploadId` as text no longer selects a
  multipart path.

### Fixed

- Cache garbage collection no longer deletes journal records after
  `cache.staging_ttl`. A quarantined write keeps the record that a restart
  needs for the retry.
- Write-back multipart enforces `cache.max_object_bytes` and rejects an upload
  above the limit with `400 EntityTooLarge`.
- An oversized write-through `PUT` invalidates the old cache entry when origin
  accepts the write. A failed origin write leaves the entry in place.
- A successful `DeleteObjects` request invalidates the cached keys. A pending
  write-back becomes a staged delete, so the sync cannot recreate the key. The
  work is bounded to 16 keys at a time.
- Cluster mode rejects object subresource requests with `501 NotImplemented`.
  This covers ACLs, tagging, retention, legal hold, object attributes, torrent,
  and `versionId`. The sidecar does not delete or overwrite the object.
- The container image workflow now starts from the release workflow through the
  cargo-dist custom job. It also retries a denied first push three times.

### Security

No security fixes in this release.

### Known limits

- A cache entry is two S3 objects. There is no atomic switch between them.
- Cache mode does not evaluate `If-Range`. A conditional range request can
  return `206 PartialContent` when the validator does not match.
- The cache header allowlist is fixed. The cache does not preserve object tags,
  object lock headers, website redirect headers, or encryption headers.
- SSE-C works only in cache mode `read` where the cache is bypassed. Cache
  write modes return `501 NotImplemented`.
- Cluster mode requires default encryption on the backing bucket.
- There is no local metrics endpoint. Metrics export over OTLP only.
- A slow client has no total response deadline. The idle timeout bounds only
  the gap between chunks.
- Object tagging, versioning, ACLs, bucket policies, and `Select` are not
  supported.
- Cluster mode rejects object tagging, ACL, retention, legal hold, object
  attribute, and `versionId` requests with `501 NotImplemented`.
- The Consul watcher scans the full metadata prefix on each change.
- Replication retries have no maximum attempt count.
- Multipart checksum headers are dropped.
- A TLS listener serves one certificate. Client certificate authentication and
  automatic certificate reload are not implemented.
- Cache-mode `ListObjects` reads origin. A key with a pending write-back is not
  listed until the sync completes.
- An in-flight write-back multipart upload lives in memory. A restart loses it.
  An abandoned upload has no expiry until a restart.
- `ListMultipartUploads` reads origin. It does not list a cache upload.
- `cache.DeleteBucket` does not purge cached objects.
- Batch delete invalidation is best effort. A failed or quarantined journal can
  recreate a deleted key after a restart. The single-object delete path has the
  same limit.
- The first container image push may be denied until GitHub links the GHCR
  package to the repository. The workflow retries three times.

### Validation

`cargo fmt --all --check`, `cargo clippy --all-targets --all-features --locked
-- -D warnings`, `cargo test --all-targets --all-features --locked`,
`RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked`,
`cargo deny check`, `dist plan --output-format=human`, `dist generate --mode ci
--check`, and `git diff --check` pass on the release branch.

Real Consul and RustFS/MinIO end-to-end tests were not run for this release.

## [0.2.0] - 2026-11-14

### Added

- Cache mode persists and replays these object headers: `x-amz-meta-*`,
  `Content-Type`, `Content-Encoding`, `Content-Disposition`, `Cache-Control`,
  `Content-Language`, `Expires`, and `x-amz-storage-class`.
- Cache mode evaluates `If-None-Match`, `If-Modified-Since`, `If-Match`, and
  `If-Unmodified-Since` on a cached read.
- Cluster mode preserves `Content-Language`, `Expires`, and `x-amz-storage-class`
  on PUT, copy, and multipart writes.
- The sidecar enforces a configured session token for header authentication and
  presigned authentication.
- The sidecar forwards a request body that has no `Content-Length` and no
  `Transfer-Encoding`.
- Object keys keep `.` and `..` segments.
- The sidecar validates internal `/_blob/{uuid}` identifiers.
- The sidecar redacts object keys in request logs and cache logs when
  `otel.record_object_keys` is false.
- A multi-arch container image release workflow publishes images to GHCR.

### Changed

This section lists behavior changes.

- A configured client `session_token` is now required on each request.
- Cache mode `write` and `read_write` return `501 NotImplemented` for SSE-C
  requests.
- Cache mode `read` bypasses the cache for SSE-C requests.
- Cluster mode returns `501 NotImplemented` for client encryption headers. The
  backing bucket must use default encryption.
- Cache GET and HEAD responses now include the preserved object headers.
- Cache write-back PUT and DELETE now evaluate conditional headers.

### Fixed

- The cache removed a newer pending entry when an older sync finished.
- Object keys with dot segments could be normalized to a different key.
- Invalid `/_blob/` identifiers could reach the backing S3 path.
- `UploadPart` and `CompleteMultipartUpload` did not reject encryption headers in
  cluster mode.

### Security

- The sidecar now checks session tokens in constant time.
- The sidecar validates internal blob identifiers before any backing S3 call.
- Cache write modes no longer silently drop customer-key encryption headers.

### Known limits

- A cache entry is two S3 objects. There is no atomic switch between them.
- Cache mode does not evaluate `If-Range`.
- The cache header allowlist is fixed.
- SSE-C works only in cache mode `read`.
- Cluster mode requires default encryption on the backing bucket.
- There is no local metrics endpoint.
- A slow client has no total response deadline.
- Object tagging, versioning, ACLs, bucket policies, and `Select` are not
  supported.
- The Consul watcher scans the full metadata prefix on each change.
- Replication retries have no maximum attempt count.
- Multipart checksum headers are dropped.
- A TLS listener serves one certificate. Client certificate authentication and
  automatic certificate reload are not implemented.
- `ListBuckets` and bucket operations that need a real bucket view return
  `NotImplemented` in cluster mode.

### Validation

`cargo fmt`, `cargo clippy -D warnings`, `cargo test`, `cargo doc -D warnings`,
`cargo deny`, and `dist plan` pass on the release branch.

Real Consul and RustFS/MinIO end-to-end tests were not run for this release.
