# Changelog

All notable changes to this project are documented in this file.

The format follows Keep a Changelog. This project uses Semantic Versioning.

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
- SSE-C works only in pure proxy mode and in cache mode `read`.
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
