# Release

This document gives the release process for s3-sidecar. Run every step on the
release branch. An agent must not tag, push, or publish without an explicit
instruction.

## Purpose and version policy

The repository has no `v0.1.0` tag. `v0.2.0` is the first public release tag.

The version follows Semantic Versioning. The `package.version` field in
`Cargo.toml` is the source of truth. Update `Cargo.lock` in the same change.
`otel.service_version` uses the crate version by default.

A release candidate uses the `vX.Y.Z-rc.N` form. A prerelease must not update the
`latest` container tag.

## Pre-tag checks

Run these commands from the repository root. All commands must pass.
`git status --short` must show no output.

```bash
git status --short
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo deny check
dist plan --output-format=human
```

`dist plan --output-format=human` must show version `0.2.0`.

Run `mise exec -- <command>` when `mise.toml` supplies the tool. Do not use
`--locked` before you update `Cargo.lock`.

## Release candidate

1. Create the candidate tag.

   ```bash
   git tag -a v0.2.0-rc.1 -m "v0.2.0-rc.1"
   ```

2. Push the tag.

   ```bash
   git push origin v0.2.0-rc.1
   ```

3. Wait for the release workflow to complete.

4. Verify the five target archives:
   - `aarch64-apple-darwin`
   - `aarch64-unknown-linux-gnu`
   - `x86_64-apple-darwin`
   - `x86_64-unknown-linux-gnu`
   - `x86_64-pc-windows-msvc`

5. Verify the two container architectures: `amd64` and `arm64`.

6. Confirm that the candidate did not update the `latest` container tag.

## Final release

1. Confirm that the candidate is clean.

2. Create the final tag.

   ```bash
   git tag -a v0.2.0 -m "v0.2.0"
   ```

3. Push the tag.

   ```bash
   git push origin v0.2.0
   ```

4. Wait for the release workflow to complete.

5. Verify the GitHub release assets.

6. Verify the `v0.2.0` container manifest.

7. Verify the `latest` container tag.

8. Verify the published checksums.

## Container image workflow

The `dist-workspace.toml` file declares the container image workflow as a
cargo-dist `post-announce-jobs` custom job. cargo-dist runs this custom job
after the release is created and after the announce phase. Do not edit
`.github/workflows/release.yml` by hand. That file is generated. Run
`mise exec -- dist generate --mode ci` after you change `dist-workspace.toml`.

A human can publish a release in the GitHub UI. That action starts the image
workflow through the `release: published` event.

### Container registry access

The first push creates the GHCR package. The first push can return `denied`
while GitHub links the package to the repository. The workflow retries the push
three times. If the retries still fail, a human grants this repository write
access to the package in the package settings.

## Human-only steps

A human performs these steps:
- Push a branch or a tag.
- Create a tag.
- Publish a release.

An agent must not do these steps without an explicit instruction.

## Rollback

Delete a bad tag only before the image is published. After publication, publish
a new patch version. Do not rewrite a published release.

## Release notes

Keep the documented known limits in the release notes. Read the known limits in
`CHANGELOG.md` and `docs/operations.md`.
