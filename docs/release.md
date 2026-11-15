# Release

This document gives the release process for s3-sidecar. Run every step on the
release branch. An agent must not tag, push, or publish without an explicit
instruction.

## Purpose and version policy

The repository has no `v0.1.0` tag. `v0.2.0` was the first public release tag.
`v0.3.0` is the next release tag.

The version follows Semantic Versioning. The `package.version` field in
`Cargo.toml` is the source of truth. Update `Cargo.lock` in the same change.
`otel.service_version` uses the crate version by default.

The tag version must equal `package.version`, including any pre-release suffix.
cargo-dist selects packages whose version is exactly equal to the tag version. A
`v0.3.0-rc.1` tag therefore needs `version = "0.3.0-rc.1"` in `Cargo.toml`. Use
`scripts/set-version.sh <version>` to set both files. Use
`scripts/check-release-tag.sh <tag>` to confirm the match before you create the
tag.

A release candidate uses the `vX.Y.Z-rc.N` form. A prerelease must not update the
`latest` container tag. cargo-dist marks a GitHub release as a pre-release when
the tag has a pre-release suffix. The container image workflow reads
`isPrerelease` and skips the `latest` tag for that release.

## Pre-tag checks

Run these commands from the repository root. All commands must pass.
`git status --short` must show no output. `<tag>` is the tag that you intend to
create.

```bash
git status --short
scripts/check-release-tag.sh <tag>
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo deny check
dist plan --output-format=human
dist generate --mode ci --check
```

`scripts/check-release-tag.sh <tag>` must print `The tag matches the crate
version.` `dist plan --output-format=human` must show the current
`package.version`. The tag-push job runs `dist host --tag=<tag> --steps=create`.
That job stops with `This workspace doesn't have anything for dist to Release!`
when the tag version and `package.version` differ. The tag check prevents that
stop. `dist generate --mode ci --check` must report no difference. A difference
means the generated workflow is stale. Run `mise exec -- dist generate --mode ci`
and commit the result.

Run `mise exec -- <command>` when `mise.toml` supplies the tool. Do not use
`--locked` before you update `Cargo.lock`.

The `cargo-dist` version in `mise.toml` equals `cargo-dist-version` in
`dist-workspace.toml`, and a version change updates both files.

## Release candidate

1. Set the candidate version.

   ```bash
   scripts/set-version.sh 0.3.0-rc.1
   ```

   The script changes `Cargo.toml` and `Cargo.lock`. Do not edit either file by
   hand.

2. Commit the version change.

   ```bash
   git add Cargo.toml Cargo.lock
   git commit -m "chore(release): 0.3.0-rc.1"
   ```

3. Run the pre-tag checks. The tree is clean here. Pass the candidate tag to the
   tag check.

   ```bash
   scripts/check-release-tag.sh v0.3.0-rc.1
   ```

   `dist plan --output-format=human` must show `0.3.0-rc.1`.

4. Create the candidate tag.

   ```bash
   git tag -a v0.3.0-rc.1 -m "v0.3.0-rc.1"
   ```

5. Push the tag.

   ```bash
   git push origin v0.3.0-rc.1
   ```

6. Wait for the release workflow to complete.

7. Verify the five target archives:
   - `aarch64-apple-darwin`
   - `aarch64-unknown-linux-gnu`
   - `x86_64-apple-darwin`
   - `x86_64-unknown-linux-gnu`
   - `x86_64-pc-windows-msvc`

8. Verify the two container architectures: `amd64` and `arm64`.

9. Confirm that the GitHub release is marked as a pre-release.

10. Confirm that the candidate did not update the `latest` container tag.

## Final release

1. Confirm that the candidate is clean.

2. Set the final version.

   ```bash
   scripts/set-version.sh 0.3.0
   ```

3. Commit the version change.

   ```bash
   git add Cargo.toml Cargo.lock
   git commit -m "chore(release): 0.3.0"
   ```

4. Run the pre-tag checks. The tree is clean here. Pass the final tag to the tag
   check.

   ```bash
   scripts/check-release-tag.sh v0.3.0
   ```

   `dist plan --output-format=human` must show `0.3.0`.

5. Create the final tag.

   ```bash
   git tag -a v0.3.0 -m "v0.3.0"
   ```

6. Push the tag.

   ```bash
   git push origin v0.3.0
   ```

7. Wait for the release workflow to complete.

8. Verify the GitHub release assets.

9. Verify the `v0.3.0` container manifest.

10. Verify the `latest` container tag.

11. Verify the published checksums.

## Container image workflow

The `dist-workspace.toml` file declares the container image workflow as a
cargo-dist `post-announce-jobs` custom job. cargo-dist runs this custom job
after the release is created and after the announce phase. The Release workflow
calls `.github/workflows/release-container-image.yml` through that custom job.
Do not edit `.github/workflows/release.yml` by hand. That file is generated.
Run `mise exec -- dist generate --mode ci` after you change
`dist-workspace.toml`.

A release created with `GITHUB_TOKEN` does not trigger a separate `release:
published` workflow. GitHub does not start a new workflow run from an event
that the `GITHUB_TOKEN` caused. The custom job in the Release workflow is the
path that publishes the image for an automated release.

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
