#!/usr/bin/env bash
# Check that a release tag matches the crate version.
#
# Usage: scripts/check-release-tag.sh <tag>
#
# Accepted tag forms: v0.3.0, 0.3.0, s3-sidecar-v0.3.0, s3-sidecar/0.3.0

set -euo pipefail

usage() {
  echo "Usage: scripts/check-release-tag.sh <tag>" >&2
}

# The SemVer 2.0.0 pattern, with an optional pre-release and build suffix.
semver_pattern='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-((0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*)(\.(0|[1-9][0-9]*|[0-9]*[a-zA-Z-][0-9a-zA-Z-]*))*))?([+]([0-9a-zA-Z-]+(\.[0-9a-zA-Z-]+)*))?$'

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

tag=$1

if [[ -z $tag ]]; then
  echo "ERROR: The tag is empty." >&2
  exit 2
fi

tag_version=${tag#s3-sidecar/}
tag_version=${tag_version#s3-sidecar-}
tag_version=${tag_version#v}

if [[ -z $tag_version || ! $tag_version =~ $semver_pattern ]]; then
  echo "ERROR: The tag is not a valid version tag: ${tag}" >&2
  exit 2
fi

root=$(cd "$(dirname "$0")/.." && pwd)
manifest=${root}/Cargo.toml

if [[ ! -f $manifest ]]; then
  echo "ERROR: Cargo.toml is missing." >&2
  exit 2
fi

crate_version=$(
  awk '
    /^\[/ { in_package = ($0 == "[package]") }
    in_package && /^version[[:space:]]*=/ {
      line = $0
      sub(/^[^"]*"/, "", line)
      sub(/".*$/, "", line)
      print line
      exit
    }
  ' "$manifest"
)

if [[ -z $crate_version ]]; then
  echo "ERROR: Cargo.toml has no version in [package]." >&2
  exit 2
fi

if [[ $tag_version == "$crate_version" ]]; then
  echo "The tag matches the crate version."
  exit 0
fi

printf 'Tag version: %s\n' "$tag_version"
printf 'Crate version: %s\n' "$crate_version"
printf 'Run scripts/set-version.sh %s, then commit the change before you create the tag.\n' "$tag_version"
exit 1
