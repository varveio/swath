#!/usr/bin/env bash
# Stage and verify the exact public release asset set. No release-facing glob belongs here:
# Shadow also creates distributions, and a broad swath-* copy can silently publish them.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

usage() {
  echo "usage: $0 stage|checksums|verify-unsigned|verify-signed|list-signable|list-release <version> <asset-directory>" >&2
  echo "       $0 --self-test" >&2
  exit 2
}

validate_version() {
  [[ $1 =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?$ ]] || {
    echo "invalid release version: $1" >&2
    exit 2
  }
}

payload_names() {
  local version=$1
  printf '%s\n' \
    "swath-${version}.jar" \
    "swath-${version}.spdx.json" \
    "swath-${version}.tar.gz" \
    "swath-${version}.zip"
}

unsigned_names() {
  local version=$1
  payload_names "$version"
  printf '%s\n' SHA256SUMS
}

signed_names() {
  local version=$1 name
  while IFS= read -r name; do
    printf '%s\n' "$name" "${name}.sigstore.json"
  done < <(unsigned_names "$version")
}

assert_exact() {
  local version=$1 directory=$2 set_name=$3 expected actual name
  [[ -d $directory ]] || { echo "release asset directory does not exist: $directory" >&2; exit 1; }

  case "$set_name" in
    payload) expected=$(payload_names "$version" | LC_ALL=C sort) ;;
    unsigned) expected=$(unsigned_names "$version" | LC_ALL=C sort) ;;
    signed) expected=$(signed_names "$version" | LC_ALL=C sort) ;;
    *) usage ;;
  esac
  actual=$(
    shopt -s nullglob dotglob
    entries=("$directory"/*)
    ((${#entries[@]} == 0)) || printf '%s\n' "${entries[@]##*/}"
  )
  actual=$(printf '%s\n' "$actual" | LC_ALL=C sort)
  if [[ $actual != "$expected" ]]; then
    echo "release asset set is not exact ($set_name expected)" >&2
    diff -u <(printf '%s\n' "$expected") <(printf '%s\n' "$actual") >&2 || true
    exit 1
  fi

  while IFS= read -r name; do
    [[ -f "$directory/$name" && ! -L "$directory/$name" && -s "$directory/$name" ]] || {
      echo "release asset is not a non-empty regular file: $name" >&2
      exit 1
    }
  done <<< "$expected"
}

stage() {
  local version=$1 directory=$2 root=${SWATH_RELEASE_ROOT:-$REPO_ROOT}
  local jar="$root/swath-cli/build/libs/swath.jar"
  local zip="$root/swath-cli/build/distributions/swath-${version}.zip"
  local tar="$root/swath-cli/build/distributions/swath-${version}.tar.gz"
  [[ ! -e $directory ]] || { echo "release asset directory already exists: $directory" >&2; exit 1; }
  for source in "$jar" "$zip" "$tar"; do
    [[ -f $source && -s $source ]] || { echo "release build output missing or empty: $source" >&2; exit 1; }
  done
  mkdir -p "$directory"
  cp -- "$jar" "$directory/swath-${version}.jar"
  cp -- "$zip" "$directory/swath-${version}.zip"
  cp -- "$tar" "$directory/swath-${version}.tar.gz"
}

self_test() {
  local scratch version=1.2.3 assets=assets name
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  mkdir -p "$scratch/swath-cli/build/libs" "$scratch/swath-cli/build/distributions"
  printf jar > "$scratch/swath-cli/build/libs/swath.jar"
  printf zip > "$scratch/swath-cli/build/distributions/swath-${version}.zip"
  printf tar > "$scratch/swath-cli/build/distributions/swath-${version}.tar.gz"
  # This is the issue #65 regression fixture: it must remain outside the staged set.
  printf shadow > "$scratch/swath-cli/build/distributions/swath-cli-shadow-${version}.zip"
  SWATH_RELEASE_ROOT=$scratch "$0" stage "$version" "$scratch/$assets"
  printf sbom > "$scratch/$assets/swath-${version}.spdx.json"
  "$0" checksums "$version" "$scratch/$assets"
  "$0" verify-unsigned "$version" "$scratch/$assets"
  while IFS= read -r name; do
    printf bundle > "${name}.sigstore.json"
  done < <("$0" list-signable "$version" "$scratch/$assets")
  "$0" verify-signed "$version" "$scratch/$assets"
  printf unwanted > "$scratch/$assets/swath-${version}-shadow.zip"
  if "$0" verify-signed "$version" "$scratch/$assets" >/dev/null 2>&1; then
    echo "self-test failed: an unexpected asset was accepted" >&2
    exit 1
  fi
  echo "release asset self-test passed"
}

[[ $# -gt 0 ]] || usage
if [[ $1 == --self-test ]]; then
  [[ $# -eq 1 ]] || usage
  self_test
  exit
fi
[[ $# -eq 3 ]] || usage
command=$1 version=$2 directory=$3
validate_version "$version"

case "$command" in
  stage)
    stage "$version" "$directory"
    ;;
  checksums)
    assert_exact "$version" "$directory" payload
    (cd "$directory" && sha256sum \
      "swath-${version}.jar" "swath-${version}.spdx.json" \
      "swath-${version}.tar.gz" "swath-${version}.zip" > SHA256SUMS)
    assert_exact "$version" "$directory" unsigned
    ;;
  verify-unsigned)
    assert_exact "$version" "$directory" unsigned
    (cd "$directory" && sha256sum --check --status SHA256SUMS) || {
      echo "release asset checksum verification failed" >&2
      exit 1
    }
    ;;
  verify-signed)
    assert_exact "$version" "$directory" signed
    (cd "$directory" && sha256sum --check --status SHA256SUMS) || {
      echo "release asset checksum verification failed" >&2
      exit 1
    }
    ;;
  list-signable)
    assert_exact "$version" "$directory" unsigned
    while IFS= read -r name; do printf '%s/%s\n' "$directory" "$name"; done < <(unsigned_names "$version")
    ;;
  list-release)
    assert_exact "$version" "$directory" signed
    while IFS= read -r name; do printf '%s/%s\n' "$directory" "$name"; done < <(signed_names "$version")
    ;;
  *) usage ;;
esac
