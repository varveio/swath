#!/usr/bin/env bash
# Build, stage and verify the exact public release asset set: one archive per target holding
# swath and swath-replay with the legal files, the SPDX SBOM and SHA256SUMS. No release-facing
# glob belongs here: the set is named in full, so a stray file can never be published.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# The release targets, GOOS/GOARCH. Windows archives are zip, the others tar.gz.
TARGETS=(linux/amd64 linux/arm64 darwin/arm64 windows/amd64)
COMMANDS=(swath swath-replay)
LEGAL=(LICENSE NOTICE)

usage() {
  echo "usage: $0 build <version> <binary-directory>" >&2
  echo "       $0 stage|image-context <version> <binary-or-asset-directory> <output-directory>" >&2
  echo "       $0 checksums|verify-unsigned|verify-signed|list-signable|list-release <version> <asset-directory>" >&2
  echo "       $0 --self-test" >&2
  exit 2
}

validate_version() {
  [[ $1 =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?$ ]] || {
    echo "invalid release version: $1" >&2
    exit 2
  }
}

# platform TARGET: the OS-ARCH directory and archive suffix of a GOOS/GOARCH target.
platform() { printf '%s-%s' "${1%/*}" "${1#*/}"; }

exe() { [[ ${1%/*} == windows ]] && printf .exe || true; }

archive_name() {
  local version=$1 target=$2
  if [[ ${target%/*} == windows ]]; then
    printf 'swath-%s-%s.zip' "$version" "$(platform "$target")"
  else
    printf 'swath-%s-%s.tar.gz' "$version" "$(platform "$target")"
  fi
}

payload_names() {
  local version=$1 target
  for target in "${TARGETS[@]}"; do
    archive_name "$version" "$target"
    printf '\n'
  done
  printf '%s\n' "swath-${version}.spdx.json"
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

# build: every command for every target through go/build.sh, stamped with the version (and
# $SWATH_REVISION, or the checkout's HEAD), into BINARIES/OS-ARCH/.
build() {
  local version=$1 binaries=$2 target cmd out
  [[ ! -e $binaries ]] || { echo "binary directory already exists: $binaries" >&2; exit 1; }
  mkdir -p "$binaries"
  binaries=$(cd "$binaries" && pwd)
  for target in "${TARGETS[@]}"; do
    for cmd in "${COMMANDS[@]}"; do
      out="$binaries/$(platform "$target")/$cmd$(exe "$target")"
      echo "building $cmd for $target"
      GOOS=${target%/*} GOARCH=${target#*/} SWATH_VERSION=$version \
        "$REPO_ROOT/go/build.sh" -o "$out" "./cmd/$cmd"
    done
  done
}

# stage: one archive per target, a top-level swath-VERSION-OS-ARCH/ directory holding the
# commands and the legal files, owned by root and with the commands executable.
stage() {
  local version=$1 binaries=$2 directory=$3 root=${SWATH_RELEASE_ROOT:-$REPO_ROOT}
  local target cmd file name tree work
  [[ ! -e $directory ]] || { echo "release asset directory already exists: $directory" >&2; exit 1; }
  for target in "${TARGETS[@]}"; do
    for cmd in "${COMMANDS[@]}"; do
      file="$binaries/$(platform "$target")/$cmd$(exe "$target")"
      [[ -f $file && -s $file ]] || { echo "release binary missing or empty: $file" >&2; exit 1; }
    done
  done
  for file in "${LEGAL[@]}"; do
    [[ -f $root/$file && -s $root/$file ]] || { echo "legal file missing or empty: $root/$file" >&2; exit 1; }
  done
  mkdir -p "$directory"
  directory=$(cd "$directory" && pwd)
  work=$(mktemp -d)
  for target in "${TARGETS[@]}"; do
    name="swath-${version}-$(platform "$target")"
    tree="$work/$name"
    mkdir -m 0755 "$tree"
    for cmd in "${COMMANDS[@]}"; do
      install -m 0755 "$binaries/$(platform "$target")/$cmd$(exe "$target")" "$tree/"
    done
    for file in "${LEGAL[@]}"; do
      install -m 0644 "$root/$file" "$tree/"
    done
    if [[ ${target%/*} == windows ]]; then
      (cd "$work" && python3 -m zipfile -c "$directory/$(archive_name "$version" "$target")" "$name")
    else
      tar -C "$work" --sort=name --owner=0 --group=0 --numeric-owner \
        -czf "$directory/$(archive_name "$version" "$target")" "$name"
    fi
  done
  rm -rf -- "$work"
}

# image-context: the linux commands out of the staged archives, as Dockerfile.go's build stage
# would have written them (out/linux-ARCH/), for `build-contexts: build=DIRECTORY`.
image_context() {
  local version=$1 assets=$2 context=$3 target name
  [[ ! -e $context ]] || { echo "image context already exists: $context" >&2; exit 1; }
  for target in "${TARGETS[@]}"; do
    [[ ${target%/*} == linux ]] || continue
    name="swath-${version}-$(platform "$target")"
    mkdir -p "$context/out/$(platform "$target")"
    tar -xzf "$assets/$(archive_name "$version" "$target")" -C "$context/out/$(platform "$target")" \
      --strip-components=1 "${COMMANDS[@]/#/$name/}"
  done
}

self_test() {
  local scratch version=1.2.3 assets target cmd name
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  assets=$scratch/assets
  for target in "${TARGETS[@]}"; do
    mkdir -p "$scratch/bin/$(platform "$target")"
    for cmd in "${COMMANDS[@]}"; do
      printf '%s %s' "$cmd" "$target" > "$scratch/bin/$(platform "$target")/$cmd$(exe "$target")"
    done
  done
  for name in "${LEGAL[@]}"; do printf '%s' "$name" > "$scratch/$name"; done

  mv "$scratch/bin/linux-arm64/swath" "$scratch/swath.moved"
  if SWATH_RELEASE_ROOT=$scratch "$0" stage "$version" "$scratch/bin" "$assets" >/dev/null 2>&1; then
    echo "self-test failed: a missing binary was staged" >&2
    exit 1
  fi
  mv "$scratch/swath.moved" "$scratch/bin/linux-arm64/swath"

  SWATH_RELEASE_ROOT=$scratch "$0" stage "$version" "$scratch/bin" "$assets"
  printf sbom > "$assets/swath-${version}.spdx.json"
  "$0" checksums "$version" "$assets"
  "$0" verify-unsigned "$version" "$assets"

  # The archives hold the commands and legal files under one directory, executable.
  tar -tvzf "$assets/swath-${version}-linux-amd64.tar.gz" |
    grep -q "^-rwxr-xr-x 0/0 .* swath-${version}-linux-amd64/swath-replay$" || {
      echo "self-test failed: the linux archive lacks an executable swath-replay" >&2
      exit 1
    }
  python3 -m zipfile -l "$assets/swath-${version}-windows-amd64.zip" |
    grep -q "^swath-${version}-windows-amd64/swath.exe " || {
      echo "self-test failed: the windows archive lacks swath.exe" >&2
      exit 1
    }

  # The image context carries the archived bytes.
  "$0" image-context "$version" "$assets" "$scratch/context"
  for target in linux/amd64 linux/arm64; do
    for cmd in "${COMMANDS[@]}"; do
      cmp -s "$scratch/bin/$(platform "$target")/$cmd" "$scratch/context/out/$(platform "$target")/$cmd" || {
        echo "self-test failed: the image context's $target $cmd is not the archived one" >&2
        exit 1
      }
    done
  done

  while IFS= read -r name; do
    printf bundle > "${name}.sigstore.json"
  done < <("$0" list-signable "$version" "$assets")
  "$0" verify-signed "$version" "$assets"
  printf unwanted > "$assets/swath-${version}-linux-386.tar.gz"
  if "$0" verify-signed "$version" "$assets" >/dev/null 2>&1; then
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
command=${1:-}
case "$command" in
  stage|image-context) [[ $# -eq 4 ]] || usage ;;
  *) [[ $# -eq 3 ]] || usage ;;
esac
version=$2 directory=$3
validate_version "$version"

case "$command" in
  build)
    build "$version" "$directory"
    ;;
  stage)
    stage "$version" "$directory" "$4"
    ;;
  image-context)
    assert_exact "$version" "$directory" unsigned
    image_context "$version" "$directory" "$4"
    ;;
  checksums)
    assert_exact "$version" "$directory" payload
    payload=$(payload_names "$version")
    mapfile -t names <<< "$payload"
    (cd "$directory" && sha256sum "${names[@]}" > SHA256SUMS)
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
