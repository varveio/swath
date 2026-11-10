#!/usr/bin/env bash
# Deep runtime smoke for a swath container image.
#
#   smoke-container.sh <image-ref> [work-directory] [java|go]
#
# The implementation selects the command line: `java` (the default) for the Java image,
# `go` for the Go image (Dockerfile.go). Both list a real prefix into a Parquet dataset, read
# it back, and resume the completed dataset as a no-op.
#
# Java: exercises the shaded uber-jar's real paths that `--help` cannot reach — the merged
# ServiceLoader/FileSystem service files, the sqlite native-library extraction, the Parquet
# write path, and managed resume. Each of those can be broken by a shading or dependency-
# exclusion change while the image still starts and prints help.
#
# Go: exercises the static binary's HTTPS path through the image's CA certificates, the
# dataset writer as the image's own user, and the journal's resume of a committed dataset
# (`nothing to resume`, exit 0). The rows read back must equal the manifest's row total, and
# the manifest must record complete coverage.
#
# Correctness is judged by an INDEPENDENT Parquet reader (DuckDB), not by swath re-reading
# its own output: the promise in the docs is that any Parquet reader queries the dataset
# directly, so that is what gets tested. Exit status alone proves nothing here — a run can
# exit 0 having written an unreadable or empty dataset. A Go dataset is read by the part
# list of its manifest.json, as its readers are told to (go/docs/dataset.md).
#
# Run by release.yml publish against the release image, by digest, before its tags are
# applied, and by ci.yml go-build against the Go image, offline against bench/s3fake
# (SWATH_SMOKE_ENDPOINT), so the release smoke is exercised on every Go pull request.
#
# Runnable locally against any image reference:
#   docker build -t swath:local . && scripts/ci/smoke-container.sh swath:local
#   scripts/ci/smoke-container.sh swath:dev gosmoke go
set -euo pipefail

if [[ $# -lt 1 || $# -gt 3 ]]; then
  echo "usage: $0 <image-ref> [work-directory] [java|go]" >&2
  exit 2
fi

image=$1
workdir=${2:-containersmoke}
impl=${3:-java}
case "$impl" in
  java|go) ;;
  *) echo "unknown implementation: $impl (java or go)" >&2; exit 2 ;;
esac

command -v duckdb >/dev/null || {
  echo "smoke requires the duckdb CLI on PATH" >&2
  exit 2
}
if [[ $impl == go ]]; then
  command -v jq >/dev/null || { echo "the go smoke requires jq on PATH" >&2; exit 2; }
fi

# The listing target. Any small, stable, anonymously-readable prefix works — the smoke
# asserts "at least one object", not a specific count, and swath issues LIST calls only
# (object bodies are never fetched, so the objects' size is irrelevant).
#
# The default is a CMAS (Community Modeling and Analysis System) example case for the
# SMOKE emissions model — 7 objects, us-east-1, anonymous, not requester-pays. Note the
# name is a coincidence: "SMOKE" there is Sparse Matrix Operator Kernel Emissions, not a
# smoke test. It is a third-party research-consortium bucket, NOT a bucket we control and
# not (as far as we can confirm) part of the Registry of Open Data on AWS.
#
# Hence the override. A release publish now depends on this listing succeeding, so if the
# bucket is ever removed, renamed, or flipped to requester-pays, an operator can point the
# smoke elsewhere by setting SWATH_SMOKE_BUCKET — a repository variable and a re-tag,
# rather than a code change, a PR and a merge while a half-published release waits.
readonly BUCKET=${SWATH_SMOKE_BUCKET:-s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/}

# Go only: SWATH_SMOKE_ENDPOINT points the listing at an S3-compatible endpoint on the host
# (path-style, the container on the host network), so CI smokes the image without a network.
readonly ENDPOINT=${SWATH_SMOKE_ENDPOINT:-}
if [[ -n $ENDPOINT && $impl != go ]]; then
  echo "SWATH_SMOKE_ENDPOINT applies to the go smoke only" >&2
  exit 2
fi

# The image runs as a non-root numeric UID (10001), so the mounted output directory has to
# be writable by it. `--user` is deliberately NOT used: the smoke should exercise the
# image's own default identity, since that is what a user gets by default and a regression
# that stops it writing as itself is exactly what this must catch.
#
# The consequence is that the outputs are owned by 10001, in subdirectories owned by 10001 —
# which the invoking host user generally cannot unlink. So this refuses to reuse a work
# directory rather than pre-deleting one (a `rm -rf` here would fail on the second local
# run). CI always starts from a fresh runner, so this only ever affects repeat local use.
if [[ -e "$workdir" ]]; then
  cat >&2 <<EOF
work directory already exists: $workdir
Its contents are owned by the image's UID (10001), so removing it needs elevation:
  sudo rm -rf "$workdir"
Or pass a different work directory as the second argument.
EOF
  exit 2
fi
mkdir -p "$workdir"
chmod 777 "$workdir"
workdir=$(cd "$workdir" && pwd)

run=(docker run --rm -v "$workdir:/out")
list=(list "$BUCKET" --no-sign-request --region us-east-1 --format parquet -o /out/data)
if [[ $impl == go ]]; then
  list=(list "$BUCKET" --anonymous --region us-east-1 -o /out/data)
  if [[ -n $ENDPOINT ]]; then
    run+=(--network host)
    list+=(--endpoint "$ENDPOINT" --path-style)
  fi
fi

count_objects() {
  if [[ $impl == java ]]; then
    duckdb -noheader -list -c "select count(*) from '$workdir/data/**/*.parquet'"
    return
  fi
  local manifest=$workdir/data/manifest.json parts rows
  jq -e '.extensions.swath.coverage.complete == true' "$manifest" >/dev/null || {
    echo "smoke failed: the manifest does not record complete coverage" >&2
    return 1
  }
  parts=$(jq -r --arg dir "$workdir/data" --arg q "'" '[.parts[] | $q + $dir + "/" + .path + $q] | join(", ")' "$manifest")
  rows=$(jq '[.parts[].rows] | add // 0' "$manifest")
  [[ -n $parts ]] || { echo 0; return; }
  local read
  read=$(duckdb -noheader -list -c "select count(*) from read_parquet([$parts], union_by_name = true)")
  test "$read" -eq "$rows" || {
    echo "smoke failed: DuckDB read $read rows, the manifest lists $rows" >&2
    return 1
  }
  echo "$read"
}

echo "smoking $image ($impl)"
"${run[@]}" "$image" "${list[@]}"

objects=$(count_objects)
echo "objects read back from Parquet: $objects"
test "$objects" -ge 1 || {
  echo "smoke failed: no objects readable from the written dataset" >&2
  exit 1
}

# The supported resume command takes the managed output-directory run handle. Against a
# completed dataset it must be a clean no-op — the object count must be UNCHANGED, not
# merely a zero exit.
"${run[@]}" "$image" resume /out/data

after=$(count_objects)
echo "objects after resume no-op: $after"
test "$after" -eq "$objects" || {
  echo "smoke failed: resume changed the object count ($objects -> $after)" >&2
  exit 1
}

echo "container smoke passed: $image"
