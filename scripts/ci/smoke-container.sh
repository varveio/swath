#!/usr/bin/env bash
# Deep runtime smoke for the swath lister image (the Dockerfile).
#
#   smoke-container.sh <image-ref> [work-directory]
#
# Lists a real prefix into a dataset, reads it back, and resumes the completed dataset as a
# no-op. It exercises the static binary's HTTPS path through the image's CA certificates, the
# dataset writer as the image's own user, and the journal's resume of a committed dataset
# (`nothing to resume`, exit 0). The rows read back must equal the manifest's row total, and
# the manifest must record complete coverage.
#
# Correctness is judged by an INDEPENDENT Parquet reader (DuckDB), not by swath re-reading
# its own output: the promise in the docs is that any Parquet reader queries the dataset
# directly, so that is what gets tested. Exit status alone proves nothing here — a run can
# exit 0 having written an unreadable or empty dataset. The dataset is read by the part list
# of its manifest.json, as its readers are told to (docs/dataset.md).
#
# Run by release.yml publish against the release image, by digest, before its tags are
# applied, and by ci.yml go-build against the image it builds, offline against bench/s3fake
# (SWATH_SMOKE_ENDPOINT), so the release smoke is exercised on every Go pull request.
#
# Runnable locally against any image reference:
#   docker build -t swath:dev . && scripts/ci/smoke-container.sh swath:dev
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <image-ref> [work-directory]" >&2
  exit 2
fi

image=$1
workdir=${2:-containersmoke}

for tool in duckdb jq; do
  command -v "$tool" >/dev/null || { echo "smoke requires the $tool CLI on PATH" >&2; exit 2; }
done

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

# SWATH_SMOKE_ENDPOINT points the listing at an S3-compatible endpoint on the host
# (path-style, the container on the host network), so CI smokes the image without a network.
readonly ENDPOINT=${SWATH_SMOKE_ENDPOINT:-}

# The image runs as a non-root numeric UID (65532), so the mounted output directory has to
# be writable by it. `--user` is deliberately NOT used: the smoke should exercise the
# image's own default identity, since that is what a user gets by default and a regression
# that stops it writing as itself is exactly what this must catch.
#
# The consequence is that the outputs are owned by 65532, in subdirectories owned by 65532 —
# which the invoking host user generally cannot unlink. So this refuses to reuse a work
# directory rather than pre-deleting one (a `rm -rf` here would fail on the second local
# run). CI always starts from a fresh runner, so this only ever affects repeat local use.
if [[ -e "$workdir" ]]; then
  cat >&2 <<EOF
work directory already exists: $workdir
Its contents are owned by the image's UID (65532), so removing it needs elevation:
  sudo rm -rf "$workdir"
Or pass a different work directory as the second argument.
EOF
  exit 2
fi
mkdir -p "$workdir"
chmod 777 "$workdir"
workdir=$(cd "$workdir" && pwd)

run=(docker run --rm -v "$workdir:/out")
list=(list "$BUCKET" --anonymous --s3-region us-east-1 -o /out/data)
if [[ -n $ENDPOINT ]]; then
  run+=(--network host)
  list+=(--endpoint-url "$ENDPOINT" --s3-path-style)
fi

count_objects() {
  local manifest=$workdir/data/manifest.json parts rows read
  jq -e '.extensions.swath.coverage.complete == true' "$manifest" >/dev/null || {
    echo "smoke failed: the manifest does not record complete coverage" >&2
    return 1
  }
  parts=$(jq -r --arg dir "$workdir/data" --arg q "'" '[.parts[] | $q + $dir + "/" + .path + $q] | join(", ")' "$manifest")
  rows=$(jq '[.parts[].rows] | add // 0' "$manifest")
  [[ -n $parts ]] || { echo 0; return; }
  read=$(duckdb -noheader -list -c "select count(*) from read_parquet([$parts], union_by_name = true)")
  test "$read" -eq "$rows" || {
    echo "smoke failed: DuckDB read $read rows, the manifest lists $rows" >&2
    return 1
  }
  echo "$read"
}

echo "smoking $image"
"${run[@]}" "$image" "${list[@]}"

objects=$(count_objects)
echo "objects read back from Parquet: $objects"
test "$objects" -ge 1 || {
  echo "smoke failed: no objects readable from the written dataset" >&2
  exit 1
}

# Resume takes the dataset directory. Against a completed dataset it must be a clean no-op —
# the object count must be UNCHANGED, not merely a zero exit.
"${run[@]}" "$image" resume /out/data

after=$(count_objects)
echo "objects after resume no-op: $after"
test "$after" -eq "$objects" || {
  echo "smoke failed: resume changed the object count ($objects -> $after)" >&2
  exit 1
}

echo "container smoke passed: $image"
