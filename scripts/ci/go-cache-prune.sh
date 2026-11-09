#!/usr/bin/env bash
# Bounded retention for the Go caches (ci.yml `go-cache-prune`, after the Go jobs of a push to
# main): every `go-build-` and `go-mod-` cache on refs/heads/main except the newest of its family
# is deleted. Needs GH_TOKEN with actions: write and GITHUB_REPOSITORY.
#
# Keys are go-build-OS-ARCH-JOB-DEPS-SHA and go-mod-OS-ARCH-DEPS (.github/actions/setup-go), DEPS
# 64 and SHA 40 hex digits, so the family is the key with those stripped (and the cache version,
# which the path and compression set): one per job and platform for build caches, one per
# platform for module caches. Raw prefixes would overlap: go-race-cmd- also matches
# go-race-cmd-kill-. The newest is by creation time, not this run's commit: main runs overlap,
# and an older run that finishes last must not delete a newer run's caches. Only caches listed
# in one snapshot taken before any deletion are candidates, each older than a cache of its
# family in that snapshot, so a family is never left empty and a cache saved meanwhile is never
# touched. A superseded module cache is still read by pull requests branched before the go.mod
# change, through the restore-key prefix, which then yields the newest set and downloads the
# difference. A cache already gone (404) is not an error. Keys of any other shape are left alone.
set -euo pipefail

: "${GITHUB_REPOSITORY:?}"
summary=${GITHUB_STEP_SUMMARY:-/dev/stdout}

# All pages: the API returns at most 100 per page. Its default order is by last access, which a
# restore between two page requests changes, so a cache could be listed twice (and one copy
# deleted as older than the other). Creation order is fixed, and ids are deduplicated anyway.
snapshot=$(for prefix in go-build- go-mod-; do
  gh api --paginate \
    "repos/$GITHUB_REPOSITORY/actions/caches?ref=refs/heads/main&key=$prefix&sort=created_at&direction=asc&per_page=100" \
    --jq '.actions_caches[]'
done | jq -s 'unique_by(.id)')

# One object per family: the retained cache and the ones to delete, oldest first.
plan=$(jq '
  [ .[]
    | select(.ref == "refs/heads/main")
    | (.key | [capture("^(?<family>go-build-.+)-[0-9a-f]{64}-[0-9a-f]{40}$"),
               capture("^(?<family>go-mod-[^-]+-[^-]+)-[0-9a-f]{64}$")] | .[0]) as $m
    | select($m != null)
    | . + {family: $m.family} ]
  | group_by([.family, .version])
  | map(sort_by(.created_at, .id)
        | {family: .[0].family, keep: .[-1], delete: .[:-1]})
  | sort_by(.family)' <<<"$snapshot")

failed=0
deleted=0
freed=0
while IFS=$'\t' read -r id key size; do
  if out=$(gh api -X DELETE "repos/$GITHUB_REPOSITORY/actions/caches/$id" 2>&1); then
    echo "deleted $key ($id)"
    deleted=$((deleted + 1))
    freed=$((freed + size))
  elif grep -q 'HTTP 404' <<<"$out"; then
    echo "already gone: $key ($id)"
  else
    echo "::error::deleting cache $id ($key): $out"
    failed=$((failed + 1))
  fi
done < <(jq -r '.[].delete[] | [.id, .key, .size_in_bytes] | @tsv' <<<"$plan")

{
  echo "### Go cache retention"
  echo
  echo "$(jq 'length' <<<"$snapshot") \`go-build-\` and \`go-mod-\` caches on main in the snapshot; deleted $deleted, freeing $((freed / 1048576)) MiB."
  echo
  echo "| family | version | kept (created) | kept MiB | older deleted |"
  echo "| --- | --- | --- | --- | --- |"
  jq -r '.[] | "| \(.family) | \(.keep.version[0:12]) | \(.keep.created_at) | \(.keep.size_in_bytes / 1048576 | floor) | \(.delete | length) |"' <<<"$plan"
} >>"$summary"

if [ "$failed" -ne 0 ]; then
  echo "::error::$failed cache deletion(s) failed"
  exit 1
fi
