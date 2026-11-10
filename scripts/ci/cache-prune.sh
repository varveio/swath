#!/usr/bin/env bash
# Bounded retention for Go and CodeQL caches (cache-maintenance.yml): keep the newest
# recognized main cache per family. CodeQL PR dependency caches are discarded: main caches are shared fallback data.
# Runs after main CI, on CodeQL completion events and hourly. Needs GH_TOKEN with
# actions: write and GITHUB_REPOSITORY.
#
# A key is go-build-OS-ARCH-JOB-DEPS-SHA (.github/actions/setup-go), DEPS 64 and SHA 40 hex
# digits, so the family is the key with those two stripped (and the cache version, which the
# path and compression set). Raw prefixes would overlap: go-race-cmd- also matches
# go-race-cmd-kill-. The newest is by creation time, not this run's commit: main runs overlap,
# and an older run that finishes last must not delete a newer run's caches. Only caches listed
# in one snapshot taken before any deletion are candidates, each older than a cache of its
# main family in that snapshot, so main families stay populated. Caches saved meanwhile
# are never touched. A cache already gone (404) is not an error. Keys of any other shape are left alone.
set -euo pipefail

# --plan consumes an API cache array and prints a deletion plan without credentials or writes.
# Unknown keys are deliberately outside this policy. Ref and cache version are separate scopes.
plan_caches() {
  jq '
    unique_by(.id)
    | [ .[]
        | if (.key | test("^go-build-.+-[0-9a-f]{64}-[0-9a-f]{40}$")) and .ref == "refs/heads/main" then
            . + {family: (.key | sub("-[0-9a-f]{64}-[0-9a-f]{40}$"; "")), discard: false}
          elif (.key | test("^go-mod-.+-[0-9a-f]{64}$")) and .ref == "refs/heads/main" then
            . + {family: (.key | sub("-[0-9a-f]{64}$"; "")), discard: false}
          elif (.key | test("^codeql-dependencies-[0-9]+-.+-[0-9a-f]{64}$"))
            and (.ref == "refs/heads/main" or (.ref | test("^refs/pull/[0-9]+/(head|merge)$"))) then
            . + {family: (.key | sub("-[0-9a-f]{64}$"; "")), discard: (.ref != "refs/heads/main")}
          elif (.key | test("^codeql-overlay-base-database-[0-9]+-.+-[0-9a-f]{40}-[0-9]+$")) and .ref == "refs/heads/main" then
            . + {family: (.key | sub("-[0-9a-f]{40}-[0-9]+$"; "")), discard: false}
          else empty end ]
    | group_by([.family, .version, .ref])
    | map(sort_by(.created_at, .id)
          | {family: .[0].family, ref: .[0].ref,
             keep: (if .[0].discard then null else .[-1] end),
             delete: (if .[0].discard then . else .[:-1] end)})
    | sort_by(.family, .ref)'
}

if [ "${1:-}" = --plan ]; then
  plan_caches
  exit
fi
if [ "$#" -ne 0 ]; then echo "usage: $0 [--plan]" >&2; exit 2; fi
: "${GITHUB_REPOSITORY:?}"
summary=${GITHUB_STEP_SUMMARY:-/dev/stdout}

# Fixed creation order plus deduplication prevents a moving last-access order from making
# the same cache appear on two pages. Only IDs in this snapshot can be deleted.
snapshot=$(gh api --paginate \
  "repos/$GITHUB_REPOSITORY/actions/caches?sort=created_at&direction=asc&per_page=100" \
  --jq '.actions_caches[]' | jq -s 'unique_by(.id)')
plan=$(plan_caches <<<"$snapshot")

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
  echo "### CI cache retention"
  echo
  echo "$(jq 'length' <<<"$snapshot") caches in the snapshot; deleted $deleted, freeing $((freed / 1048576)) MiB."
  echo
  echo "| family | ref | kept ID | older/PR caches deleted |"
  echo "| --- | --- | --- | --- |"
  jq -r '.[] | "| \(.family) | \(.ref) | \(.keep.id // "none") | \(.delete | length) |"' <<<"$plan"
} >>"$summary"

if [ "$failed" -ne 0 ]; then
  echo "::error::$failed cache deletion(s) failed"
  exit 1
fi
