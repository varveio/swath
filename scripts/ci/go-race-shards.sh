#!/usr/bin/env bash
# The shards of the Go race tests (ci.yml `go-race`): the one table that says which tests run
# where, so a shard cannot be added to the workflow and forgotten here, or the reverse.
#
#   go-race-shards.sh --matrix   the matrix ci.yml expands, as JSON (needs no Go)
#   go-race-shards.sh SHARD      the `go test` arguments of SHARD, one per line (run in go/)
#   go-race-shards.sh --check    every package's tests run in exactly one shard (run in go/)
#
# A row names a package and a -run pattern: the package's remainder shard -skips the union of
# its other rows' patterns, so the split of one package is complete and disjoint by
# construction, and `rest` is every package no row names, so a new package lands somewhere
# without an edit. --check still verifies it against the real test lists: each pattern must
# select at least one test and no test may be selected twice. Patterns are anchored and use
# only the syntax Go's RE2 and grep -E agree on.
#
# The heavy shards race on the arm64 runner only: its weaker memory model makes it the better
# race target, and the same -race suite took ~30 % less time there than on amd64
# (internal/listing 490 s against 700 s). The nightly full, unsharded race run on amd64 covers
# the other side. `rest` stays on amd64.
set -euo pipefail

arm=ubuntu-24.04-arm
amd=ubuntu-latest
# name, runner, package (-: every package no row names), -run pattern (-: the package's remainder)
table=(
  "crash        $arm ./internal/listing ^TestCrashMatrix\$"
  "durable      $arm ./internal/listing ^(TestDurable.*|TestDiskFaultMatrix)\$"
  "listing-rest $arm ./internal/listing -"
  "cmd-kill     $arm ./cmd/swath        ^(TestSIGKILL.*|TestForcedExit.*)\$"
  "cmd          $arm ./cmd/swath        -"
  "rest         $amd -                  -"
)

# Field $2 of row $1, empty for -.
field() {
  local f
  f=$(awk -v n="$2" '{print $n}' <<<"$1")
  if [ "$f" != - ]; then printf '%s' "$f"; fi
}

# Rows of package $1.
rows_of() {
  local row
  for row in "${table[@]}"; do
    if [ "$(field "$row" 3)" = "$1" ]; then printf '%s\n' "$row"; fi
  done
}

# The union of the -run patterns of the rows that split package $1.
split_union() {
  local row union=
  for row in "${table[@]}"; do
    if [ "$(field "$row" 3)" = "$1" ] && [ -n "$(field "$row" 4)" ]; then
      union+="${union:+|}$(field "$row" 4)"
    fi
  done
  printf '%s' "$union"
}

named_packages() {
  local row
  for row in "${table[@]}"; do field "$row" 3; echo; done | sed '/^$/d' | sort -u
}

# Every package of the module, as ./relative paths.
all_packages() {
  local module
  module=$(go list -m)
  go list ./... | sed "s#^$module\$#.#; s#^$module/#./#" | sort -u
}

shard_args() {
  local row pkg run union
  for row in "${table[@]}"; do
    [ "$(field "$row" 1)" = "$1" ] || continue
    pkg=$(field "$row" 3)
    run=$(field "$row" 4)
    if [ -z "$pkg" ]; then
      comm -23 <(all_packages) <(named_packages)
    elif [ -n "$run" ]; then
      printf '%s\n' -run "$run" "$pkg"
    else
      union=$(split_union "$pkg")
      if [ -n "$union" ]; then printf '%s\n' -skip "$union"; fi
      printf '%s\n' "$pkg"
    fi
    return 0
  done
  echo "unknown shard: $1" >&2
  return 2
}

check() {
  local all named row pkg run union tests n fail=0
  all=$(all_packages)
  named=$(named_packages)
  # A row naming a package that no longer exists would run nothing and pass.
  if missing=$(comm -13 <(echo "$all") <(echo "$named")) && [ -n "$missing" ]; then
    echo "::error::shard table names packages go list does not: $missing"
    fail=1
  fi
  # Packages: each shard's package list, then every package of the module counted. A split
  # package appears once per row; its rows are checked test by test below.
  local listed=
  for row in "${table[@]}"; do
    listed+=$(shard_args "$(field "$row" 1)" | grep -v '^-' | grep -v '^\^' || true)$'\n'
  done
  for pkg in $all; do
    rows=$(grep -cxF -- "$pkg" <<<"$listed" || true)
    split=$(rows_of "$pkg" | grep -c . || true)
    if [ "$rows" -lt 1 ]; then
      echo "::error::package $pkg is in no shard"; fail=1
    elif [ "$rows" -gt 1 ] && [ "$split" != "$rows" ]; then
      echo "::error::package $pkg is in $rows shards"; fail=1
    fi
  done
  # Tests of each split package: every pattern selects something, no test twice.
  for pkg in $named; do
    union=$(split_union "$pkg")
    [ -n "$union" ] || continue
    tests=$(go test -list . "$pkg" | grep -E '^(Test|Fuzz|Example)' || true)
    for row in "${table[@]}"; do
      [ "$(field "$row" 3)" = "$pkg" ] || continue
      run=$(field "$row" 4)
      [ -n "$run" ] || continue
      # grep exits 2 on a pattern it cannot parse: that fails the check, not "no match".
      n=$(grep -cE -- "$run" <<<"$tests") || [ $? -eq 1 ]
      if [ "$n" -lt 1 ]; then
        echo "::error::shard $(field "$row" 1): $run selects no test in $pkg"; fail=1
      fi
    done
    while read -r t; do
      n=0
      for row in "${table[@]}"; do
        [ "$(field "$row" 3)" = "$pkg" ] || continue
        run=$(field "$row" 4)
        if [ -n "$run" ] && grep -qE -- "$run" <<<"$t"; then n=$((n + 1)); fi
      done
      if [ "$n" -gt 1 ]; then
        echo "::error::$pkg $t is selected by $n shards"; fail=1
      fi
    done <<<"$tests"
    echo "$pkg: $(wc -l <<<"$tests") tests over $(rows_of "$pkg" | grep -c .) shards"
  done
  for row in "${table[@]}"; do
    echo "shard $(field "$row" 1) ($(field "$row" 2)): $(shard_args "$(field "$row" 1)" | paste -sd' ' -)"
  done
  return "$fail"
}

case "${1:-}" in
  --matrix)
    sep=
    printf '['
    for row in "${table[@]}"; do
      printf '%s{"shard":"%s","runner":"%s"}' "$sep" "$(field "$row" 1)" "$(field "$row" 2)"
      sep=,
    done
    printf ']\n'
    ;;
  --check) check ;;
  ''|-*) sed -n '2,8s/^# \{0,1\}//p' "$0" >&2; exit 2 ;;
  *) shard_args "$1" ;;
esac
