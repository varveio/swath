#!/usr/bin/env bash
# The shards of the Go race tests (ci.yml `go-race`): the one table that says which tests run
# where, so a shard cannot be added to the workflow and forgotten here, or the reverse.
#
#   go-race-shards.sh --matrix      the matrix ci.yml expands, as JSON (needs no Go)
#   go-race-shards.sh SHARD         the `go test` arguments of SHARD, one per line (run in go/)
#   go-race-shards.sh --check       every package's tests run in exactly one shard (run in go/)
#   go-race-shards.sh --self-test   the table checks against synthetic package lists (needs no Go)
#
# A row names its packages and a -run pattern. A row with a pattern splits one package: the
# package's remainder row (same package, pattern -) -skips the union of its other rows'
# patterns, so the split is complete and disjoint by construction. A row without a pattern runs
# whole packages: a comma-separated list of package paths and ./x/... prefixes (./x and every
# package under ./x/, never ./xy), each matching at least one package. The one row naming no
# package, `rest`, is every package no other row names, so a new package lands somewhere without
# an edit. Selectors are expanded against one `go list -race ./...`, and go test gets the
# explicit package paths.
#
# --check verifies the table against the real package and test lists: shard names are unique,
# there is one `rest` and it is not empty, every selector matches, no package is in two
# whole-package rows (or named twice in one), no whole-package row covers a split package, every
# split package has exactly one remainder row, each row (pattern or remainder) selects at least
# one test, and no test is selected twice. Packages and tests are discovered with -race, as the
# shards build them, so a package or test file under `//go:build race` is not invisible to the
# check. Patterns are anchored and use only the syntax Go's RE2 and grep -E agree on.
#
# The heavy shards race on the arm64 runner only: its weaker memory model makes it the better
# race target, and the same -race suite took ~30 % less time there than on amd64
# (internal/listing 490 s against 700 s). The nightly full, unsharded race run on amd64 covers
# the other side. The `rest*` shards stay on amd64: the arm64 gain was measured on
# internal/listing only, and some packages record amd64-specific results (parquetout's
# footeronly_test.go).
set -euo pipefail

arm=ubuntu-24.04-arm
amd=ubuntu-latest
# name, runner, packages (-: every package no row names), -run pattern (-: whole packages, or
# the remainder of a split package)
table=(
  "crash        $arm ./internal/listing                         ^TestCrashMatrix\$"
  "durable      $arm ./internal/listing                         ^(TestDurable.*|TestDiskFaultMatrix)\$"
  "listing-rest $arm ./internal/listing                         -"
  "cmd-kill     $arm ./cmd/swath                                ^(TestSIGKILL.*|TestForcedExit.*)\$"
  "cmd          $arm ./cmd/swath                                -"
  "rest-replay  $amd ./internal/replay/...,./bench/replaydiff   -"
  "rest-source  $amd ./internal/source/...                      -"
  "rest         $amd -                                         -"
)

# Every package of the module, one ./relative path per line: set by load_packages (go list) or by
# the self-test.
ALL=

# Field $2 of row $1, empty for -.
field() {
  local f
  f=$(awk -v n="$2" '{print $n}' <<<"$1")
  if [ "$f" != - ]; then printf '%s' "$f"; fi
}

load_packages() {
  local module
  module=$(go list -m)
  ALL=$(go list -race ./... | sed "s#^$module\$#.#; s#^$module/#./#" | sort -u)
  if [ -z "$ALL" ]; then echo "go list -race ./... lists no package" >&2; return 1; fi
}

# The packages selector list $1 (comma-separated paths and ./x/... prefixes) names, one per line,
# in selector order and not deduplicated, so an overlap shows as a repeat. Fails on a selector
# that matches no package.
expand() {
  local sel matched
  local -a sels
  IFS=, read -ra sels <<<"$1"
  for sel in "${sels[@]}"; do
    if [[ $sel == */... ]]; then
      matched=$(awk -v p="${sel%/...}" '$0 == p || index($0, p "/") == 1' <<<"$ALL")
    else
      matched=$(awk -v p="$sel" '$0 == p' <<<"$ALL")
    fi
    if [ -z "$matched" ]; then
      echo "::error::selector '$sel' matches no package" >&2
      return 1
    fi
    printf '%s\n' "$matched"
  done
}

# Packages split by -run rows, sorted.
split_packages() {
  local row
  for row in "${table[@]}"; do
    if [ -n "$(field "$row" 4)" ]; then field "$row" 3; echo; fi
  done | sed '/^$/d' | sort -u
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

# Every package some row names, sorted.
named_packages() {
  local row sel
  for row in "${table[@]}"; do
    sel=$(field "$row" 3)
    if [ -n "$sel" ]; then expand "$sel"; fi
  done | sort -u
}

# The `rest` packages: every package no row names.
rest_packages() {
  local named
  named=$(named_packages)
  comm -23 <(echo "$ALL") <(echo "$named") | sed '/^$/d'
}

shard_args() {
  local row sel run union
  for row in "${table[@]}"; do
    [ "$(field "$row" 1)" = "$1" ] || continue
    sel=$(field "$row" 3)
    run=$(field "$row" 4)
    union=
    if [ -n "$sel" ] && [ -z "$run" ]; then union=$(split_union "$sel"); fi
    if [ -z "$sel" ]; then
      rest_packages
    elif [ -n "$run" ]; then
      printf '%s\n' -run "$run" "$sel"
    elif [ -n "$union" ]; then
      printf '%s\n' -skip "$union" "$sel"
    else
      expand "$sel"
    fi
    return 0
  done
  echo "unknown shard: $1" >&2
  return 2
}

# The checks of the table that need only the package list ($ALL). Prints every failure, then
# returns 1.
validate() {
  local row name sel run pkgs split owned= names= dup n globals=0 fail=0
  split=$(split_packages)
  for row in "${table[@]}"; do
    name=$(field "$row" 1)
    sel=$(field "$row" 3)
    run=$(field "$row" 4)
    names+="$name"$'\n'
    if [ -z "$sel" ]; then
      globals=$((globals + 1))
      if [ -n "$run" ]; then echo "::error::shard $name: the rest row takes no -run pattern"; fail=1; fi
      continue
    fi
    if [ -n "$run" ] && [[ $sel == *,* || $sel == */... ]]; then
      echo "::error::shard $name: a -run row names one package, not '$sel'"; fail=1
      continue
    fi
    if ! pkgs=$(expand "$sel"); then
      echo "::error::shard $name: '$sel' does not expand"; fail=1
      continue
    fi
    # A -run row, or a split package's remainder row: their tests are checked in --check.
    if [ -n "$run" ] || grep -qxF -- "$sel" <<<"$split"; then continue; fi
    if n=$(comm -12 <(sort <<<"$pkgs") <(echo "$split")) && [ -n "$n" ]; then
      echo "::error::shard $name runs whole packages that other rows split: $n"; fail=1
    fi
    owned+="$pkgs"$'\n'
  done
  if dup=$(sed '/^$/d' <<<"$names" | sort | uniq -d) && [ -n "$dup" ]; then
    echo "::error::shard names used twice: $dup"; fail=1
  fi
  if [ "$globals" != 1 ]; then
    echo "::error::$globals rows name no package, not 1"; fail=1
  fi
  if dup=$(sed '/^$/d' <<<"$owned" | sort | uniq -d) && [ -n "$dup" ]; then
    echo "::error::packages selected more than once by whole-package rows: $dup"; fail=1
  fi
  # Each split package has exactly one remainder row: without it, the tests its patterns miss
  # would run nowhere; with two, they would run twice.
  for sel in $split; do
    n=0
    for row in "${table[@]}"; do
      if [ "$(field "$row" 3)" = "$sel" ] && [ -z "$(field "$row" 4)" ]; then n=$((n + 1)); fi
    done
    if [ "$n" != 1 ]; then
      echo "::error::package $sel has $n remainder rows, not 1"; fail=1
    fi
  done
  if [ "$fail" = 0 ] && [ -z "$(rest_packages)" ]; then
    echo "::error::the rest shard has no package"; fail=1
  fi
  return "$fail"
}

check() {
  local row pkg run tests n rest_n fail=0
  validate || fail=1
  # Tests of each split package: every row selects something, no test twice.
  for pkg in $(split_packages); do
    tests=$(go test -race -list . "$pkg" | grep -E '^(Test|Fuzz|Example)' || true)
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
    rest_n=0
    while read -r t; do
      n=0
      for row in "${table[@]}"; do
        [ "$(field "$row" 3)" = "$pkg" ] || continue
        run=$(field "$row" 4)
        if [ -n "$run" ] && grep -qE -- "$run" <<<"$t"; then n=$((n + 1)); fi
      done
      if [ "$n" -gt 1 ]; then
        echo "::error::$pkg $t is selected by $n shards"; fail=1
      elif [ "$n" -eq 0 ]; then
        rest_n=$((rest_n + 1))
      fi
    done <<<"$tests"
    if [ "$rest_n" -lt 1 ]; then
      echo "::error::the remainder shard of $pkg selects no test"; fail=1
    fi
    echo "$pkg: $(wc -l <<<"$tests") tests over $(printf '%s\n' "${table[@]}" | awk -v p="$pkg" '$3 == p' | wc -l) shards"
  done
  [ "$fail" = 0 ] || return 1
  for row in "${table[@]}"; do
    echo "shard $(field "$row" 1) ($(field "$row" 2)): $(shard_args "$(field "$row" 1)" | paste -sd' ' -)"
  done
}

# validate and the expansion against a synthetic package list: each case is a table and whether
# validate must accept it.
self_test() {
  local fail=0 got
  ALL=$(printf '%s\n' ./a ./a/b ./ab ./c ./d)
  expect() {  # expect ok|fail DESCRIPTION ROW...
    local want=$1 what=$2 verdict=ok
    shift 2
    table=("$@")
    validate >/dev/null 2>&1 || verdict=fail
    if [ "$verdict" != "$want" ]; then echo "self-test: $what: validate says $verdict, want $want"; fail=1; fi
  }
  expect ok   "comma list and prefix"       "x r ./a/...,./c -" "rest r - -"
  expect fail "overlapping rows"            "x r ./a/... -" "y r ./a/b -" "rest r - -"
  expect fail "overlap within a row"        "x r ./a/...,./a/b -" "rest r - -"
  expect fail "prefix matching nothing"     "x r ./z/... -" "rest r - -"
  expect fail "package that does not exist" "x r ./a/z -" "rest r - -"
  expect fail "duplicate shard name"        "x r ./a -" "x r ./c -" "rest r - -"
  expect fail "two rest rows"               "x r ./a -" "rest r - -" "more r - -"
  expect fail "empty rest"                  "x r ./... -" "rest r - -"
  expect ok   "split package"               "s r ./a ^TestX\$" "s-rest r ./a -" "rest r - -"
  expect fail "prefix covering a split"     "s r ./a ^TestX\$" "s-rest r ./a -" "w r ./a/... -" "rest r - -"
  expect fail "split with no remainder"     "s r ./a ^TestX\$" "rest r - -"
  expect fail "-run row with a prefix"      "s r ./a/... ^TestX\$" "s-rest r ./a -" "rest r - -"
  table=("x r ./a/...,./c -" "rest r - -")
  got=$(shard_args x | paste -sd' ' -)
  if [ "$got" != "./a ./a/b ./c" ]; then echo "self-test: x expands to '$got', want './a ./a/b ./c'"; fail=1; fi
  got=$(shard_args rest | paste -sd' ' -)
  if [ "$got" != "./ab ./d" ]; then echo "self-test: rest expands to '$got', want './ab ./d'"; fail=1; fi
  if [ "$fail" = 0 ]; then echo "self-test: ok"; fi
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
  --self-test) self_test ;;
  --check) load_packages; check ;;
  ''|-*) sed -n '2,9s/^# \{0,1\}//p' "$0" >&2; exit 2 ;;
  *) load_packages; shard_args "$1" ;;
esac
