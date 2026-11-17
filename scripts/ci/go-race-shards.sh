#!/usr/bin/env bash
# The shards of the Go race tests (ci.yml `go-race`): the one table that says which tests run
# where, so a shard cannot be added to the workflow and forgotten here, or the reverse.
#
#   go-race-shards.sh --matrix      the matrix ci.yml expands, as JSON (needs no Go)
#   go-race-shards.sh SHARD         the `go test` arguments of SHARD, one per line (run at the root)
#   go-race-shards.sh --check       every package's tests run in exactly one shard (run at the root)
#   go-race-shards.sh --arm64-extra packages not already raced on arm64 (run at the root)
#   go-race-shards.sh --windows-matrix the Windows legs of ci.yml `go-xos`, as JSON (needs no Go)
#   go-race-shards.sh --windows LEG the `go test` arguments of each shard of LEG, one shard per line
#   go-race-shards.sh --macos-matrix the macOS legs of ci.yml `go-xos`, as JSON (needs no Go)
#   go-race-shards.sh --macos LEG   the `go test` arguments of macOS leg LEG, one command per line
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
# split package has exactly one remainder row, each row of a split package (pattern or
# remainder) selects at least one test, and no test of a split package is selected twice.
# Whole-package rows are checked for packages only: one whose packages have no tests runs nothing. Packages and tests are discovered with -race, as the
# shards build them, so a package or test file under `//go:build race` is not invisible to the
# check. Patterns are anchored and use only the syntax Go's RE2 and grep -E agree on.
#
# A row may share another row's build cache (the fifth field) only when both run the same packages
# on the same runner and the other row keeps its own; --check and --self-test enforce it. Both
# matrices name each leg's cache key (`cache`): `go-race-SHARD` for the shard that owns it, and for
# a Windows or macOS leg `go-xos` (the full leg), `go-xos-SHARD` when all its shards use SHARD's
# cache, else `go-xos-LEG`.
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
# the remainder of a split package), build cache (-: the shard's own; else the shard whose cache
# it shares). Rows that compile the same packages with the same flags on the same runner can
# share one cache instead of each growing its own: the `internal/listing` rows build one test
# binary and their tests build nothing else; the `cmd/swath` rows' tests both build the swath
# binary with the swathtest tag, and `cmd`'s also without it (a few entries, rebuilt when the
# kill shard saved the shared cache first).
table=(
  "crash        $arm ./internal/listing                         ^TestCrashMatrix\$                       listing-rest"
  "durable      $arm ./internal/listing                         ^(TestDurable.*|TestDiskFaultMatrix)\$   listing-rest"
  "remote-crash $arm ./internal/listing                         ^TestRemoteCrashMatrix\$                 listing-rest"
  "listing-rest $arm ./internal/listing                         -                                        -"
  "cmd-kill     $arm ./cmd/swath                                ^(TestSIGKILL.*|TestForcedExit.*)\$      cmd"
  "cmd          $arm ./cmd/swath                                -                                        -"
  "rest-replay  $amd ./internal/replay/...,./bench/replaydiff   -                                        -"
  "rest-source  $amd ./internal/source/...                      -                                        -"
  "rest         $amd -                                         -                                        -"
)

# The Windows legs of ci.yml `go-xos` on a full platform run, one race shard per leg where the
# shards are long. Windows runs the suite without -race about three times slower than macOS
# (internal/listing 17 min alone, 28 min beside the other packages, 2026-11-16), and a leg that ran
# several long shards one after another came within minutes of its limit, so its run is split
# along the shards above: a leg names shards, comma-separated, and runs one `go test` per shard in
# order. Every shard is in exactly one leg. Each `internal/listing` and `cmd/swath` shard is a leg
# (each takes 5 to 9 min there, `remote-crash` 5 of them, 2026-11-17), and the `rest*` shards
# share one. The leg holding `rest` is the full one: it also vets the module and tests the nested
# modules.
windows=(
  "crash           crash"
  "durable         durable"
  "remote-crash    remote-crash"
  "listing-rest    listing-rest"
  "cmd-kill        cmd-kill"
  "cmd             cmd"
  "rest            rest-replay,rest-source,rest"
)

# The macOS legs of ci.yml `go-xos` on a full platform run, in the same form. One macOS job ran
# `internal/listing` and `cmd/swath` side by side on three vCPUs and was the critical job of a full
# run (14.5 to 15.7 min, 2026-11-17); alone, without -race, `internal/listing` takes about 5.4 min
# of test time there and `cmd/swath` 9.2. So `internal/listing` is a leg and everything else the
# full one. Unlike a Windows leg, a macOS leg runs its packages in one `go test`, side by side as
# the single job did: a split package whose rows are all in the leg runs whole (one process, all
# its tests), and a row of a split package whose other rows are elsewhere runs in a `go test` of
# its own after it.
macos=(
  "listing         crash,durable,remote-crash,listing-rest"
  "rest            cmd-kill,cmd,rest-replay,rest-source,rest"
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

# The shard whose build cache shard $1 uses: its fifth field, or itself.
cache_of() {
  local row owner
  for row in "${table[@]}"; do
    [ "$(field "$row" 1)" = "$1" ] || continue
    owner=$(field "$row" 5)
    printf '%s' "${owner:-$1}"
    return 0
  done
  echo "unknown shard: $1" >&2
  return 2
}

# The leg table of platform $1 (windows or macos), one row per line.
legs_of() {
  case "$1" in
    windows) printf '%s\n' "${windows[@]}" ;;
    macos) printf '%s\n' "${macos[@]}" ;;
    *) echo "unknown platform: $1" >&2; return 2 ;;
  esac
}

# The shards of leg $2 of platform $1, one per line.
leg_shards() {
  local row
  while read -r row; do
    [ "$(field "$row" 1)" = "$2" ] || continue
    field "$row" 2 | tr , '\n'
    echo
    return 0
  done < <(legs_of "$1")
  echo "unknown $1 leg: $2" >&2
  return 2
}

# The build-cache key name of leg $2 of platform $1: go-xos for the full leg, go-xos-SHARD when
# every shard of the leg uses SHARD's cache, else go-xos-LEG. The key's OS part keeps the platforms
# apart.
leg_cache() {
  local shards shard owners
  if [ "$2" = "$(full_leg "$1")" ]; then printf 'go-xos'; return 0; fi
  shards=$(leg_shards "$1" "$2") || return
  owners=$(for shard in $shards; do cache_of "$shard"; echo; done | sort -u)
  if [ "$(wc -l <<<"$owners")" = 1 ]; then printf 'go-xos-%s' "$owners"; else printf 'go-xos-%s' "$2"; fi
}

# $1 is -race, or empty for the packages of a build without it (the Windows legs, whose runner may
# have no cgo, which -race needs).
load_packages() {
  local module
  module=$(go list -m)
  ALL=$(go list ${1:+"$1"} ./... | sed "s#^$module\$#.#; s#^$module/#./#" | sort -u)
  if [ -z "$ALL" ]; then echo "go list $1 ./... lists no package" >&2; return 1; fi
}

# The ordinary PR cross-architecture leg need not rerun the heavy arm64 race
# packages. Derive its complement from the same table, including future shards.
arm64_extra() {
  local row packages covered= other= result
  for row in "${table[@]}"; do
    if [ -z "$(field "$row" 3)" ]; then
      packages=$(rest_packages)
    else
      packages=$(expand "$(field "$row" 3)")
    fi
    if [ "$(field "$row" 2)" = "$arm" ]; then
      covered+="$packages"$'\n'
    else
      other+="$packages"$'\n'
    fi
  done
  # If a split package ever spans architectures, keep its full arm64 supplement.
  covered=$(comm -23 <(sed '/^$/d' <<<"$covered" | sort -u) <(sed '/^$/d' <<<"$other" | sort -u))
  result=$(comm -23 <(sort -u <<<"$ALL") <(sed '/^$/d' <<<"$covered" | sort -u))
  [ -n "$result" ] || { echo '::error::no arm64 complement; update the xos matrix' >&2; return 1; }
  printf '%s\n' "$result"
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

# The `go test` arguments of each shard of Windows leg $1, one shard per line, space-separated
# (no argument holds a space).
windows_args() {
  local shards shard args
  shards=$(leg_shards windows "$1") || return
  for shard in $shards; do
    args=$(shard_args "$shard") || return
    paste -sd' ' - <<<"$args"
  done
}

# The `go test` arguments of macOS leg $1, one command per line, space-separated: first every
# whole package of the leg, a split package whose rows are all in the leg among them, then each row
# of a split package whose other rows are in another leg.
macos_args() {
  local shards shard row sel split args whole= alone=
  shards=$(leg_shards macos "$1") || return
  split=$(split_packages)
  for shard in $shards; do
    row=$(printf '%s\n' "${table[@]}" | awk -v s="$shard" '$1 == s')
    sel=$(field "$row" 3)
    if [ -n "$sel" ] && grep -qxF -- "$sel" <<<"$split"; then
      if [ -z "$(comm -23 <(split_rows "$sel") <(sort <<<"$shards"))" ]; then
        whole+="$sel"$'\n'
      else
        alone+="$(shard_args "$shard" | paste -sd' ' -)"$'\n'
      fi
    else
      args=$(shard_args "$shard") || return
      whole+="$args"$'\n'
    fi
  done
  whole=$(sed '/^$/d' <<<"$whole" | awk '!seen[$0]++' | paste -sd' ' -)
  if [ -n "$whole" ]; then printf '%s\n' "$whole"; fi
  printf '%s' "$alone"
}

# The rows that split package $1 (its -run rows and its remainder), sorted.
split_rows() {
  printf '%s\n' "${table[@]}" | awk -v p="$1" '$3 == p {print $1}' | sort
}

# The legs of platform $1 against the shard table: leg names unique, every leg naming known shards,
# every shard in exactly one leg, and exactly one leg holding `rest` (the row naming no package).
# Prints every failure, then returns 1.
validate_legs() {
  local row name shard named= legs= dup fail=0
  while read -r row; do
    legs+="$(field "$row" 1)"$'\n'
    named+="$(field "$row" 2 | tr , '\n')"$'\n'
  done < <(legs_of "$1")
  if dup=$(sed '/^$/d' <<<"$legs" | sort | uniq -d) && [ -n "$dup" ]; then
    echo "::error::$1 legs named twice: $dup"; fail=1
  fi
  for row in "${table[@]}"; do
    name=$(field "$row" 1)
    if [ "$(grep -cxF -- "$name" <<<"$named")" != 1 ]; then
      echo "::error::shard $name is in $(grep -cxF -- "$name" <<<"$named") $1 legs, not 1"; fail=1
    fi
  done
  for shard in $(sed '/^$/d' <<<"$named"); do
    if ! printf '%s\n' "${table[@]}" | awk -v s="$shard" '$1 == s {f=1} END {exit !f}'; then
      echo "::error::$1 legs name an unknown shard: $shard"; fail=1
    fi
  done
  if [ "$fail" = 0 ] && [ -z "$(full_leg "$1")" ]; then
    echo "::error::no $1 leg holds the shard naming no package"; fail=1
  fi
  return "$fail"
}

# The leg of platform $1 holding the shard that names no package (`rest`).
full_leg() {
  local row rest
  rest=$(printf '%s\n' "${table[@]}" | awk '$3 == "-" {print $1}')
  while read -r row; do
    if tr , '\n' <<<"$(field "$row" 2)" | grep -qxF -- "$rest"; then field "$row" 1; echo; fi
  done < <(legs_of "$1")
}

# The legs of platform $1 as the JSON matrix of ci.yml `go-xos` on runner $2.
legs_matrix() {
  local row leg full scope sep=
  full=$(full_leg "$1")
  printf '['
  while read -r row; do
    leg=$(field "$row" 1)
    scope=leg
    if [ "$leg" = "$full" ]; then scope=full; fi
    printf '%s{"os":"%s","runner":"%s","scope":"%s","leg":"%s","cache":"%s"}' "$sep" "$2" "$2" "$scope" "$leg" "$(leg_cache "$1" "$leg")"
    sep=,
  done < <(legs_of "$1")
  printf ']\n'
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
  # A shared build cache: the owner exists, keeps its own, and runs the same packages on the same
  # runner, so the entries one shard saves are the ones the others use.
  local owner other
  for row in "${table[@]}"; do
    name=$(field "$row" 1)
    owner=$(field "$row" 5)
    [ -n "$owner" ] || continue
    other=$(printf '%s\n' "${table[@]}" | awk -v s="$owner" '$1 == s')
    if [ -z "$other" ]; then
      echo "::error::shard $name shares the build cache of an unknown shard: $owner"; fail=1
    elif [ "$(field "$other" 5)" != "" ]; then
      echo "::error::shard $name shares the build cache of $owner, which shares another's"; fail=1
    elif [ "$(field "$other" 2)" != "$(field "$row" 2)" ] || [ "$(field "$other" 3)" != "$(field "$row" 3)" ]; then
      echo "::error::shard $name shares the build cache of $owner, which runs other packages or on another runner"; fail=1
    fi
  done
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
  validate_legs windows || fail=1
  validate_legs macos || fail=1
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
    echo "shard $(field "$row" 1) ($(field "$row" 2), cache go-race-$(cache_of "$(field "$row" 1)")): $(shard_args "$(field "$row" 1)" | paste -sd' ' -)"
  done
  for row in "${windows[@]}"; do
    echo "Windows leg $(field "$row" 1) (cache $(leg_cache windows "$(field "$row" 1)")): $(field "$row" 2)"
  done
  for row in "${macos[@]}"; do
    echo "macOS leg $(field "$row" 1) (cache $(leg_cache macos "$(field "$row" 1)")): $(macos_args "$(field "$row" 1)" | sed 's/^/go test /' | paste -sd';' -)"
  done
}

# validate and the expansion against a synthetic package list: each case is a table and whether
# validate must accept it.
self_test() {
  local fail=0 got
  if ! validate_legs windows; then echo "self-test: the Windows legs do not cover the shard table"; fail=1; fi
  if ! validate_legs macos; then echo "self-test: the macOS legs do not cover the shard table"; fail=1; fi
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
  expect ok   "shared build cache"          "s r ./a ^TestX\$ s-rest" "s-rest r ./a - -" "rest r - -"
  expect fail "cache of an unknown shard"   "s r ./a ^TestX\$ nope" "s-rest r ./a - -" "rest r - -"
  expect fail "cache of a sharing shard"    "s r ./a ^TestX\$ t" "t r ./a ^TestY\$ s-rest" "s-rest r ./a - -" "rest r - -"
  expect fail "cache of other packages"     "s r ./a ^TestX\$ c" "s-rest r ./a - -" "c r ./c - -" "rest r - -"
  expect fail "cache on another runner"     "s r ./a ^TestX\$ s-rest" "s-rest q ./a - -" "rest r - -"
  table=("x r ./a/...,./c -" "rest r - -")
  got=$(shard_args x | paste -sd' ' -)
  if [ "$got" != "./a ./a/b ./c" ]; then echo "self-test: x expands to '$got', want './a ./a/b ./c'"; fail=1; fi
  got=$(shard_args rest | paste -sd' ' -)
  if [ "$got" != "./ab ./d" ]; then echo "self-test: rest expands to '$got', want './ab ./d'"; fail=1; fi
  table=("x $arm ./a/...,./c -" "rest $amd - -")
  got=$(arm64_extra | paste -sd' ' -)
  if [ "$got" != "./ab ./d" ]; then echo "self-test: arm64 complement is '$got'"; fail=1; fi
  table=("s $arm ./a ^TestX\$" "s-rest $arm ./a -" "rest $amd - -")
  got=$(arm64_extra | paste -sd' ' -)
  if [ "$got" != "./a/b ./ab ./c ./d" ]; then echo "self-test: split arm64 complement is '$got'"; fail=1; fi
  table=("x $amd ./a -" "rest $arm - -")
  got=$(arm64_extra)
  if [ "$got" != "./a" ]; then echo "self-test: rest arm64 complement is '$got'"; fail=1; fi
  table=("s $arm ./a ^TestX\$" "s-rest $amd ./a -" "rest $amd - -")
  got=$(arm64_extra | paste -sd' ' -)
  if [ "$got" != "./a ./a/b ./ab ./c ./d" ]; then echo "self-test: mixed-architecture complement is '$got'"; fail=1; fi
  table=("a r ./a -" "b r ./b -" "rest r - -")
  expect_windows() {  # expect_windows ok|fail DESCRIPTION LEG...
    local want=$1 what=$2 verdict=ok
    shift 2
    windows=("$@")
    validate_legs windows >/dev/null 2>&1 || verdict=fail
    if [ "$verdict" != "$want" ]; then echo "self-test: $what: validate_windows says $verdict, want $want"; fail=1; fi
  }
  expect_windows ok   "every shard once"        "x a,b" "y rest"
  expect_windows fail "shard in no leg"         "x a" "y rest"
  expect_windows fail "shard in two legs"       "x a,b" "y b,rest"
  expect_windows fail "unknown shard"           "x a,b,c" "y rest"
  expect_windows fail "leg named twice"         "x a,b" "x rest"
  windows=("x a,b" "y rest")
  got=$(full_leg windows)
  if [ "$got" != y ]; then echo "self-test: the full Windows leg is '$got', want y"; fail=1; fi
  table=("a r ./a ^TestX\$ b" "b r ./a - -" "c r ./c - -" "rest r - -")
  windows=("x a,b" "z c" "w a,c" "y rest")
  got=$(for leg in x z w y; do leg_cache windows "$leg"; echo; done | paste -sd' ' -)
  if [ "$got" != "go-xos-b go-xos-c go-xos-w go-xos" ]; then echo "self-test: Windows leg caches are '$got'"; fail=1; fi
  table=("a r ./a -" "b r ./b -" "rest r - -")
  windows=("x a,b" "y rest")
  ALL=$(printf '%s\n' ./a ./b ./c ./d)
  got=$(windows_args x)
  if [ "$got" != $'./a\n./b' ]; then echo "self-test: Windows leg x runs '$got'"; fail=1; fi
  got=$(windows_args y)
  if [ "$got" != "./c ./d" ]; then echo "self-test: Windows leg y runs '$got'"; fail=1; fi
  # A macOS leg runs its whole packages in one go test, a split package whose rows are all in the
  # leg among them, and the rows of a split package whose other rows are elsewhere one by one.
  table=("s r ./a ^TestX\$" "t r ./a ^TestY\$" "s-rest r ./a -" "b r ./b -" "rest r - -")
  macos=("x s,s-rest,b" "y t,rest")
  got=$(macos_args x)
  if [ "$got" != $'./b\n-run ^TestX$ ./a\n-skip ^TestX$|^TestY$ ./a' ]; then echo "self-test: macOS leg x runs '$got'"; fail=1; fi
  macos=("x s,t,s-rest,b" "y rest")
  got=$(macos_args x)
  if [ "$got" != "./a ./b" ]; then echo "self-test: macOS leg x runs '$got', want './a ./b'"; fail=1; fi
  got=$(macos_args y)
  if [ "$got" != "./c ./d" ]; then echo "self-test: macOS leg y runs '$got', want './c ./d'"; fail=1; fi
  got=$(legs_matrix macos m | jq -c '[.[] | [.leg, .scope, .cache]]')
  if [ "$got" != '[["x","leg","go-xos-x"],["y","full","go-xos"]]' ]; then echo "self-test: macOS matrix is '$got'"; fail=1; fi
  if [ "$fail" = 0 ]; then echo "self-test: ok"; fi
  return "$fail"
}

case "${1:-}" in
  --matrix)
    sep=
    printf '['
    for row in "${table[@]}"; do
      printf '%s{"shard":"%s","runner":"%s","cache":"go-race-%s"}' "$sep" "$(field "$row" 1)" "$(field "$row" 2)" "$(cache_of "$(field "$row" 1)")"
      sep=,
    done
    printf ']\n'
    ;;
  --windows-matrix) legs_matrix windows windows-latest ;;
  --macos-matrix) legs_matrix macos macos-latest ;;
  --windows) [ -n "${2:-}" ] || { echo "--windows needs a leg" >&2; exit 2; }; load_packages; windows_args "$2" ;;
  --macos) [ -n "${2:-}" ] || { echo "--macos needs a leg" >&2; exit 2; }; load_packages; macos_args "$2" ;;
  --self-test) self_test ;;
  --arm64-extra) load_packages -race; arm64_extra ;;
  --check) load_packages -race; check ;;
  ''|-*) sed -n '2,13s/^# \{0,1\}//p' "$0" >&2; exit 2 ;;
  *) load_packages -race; shard_args "$1" ;;
esac
