#!/usr/bin/env bash
# A CI job's Go build cache (GOCACHE), trimmed to the entries the job used before it is saved
# (.github/actions/setup-go and .github/actions/save-go-cache), so that a saved cache does not
# grow with every job that restores it, adds to it and saves it again.
#
#   go-build-cache.sh age DIR REF   after the restore: every entry's mtime, and REF's, a day back
#   go-build-cache.sh trim DIR REF  after the job: delete what the job did not use; prints sizes
#   go-build-cache.sh --self-test   both against a synthetic cache (needs no Go)
#
# What the go command does with mtimes (cmd/go/internal/cache/cache.go, Go 1.27): DIR/xx/ID-a is an
# action's index entry, one line `v1 ACTION OUTPUT SIZE TIME`, and DIR/xx/OUTPUT-d its output (a
# directory for a cached executable). Get, through markUsed, sets the -a file's mtime to now only
# when it is at least mtimeInterval (1 hour) old; putIndexEntry writes it with the current time.
# So once every -a file is a day old, an -a file newer than REF is one this job looked up or
# wrote. An output's mtime is no such signal: copyFile leaves an identical existing output
# untouched when a new action produces it. Outputs are kept by reference instead: trim deletes
# the -a files still at REF's time, then every -d file that no remaining -a file names.
#
# A day is short of trimLimit (5 days) plus mtimeInterval, the age at which the go command's own
# Trim (run when a go command closes the cache, at most once per trimInterval) deletes an entry,
# so no aged entry is deleted while the job runs. Everything outside the 256 xx directories
# (README, trim.txt, testexpire.txt, the fuzz corpus) is left alone.
set -euo pipefail

# The cache directory in this shell's form (Git Bash on Windows needs /c/..., not C:\...).
unix_path() {
  if command -v cygpath >/dev/null; then cygpath -u "$1"; else printf '%s\n' "$1"; fi
}

# The entries of cache $1 with suffix $2 (a or d), one path per line.
entries() {
  find "$1" -mindepth 2 -maxdepth 2 -path "$1/[0-9a-f][0-9a-f]/*" -name "*-$2"
}

# Sets REF ($2) and every entry of cache $1 a day back.
age() {
  local dir ref t stamp
  dir=$(unix_path "$1")
  ref=$(unix_path "$2")
  t=$(($(date +%s) - 86400))
  # GNU date (Linux, Git Bash) takes -d @SECONDS; BSD date (macOS) takes -r SECONDS.
  stamp=$(date -d "@$t" +%Y%m%d%H%M.%S 2>/dev/null || date -r "$t" +%Y%m%d%H%M.%S)
  touch -t "$stamp" "$ref"
  if [ ! -d "$dir" ]; then
    echo "no Go build cache at $dir: nothing to age"
    return 0
  fi
  find "$dir" -mindepth 2 -maxdepth 2 -path "$dir/[0-9a-f][0-9a-f]/*" \( -name '*-a' -o -name '*-d' \) \
    -exec touch -r "$ref" {} +
  echo "aged the $(entries "$dir" a | wc -l | tr -d ' ') index entries of $dir to $stamp"
}

# Deletes from cache $1 the -a files not newer than REF ($2), then every -d file no remaining -a
# file names.
trim() {
  local dir ref before after a_before a_after kept line
  dir=$(unix_path "$1")
  ref=$(unix_path "$2")
  [ -f "$ref" ] || { echo "::error::no aging reference $ref: the cache was not aged by setup-go"; return 1; }
  if [ ! -d "$dir" ]; then
    echo "no Go build cache at $dir: nothing to trim"
    return 0
  fi
  before=$(du -sk "$dir" | cut -f1)
  a_before=$(entries "$dir" a | wc -l | tr -d ' ')
  find "$dir" -mindepth 2 -maxdepth 2 -path "$dir/[0-9a-f][0-9a-f]/*" -name '*-a' ! -newer "$ref" \
    -exec rm -f {} +
  # Every output a remaining index entry names (its third field); then every output named by none.
  kept=$(mktemp)
  find "$dir" -mindepth 2 -maxdepth 2 -path "$dir/[0-9a-f][0-9a-f]/*" -name '*-a' -exec cat {} + |
    awk '{print $3 "-d"}' >"$kept"
  entries "$dir" d | awk -F/ -v kept="$kept" 'BEGIN {while ((getline l <kept) > 0) k[l]} !($NF in k)' |
    tr '\n' '\0' |
    xargs -0 rm -rf --
  rm -f "$kept"
  after=$(du -sk "$dir" | cut -f1)
  a_after=$(entries "$dir" a | wc -l | tr -d ' ')
  line="Go build cache ${GO_BUILD_CACHE_KEY:-$dir}: kept $a_after of $a_before entries, $((after / 1024)) of $((before / 1024)) MiB on disk"
  echo "$line"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then echo "$line" >>"$GITHUB_STEP_SUMMARY"; fi
}

self_test() {
  local tmp dir ref fail=0 got want
  tmp=$(mktemp -d)
  dir=$tmp/cache
  ref=$tmp/aged
  mkdir -p "$dir/00" "$dir/ab" "$dir/fuzz"
  hex() { printf '%064d' "$1"; }
  index() {  # index NAME OUTPUT: an -a file NAME naming OUTPUT
    printf 'v1 %s %s %20d %20d\n' "$(hex 9)" "$(hex "$2")" 1 1 >"$dir/$1"
  }
  index "00/$(hex 1)-a" 11  # used: looked up this job
  index "00/$(hex 2)-a" 12  # unused, its output named by a used entry too
  index "00/$(hex 3)-a" 13  # unused
  index "ab/$(hex 5)-a" 15  # used, its output an executable directory
  index "ab/$(hex 6)-a" 16  # unused, an executable directory
  for out in 11 12 13 14; do echo data >"$dir/00/$(hex "$out")-d"; done  # 14: named by none
  mkdir "$dir/00/$(hex 15)-d" "$dir/00/$(hex 16)-d"
  echo exe >"$dir/00/$(hex 15)-d/swath"
  echo exe >"$dir/00/$(hex 16)-d/swath"
  echo 0 >"$dir/trim.txt"
  echo corpus >"$dir/fuzz/x"
  age "$dir" "$ref" >/dev/null
  # The job: looks entry 1 up (its output untouched, as copyFile may leave it), writes entry 4
  # naming output 12, which already exists, and looks up entry 5.
  touch "$dir/00/$(hex 1)-a" "$dir/ab/$(hex 5)-a"
  index "00/$(hex 4)-a" 12
  GITHUB_STEP_SUMMARY='' trim "$dir" "$ref" >/dev/null
  got=$(cd "$dir" && find . -type f | sort | paste -sd' ' -)
  want=$(printf '%s\n' "./00/$(hex 1)-a" "./00/$(hex 4)-a" "./ab/$(hex 5)-a" \
    "./00/$(hex 11)-d" "./00/$(hex 12)-d" "./00/$(hex 15)-d/swath" ./trim.txt ./fuzz/x | sort | paste -sd' ' -)
  if [ "$got" != "$want" ]; then
    echo "self-test: trim kept"; tr ' ' '\n' <<<"$got"; echo "want"; tr ' ' '\n' <<<"$want"
    fail=1
  fi
  if [ -e "$dir/00/$(hex 16)-d" ]; then echo "self-test: an unused executable directory was kept"; fail=1; fi
  # A job that used nothing keeps no entry.
  age "$dir" "$ref" >/dev/null
  GITHUB_STEP_SUMMARY='' trim "$dir" "$ref" >/dev/null
  got=$(cd "$dir" && find . -type f | sort | paste -sd' ' -)
  if [ "$got" != "./fuzz/x ./trim.txt" ]; then echo "self-test: a job that used nothing kept $got"; fail=1; fi
  if ! age "$tmp/none" "$ref" >/dev/null || ! trim "$tmp/none" "$ref" >/dev/null; then
    echo "self-test: a missing cache directory fails"; fail=1
  fi
  rm -rf "$tmp"
  if [ "$fail" = 0 ]; then echo "self-test: ok"; fi
  return "$fail"
}

case "${1:-}" in
  age) [ "$#" = 3 ] || { echo "usage: $0 age DIR REF" >&2; exit 2; }; age "$2" "$3" ;;
  trim) [ "$#" = 3 ] || { echo "usage: $0 trim DIR REF" >&2; exit 2; }; trim "$2" "$3" ;;
  --self-test) self_test ;;
  *) sed -n '6,8s/^# \{0,1\}//p' "$0" >&2; exit 2 ;;
esac
