#!/usr/bin/env bash
# Prove that a release job is testing the tag commit itself and that it belongs to main.
set -euo pipefail

verify() {
  local expected=$1 tag_ref=$2 main_ref=$3 head tag_commit
  head=$(git rev-parse --verify 'HEAD^{commit}')
  expected=$(git rev-parse --verify "${expected}^{commit}")
  tag_commit=$(git rev-parse --verify "${tag_ref}^{commit}")
  [[ $head == "$expected" ]] || {
    echo "checkout HEAD $head does not equal event SHA $expected" >&2
    return 1
  }
  [[ $tag_commit == "$expected" ]] || {
    echo "tag $tag_ref resolves to $tag_commit, not event SHA $expected" >&2
    return 1
  }
  git rev-parse --verify "${main_ref}^{commit}" >/dev/null
  git merge-base --is-ancestor "$expected" "$main_ref" || {
    echo "tag commit $expected is not an ancestor of $main_ref" >&2
    return 1
  }
  echo "release source verified: $tag_ref -> $expected; ancestor of $main_ref"
}

self_test() {
  local scratch release
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  git -C "$scratch" init -q -b main
  git -C "$scratch" config user.name test
  git -C "$scratch" config user.email test@example.invalid
  git -C "$scratch" commit -q --allow-empty -m base
  git -C "$scratch" commit -q --allow-empty -m release
  release=$(git -C "$scratch" rev-parse HEAD)
  git -C "$scratch" tag -a v1.2.3 -m v1.2.3
  git -C "$scratch" commit -q --allow-empty -m snapshot
  git -C "$scratch" checkout -q --detach "$release"
  (cd "$scratch" && verify "$release" refs/tags/v1.2.3 refs/heads/main) >/dev/null
  if (cd "$scratch" && verify HEAD^ refs/tags/v1.2.3 refs/heads/main) >/dev/null 2>&1; then
    echo "self-test failed: mismatched event SHA was accepted" >&2
    exit 1
  fi
  git -C "$scratch" branch unrelated HEAD^
  git -C "$scratch" checkout -q unrelated
  git -C "$scratch" commit -q --allow-empty -m unrelated
  git -C "$scratch" checkout -q --detach "$release"
  if (cd "$scratch" && verify "$release" refs/tags/v1.2.3 refs/heads/unrelated) >/dev/null 2>&1; then
    echo "self-test failed: tag outside main ancestry was accepted" >&2
    exit 1
  fi
  echo "release source self-test passed"
}

if [[ ${1:-} == --self-test ]]; then
  [[ $# -eq 1 ]] || { echo "usage: $0 --self-test | <expected-sha> <tag-ref> <main-ref>" >&2; exit 2; }
  self_test
elif [[ $# -eq 3 ]]; then
  verify "$1" "$2" "$3"
else
  echo "usage: $0 --self-test | <expected-sha> <tag-ref> <main-ref>" >&2
  exit 2
fi
