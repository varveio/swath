#!/usr/bin/env bash
# Prove that a release job is testing the tag commit itself, that it belongs to main, and that
# every module's release tag names that same commit.
set -euo pipefail

usage() {
  echo "usage: $0 --self-test | tags <vX.Y.Z> | <expected-sha> <main-ref> <tag-ref>..." >&2
  exit 2
}

# The tags of one release: vX.Y.Z for the root module and DIR/vX.Y.Z for each nested module
# (each go.mod below the root), the directory prefix Go requires of a nested module's version.
tags() {
  local tag=$1 gomods gomod
  gomods=$(git ls-files -- '*/go.mod')
  echo "$tag"
  for gomod in $gomods; do
    echo "${gomod%/go.mod}/$tag"
  done
}

verify() {
  local expected=$1 main_ref=$2 head tag_ref tag_commit
  shift 2
  head=$(git rev-parse --verify 'HEAD^{commit}')
  expected=$(git rev-parse --verify "${expected}^{commit}")
  [[ $head == "$expected" ]] || {
    echo "checkout HEAD $head does not equal event SHA $expected" >&2
    return 1
  }
  for tag_ref in "$@"; do
    tag_commit=$(git rev-parse --verify "${tag_ref}^{commit}")
    [[ $tag_commit == "$expected" ]] || {
      echo "tag $tag_ref resolves to $tag_commit, not event SHA $expected" >&2
      return 1
    }
  done
  git rev-parse --verify "${main_ref}^{commit}" >/dev/null
  git merge-base --is-ancestor "$expected" "$main_ref" || {
    echo "tag commit $expected is not an ancestor of $main_ref" >&2
    return 1
  }
  echo "release source verified: $* -> $expected; ancestor of $main_ref"
}

self_test() {
  local scratch release got
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  git -C "$scratch" init -q -b main
  git -C "$scratch" config user.name test
  git -C "$scratch" config user.email test@example.invalid
  mkdir -p "$scratch/lib"
  touch "$scratch/go.mod" "$scratch/lib/go.mod"
  git -C "$scratch" add go.mod lib/go.mod
  git -C "$scratch" commit -q -m base
  got=$(cd "$scratch" && tags v1.2.3 | tr '\n' ' ')
  [[ $got == "v1.2.3 lib/v1.2.3 " ]] || {
    echo "self-test failed: release tags are '$got', want 'v1.2.3 lib/v1.2.3 '" >&2
    exit 1
  }
  git -C "$scratch" commit -q --allow-empty -m release
  release=$(git -C "$scratch" rev-parse HEAD)
  git -C "$scratch" tag -a v1.2.3 -m v1.2.3
  git -C "$scratch" tag -a lib/v1.2.3 -m lib/v1.2.3
  git -C "$scratch" tag -a lib/v1.2.2 -m lib/v1.2.2 HEAD^
  git -C "$scratch" commit -q --allow-empty -m snapshot
  git -C "$scratch" checkout -q --detach "$release"
  (cd "$scratch" && verify "$release" refs/heads/main refs/tags/v1.2.3 refs/tags/lib/v1.2.3) >/dev/null
  if (cd "$scratch" && verify HEAD^ refs/heads/main refs/tags/v1.2.3) >/dev/null 2>&1; then
    echo "self-test failed: mismatched event SHA was accepted" >&2
    exit 1
  fi
  if (cd "$scratch" && verify "$release" refs/heads/main refs/tags/v1.2.3 refs/tags/lib/v1.2.2) >/dev/null 2>&1; then
    echo "self-test failed: a module tag at another commit was accepted" >&2
    exit 1
  fi
  git -C "$scratch" branch unrelated HEAD^
  git -C "$scratch" checkout -q unrelated
  git -C "$scratch" commit -q --allow-empty -m unrelated
  git -C "$scratch" checkout -q --detach "$release"
  if (cd "$scratch" && verify "$release" refs/heads/unrelated refs/tags/v1.2.3) >/dev/null 2>&1; then
    echo "self-test failed: tag outside main ancestry was accepted" >&2
    exit 1
  fi
  echo "release source self-test passed"
}

if [[ ${1:-} == --self-test ]]; then
  [[ $# -eq 1 ]] || usage
  self_test
elif [[ ${1:-} == tags ]]; then
  [[ $# -eq 2 ]] || usage
  tags "$2"
elif [[ $# -ge 3 ]]; then
  verify "$@"
else
  usage
fi
