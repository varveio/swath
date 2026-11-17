#!/usr/bin/env bash
# Prove that a release job is testing the tag commit itself, that it belongs to main, and that the
# root module at that commit is what an outside project gets: it requires the nested modules at
# this release's tags, carries no replace, and those tags hold the same nested-module trees.
set -euo pipefail

usage() {
  echo "usage: $0 --self-test | tags <vX.Y.Z> | module-tags <vX.Y.Z>" >&2
  echo "       $0 modules <commit> <ref-prefix> <vX.Y.Z>" >&2
  echo "       $0 <expected-sha> <main-ref> <ref-prefix> <vX.Y.Z>" >&2
  exit 2
}

# The nested modules' directories: each go.mod below the root.
module_dirs() {
  local gomod
  for gomod in $(git ls-files -- '*/go.mod'); do
    echo "${gomod%/go.mod}"
  done
}

# The nested modules' tags of one release: DIR/vX.Y.Z, the directory prefix Go requires of a
# nested module's version.
module_tags() {
  local dir
  for dir in $(module_dirs); do
    echo "$dir/$1"
  done
}

# Every tag of one release: vX.Y.Z for the root module, then the nested modules' tags.
tags() {
  echo "$1"
  module_tags "$1"
}

# Succeeds when the go.mod text on stdin requires module $1 at version $2.
requires() {
  awk -v path="$1" -v version="$2" '
    $1 == "require" && $2 == "(" { block = 1; next }
    block && $1 == ")" { block = 0; next }
    block && $1 == path && $2 == version { found = 1 }
    $1 == "require" && $2 == path && $3 == version { found = 1 }
    END { exit !found }'
}

# The root module at commit $1 as an outside project resolves it: no replace directive (Go
# ignores one outside the main module, and `go install module@version` refuses a module that has
# one), and for each nested module DIR a requirement of its module path at version $3, where the
# tag DIR/$3 (under ref prefix $2) is an ancestor of the commit with the same DIR tree.
modules() {
  local commit prefix=$2 version=$3 gomod dir tag_commit path
  commit=$(git rev-parse --verify "$1^{commit}") || return 1
  gomod=$(git show "$commit:go.mod") || return 1
  if awk '$1 == "replace" { found = 1 } END { exit !found }' <<<"$gomod"; then
    echo "go.mod at $commit has a replace directive; a released root module carries none" >&2
    return 1
  fi
  for dir in $(module_dirs); do
    tag_commit=$(git rev-parse --verify "${prefix}${dir}/${version}^{commit}") || {
      echo "no tag $dir/$version" >&2
      return 1
    }
    git merge-base --is-ancestor "$tag_commit" "$commit" || {
      echo "tag $dir/$version ($tag_commit) is not an ancestor of $commit" >&2
      return 1
    }
    git diff --quiet "$tag_commit" "$commit" -- "$dir" || {
      echo "$dir/ changed between its tag $dir/$version ($tag_commit) and $commit" >&2
      return 1
    }
    path=$(git show "$commit:$dir/go.mod" | awk '$1 == "module" { print $2; exit }')
    requires "$path" "$version" <<<"$gomod" || {
      echo "go.mod at $commit does not require $path $version" >&2
      return 1
    }
  done
  echo "release modules verified at $commit: $(module_tags "$version" | tr '\n' ' ')"
}

verify() {
  local expected=$1 main_ref=$2 prefix=$3 version=$4 head tag_commit
  head=$(git rev-parse --verify 'HEAD^{commit}')
  expected=$(git rev-parse --verify "${expected}^{commit}")
  [[ $head == "$expected" ]] || {
    echo "checkout HEAD $head does not equal event SHA $expected" >&2
    return 1
  }
  tag_commit=$(git rev-parse --verify "${prefix}${version}^{commit}")
  [[ $tag_commit == "$expected" ]] || {
    echo "tag $version resolves to $tag_commit, not event SHA $expected" >&2
    return 1
  }
  git rev-parse --verify "${main_ref}^{commit}" >/dev/null || return 1
  git merge-base --is-ancestor "$expected" "$main_ref" || {
    echo "tag commit $expected is not an ancestor of $main_ref" >&2
    return 1
  }
  # Every failure returns explicitly: the self-test runs this where errexit is off.
  modules "$expected" "$prefix" "$version" || return 1
  echo "release source verified: $version -> $expected; ancestor of $main_ref"
}

# Commits, in scratch repository $1, a go.mod that requires example.com/m/lib at version $2,
# followed by the text $3.
commit_root() {
  printf 'module example.com/m\n\nrequire (\n\texample.com/m/lib %s\n)\n%s' "$2" "${3:-}" > "$1/go.mod"
  git -C "$1" commit -q -am "root requires lib $2"
}

# Fails the self-test unless verify, run in scratch repository $1 on the arguments after $2,
# fails with an error that contains $2.
refuses() {
  local scratch=$1 want=$2 err
  shift 2
  if err=$(cd "$scratch" && verify "$@" 2>&1 >/dev/null); then
    echo "self-test failed: verify $* was accepted, want a refusal containing '$want'" >&2
    exit 1
  fi
  [[ $err == *"$want"* ]] || {
    echo "self-test failed: verify $* refused with '$err', want '$want'" >&2
    exit 1
  }
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
  echo 'module example.com/m/lib' > "$scratch/lib/go.mod"
  touch "$scratch/go.mod"
  git -C "$scratch" add go.mod lib/go.mod
  git -C "$scratch" commit -q -m base
  got=$(cd "$scratch" && tags v1.2.3 | tr '\n' ' ')
  [[ $got == "v1.2.3 lib/v1.2.3 " ]] || {
    echo "self-test failed: release tags are '$got', want 'v1.2.3 lib/v1.2.3 '" >&2
    exit 1
  }

  # The release order: the nested tag, the commit that points the root at it, the root tag.
  git -C "$scratch" tag -a lib/v1.2.3 -m lib/v1.2.3
  commit_root "$scratch" v1.2.3
  release=$(git -C "$scratch" rev-parse HEAD)
  git -C "$scratch" tag -a v1.2.3 -m v1.2.3
  git -C "$scratch" commit -q --allow-empty -m snapshot
  git -C "$scratch" checkout -q --detach "$release"
  (cd "$scratch" && verify "$release" refs/heads/main refs/tags/ v1.2.3) >/dev/null
  refuses "$scratch" "does not equal event SHA" HEAD^ refs/heads/main refs/tags/ v1.2.3

  # Each on a branch of its own, so that only the named defect can refuse it.
  git -C "$scratch" checkout -q -b other-version "$release"
  commit_root "$scratch" v1.2.2
  git -C "$scratch" tag -a v1.2.4 -m v1.2.4
  git -C "$scratch" tag -a lib/v1.2.4 -m lib/v1.2.4
  refuses "$scratch" "does not require example.com/m/lib v1.2.4" HEAD refs/heads/other-version refs/tags/ v1.2.4

  git -C "$scratch" checkout -q -b replaced "$release"
  commit_root "$scratch" v1.2.5 $'\nreplace example.com/m/lib => ./lib\n'
  git -C "$scratch" tag -a v1.2.5 -m v1.2.5
  git -C "$scratch" tag -a lib/v1.2.5 -m lib/v1.2.5 "$release"
  refuses "$scratch" "has a replace directive" HEAD refs/heads/replaced refs/tags/ v1.2.5

  git -C "$scratch" checkout -q -b changed "$release"
  echo '// changed' >> "$scratch/lib/go.mod"
  git -C "$scratch" commit -q -am "lib changed"
  commit_root "$scratch" v1.2.6
  git -C "$scratch" tag -a lib/v1.2.6 -m lib/v1.2.6 "$release"
  git -C "$scratch" tag -a v1.2.6 -m v1.2.6
  refuses "$scratch" "lib/ changed between its tag" HEAD refs/heads/changed refs/tags/ v1.2.6

  git -C "$scratch" branch unrelated "$release^"
  git -C "$scratch" checkout -q unrelated
  git -C "$scratch" commit -q --allow-empty -m unrelated
  git -C "$scratch" checkout -q --detach "$release"
  refuses "$scratch" "is not an ancestor of refs/heads/unrelated" "$release" refs/heads/unrelated refs/tags/ v1.2.3
  echo "release source self-test passed"
}

case ${1:-} in
  --self-test)
    [[ $# -eq 1 ]] || usage
    self_test
    ;;
  tags)
    [[ $# -eq 2 ]] || usage
    tags "$2"
    ;;
  module-tags)
    [[ $# -eq 2 ]] || usage
    module_tags "$2"
    ;;
  modules)
    [[ $# -eq 4 ]] || usage
    modules "$2" "$3" "$4"
    ;;
  *)
    [[ $# -eq 4 ]] || usage
    verify "$@"
    ;;
esac
