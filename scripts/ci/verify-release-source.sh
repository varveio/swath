#!/usr/bin/env bash
# Prove that a release job is testing the tag commit itself, that it belongs to main, and that the
# root module at that commit is what an outside project gets: it carries no replace, and each nested
# module's tag that its go.mod requires holds that module's code at the commit. The nested modules
# keep their own versions: a release tags one only when its code changed since its last tag.
#
# The nested modules it covers are the root module's own: each go.mod below the root whose module
# path is under the root's (github.com/varveio/swath/reader and .../objfile). The root's go.mod must
# require every one of them, so that a new product module cannot ship untagged. A nested module
# outside that path (the acceptance suite's varve.io/swath-acceptance, a test-only module the
# product does not import and go.work does not join) is not part of what an outside project
# resolves, and is skipped.
set -euo pipefail

usage() {
  echo "usage: $0 --self-test | tags <vX.Y.Z> | required-tags <commit>" >&2
  echo "       $0 changed <commit> <ref-prefix>" >&2
  echo "       $0 modules <commit> <ref-prefix>" >&2
  echo "       $0 <expected-sha> <main-ref> <ref-prefix> <vX.Y.Z>" >&2
  exit 2
}

# The module path that the go.mod $2 at commit $1 declares.
module_path() {
  git show "$1:$2" | awk '$1 == "module" { print $2; exit }'
}

# The root module's nested modules at commit $1, one per line as DIR PATH: each go.mod below the
# root whose module path is under the root module's path.
nested_modules() {
  local commit=$1 root gomod path
  for gomod in go.mod $(git ls-files -- '*/go.mod'); do
    path=$(module_path "$commit" "$gomod") || return 1
    [[ -n $path ]] || {
      echo "$gomod at $commit declares no module" >&2
      return 1
    }
    if [[ $gomod == go.mod ]]; then
      root=$path
    elif [[ $path == "$root"/* ]]; then
      echo "${gomod%/go.mod} $path"
    fi
  done
}

# The nested modules' tags that the go.mod at commit $1 requires, one per line: DIR/vX.Y.Z, the
# directory prefix Go requires of a nested module's version.
required_tags() {
  local commit=$1 gomod modules dir path version
  gomod=$(git show "$commit:go.mod") || return 1
  modules=$(nested_modules "$commit") || return 1
  while read -r dir path; do
    [[ -n $dir ]] || continue
    version=$(awk -v path="$path" '
      $1 == "require" && $2 == "(" { block = 1; next }
      block && $1 == ")" { block = 0; next }
      block && $1 == path { print $2; exit }
      $1 == "require" && $2 == path { print $3; exit }' <<<"$gomod")
    [[ -n $version ]] || {
      echo "go.mod at $commit does not require $path" >&2
      return 1
    }
    echo "$dir/$version"
  done <<<"$modules"
}

# Every tag of the release $1 checked out at HEAD: $1 for the root module, then the nested modules'
# tags its go.mod requires.
tags() {
  echo "$1"
  required_tags HEAD
}

# The tag $2 (under ref prefix $1) as a commit, or a message that it is missing.
tag_commit() {
  git rev-parse -q --verify "$1$2^{commit}" || {
    echo "no tag $2" >&2
    return 1
  }
}

# The nested modules whose directory at commit $1 differs from the tag its go.mod requires (under
# ref prefix $2): the ones a release must tag first.
changed() {
  local commit=$1 prefix=$2 tags tag tagged
  tags=$(required_tags "$commit") || return 1
  for tag in $tags; do
    tagged=$(tag_commit "$prefix" "$tag") || return 1
    git diff --quiet "$tagged" "$commit" -- "${tag%/v*}" || echo "${tag%/v*}"
  done
}

# The root module at commit $1 as an outside project resolves it: no replace directive (Go
# ignores one outside the main module, and `go install module@version` refuses a module that has
# one), and each nested tag go.mod requires (under ref prefix $2) is an ancestor of the commit with
# the same directory tree.
modules() {
  local commit prefix=$2 gomod tags tag tagged dir
  commit=$(git rev-parse --verify "$1^{commit}") || return 1
  gomod=$(git show "$commit:go.mod") || return 1
  if awk '$1 == "replace" { found = 1 } END { exit !found }' <<<"$gomod"; then
    echo "go.mod at $commit has a replace directive; a released root module carries none" >&2
    return 1
  fi
  tags=$(required_tags "$commit") || return 1
  for tag in $tags; do
    dir=${tag%/v*}
    tagged=$(tag_commit "$prefix" "$tag") || return 1
    git merge-base --is-ancestor "$tagged" "$commit" || {
      echo "tag $tag ($tagged) is not an ancestor of $commit" >&2
      return 1
    }
    git diff --quiet "$tagged" "$commit" -- "$dir" || {
      echo "$dir/ changed between its tag $tag ($tagged) and $commit" >&2
      return 1
    }
  done
  echo "release modules verified at $commit: $(tr '\n' ' ' <<<"$tags")"
}

verify() {
  local expected=$1 main_ref=$2 prefix=$3 version=$4 head tagged
  head=$(git rev-parse --verify 'HEAD^{commit}')
  expected=$(git rev-parse --verify "${expected}^{commit}")
  [[ $head == "$expected" ]] || {
    echo "checkout HEAD $head does not equal event SHA $expected" >&2
    return 1
  }
  tagged=$(git rev-parse --verify "${prefix}${version}^{commit}")
  [[ $tagged == "$expected" ]] || {
    echo "tag $version resolves to $tagged, not event SHA $expected" >&2
    return 1
  }
  git rev-parse --verify "${main_ref}^{commit}" >/dev/null || return 1
  git merge-base --is-ancestor "$expected" "$main_ref" || {
    echo "tag commit $expected is not an ancestor of $main_ref" >&2
    return 1
  }
  # Every failure returns explicitly: the self-test runs this where errexit is off.
  modules "$expected" "$prefix" || return 1
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

# Fails the self-test unless command $2 run in scratch repository $1 prints $3 (lines joined by
# spaces).
prints() {
  local got
  got=$(cd "$1" && $2 | tr '\n' ' ')
  [[ $got == "$3" ]] || {
    echo "self-test failed: $2 printed '$got', want '$3'" >&2
    exit 1
  }
}

self_test() {
  local scratch release
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  git -C "$scratch" init -q -b main
  git -C "$scratch" config user.name test
  git -C "$scratch" config user.email test@example.invalid
  mkdir -p "$scratch/lib" "$scratch/suite"
  echo 'module example.com/m/lib' > "$scratch/lib/go.mod"
  # A nested module outside the root's path (its path shares the root's as a string prefix only),
  # which the root does not require: skipped throughout.
  echo 'module example.com/m-suite' > "$scratch/suite/go.mod"
  echo 'module example.com/m' > "$scratch/go.mod"
  git -C "$scratch" add go.mod lib/go.mod suite/go.mod
  git -C "$scratch" commit -q -m base

  # A release with a nested change: the nested tag, the commit that points the root at it, the
  # root tag.
  git -C "$scratch" tag -a lib/v1.2.3 -m lib/v1.2.3
  commit_root "$scratch" v1.2.3
  release=$(git -C "$scratch" rev-parse HEAD)
  git -C "$scratch" tag -a v1.2.3 -m v1.2.3
  prints "$scratch" "required_tags HEAD" "lib/v1.2.3 "
  prints "$scratch" "tags v1.2.3" "v1.2.3 lib/v1.2.3 "
  prints "$scratch" "changed HEAD refs/tags/" ""
  git -C "$scratch" commit -q --allow-empty -m snapshot
  git -C "$scratch" checkout -q --detach "$release"
  (cd "$scratch" && verify "$release" refs/heads/main refs/tags/ v1.2.3) >/dev/null
  refuses "$scratch" "does not equal event SHA" HEAD^ refs/heads/main refs/tags/ v1.2.3

  # A release with no nested change: one root tag, its go.mod still requiring the earlier lib tag.
  git -C "$scratch" checkout -q -b root-only "$release"
  echo '// root changed' >> "$scratch/go.mod"
  git -C "$scratch" commit -q -am "root changed"
  git -C "$scratch" tag -a v1.3.0 -m v1.3.0
  (cd "$scratch" && verify HEAD refs/heads/root-only refs/tags/ v1.3.0) >/dev/null

  # Each on a branch of its own, so that only the named defect can refuse it.
  git -C "$scratch" checkout -q -b untagged "$release"
  commit_root "$scratch" v1.2.2
  git -C "$scratch" tag -a v1.2.4 -m v1.2.4
  refuses "$scratch" "no tag lib/v1.2.2" HEAD refs/heads/untagged refs/tags/ v1.2.4

  git -C "$scratch" checkout -q -b unrequired "$release"
  printf 'module example.com/m\n' > "$scratch/go.mod"
  git -C "$scratch" commit -q -am "lib not required"
  git -C "$scratch" tag -a v1.2.7 -m v1.2.7
  refuses "$scratch" "does not require example.com/m/lib" HEAD refs/heads/unrequired refs/tags/ v1.2.7

  git -C "$scratch" checkout -q -b unrequired-new "$release"
  mkdir -p "$scratch/tool"
  echo 'module example.com/m/tool' > "$scratch/tool/go.mod"
  git -C "$scratch" add tool/go.mod
  git -C "$scratch" commit -q -m "a new nested module under the root's path, not required"
  git -C "$scratch" tag -a v1.2.8 -m v1.2.8
  refuses "$scratch" "does not require example.com/m/tool" HEAD refs/heads/unrequired-new refs/tags/ v1.2.8

  # A change to the module outside the root's path is not one a release tags.
  git -C "$scratch" checkout -q -b suite-changed "$release"
  echo '// changed' >> "$scratch/suite/go.mod"
  git -C "$scratch" commit -q -am "suite changed"
  git -C "$scratch" tag -a v1.2.9 -m v1.2.9
  prints "$scratch" "changed HEAD refs/tags/" ""
  (cd "$scratch" && verify HEAD refs/heads/suite-changed refs/tags/ v1.2.9) >/dev/null

  git -C "$scratch" checkout -q -b replaced "$release"
  commit_root "$scratch" v1.2.3 $'\nreplace example.com/m/lib => ./lib\n'
  git -C "$scratch" tag -a v1.2.5 -m v1.2.5
  refuses "$scratch" "has a replace directive" HEAD refs/heads/replaced refs/tags/ v1.2.5

  git -C "$scratch" checkout -q -b changed "$release"
  echo '// changed' >> "$scratch/lib/go.mod"
  git -C "$scratch" commit -q -am "lib changed"
  git -C "$scratch" tag -a v1.2.6 -m v1.2.6
  prints "$scratch" "changed HEAD refs/tags/" "lib "
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
  required-tags)
    [[ $# -eq 2 ]] || usage
    required_tags "$2"
    ;;
  changed)
    [[ $# -eq 3 ]] || usage
    changed "$2" "$3"
    ;;
  modules)
    [[ $# -eq 3 ]] || usage
    modules "$2" "$3"
    ;;
  *)
    [[ $# -eq 4 ]] || usage
    verify "$@"
    ;;
esac
