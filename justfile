# swath development commands.
# Run `just --list` for all commands.

# List available commands
help:
    @just --list

# A release goes in dependency order, in three steps, none of which commits to main or pushes:
#
#   just release-modules VERSION  tag the nested modules at main's HEAD (reader/vVERSION,
#                                 objfile/vVERSION); push those tags;
#   just release-prep VERSION     on a new branch, point the root's go.mod at them and check the
#                                 root as an outside project builds it; push it, merge its pull
#                                 request;
#   just release VERSION          at the merged commit, check again and tag the root (vVERSION);
#                                 pushing that tag starts the release workflow.
#
# VERSION is X.Y.Z or a X.Y.Z-rc.N release candidate; no other pre-release identifiers and no
# build metadata, matching what the release workflow enforces. The tag is the version's one
# source: the release workflow stamps it into every binary (build.sh, $SWATH_VERSION), the archive
# names and the image tags, so there is no version file to bump. Each step runs on a clean checkout
# of main whose HEAD is on origin/main. An -rc.N release publishes only its own container tags and
# a GitHub pre-release, and is NOT promoted: the final version is a fresh build. See RELEASING.md.

# Release step 1: tag the nested modules (reader/vVERSION, objfile/vVERSION) at main's HEAD
release-modules VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    tags=$(./scripts/ci/verify-release-source.sh module-tags "v{{VERSION}}")
    tags=${tags//$'\n'/ }
    just _require-new-tags $tags
    ./scripts/ci/verify-release-notes.sh "{{VERSION}}" docs/ops/dev/RELEASE_NOTES.md
    for tag in $tags; do
        git tag -a "$tag" -m "swath $tag"
    done
    echo
    echo "Tagged $tags at $(git rev-parse --short HEAD)."
    echo "Review them, then push them in one push, and prepare the root module:"
    echo "  git push --atomic origin $tags"
    echo "  just release-prep {{VERSION}}"

# Release step 2: a branch whose one commit points go.mod at the pushed nested tags
release-prep VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    branch="release-prep-{{VERSION}}"
    if git rev-parse -q --verify "refs/heads/$branch" >/dev/null; then
        echo "error: branch $branch already exists" >&2; exit 2
    fi
    just _fetch-module-tags "{{VERSION}}"
    git switch --quiet -c "$branch"
    for tag in $(./scripts/ci/verify-release-source.sh module-tags "v{{VERSION}}"); do
        path=$(awk '$1 == "module" { print $2; exit }' "${tag%/v*}/go.mod")
        GOWORK=off go get "$path@v{{VERSION}}"
    done
    GOWORK=off go mod tidy
    if git diff --quiet -- go.mod go.sum; then
        echo "error: go.mod already requires the nested modules at v{{VERSION}}; run: just release {{VERSION}}" >&2
        exit 2
    fi
    just _outside-build
    git commit --quiet -s -m "release: the root module requires the nested modules at v{{VERSION}}" -- go.mod go.sum
    ./scripts/ci/verify-release-source.sh modules HEAD refs/tags/ "v{{VERSION}}"
    echo
    echo "Committed $(git rev-parse --short HEAD) on $branch. Push it and open its pull request;"
    echo "once it is merged and main's CI is green at the merge commit, tag the root there:"
    echo "  git push -u origin $branch"
    echo "  gh pr create --fill"
    echo "  just release {{VERSION}}"

# Release step 3: check the root module as an outside project builds it, and tag it vVERSION
release VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    just _require-new-tags "v{{VERSION}}"
    just _fetch-module-tags "{{VERSION}}"
    ./scripts/ci/verify-release-source.sh modules HEAD refs/tags/ "v{{VERSION}}"
    just _outside-build
    ./scripts/ci/verify-release-notes.sh "{{VERSION}}" docs/ops/dev/RELEASE_NOTES.md
    git tag -a "v{{VERSION}}" -m "swath v{{VERSION}}"
    echo
    echo "Tagged v{{VERSION}} at $(git rev-parse --short HEAD). Review it, then push it; the push"
    echo "starts the release workflow:"
    echo "  git push origin v{{VERSION}}"
    if [[ "{{VERSION}}" == *-rc.* ]]; then
        echo
        echo "This is a PRE-RELEASE: it publishes the X.Y.Z-rc.N container tags only —"
        echo "no :latest, no :X.Y — and creates a GitHub pre-release. It is a rehearsal of"
        echo "the publish path; the final tag is a separate, fresh build."
    fi

# The checks every release step starts with: the version's form, and a clean checkout of main
# whose HEAD is on origin/main, the only commits the release workflow builds.
_release-guard VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ ! "{{VERSION}}" =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?$ ]]; then
        echo "error: VERSION must be X.Y.Z or X.Y.Z-rc.N (no other pre-release forms, no build metadata)" >&2; exit 2
    fi
    current_branch="$(git branch --show-current)"
    if [[ "$current_branch" != "main" ]]; then
        echo "error: must be run from main (currently on '${current_branch:-detached HEAD}') -- the release workflow builds only a commit on origin/main" >&2; exit 2
    fi
    if [[ -n "$(git status --porcelain)" ]]; then
        echo "error: working tree is not clean; commit or stash first" >&2; exit 2
    fi
    git fetch --quiet origin main
    if ! git merge-base --is-ancestor HEAD origin/main; then
        echo "error: HEAD is not on origin/main; push or pull first -- the release workflow refuses a tag outside main" >&2; exit 2
    fi

# Fails unless none of TAGS exists, locally or on origin.
_require-new-tags +TAGS:
    #!/usr/bin/env bash
    set -euo pipefail
    for tag in {{TAGS}}; do
        if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
            echo "error: tag $tag already exists locally" >&2; exit 2
        fi
        if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null 2>&1; then
            echo "error: tag $tag already exists on origin" >&2; exit 2
        fi
    done

# Fetches the nested modules' tags of VERSION from origin; fails if one is missing there or a local
# tag of that name names another object.
_fetch-module-tags VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    refspecs=()
    for tag in $(./scripts/ci/verify-release-source.sh module-tags "v{{VERSION}}"); do
        refspecs+=("refs/tags/$tag:refs/tags/$tag")
    done
    git fetch --quiet --no-tags origin "${refspecs[@]}" || {
        echo "error: the nested modules' tags of v{{VERSION}} must be on origin first: just release-modules {{VERSION}}, then push them" >&2
        exit 2
    }

# The root module as an outside project builds it: without the go.work workspace, against the
# nested-module versions go.mod requires, with go.mod and go.sum tidy for them. The release
# workflow runs the same check at the tag.
_outside-build:
    GOWORK=off go build ./...
    GOWORK=off go mod tidy -diff
