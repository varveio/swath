# swath development commands.
# Run `just --list` for all commands.

# List available commands
help:
    @just --list

# A release tags the root module, vVERSION; pushing that tag starts the release workflow. The root's
# go.mod requires the nested modules (reader, objfile) by version, and each required tag must hold
# that module's code at the release commit. The nested modules keep their own versions: a release
# with no nested change is one step,
#
#   just release VERSION          check the root as an outside project builds it and tag it vVERSION;
#
# and a release where a nested module changed since the tag go.mod requires releases it first:
#
#   just release-modules VERSION  tag each changed nested module at main's HEAD (DIR/vVERSION);
#                                 push those tags;
#   just release-prep VERSION     on a new branch, point the root's go.mod at them and check the
#                                 root as an outside project builds it; push it, merge its pull
#                                 request;
#   just release VERSION          at the merged commit, as above.
#
# None of them commits to main or pushes. VERSION is X.Y.Z or a X.Y.Z-rc.N release candidate; no
# other pre-release identifiers and no build metadata, matching what the release workflow enforces.
# The tag is the version's one source: the release workflow stamps it into every binary (build.sh,
# $SWATH_VERSION), the archive names and the image tags, so there is no version file to bump. Each
# step runs on a clean checkout of main whose HEAD is on origin/main. An -rc.N release publishes only
# its own container tags and a GitHub pre-release, and is NOT promoted: the final version is a fresh
# build. See RELEASING.md.

# Tag each nested module that changed since the tag go.mod requires (DIR/vVERSION) at main's HEAD
release-modules VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    tags=$(just _changed-tags "{{VERSION}}")
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

# A branch whose one commit points go.mod at the pushed tags of release-modules
release-prep VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    branch="release-prep-{{VERSION}}"
    if git rev-parse -q --verify "refs/heads/$branch" >/dev/null; then
        echo "error: branch $branch already exists" >&2; exit 2
    fi
    tags=$(just _changed-tags "{{VERSION}}")
    tags=${tags//$'\n'/ }
    just _fetch-tags $tags
    git switch --quiet -c "$branch"
    for tag in $tags; do
        path=$(awk '$1 == "module" { print $2; exit }' "${tag%/v*}/go.mod")
        GOWORK=off go get "$path@v{{VERSION}}"
    done
    GOWORK=off go mod tidy
    just _outside-build
    git commit --quiet -s -m "release: the root module requires $tags" -- go.mod go.sum
    ./scripts/ci/verify-release-source.sh modules HEAD refs/tags/
    echo
    echo "Committed $(git rev-parse --short HEAD) on $branch. Push it and open its pull request;"
    echo "once it is merged and main's CI is green at the merge commit, tag the root there:"
    echo "  git push -u origin $branch"
    echo "  gh pr create --fill"
    echo "  just release {{VERSION}}"

# Check the root module as an outside project builds it, and tag it vVERSION
release VERSION: (_release-guard VERSION)
    #!/usr/bin/env bash
    set -euo pipefail
    just _require-new-tags "v{{VERSION}}"
    just _fetch-tags $(./scripts/ci/verify-release-source.sh required-tags HEAD)
    ./scripts/ci/verify-release-source.sh modules HEAD refs/tags/ || {
        echo "error: if a nested module changed, release it first: just release-modules {{VERSION}}" >&2; exit 2
    }
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

# The tags DIR/vVERSION of the nested modules whose code changed since the tag go.mod requires, one
# per line; fails when none did.
_changed-tags VERSION:
    #!/usr/bin/env bash
    set -euo pipefail
    just _fetch-tags $(./scripts/ci/verify-release-source.sh required-tags HEAD)
    dirs=$(./scripts/ci/verify-release-source.sh changed HEAD refs/tags/)
    if [[ -z "$dirs" ]]; then
        echo "error: no nested module changed since the tag go.mod requires; run: just release {{VERSION}}" >&2; exit 2
    fi
    for dir in $dirs; do
        echo "$dir/v{{VERSION}}"
    done

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

# Fetches TAGS from origin; fails if one is missing there or a local tag of that name names
# another object.
_fetch-tags +TAGS:
    #!/usr/bin/env bash
    set -euo pipefail
    refspecs=()
    for tag in {{TAGS}}; do
        refspecs+=("refs/tags/$tag:refs/tags/$tag")
    done
    git fetch --quiet --no-tags origin "${refspecs[@]}" || {
        echo "error: these tags must be on origin first: {{TAGS}}" >&2
        exit 2
    }

# The root module as an outside project builds it: without the go.work workspace, against the
# nested-module versions go.mod requires, with go.mod and go.sum tidy for them. The release
# workflow runs the same check at the tag.
_outside-build:
    GOWORK=off go build ./...
    GOWORK=off go mod tidy -diff
