# swath development commands.
# Run `just --list` for all commands.

# List available commands
help:
    @just --list

# Prepare a release: tag main's HEAD as vVERSION. Accepts X.Y.Z or a X.Y.Z-rc.N release
# candidate; no other pre-release identifiers and no build metadata, matching what the
# release workflow enforces.
#
# The tag is the version's one source: the release workflow stamps it into every binary
# (build.sh, $SWATH_VERSION), the archive names and the image tags, so there is no version
# file to bump and no commit to make. The tagged commit must already be on origin/main, as the
# workflow's ancestry guard requires.
#
# Does NOT push — review, then `git push origin vX.Y.Z`. An -rc.N tag publishes only its own
# container tags and a GitHub pre-release, and is NOT promoted: the final tag is a fresh build.
# See RELEASING.md.
release VERSION:
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
    if git rev-parse -q --verify "refs/tags/v{{VERSION}}" >/dev/null; then
        echo "error: tag v{{VERSION}} already exists locally" >&2; exit 2
    fi
    if git ls-remote --exit-code --tags origin "refs/tags/v{{VERSION}}" >/dev/null 2>&1; then
        echo "error: tag v{{VERSION}} already exists on origin" >&2; exit 2
    fi
    ./scripts/ci/verify-release-notes.sh "{{VERSION}}" docs/ops/dev/RELEASE_NOTES.md
    git tag -a "v{{VERSION}}" -m "swath v{{VERSION}}"
    echo
    echo "Tagged v{{VERSION}} at $(git rev-parse --short HEAD)."
    echo "Review it, then: git push origin v{{VERSION}}"
    if [[ "{{VERSION}}" == *-rc.* ]]; then
        echo
        echo "This is a PRE-RELEASE: it publishes the X.Y.Z-rc.N container tags only —"
        echo "no :latest, no :X.Y — and creates a GitHub pre-release. It is a rehearsal of"
        echo "the publish path; the final tag is a separate, fresh build."
    fi
