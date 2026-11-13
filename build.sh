#!/bin/sh
# Builds a command with its version and revision stamped, from any checkout or
# worktree: ./build.sh [go build flags] [./cmd/NAME]. The package defaults to
# ./cmd/swath (the lister); name ./cmd/swath-replay (the replay server) as
# the last argument to build that instead, e.g.
#   ./build.sh -o swath
#   GOOS=linux GOARCH=arm64 ./build.sh -o swath-linux-arm64
#   ./build.sh -o swath-replay ./cmd/swath-replay
# The package is an argument rather than a variable so a cross-build loop
# over targets keeps its one invocation shape for either command.
# The version is $SWATH_VERSION (the release workflow passes its tag's
# X.Y.Z); unset, the binary reports devel. devel is stamped rather than left
# empty because the module is the repository's root, where the toolchain
# would stamp a version derived from the nearest release tag instead.
# The revision is git's HEAD (with -dirty when the checkout has local changes), or
# $SWATH_REVISION when set (CI passes its commit); without either, the
# binary falls back to the Go toolchain's VCS stamp, which a plain
# `go build` in a git worktree may omit.
set -eu
cd "$(dirname "$0")"
rev=${SWATH_REVISION:-}
if [ -z "$rev" ] && rev=$(git rev-parse HEAD 2>/dev/null); then
	if [ -n "$(git status --porcelain -- . 2>/dev/null)" ]; then
		rev="$rev-dirty"
	fi
fi
pkg=./cmd/swath
for last in "$@"; do :; done
case "${last:-}" in
./cmd/*) pkg= ;;
esac
buildinfo=github.com/varveio/swath/internal/buildinfo
CGO_ENABLED=0 exec go build -trimpath \
	-ldflags="-s -w -X $buildinfo.Version=${SWATH_VERSION:-devel} -X $buildinfo.Revision=$rev" "$@" $pkg
