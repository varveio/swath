#!/bin/sh
# Builds a command with its revision stamped, from any checkout or worktree:
# ./build.sh [go build flags] [./cmd/NAME]. The package defaults to
# ./cmd/swath (the lister); name ./cmd/swath-replay (the replay server) as
# the last argument to build that instead, e.g.
#   ./build.sh -o swath
#   GOOS=linux GOARCH=arm64 ./build.sh -o swath-linux-arm64
#   ./build.sh -o swath-replay ./cmd/swath-replay
# The package is an argument rather than a variable so a cross-build loop
# over targets keeps its one invocation shape for either command.
# The revision is git's HEAD (with -dirty when go/ has local changes), or
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
CGO_ENABLED=0 exec go build -trimpath \
	-ldflags="-s -w -X github.com/varveio/swath/go/internal/listing.revision=$rev -X main.revision=$rev" "$@" $pkg
