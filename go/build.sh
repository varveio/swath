#!/bin/sh
# Builds the swath binary with its revision stamped, from any checkout or
# worktree: ./build.sh [go build flags], e.g.
#   ./build.sh -o swath
#   GOOS=linux GOARCH=arm64 ./build.sh -o swath-linux-arm64
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
CGO_ENABLED=0 exec go build -trimpath -ldflags="-s -w -X github.com/varveio/swath/go/internal/listing.revision=$rev" "$@" ./cmd/swath
