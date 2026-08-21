#!/usr/bin/env bash
# Restore the mode-preserving promotion archive after actions/download-artifact.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <promotion-archive> <promotion-directory>" >&2
  exit 2
fi

archive=$1
destination=$2

test -s "$archive" || { echo "promotion archive missing or empty: $archive" >&2; exit 1; }
if [[ -e "$destination" ]]; then
  echo "promotion directory already exists: $destination" >&2
  exit 1
fi

mkdir -p "$destination"
tar -xpf "$archive" -C "$destination"
"$(dirname "$0")/verify-container-promotion.sh" "$destination"
