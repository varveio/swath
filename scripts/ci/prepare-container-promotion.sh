#!/usr/bin/env bash
# Prepare the only BuildKit context permitted to replace Dockerfile's build stage.
# It deliberately contains the tested CLI jar, replay installDist tree, and every
# runtime-stage legal COPY input used by either published image. The sibling tar
# archive is the CI transport: actions/upload-artifact does not preserve file modes,
# while tar preserves the launchers' executable bits.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <promotion-directory>" >&2
  exit 2
fi

destination=$1
archive=${destination}.tar
jar=swath-cli/build/libs/swath.jar
replay_dist=swath-replay/build/install/swath-replay
legal=(LICENSE NOTICE THIRD_PARTY_NOTICES.md)

test -f "$jar"
test -x "$replay_dist/bin/swath-replay"
test -x "$replay_dist/bin/swath-replay-conformance"
test -s "$replay_dist/THIRD_PARTY_NOTICES.md"
for file in "${legal[@]}"; do test -f "$file"; done

if [[ -e "$destination" || -e "$archive" ]]; then
  echo "promotion output already exists: $destination or $archive" >&2
  exit 1
fi
mkdir -p "$destination/src/swath-cli/build/libs"
mkdir -p "$destination/src/swath-replay/build/install"
cp "$jar" "$destination/src/swath-cli/build/libs/swath.jar"
cp -a "$replay_dist" "$destination/src/swath-replay/build/install/swath-replay"
for file in "${legal[@]}"; do cp "$file" "$destination/src/$file"; done

(cd "$destination" && find src -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum > SHA256SUMS)

"$(dirname "$0")/verify-container-promotion.sh" "$destination" "$jar"
tar -C "$destination" -cf "$archive" .
