#!/usr/bin/env bash
# Prepare the only BuildKit context permitted to replace Dockerfile's build stage.
# It deliberately contains the tested jar plus every runtime-stage legal COPY input.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <promotion-directory>" >&2
  exit 2
fi

destination=$1
jar=swath-cli/build/libs/swath.jar
legal=(LICENSE NOTICE THIRD_PARTY_NOTICES.md)

test -f "$jar"
for file in "${legal[@]}"; do test -f "$file"; done

if [[ -e "$destination" ]]; then
  echo "promotion directory already exists: $destination" >&2
  exit 1
fi
mkdir -p "$destination/src/swath-cli/build/libs"
cp "$jar" "$destination/src/swath-cli/build/libs/swath.jar"
for file in "${legal[@]}"; do cp "$file" "$destination/src/$file"; done

(cd "$destination" && sha256sum \
  src/swath-cli/build/libs/swath.jar src/LICENSE src/NOTICE src/THIRD_PARTY_NOTICES.md > SHA256SUMS)

"$(dirname "$0")/verify-container-promotion.sh" "$destination" "$jar"
