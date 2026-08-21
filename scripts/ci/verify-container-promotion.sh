#!/usr/bin/env bash
# Non-vacuous regression guard for the BuildKit `build=` replacement context.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <promotion-directory> [reference-jar]" >&2
  exit 2
fi

payload=$1
reference_jar=${2:-}
required=(
  src/swath-cli/build/libs/swath.jar
  src/swath-replay/build/install/swath-replay/bin/swath-replay
  src/swath-replay/build/install/swath-replay/bin/swath-replay-conformance
  src/LICENSE
  src/NOTICE
  src/THIRD_PARTY_NOTICES.md
)
for path in "${required[@]}"; do
  test -s "$payload/$path" || { echo "promotion payload missing or empty: $path" >&2; exit 1; }
done

for launcher in swath-replay swath-replay-conformance; do
  test -x "$payload/src/swath-replay/build/install/swath-replay/bin/$launcher" || {
    echo "promotion payload launcher is not executable: $launcher" >&2
    exit 1
  }
done

test -s "$payload/SHA256SUMS" || { echo "promotion payload lacks SHA256SUMS" >&2; exit 1; }
(cd "$payload" && sha256sum --check --status SHA256SUMS) || {
  echo "promotion payload checksum verification failed" >&2
  exit 1
}

for legal in LICENSE NOTICE THIRD_PARTY_NOTICES.md; do
  cmp -s "$payload/src/$legal" "$legal" || {
    echo "promotion payload legal file differs from checkout: $legal" >&2
    exit 1
  }
done

if [[ -n "$reference_jar" ]]; then
  test -f "$reference_jar" || { echo "reference jar does not exist: $reference_jar" >&2; exit 1; }
  cmp -s "$payload/src/swath-cli/build/libs/swath.jar" "$reference_jar" || {
    echo "promotion payload jar differs from reference jar" >&2
    exit 1
  }
fi
