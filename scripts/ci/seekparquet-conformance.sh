#!/usr/bin/env bash
# SeekParquet conformance of what swath reads and writes (docs/internals/testing.md), run from the repository root:
#
#   1. the conformance suite against the Go adapter of the seekparquet module version go.mod pins
#      (the reader the replay server serves datasets through, and the validator): every target it
#      claims (the reader and the four validator levels) must qualify;
#   2. datasets this build's writer produces (internal/listing TestSeekParquetFixtures: S3 current
#      objects, S3 versions with repeated keys, filesystem keys over 1,024 bytes) validated by the
#      suite's reference adapter and by the Go adapter at the full and digest levels, and read by the
#      Go adapter exactly as the reference reads them (seekparquet_conformance.py).
#
# The seekparquet checkout is the commit go.mod pins: the revision of a pseudo-version
# (vX.Y.Z-0.yyyymmddhhmmss-abcdefabcdef), or the module version's tag (go/vX.Y.Z). The reference
# adapter's Python dependencies are the suite's pinned requirements, in a virtual environment under
# WORK.
#
#   seekparquet-conformance.sh [WORK]   WORK defaults to a new temporary directory
set -euo pipefail

work=${1:-$(mktemp -d)}
mkdir -p "$work"
version=$(go list -m -f '{{.Version}}' github.com/varveio/seekparquet/go)
checkout="$work/seekparquet"
if [[ $version =~ [.-][0-9]{14}-([0-9a-f]{12})$ ]]; then
  rev=${BASH_REMATCH[1]}
else
  rev=go/$version
fi
if [ ! -d "$checkout" ]; then
  git clone --quiet --filter=blob:none --no-checkout https://github.com/varveio/seekparquet "$checkout"
fi
git -C "$checkout" -c advice.detachedHead=false checkout --quiet "$rev"
echo "seekparquet $version at $(git -C "$checkout" rev-parse HEAD)"

go -C "$checkout/go" build -o "$work/seekparquet-adapter" ./internal/cmd/conformance
(cd "$checkout/conformance" && python3 harness/run.py --adapter "$work/seekparquet-adapter")

if [ ! -x "$work/venv/bin/python" ]; then
  python3 -m venv "$work/venv"
  "$work/venv/bin/python" -m pip install --quiet --retries 10 --timeout 60 -r "$checkout/conformance/generators/requirements.txt"
fi
rm -rf "$work/datasets"
SWATH_SEEKPARQUET_FIXTURES="$work/datasets" go test -count=1 -run '^TestSeekParquetFixtures$' ./internal/listing
"$work/venv/bin/python" -I "$(dirname "$0")/seekparquet_conformance.py" \
  --adapter "$work/seekparquet-adapter" \
  --reference "$work/venv/bin/python -I $checkout/conformance/harness/reference_adapter.py" \
  "$work/datasets/s3-current" "$work/datasets/s3-versions" "$work/datasets/local-long-keys"
