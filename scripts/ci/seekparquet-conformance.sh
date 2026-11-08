#!/usr/bin/env bash
# SeekParquet conformance of what swath reads and writes (go/docs/testing.md), run from go/:
#
#   1. the conformance suite's reader target against the Go adapter of the seekparquet module
#      version go.mod pins (the reader the replay server serves datasets through), which must
#      qualify;
#   2. datasets this build's writer produces (internal/listing TestSeekParquetFixtures: S3 current
#      objects, S3 versions with repeated keys, filesystem keys over 1,024 bytes) validated by the
#      suite's reference adapter at the full and digest levels, and read by the Go adapter exactly
#      as the reference reads them (seekparquet_conformance.py).
#
# The seekparquet checkout is the module version's tag (go/vX.Y.Z); the reference adapter's Python
# dependencies are the suite's pinned requirements, in a virtual environment under WORK.
#
#   seekparquet-conformance.sh [WORK]   WORK defaults to a new temporary directory
set -euo pipefail

work=${1:-$(mktemp -d)}
mkdir -p "$work"
version=$(go list -m -f '{{.Version}}' github.com/varveio/seekparquet/go)
checkout="$work/seekparquet"
if [ ! -d "$checkout" ]; then
  git clone --quiet --depth 1 --branch "go/$version" https://github.com/varveio/seekparquet "$checkout"
fi
echo "seekparquet go/$version at $(git -C "$checkout" rev-parse HEAD)"

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
