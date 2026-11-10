#!/usr/bin/env bash
# Raw-HTTP reproducer of the MinIO empty-page finding (go/docs/internals/measurements.md, "Durable spill"): a
# ListObjectsV2 with a start-after inside a prefix that holds keys answers an empty page marked not
# truncated. Signed requests by curl alone (SigV4), no swath and no SDK.
#
# usage: minio_start_after.sh [PREFIX]   (default 1950census/)
# First: head -n 250000 nara-1950-census.keys > nara250k.keys && bench/minio.sh up nara250k.keys nara
# Environment: PORT (default 9000), BUCKET (default nara), MINIO_ACCESS_KEY / MINIO_SECRET_KEY
# (default swath / swath-secret, as bench/minio.sh). Exits 0 when the empty page is reproduced, 1 when
# the service answers correctly.
set -euo pipefail
PORT=${PORT:-9000} BUCKET=${BUCKET:-nara} PREFIX=${1:-1950census/}
AK=${MINIO_ACCESS_KEY:-swath} SK=${MINIO_SECRET_KEY:-swath-secret}
enc() { python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1], safe=""))' "$1"; }
list() { # QUERY: prints the request, then KeyCount, IsTruncated and the first key of the answer
  echo "GET /$BUCKET?$1"
  curl -sS --fail --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" "http://127.0.0.1:$PORT/$BUCKET?$1" |
    python3 -c 'import re, sys; b = sys.stdin.read(); g = lambda t: (re.findall("<%s>([^<]*)</%s>" % (t, t), b) or ["-"])[0]; print("  KeyCount=%s IsTruncated=%s first=%s" % (g("KeyCount"), g("IsTruncated"), g("Key")))'
}
p=$(enc "$PREFIX")
echo "1. keys under $PREFIX exist:"
out=$(list "list-type=2&prefix=$p&max-keys=3")
echo "$out"
first=$(sed -n 's/.*first=//p' <<< "$out")
echo "2. start-after=$PREFIX, no prefix, no delimiter (S3: the keys after it, $first first):"
a=$(list "list-type=2&start-after=$p&max-keys=3")
echo "$a"
echo "3. start-after=$first, a stored key:"
b=$(list "list-type=2&start-after=$(enc "$first")&max-keys=3")
echo "$b"
if grep -q "KeyCount=0 IsTruncated=false" <<< "$a$b"; then
  echo "REPRODUCED: an empty page marked not truncated although keys follow the start-after"
  exit 0
fi
echo "not reproduced"
exit 1
