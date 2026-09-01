#!/usr/bin/env bash
# Fetches the site's video assets into a built site tree and verifies each one's digest.
#
# The videos are not in git. A re-record replaces roughly 1.5 MB of binary content, and git
# keeps every version forever, so they live as assets on a GitHub release and the built site
# pulls them in at deploy time. `site/data/media.json` is the pin: it names the release, the
# asset, and the sha256 the deploy must see. A mismatch fails here rather than publishing
# bytes nobody reviewed -- release assets are mutable in principle, so the digest is what
# makes this reference trustworthy.
#
# Usage: scripts/ci/fetch-site-media.sh <site-dir>
#   site-dir  a tree produced by build-site.sh; files land at their `path` inside it
# Exit codes: 0 = every file fetched and verified, 1 = a download or digest failed,
#             2 = script error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SITE_DIR="${1:-}"
MANIFEST="${REPO_ROOT}/site/data/media.json"
REPOSITORY="${GITHUB_REPOSITORY:-varveio/swath}"

if [[ -z "${SITE_DIR}" || ! -d "${SITE_DIR}" ]]; then
  echo "usage: $0 <site-dir>   (a directory produced by build-site.sh)" >&2
  exit 2
fi
if [[ ! -f "${MANIFEST}" ]]; then
  echo "no media manifest at ${MANIFEST}" >&2
  exit 2
fi

release="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["release"])' "${MANIFEST}")"
entries="$(python3 -c '
import json, sys
manifest = json.load(open(sys.argv[1]))
for entry in manifest["files"]:
    print(entry["path"], entry["asset"], entry["sha256"], sep="\t")
' "${MANIFEST}")"

if [[ -z "${entries}" ]]; then
  echo "media manifest declares no files" >&2
  exit 2
fi

status=0
while IFS=$'\t' read -r path asset digest; do
  url="https://github.com/${REPOSITORY}/releases/download/${release}/${asset}"
  target="${SITE_DIR}/${path}"
  mkdir -p "$(dirname "${target}")"
  echo "fetching ${url}"
  if ! curl --fail --silent --show-error --location --retry 3 --retry-delay 2 \
            --output "${target}" "${url}"; then
    echo "  FAILED to download ${url}" >&2
    status=1
    continue
  fi
  actual="$(sha256sum "${target}" | cut -d' ' -f1)"
  if [[ "${actual}" != "${digest}" ]]; then
    echo "  DIGEST MISMATCH for ${path}" >&2
    echo "    expected ${digest}" >&2
    echo "    actual   ${actual}" >&2
    echo "  The release asset is not the reviewed file. Do not publish it: either the asset" >&2
    echo "  was replaced in place, or site/data/media.json is stale." >&2
    rm -f "${target}"
    status=1
    continue
  fi
  echo "  ok ${path} ($(wc -c <"${target}") bytes)"
done <<<"${entries}"

exit "${status}"
