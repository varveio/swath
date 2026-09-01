#!/usr/bin/env bash
# Assembles the deployable website from `site/` into an output directory.
#
# The site is authored as static files, so the build is a copy -- but it is a copy with a
# definition, and this script is that definition: whatever lands in the output directory is
# exactly what the deployment workflow pushes to `gh-pages`. Running the consistency checks
# against this output rather than against `site/` is what makes them checks of the published
# website instead of checks of its source.
#
# Only `site/README.md` is left behind. It tells a contributor where the editorial source
# lives and has no reason to be served from the site root.
#
# Usage: scripts/ci/build-site.sh [output-dir]   (default: build/site)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE="${REPO_ROOT}/site"
OUT="${1:-${REPO_ROOT}/build/site}"

if [[ ! -d "${SOURCE}" ]]; then
  echo "no site source at ${SOURCE}" >&2
  exit 2
fi

# The output directory is deleted before it is filled, so refuse the paths where that would
# destroy work instead of clearing a build product: the filesystem root, the repository or
# source root itself, any ancestor of either, and anything inside `site/` (which would also
# make the copy feed itself).
OUT="$(mkdir -p "${OUT}" && cd "${OUT}" && pwd -P)"
for forbidden in / "${REPO_ROOT}" "${SOURCE}"; do
  if [[ "${OUT}" == "${forbidden}" || "${forbidden}" == "${OUT}"/* ]]; then
    echo "refusing to build into ${OUT}: it contains or is ${forbidden}" >&2
    exit 2
  fi
done
if [[ "${OUT}" == "${SOURCE}"/* ]]; then
  echo "refusing to build into ${OUT}: it is inside the site source" >&2
  exit 2
fi

rm -rf "${OUT}"
mkdir -p "${OUT}"
# tar rather than cp so the exclusion applies to the copied set itself, and so dotfiles
# -- .nojekyll, which GitHub Pages needs -- come across without a glob that forgets them.
tar -c -C "${SOURCE}" --exclude=./README.md . | tar -x -C "${OUT}"

echo "built site -> ${OUT}"
find "${OUT}" -type f | sed "s|^${OUT}/|  |" | sort
