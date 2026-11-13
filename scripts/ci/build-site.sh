#!/usr/bin/env bash
# Builds the deployable website into an output directory: exactly what the deployment workflow
# publishes, and what scripts/ci/check-site.py checks.
#
# The site is two parts, assembled into one tree:
#   - VitePress (site/.vitepress/config.mts) builds the home page (site/index.md) and the user
#     documentation (docs/, served under /docs/) into the output directory;
#   - site/static/ is copied over it verbatim: CNAME, .nojekyll, the social card and data/.
# A static file may not replace a built one; that would silently unpublish a page.
#
# Needs Node.js and npm. The npm dependencies are installed from site/package-lock.json with
# `npm ci`, so the build uses the reviewed versions only.
#
# Usage: scripts/ci/build-site.sh [output-dir]   (default: build/site)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SOURCE="${REPO_ROOT}/site"
STATIC="${SOURCE}/static"
OUT="${1:-${REPO_ROOT}/build/site}"

if [[ ! -f "${SOURCE}/package.json" || ! -d "${STATIC}" ]]; then
  echo "no site source at ${SOURCE}" >&2
  exit 2
fi

# The output directory is deleted before it is filled, so refuse the paths where that would
# destroy work instead of clearing a build product: the filesystem root, the repository or
# source root itself, any ancestor of either, and anything inside `site/` or `docs/`.
OUT="$(mkdir -p "${OUT}" && cd "${OUT}" && pwd -P)"
for forbidden in / "${REPO_ROOT}" "${SOURCE}" "${REPO_ROOT}/docs"; do
  if [[ "${OUT}" == "${forbidden}" || "${forbidden}" == "${OUT}"/* ]]; then
    echo "refusing to build into ${OUT}: it contains or is ${forbidden}" >&2
    exit 2
  fi
done
for inside in "${SOURCE}" "${REPO_ROOT}/docs"; do
  if [[ "${OUT}" == "${inside}"/* ]]; then
    echo "refusing to build into ${OUT}: it is inside ${inside}" >&2
    exit 2
  fi
done

rm -rf "${OUT}"
mkdir -p "${OUT}"

(cd "${SOURCE}" && npm ci --no-audit --no-fund && npx vitepress build --outDir "${OUT}")

# tar rather than cp so dotfiles -- .nojekyll -- come across without a glob that forgets them.
collisions="$(cd "${STATIC}" && find . -type f | while read -r f; do
  if [[ -e "${OUT}/${f#./}" ]]; then echo "  ${f#./}"; fi
done)"
if [[ -n "${collisions}" ]]; then
  echo "site/static would replace built files:" >&2
  echo "${collisions}" >&2
  exit 1
fi
tar -c -C "${STATIC}" . | tar -x -C "${OUT}"

echo "built site -> ${OUT}"
find "${OUT}" -type f -name '*.html' | sed "s|^${OUT}/|  |" | sort
