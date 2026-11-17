#!/usr/bin/env bash
# Renders the home page's lead terminal example (HERO in site/.vitepress/theme/index.ts) to
# site/readme/hero.png, the image at the top of the repository README. It builds the site into a
# temporary directory, serves it, and screenshots the terminal with headless Chromium (render.cjs).
# Run it after changing HERO or the terminal's styles, and commit the PNG with the change.
#
# Usage: site/readme/render.sh   (needs Node.js, npm and python3; when node cannot already resolve
#                                 `playwright`, installs Playwright and its Chromium)
set -euo pipefail

README_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SITE="$(cd "${README_DIR}/.." && pwd)"
PLAYWRIGHT=playwright@1.56.1
PORT="${PORT:-8766}"

work="$(mktemp -d)"
server=""
cleanup() {
  [[ -n "${server}" ]] && kill "${server}" 2>/dev/null
  rm -rf "${work}"
}
trap cleanup EXIT

cd "${SITE}"
[[ -d node_modules/vitepress ]] || npm ci --no-audit --no-fund
npx vitepress build --outDir "${work}/site" >/dev/null

if ! node -e "require.resolve('playwright')" 2>/dev/null; then
  npm install --prefix "${work}/playwright" --no-save --no-audit --no-fund "${PLAYWRIGHT}"
  export NODE_PATH="${work}/playwright/node_modules"
  "${NODE_PATH}/.bin/playwright" install chromium
fi

python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${work}/site" >/dev/null 2>&1 &
server=$!
for _ in $(seq 50); do
  curl -fs "http://127.0.0.1:${PORT}/" >/dev/null && break
  sleep 0.1
done

node "${README_DIR}/render.cjs" "http://127.0.0.1:${PORT}/" "${README_DIR}/hero.png"
echo "rendered ${README_DIR}/hero.png"
