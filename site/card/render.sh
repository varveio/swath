#!/usr/bin/env bash
# Renders the social card, site/card/card.svg, to site/static/card.png (1200x630) with headless
# Chromium. The SVG loads its fonts from site/node_modules, so it is served over HTTP from site/
# (a file:// page may not load fonts) after `npm ci`.
#
# Usage: site/card/render.sh   (needs Node.js, npm and python3; downloads Playwright's Chromium)
set -euo pipefail

SITE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLAYWRIGHT=playwright@1.56.1
PORT="${PORT:-8765}"

cd "${SITE}"
[[ -d node_modules/@fontsource-variable/inter ]] || npm ci --no-audit --no-fund
npx --yes "${PLAYWRIGHT}" install chromium

python3 -m http.server "${PORT}" --bind 127.0.0.1 --directory "${SITE}" >/dev/null 2>&1 &
server=$!
trap 'kill "${server}"' EXIT
for _ in $(seq 50); do
  curl -fs "http://127.0.0.1:${PORT}/card/card.svg" >/dev/null && break
  sleep 0.1
done

npx --yes "${PLAYWRIGHT}" screenshot --viewport-size "1200, 630" --wait-for-timeout 1000 \
  "http://127.0.0.1:${PORT}/card/card.svg" "${SITE}/static/card.png"
echo "rendered ${SITE}/static/card.png"
