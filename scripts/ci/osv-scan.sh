#!/usr/bin/env bash
# Supply-chain scan of what swath actually ships: the jars in each application distribution,
# and the runtime container image (its JRE base carries OS packages no Gradle constraint
# reaches). Findings are checked against config/osv/osv-scanner.toml, which holds the argued
# exceptions; anything not listed there fails the run.
#
# The distributions are scanned rather than the source tree because the source tree has no
# lockfile — the shipped `lib/` directory IS the resolved closure, and reading it means the
# scan can never disagree with what a user downloads. `--no-ignore` is required: `lib/` lives
# under build/, which .gitignore excludes and the scanner honours by default.
#
# Usage: scripts/ci/osv-scan.sh <target>...
#   target  a directory of jars, or `image:<name>` for a local container image
# Exit codes: 0 = clean or fully excepted, 1 = findings, 2 = script error.
set -euo pipefail

# Pinned by version AND checksum: this binary decides whether a release is allowed to ship.
# Hand-bumped (Dependabot does not track a raw release download) -- when raising it, take the
# new sum from the release's osv-scanner_SHA256SUMS.
OSV_SCANNER_VERSION="2.5.1"
OSV_SCANNER_SHA256="f9f25499a2c8cc367b3af45df2ea7eeca7fbccceab9c35079968f4b3652194be"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${REPO_ROOT}/config/osv/osv-scanner.toml"

if [ "$#" -eq 0 ]; then
    echo "usage: $0 <directory|image:NAME>..." >&2
    exit 2
fi

scanner="${RUNNER_TEMP:-/tmp}/osv-scanner-${OSV_SCANNER_VERSION}"
if [ ! -x "$scanner" ]; then
    curl -fsSL -o "$scanner" \
        "https://github.com/google/osv-scanner/releases/download/v${OSV_SCANNER_VERSION}/osv-scanner_linux_amd64"
    echo "${OSV_SCANNER_SHA256}  ${scanner}" | sha256sum --check -
    chmod +x "$scanner"
fi
"$scanner" --version

status=0
for target in "$@"; do
    echo "=== osv-scanner: ${target}"
    case "$target" in
        image:*)
            # `scan image` reads the image's OS package database and the language artifacts
            # inside it, which is the only way the JRE base layer gets looked at at all.
            "$scanner" scan image --config "$CONFIG" "${target#image:}" || status=1
            ;;
        *)
            # The `artifact` plugin is what reads a bare .jar (Maven coordinates from the
            # embedded pom.properties); the default plugin set only understands lockfiles.
            "$scanner" scan source --config "$CONFIG" \
                --experimental-plugins artifact --no-ignore --recursive "$target" || status=1
            ;;
    esac
done

if [ "$status" -ne 0 ]; then
    echo "::error::OSV found advisories that config/osv/osv-scanner.toml does not except." >&2
fi
exit "$status"
