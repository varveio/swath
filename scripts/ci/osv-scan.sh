#!/usr/bin/env bash
# Supply-chain scan of what swath actually ships: the jars in each application distribution,
# and the runtime container image (its JRE base carries OS packages no Gradle constraint
# reaches). Jar findings are checked against the argued Java exceptions in
# config/osv/osv-scanner.toml. Image findings are written as raw JSON and passed to the
# checked-in Ubuntu-priority policy in osv-image-policy.py.
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
IMAGE_POLICY="${REPO_ROOT}/scripts/ci/osv-image-policy.py"
REPORT_DIR="${OSV_REPORT_DIR:-${REPO_ROOT}/build/reports/osv}"

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
            # Keep unfiltered scanner output as an audit artifact. The policy reads the
            # Java-only exception file itself, so OS-package findings cannot be hidden in it.
            mkdir -p "$REPORT_DIR"
            image_name="${target#image:}"
            report_name="${image_name//\//_}"
            report_name="${report_name//:/_}"
            report="${REPORT_DIR}/${report_name}.json"
            scanner_status=0
            "$scanner" scan image --format json \
                --output-file "$report" "$image_name" || scanner_status=$?
            if [ "$scanner_status" -gt 1 ]; then
                echo "::error::OSV-Scanner failed for ${target} with exit code ${scanner_status}." >&2
                exit 2
            fi
            policy_status=0
            python3 "$IMAGE_POLICY" --scanner-exit-code "$scanner_status" \
                --java-exceptions "$CONFIG" "$report" \
                || policy_status=$?
            if [ "$policy_status" -gt 1 ]; then
                echo "::error::The OSV image report could not be evaluated safely." >&2
                exit 2
            fi
            if [ "$policy_status" -eq 1 ]; then
                status=1
            fi
            ;;
        *)
            # The `artifact` plugin is what reads a bare .jar (Maven coordinates from the
            # embedded pom.properties); the default plugin set only understands lockfiles.
            scanner_status=0
            "$scanner" scan source --config "$CONFIG" \
                --experimental-plugins artifact --no-ignore --recursive "$target" \
                || scanner_status=$?
            if [ "$scanner_status" -gt 1 ]; then
                echo "::error::OSV-Scanner failed for ${target} with exit code ${scanner_status}." >&2
                exit 2
            fi
            if [ "$scanner_status" -eq 1 ]; then
                status=1
            fi
            ;;
    esac
done

if [ "$status" -ne 0 ]; then
    echo "::error::OSV found a non-excepted Java advisory or an image advisory rejected by policy." >&2
fi
exit "$status"
