#!/usr/bin/env bash
# CI entry point for the instrumentation-drift guard (AGENTS.md "Instrument every new
# algorithm path" + docs/internals/metrics-internals.md §5/§5a). The real logic is in
# the Python script alongside this one (structural parsing is far less painful there
# than in bash); this wrapper just resolves paths and forwards arguments so CI can
# invoke a stable `.sh` entry point. No dependencies beyond bash + python3 (stdlib
# only -- see the module docstring in the .py file).
#
# Usage: scripts/ci/check-instrumentation-drift.sh [--self-test]
# Exit codes: 0 = clean, 1 = drift found, 2 = script error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

exec python3 "${SCRIPT_DIR}/check-instrumentation-drift.py" --repo-root "${REPO_ROOT}" "$@"
