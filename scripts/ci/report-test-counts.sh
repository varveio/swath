#!/usr/bin/env bash
# Reports how many tests a tier just executed, and fails if the answer is none.
#
# The opt-in tiers (`-Pdeep`, `-PonlyPerf`, the `-Dswath.it.sigkill=true` ITs) select their
# tests by tag, so a mis-typed property or a tag that no longer matches anything yields an
# empty run -- and an empty run is GREEN. That is not hypothetical: the nightly deep job
# alternated 9 min and 2.4 min for weeks because Gradle served `:swath-core:test` FROM-CACHE,
# and nothing in the log said the tier had executed zero tests. The conventions plugin now
# stops the caching; this script is the assertion that the tier ran at all, so the next way
# of arriving at an empty run is loud instead of silent.
#
# Counts come from the JUnit XML Gradle writes per module. Gradle recreates a module's
# results directory when its `test` task executes, so a report taken immediately after a
# tier's step describes that tier -- run this right after the step it is reporting on, not
# at the end of the job.
#
# Usage: scripts/ci/report-test-counts.sh <label> [module...]
#   label   what ran, e.g. "deep tier" -- used in the log and the job summary
#   module  restrict to these module directories (default: every module that has results)
# Exit codes: 0 = tests executed, 1 = nothing executed, 2 = script error.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

exec python3 "${SCRIPT_DIR}/report-test-counts.py" --repo-root "${REPO_ROOT}" "$@"
