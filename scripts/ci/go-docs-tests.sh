#!/usr/bin/env bash
# The docs tier (ci.yml `go-docs`): the main module's tests that read pages under docs/, run at the
# repository root. They are the tests named TestDocs* (docs.TestPrefix) in the packages whose tests
# import github.com/varveio/swath/docs: docs.Path refuses any other test name, and
# docs/readers_test.go refuses a Go file that names a path under docs/ and a regular (non-test) file
# that imports the docs package, so the selection cannot drift from the code. `go test ./docs`
# (links, anchors, those guards) runs in go-lint.
#
#   go-docs-tests.sh   list the packages, run their TestDocs* tests, fail on a skip or a package
#                      that runs none
set -euo pipefail

module=$(go list -m)
helper="$module/docs"
pattern='^TestDocs'
# Captured first, so a failing go list fails the script instead of selecting fewer packages.
imports=$(go list -f '{{.ImportPath}}{{range .TestImports}} {{.}}{{end}}{{range .XTestImports}} {{.}}{{end}}' ./...)
# The docs package's own tests import it too; they run in go-lint, and none is named TestDocs.
selected=$(awk -v helper="$helper" '$1 != helper { for (i = 2; i <= NF; i++) if ($i == helper) { print $1; next } }' <<<"$imports")
packages=()
if [ -n "$selected" ]; then
  mapfile -t packages <<<"$selected"
fi
if [ "${#packages[@]}" -eq 0 ]; then
  echo "no package's tests import $helper" >&2
  exit 1
fi
printf "go test -count=1 -v -run '%s'" "$pattern"; printf " '%s'" "${packages[@]}"; echo
log=$(mktemp)
trap 'rm -f "$log"' EXIT
set +e
go test -count=1 -v -run "$pattern" "${packages[@]}" | tee "$log"
status=${PIPESTATUS[0]}
set -e
if [ "$status" -ne 0 ]; then
  exit "$status"
fi
if grep -q -- '--- SKIP' "$log"; then
  echo "::error::a docs-tier test skipped"
  exit 1
fi
# go test prints this for a package where the pattern matched no test.
if grep -q 'no tests to run' "$log"; then
  echo "::error::a package whose tests import $helper has no $pattern test"
  exit 1
fi
