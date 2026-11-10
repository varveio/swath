#!/usr/bin/env bash
# Writes the third-party notices of swath and swath-replay to OUTPUT: ./notices.sh OUTPUT
#
# First a license gate: every module linked into either command must carry a license that
# go-licenses classifies and that ALLOWED_LICENSES names, or the script fails. The list is the
# Java build's allow-list (config/license/allowed-licenses.json) as SPDX identifiers: permissive
# licenses (plus ISC, MIT-equivalent and common among Go modules) and the weak-copyleft EPL and
# CDDL, no GPL, LGPL or AGPL. Then the notices: an
# inventory of the linked modules and each one's license text, and the Go distribution's own
# license, since its runtime and standard library are linked into every binary.
#
# Both run for each release GOOS, and the notices must agree, so a module linked on one target
# only cannot go unlisted. go-licenses is built with the toolchain that loads the packages, as its
# standard-library detection requires. It reads the license files from the module cache and
# looks their URLs up online.
set -euo pipefail
cd "$(dirname "$0")"

GO_LICENSES_VERSION=v2.0.1
ALLOWED_LICENSES=Apache-2.0,MIT,MIT-0,ISC,BSD-2-Clause,BSD-3-Clause,EPL-1.0,EPL-2.0,CDDL-1.0,CDDL-1.1,CC0-1.0
FIRST_PARTY=github.com/varveio/swath/go
COMMANDS=(./cmd/swath ./cmd/swath-replay)
GOOSES=(linux darwin windows)

[[ $# -eq 1 ]] || { echo "usage: $0 OUTPUT" >&2; exit 2; }
output=$1

work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
go_version=$(go env GOVERSION)
GOTOOLCHAIN=$go_version GOBIN="$work" go install "github.com/google/go-licenses/v2@${GO_LICENSES_VERSION}"

cat > "$work/notices.tpl" <<'EOF'
Modules
{{range .}}
- {{.Name}} {{.Version}}: {{.LicenseName}}{{end}}
{{$prev := ""}}{{range .}}{{if ne .Name $prev}}
================================================================================
{{.Name}} {{.Version}}

{{.LicenseText}}{{end}}{{$prev = .Name}}{{end}}
EOF

for goos in "${GOOSES[@]}"; do
  GOOS=$goos "$work/go-licenses" check --ignore "$FIRST_PARTY" \
    --allowed_licenses "$ALLOWED_LICENSES" "${COMMANDS[@]}"
  GOOS=$goos "$work/go-licenses" report --ignore "$FIRST_PARTY" \
    --template "$work/notices.tpl" "${COMMANDS[@]}" > "$work/modules-$goos"
  # check passes a license file it cannot classify; the report names it Unknown.
  if grep -E '^- .*: Unknown$' "$work/modules-$goos" >&2; then
    echo "go-licenses could not classify the license of the modules above" >&2
    exit 1
  fi
  cmp -s "$work/modules-${GOOSES[0]}" "$work/modules-$goos" || {
    echo "the modules linked for $goos differ from ${GOOSES[0]}'s:" >&2
    diff -u "$work/modules-${GOOSES[0]}" "$work/modules-$goos" >&2 || true
    exit 1
  }
done

{
  printf 'Third-party notices for swath and swath-replay\n\n'
  printf 'The binaries statically link the Go modules listed below, whose license texts follow\n'
  printf 'the list, and the runtime and standard library of the Go distribution (%s), whose\n' "$go_version"
  printf 'license closes this file.\n\n'
  cat "$work/modules-${GOOSES[0]}"
  printf '\n================================================================================\n'
  printf 'Go %s\n\n' "$go_version"
  cat "$(go env GOROOT)/LICENSE"
} > "$output"
