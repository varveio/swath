#!/usr/bin/env bash
# Every tracked Go file starts with the project's two-line license header:
#
#   // Copyright <year> Varve Systems Ltd
#   // SPDX-License-Identifier: Apache-2.0
#
#   check-spdx.sh              the tracked .go files of the repository
#   check-spdx.sh FILE...      these files
#   check-spdx.sh --self-test  the check itself, on files that pass and files that do not
set -euo pipefail

# check FILE...: names every file without the header, and fails if there is one.
check() {
  local file line1 line2 bad=0
  for file in "$@"; do
    line1= line2=
    { IFS= read -r line1 || true; IFS= read -r line2 || true; } <"$file"
    if ! [[ $line1 =~ ^//\ Copyright\ [0-9]{4}\ Varve\ Systems\ Ltd$ &&
            $line2 == '// SPDX-License-Identifier: Apache-2.0' ]]; then
      echo "$file: does not start with the license header" >&2
      bad=1
    fi
  done
  return "$bad"
}

self_test() {
  local dir
  dir=$(mktemp -d)
  trap 'rm -rf -- "$dir"' RETURN
  printf '// Copyright 2026 Varve Systems Ltd\n// SPDX-License-Identifier: Apache-2.0\n\npackage x\n' >"$dir/good.go"
  printf '// Copyright 2031 Varve Systems Ltd\n// SPDX-License-Identifier: Apache-2.0\n' >"$dir/later-year.go"
  check "$dir/good.go" "$dir/later-year.go" 2>/dev/null || { echo "self-test: a valid header failed" >&2; return 1; }
  printf 'package x\n' >"$dir/none.go"
  : >"$dir/empty.go"
  printf '// Copyright 2026 Varve Systems Ltd\n' >"$dir/one-line.go"
  printf '\n// Copyright 2026 Varve Systems Ltd\n// SPDX-License-Identifier: Apache-2.0\n' >"$dir/blank-first.go"
  printf '// SPDX-License-Identifier: Apache-2.0\n// Copyright 2026 Varve Systems Ltd\n' >"$dir/swapped.go"
  printf '// Copyright Varve Systems Ltd\n// SPDX-License-Identifier: Apache-2.0\n' >"$dir/no-year.go"
  printf '// Copyright 2026 Varve Systems Ltd\n// SPDX-License-Identifier: MIT\n' >"$dir/other-license.go"
  printf '// Copyright 2026 Varve Systems Ltd\r\n// SPDX-License-Identifier: Apache-2.0\r\n' >"$dir/crlf.go"
  printf '// Copyright 2026 Someone Else\n// SPDX-License-Identifier: Apache-2.0\n' >"$dir/other-holder.go"
  local f
  for f in none empty one-line blank-first swapped no-year other-license crlf other-holder; do
    if check "$dir/$f.go" 2>/dev/null; then
      echo "self-test: $f.go passed without a valid header" >&2
      return 1
    fi
  done
  # One bad file among good ones fails the whole check.
  if check "$dir/good.go" "$dir/none.go" "$dir/later-year.go" 2>/dev/null; then
    echo "self-test: a bad file among good ones passed" >&2
    return 1
  fi
  echo "check-spdx self-test passed"
}

case "${1:-}" in
--self-test) self_test ;;
"")
  cd "$(dirname "$0")/../.."
  mapfile -d '' files < <(git ls-files -z -- '*.go')
  [ "${#files[@]}" -gt 0 ] || { echo "no tracked .go files" >&2; exit 1; }
  check "${files[@]}"
  echo "${#files[@]} Go files carry the license header"
  ;;
*) check "$@" ;;
esac
