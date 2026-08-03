#!/usr/bin/env bash
# Require a tag-bound human summary before generated pull-request notes are appended.
set -euo pipefail

verify() {
  local version=$1 notes=$2
  [[ -f $notes ]] || { echo "release notes file does not exist: $notes" >&2; return 1; }
  awk -v title="# swath ${version}" '
    BEGIN {
      required[1] = "## User-facing changes"
      required[2] = "## Evidence"
      required[3] = "## Limits and known issues"
    }
    {
      line = $0
      visible = ""
      while (length(line)) {
        if (in_comment) {
          comment_end = index(line, "-->")
          if (!comment_end) { line = ""; break }
          line = substr(line, comment_end + 3)
          in_comment = 0
        }
        comment_start = index(line, "<!--")
        if (!comment_start) { visible = visible line; break }
        visible = visible substr(line, 1, comment_start - 1)
        line = substr(line, comment_start + 4)
        in_comment = 1
      }
      $0 = visible
    }
    /^# / && !saw_title++ { if ($0 != title) error = "first H1 must be exactly: " title }
    /^## / {
      section++
      if (section > 3 || $0 != required[section]) error = "required H2 sections are missing or out of order"
      next
    }
    section > 0 && $0 !~ /^[[:space:]]*$/ && $0 !~ /^[[:space:]]*<!--/ { content[section] = 1 }
    END {
      if (!saw_title) error = "release notes need an H1 title"
      if (section != 3) error = "release notes need exactly the three required H2 sections"
      for (i = 1; i <= 3; i++) if (!content[i]) error = required[i] " has no human-authored content"
      if (error) { print error > "/dev/stderr"; exit 1 }
    }
  ' "$notes"
}

self_test() {
  local scratch
  scratch=$(mktemp -d)
  SELF_TEST_SCRATCH=$scratch
  trap 'rm -rf -- "${SELF_TEST_SCRATCH:?}"' EXIT
  printf '%s\n' '# swath 1.2.3' '' '## User-facing changes' '- Faster listing.' '' \
    '## Evidence' '- Benchmarked on representative data.' '' '## Limits and known issues' '- None known.' > "$scratch/good.md"
  verify 1.2.3 "$scratch/good.md"
  printf '%s\n' '# swath X.Y.Z' '' '## User-facing changes' '<!-- write this -->' '' \
    '## Evidence' '<!-- write this -->' '' '## Limits and known issues' '<!-- write this -->' > "$scratch/template.md"
  if verify 1.2.3 "$scratch/template.md" >/dev/null 2>&1; then
    echo "self-test failed: untouched template was accepted" >&2
    exit 1
  fi
  printf '%s\n' '# swath 1.2.3' '' '## User-facing changes' '<!--' 'placeholder text' '-->' '' \
    '## Evidence' '<!--' 'placeholder text' '-->' '' '## Limits and known issues' '<!--' 'placeholder text' '-->' > "$scratch/multiline-comments.md"
  if verify 1.2.3 "$scratch/multiline-comments.md" >/dev/null 2>&1; then
    echo "self-test failed: text inside multi-line comments counted as release-note content" >&2
    exit 1
  fi
  echo "release notes self-test passed"
}

if [[ ${1:-} == --self-test ]]; then
  [[ $# -eq 1 ]] || { echo "usage: $0 --self-test | <version> <notes-file>" >&2; exit 2; }
  self_test
elif [[ $# -eq 2 ]]; then
  [[ $1 =~ ^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?$ ]] || {
    echo "invalid release version: $1" >&2
    exit 2
  }
  verify "$1" "$2"
else
  echo "usage: $0 --self-test | <version> <notes-file>" >&2
  exit 2
fi
