#!/usr/bin/env bash
# Registers cheap immutable metadata for one v4 page-run benchmark corpus.
# The benchmark JVM remains the authority for decoding and validating segment contents.
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage:
  corpus-record.sh register --corpus DIR --evidence FILE [--record FILE]
  corpus-record.sh validate --corpus DIR [--record FILE]
EOF
  exit 2
}

die() {
  echo "corpus-record: $*" >&2
  exit 1
}

if (( BASH_VERSINFO[0] < 4 )); then
  die "bash 4 or newer is required"
fi

record_value() {
  local key=$1
  local value
  value=$(awk -v key="${key}" '
    index($0, key "=") == 1 {
      count++
      value = substr($0, length(key) + 2)
    }
    END {
      if (count == 1) print value
      else exit 1
    }
  ' "${record}") || die "record must contain exactly one ${key}= entry: ${record}"
  printf '%s\n' "${value}"
}

validate_record_schema() {
  local line
  local key
  local expected
  local -A seen=()
  while IFS= read -r line || [[ -n ${line} ]]; do
    [[ ${line} == *=* && ${line%%=*} != "" ]] \
      || die "record has an invalid field: ${record}"
    key=${line%%=*}
    case ${key} in
      format|corpus|rows|segments|bytes|corpus_id|multiset|created_by_head) ;;
      *) die "record has an unknown field ${key}: ${record}" ;;
    esac
    [[ -z ${seen["${key}"]+present} ]] \
      || die "record has a duplicate field ${key}: ${record}"
    seen["${key}"]=1
  done < "${record}"
  for expected in format corpus rows segments bytes corpus_id multiset created_by_head; do
    [[ -n ${seen["${expected}"]+present} ]] \
      || die "record is missing field ${expected}: ${record}"
  done
}

line_value() {
  local line=$1
  local key=$2
  local token
  local -a tokens
  read -r -a tokens <<< "${line}"
  for token in "${tokens[@]}"; do
    case ${token} in
      "${key}"=*) printf '%s\n' "${token#*=}"; return 0 ;;
    esac
  done
  return 1
}

validate_positive_integer() {
  local label=$1
  local value=$2
  [[ ${value} =~ ^[0-9]+$ ]] && (( value > 0 )) \
    || die "${label} must be an integer > 0"
}

validate_digest() {
  local label=$1
  local value=$2
  local length=$3
  [[ ${value} =~ ^[0-9a-f]+$ ]] && (( ${#value} == length )) \
    || die "${label} must contain exactly ${length} lowercase hexadecimal characters"
}

[[ $# -ge 1 ]] || usage
mode=$1
shift
[[ ${mode} == register || ${mode} == validate ]] || usage

corpus=
evidence=
record=
while [[ $# -gt 0 ]]; do
  case $1 in
    --corpus) [[ $# -ge 2 ]] || usage; corpus=$2; shift 2 ;;
    --evidence) [[ $# -ge 2 ]] || usage; evidence=$2; shift 2 ;;
    --record) [[ $# -ge 2 ]] || usage; record=$2; shift 2 ;;
    *) usage ;;
  esac
done

[[ -n ${corpus} ]] || usage
[[ -d ${corpus} ]] || die "corpus is not a directory: ${corpus}"
corpus=$(realpath "${corpus}")
if [[ -z ${record} ]]; then
  record=$(dirname "${corpus}")/CORPUS.varve
else
  record=$(realpath -m "${record}")
fi

segments=$(find "${corpus}" -maxdepth 1 -type f -name '*.pageseg' -printf '.\n' | wc -l)
bytes=$(find "${corpus}" -maxdepth 1 -type f -name '*.pageseg' -printf '%s\n' \
  | awk '{ total += $1 } END { printf "%.0f\n", total }')
validate_positive_integer segments "${segments}"
validate_positive_integer bytes "${bytes}"

if [[ ${mode} == register ]]; then
  [[ -n ${evidence} ]] || usage
  evidence=$(realpath "${evidence}")
  [[ -f ${evidence} ]] || die "benchmark evidence is not a regular file: ${evidence}"
  mapfile -t corpus_lines < <(grep -o 'BENCH_CORPUS [^<]*' "${evidence}" || true)
  mapfile -t oracle_lines < <(grep -o 'BENCH_INPUT_ORACLE [^<]*' "${evidence}" || true)
  (( ${#corpus_lines[@]} == 1 && ${#oracle_lines[@]} == 1 )) \
    || die "evidence must contain exactly one BENCH_CORPUS and BENCH_INPUT_ORACLE line"

  corpus_id=$(line_value "${corpus_lines[0]}" corpus_id) \
    || die "BENCH_CORPUS has no corpus_id"
  oracle_corpus_id=$(line_value "${oracle_lines[0]}" corpus_id) \
    || die "BENCH_INPUT_ORACLE has no corpus_id"
  measured_rows=$(line_value "${oracle_lines[0]}" rows) \
    || die "BENCH_INPUT_ORACLE has no rows"
  measured_segments=$(line_value "${corpus_lines[0]}" segments) \
    || die "BENCH_CORPUS has no segments"
  measured_bytes=$(line_value "${corpus_lines[0]}" bytes) \
    || die "BENCH_CORPUS has no bytes"
  multiset=$(line_value "${oracle_lines[0]}" multiset_digest) \
    || die "BENCH_INPUT_ORACLE has no multiset_digest"
  validate_positive_integer rows "${measured_rows}"
  validate_digest corpus_id "${corpus_id}" 64
  validate_digest multiset "${multiset}" 128
  [[ ${oracle_corpus_id} == "${corpus_id}" ]] || die "evidence corpus IDs disagree"
  [[ ${measured_segments} == "${segments}" ]] || die "evidence segment count disagrees"
  [[ ${measured_bytes} == "${bytes}" ]] || die "evidence byte count disagrees"
  [[ ! -e ${record} ]] || die "record already exists: ${record}"

  script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  repo_root=$(cd "${script_dir}/../.." && pwd)
  created_by_head=$(git -C "${repo_root}" rev-parse HEAD)
  temporary=$(mktemp "${record}.tmp.XXXXXX")
  trap 'rm -f "${temporary}"' EXIT
  {
    echo 'format=swath-page-run-corpus-v4'
    echo "corpus=${corpus}"
    echo "rows=${measured_rows}"
    echo "segments=${segments}"
    echo "bytes=${bytes}"
    echo "corpus_id=${corpus_id}"
    echo "multiset=${multiset}"
    echo "created_by_head=${created_by_head}"
  } > "${temporary}"
  chmod 0644 "${temporary}"
  mv "${temporary}" "${record}"
  trap - EXIT
  echo "registered ${record}"
else
  [[ -z ${evidence} ]] || usage
  [[ -f ${record} ]] || die "corpus record is absent: ${record}"
  validate_record_schema
  [[ $(record_value format) == swath-page-run-corpus-v4 ]] \
    || die "record is not for the current v4 page-run format: ${record}"
  [[ $(record_value corpus) == "${corpus}" ]] || die "record corpus path disagrees"
  [[ $(record_value segments) == "${segments}" ]] || die "record segment count disagrees"
  [[ $(record_value bytes) == "${bytes}" ]] || die "record byte count disagrees"
  rows=$(record_value rows)
  corpus_id=$(record_value corpus_id)
  multiset=$(record_value multiset)
  created_by_head=$(record_value created_by_head)
  validate_positive_integer rows "${rows}"
  validate_digest corpus_id "${corpus_id}" 64
  validate_digest multiset "${multiset}" 128
  validate_digest created_by_head "${created_by_head}" 40
  echo "validated ${record}"
  echo "rows=${rows} segments=${segments} bytes=${bytes}"
  echo "corpus_id=${corpus_id} multiset=${multiset}"
fi
