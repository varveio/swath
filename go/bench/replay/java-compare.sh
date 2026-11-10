#!/usr/bin/env bash
# Copyright 2026 Varve Systems Ltd
# SPDX-License-Identifier: Apache-2.0
#
# java-compare.sh [--probe] [--keep-going] OUT
#
# The Go and Java replay servers compared (#352, the amended gate): the Java
# server on each Java-era fixture, the Go server on its footer-only SeekParquet
# conversion with the list index on (its default). Deleted with the Java server
# and the legacy fixture readers (#353, #267). It writes a receipts tree to OUT
# (new or empty) and exits 0 only when every step passed:
#
#   env.txt                    the machine, the toolchains, the commits
#   expected.txt               the evidence this mode must produce to pass
#   build/                     binaries' SHA-256 and go version -m, build logs
#   fixtures/                  make-fixtures.sh's log, the study inputs' digests
#   conversion/FIXTURE/        the converter's log and manifest, parts.tsv (each
#                              input and output part: bytes, SHA-256, the extent
#                              before the footer located from each file's own
#                              Parquet trailer, and that whole prefix compared
#                              byte for byte), validate.json (seekparquet
#                              validate --level full)
#   replaydiff/FIXTURE-ROUTE*  bench/replaydiff's JSON and text reports, S3, GCS
#                              and Azure, the Go server with -go-fixture
#   listing/FIXTURE/           a full swath list through each server: the run
#                              reports, diffds.json (bench/diffds.py, run in
#                              WORK: past 20 million rows it streams both
#                              listings in bounded memory, and any spill of
#                              its second pass goes under WORK)
#   startup.tsv                exec to a 200 from /healthz, and RSS then, per
#                              server and fixture; listing.tsv adds the server's
#                              peak RSS (VmHWM) over its start and the listing
#   failures.txt               every failed step, and an unexpected exit
#   summary.md, summary.json   the verdict and every count
#
# The verdict fails closed: PASS needs no failed step and every piece of
# evidence expected.txt names (each conversion checked and valid, each
# replaydiff report with no failure, both listings of each fixture with the
# fixture's row count, diffds identical, STARTUP_REPS starts per server and
# fixture); anything missing is INCOMPLETE, which exits non-zero like FAIL. The
# summary and, with SYNC_TO, the final copy of the receipts are written on
# every exit, and a run whose summary or final copy fails exits non-zero.
#
# Startup is measured on fresh processes without dropping the OS page cache:
# the fixtures were just written, converted, hashed and read, so the starts
# are generally warm, and are recorded as cache-uncontrolled. For each fixture
# the Java server starts STARTUP_REPS times, then once for its listing, then
# the Go server the same way (startup.tsv's seq is the global order).
#
# The fixtures: make-fixtures.sh's synthetic-10k, synthetic-1m (sorted and
# rolled) and evidence-gcs, each converted here; the study's NARA, FourCast and
# blockchain Java-era fixtures, fetched from the study bucket, their digests
# checked against the pinned ones, and converted here. replaydiff runs on the
# make-fixtures fixtures only: its first listing keeps every key in memory
# (main.go listAll), so the study fixtures are compared by full listings
# (diffds) alone. evidence-azure is not compared: the Java sort-fixture refuses
# its names (make-fixtures.sh writes it as a capture only), so there is no
# Java-era fixture to convert; its coverage is the Azure evidence replay's.
#
# --probe compares synthetic-10k alone. --keep-going records a failed step and
# continues; without it the first failed step (an unexplained difference, a
# converter refusal, a listing that disagrees) stops the run. A failed build
# always stops it.
#
# Inputs (environment):
#   JAVA_HOME      the JDK 25 the Java replay server builds and runs on
#   go             on PATH, the toolchain go.mod names
#   PYTHON         a python3 that imports duckdb (bench/diffds.py); default python3
#   WORK           a scratch directory on a local disk (the full run uses about
#                  40 GB); default OUT.work
#   FIXTURES_SRC   the study fixtures: a gs:// URL (read with gcloud storage) or
#                  a directory laid out as the bucket's fixtures/ prefix (the
#                  Batch job mounts the bucket); default
#                  gs://s3-listing-study-results-29c02004/fixtures
#   CONVERTER_SRC  a clean git checkout of the converter commit below (checked);
#                  default: fetched from github.com/varveio/swath into WORK
#   SWATH_SHA      this tree's commit, recorded; default git rev-parse HEAD
#   SYNC_TO        when set, OUT is copied here after every step (best effort)
#                  and on exit (required: a failed final copy fails the run)
#   STARTUP_REPS   server starts measured per server and fixture (default 3, at
#                  least 1)
#   LIST_WORKERS   swath list --workers (default 64)
set -euo pipefail

# The converter: bench/replay/adoptfixture, archived on the branch
# archive/replay-adopt-fixture and not merged (it imports the legacy reader).
CONVERTER_COMMIT=cd453c59d0cca3d9aa9d637589c2b92420933e9f
CONVERTER_REPO=https://github.com/varveio/swath.git

# The study's Java-era fixtures: name, region, path under FIXTURES_SRC, the
# study's digest of their parts (SHA-256 of the C-sorted "name<TAB>bytes<TAB>
# sha256" lines of part-*.parquet), and the study's own staged conversion
# (compared for the record, not gated).
STUDY_FIXTURES=(
	"nara-1950-census|us-east-2|nara-1950-census/current-eba31d9|48468b7c1ea8bf657fcb089dbcb1d940d3af62b748a7cb66f6dccc8876905db4|nara-1950-census/seekparquet-6ce6d7d12442"
	"noaa-nws-fourcastnetgfs-pds|us-east-1|noaa-nws-fourcastnetgfs-pds/1b8be9d08d1006a5dc15dd0f190862c3871d664d3a8b68b4f7d2ddd7be8b0ad4|1b8be9d08d1006a5dc15dd0f190862c3871d664d3a8b68b4f7d2ddd7be8b0ad4|noaa-nws-fourcastnetgfs-pds/seekparquet-eb0e7f5af5a2"
	"aws-public-blockchain|us-east-2|aws-public-blockchain/current-eba31d9|f4a839f23a1be94047491339dfd1ae5dd40d075c5a7c2eac1e07156dd985d4b0|aws-public-blockchain/seekparquet-c4a42eb4e7fb"
)

probe=0
keep_going=0
while [ $# -gt 0 ]; do
	case $1 in
	--probe) probe=1 ;;
	--keep-going) keep_going=1 ;;
	-*) echo "unknown option $1" >&2; exit 2 ;;
	*) break ;;
	esac
	shift
done
if [ $# -ne 1 ]; then
	echo "usage: $0 [--probe] [--keep-going] OUT" >&2
	exit 2
fi
out=$1
if [ -e "$out" ] && [ -n "$(ls -A "$out")" ]; then
	echo "$out is not empty" >&2
	exit 2
fi
: "${JAVA_HOME:?JAVA_HOME must name a JDK 25}"
export JAVA_HOME
PYTHON=${PYTHON:-python3}
FIXTURES_SRC=${FIXTURES_SRC:-gs://s3-listing-study-results-29c02004/fixtures}
STARTUP_REPS=${STARTUP_REPS:-3}
LIST_WORKERS=${LIST_WORKERS:-64}
[[ $STARTUP_REPS =~ ^[1-9][0-9]*$ ]] || { echo "STARTUP_REPS must be a positive integer, not '$STARTUP_REPS'" >&2; exit 2; }
[[ $LIST_WORKERS =~ ^[1-9][0-9]*$ ]] || { echo "LIST_WORKERS must be a positive integer, not '$LIST_WORKERS'" >&2; exit 2; }
for tool in go git curl sha256sum; do
	command -v "$tool" >/dev/null || { echo "$tool is not on PATH" >&2; exit 2; }
done
"$JAVA_HOME/bin/java" -version >/dev/null 2>&1 || { echo "no java in JAVA_HOME=$JAVA_HOME" >&2; exit 2; }
"$PYTHON" -c 'import duckdb' 2>/dev/null || { echo "$PYTHON cannot import duckdb (bench/diffds.py needs it)" >&2; exit 2; }
here=$(cd "$(dirname "$0")" && pwd)
checkout=$(cd "$here/../../.." && pwd)
gosrc=$checkout/go
if [ -z "${SWATH_SHA:-}" ]; then
	SWATH_SHA=$(git -C "$checkout" rev-parse HEAD 2>/dev/null) || { echo "set SWATH_SHA: $checkout is not a git checkout" >&2; exit 2; }
	if [ -n "$(git -C "$checkout" status --porcelain -- go swath-replay 2>/dev/null)" ]; then
		SWATH_SHA=$SWATH_SHA-dirty
	fi
fi
mkdir -p "$out"
out=$(cd "$out" && pwd)
work=${WORK:-$out.work}
mkdir -p "$work"
work=$(cd "$work" && pwd)
bin=$work/bin
fx_root=$work/fixtures
java_install=$checkout/swath-replay/build/install/swath-replay
java_replay=$java_install/bin/swath-replay
mkdir -p "$out/build" "$out/fixtures" "$out/conversion" "$out/replaydiff" "$out/listing" "$out/logs"

failures=0
current_step=setup
pid=
step() {
	current_step=$*
	printf '\n== %s %s\n' "$(date -u +%FT%TZ)" "$*"
}
record_failure() {
	printf '%s\t%s\n' "$(date -u +%FT%TZ)" "$*" >>"$out/failures.txt"
	echo "FAILED: $*" >&2
	failures=$((failures + 1))
}
# fail REASON: a failed step, recorded in failures.txt; without --keep-going
# the run stops here. fatal REASON stops it either way.
fail() {
	record_failure "$@"
	if [ "$keep_going" = 0 ]; then
		exit 1
	fi
}
fatal() {
	record_failure "$@"
	exit 1
}
now_ns() { date +%s%N; }
# sync_out: the receipts so far to SYNC_TO, best effort (a Spot VM can go at
# any time); the copy on exit is the one that counts.
sync_out() {
	if [ -n "${SYNC_TO:-}" ]; then
		{ mkdir -p "$SYNC_TO" && cp -r "$out/." "$SYNC_TO/"; } || echo "warning: copying the receipts to $SYNC_TO failed" >&2
	fi
}

# summarize: summary.md and summary.json from the receipts and expected.txt.
# Exits 0 for PASS and 3 for FAIL or INCOMPLETE; any other status means the
# summary could not be written.
summarize() {
	"$PYTHON" -I - "$out" "$probe" "$failures" <<'PY'
import csv, glob, json, os, sys
out, probe, failures = sys.argv[1], sys.argv[2] == "1", int(sys.argv[3])
P = lambda *a: os.path.join(out, *a)

def tsv(name):
    return list(csv.DictReader(open(P(name)), delimiter="\t")) if os.path.exists(P(name)) else []

def load(path):
    try:
        return json.load(open(path))
    except (OSError, ValueError):
        return None

env = dict(l.rstrip("\n").split(" ", 1) for l in open(P("env.txt")) if " " in l) if os.path.exists(P("env.txt")) else {}
expected = [l.split() for l in open(P("expected.txt")) if l.strip()] if os.path.exists(P("expected.txt")) else []
missing = [] if expected else ["expected.txt: the evidence this mode needs was never written"]
conversion, listing, startup, rd_tsv = tsv("conversion.tsv"), tsv("listing.tsv"), tsv("startup.tsv"), tsv("replaydiff.tsv")
reports, diffds = [], {}
for kind, *rest in expected:
    if kind == "conversion":
        name = rest[0]
        row = [c for c in conversion if c["fixture"] == name]
        check = load(P("conversion", name, "check.json"))
        if len(row) != 1 or row[0]["validate"] != "0":
            missing.append(f"conversion {name}: no conversion.tsv row with seekparquet validate exit 0")
        if not check or check.get("problems") != [] or not os.path.exists(P("conversion", name, "parts.tsv")):
            missing.append(f"conversion {name}: no clean identity check (check.json, parts.tsv)")
    elif kind == "replaydiff":
        run = rest[0]
        r = load(P("replaydiff", run + ".json"))
        exits = [x["exit"] for x in rd_tsv if x["fixture"] + "-" + x["route"] == run]
        if r is None or exits != ["0"] or r.get("failed") != 0 or not r.get("compared"):
            missing.append(f"replaydiff {run}: no report with compared > 0, failed 0 and exit 0")
        if r is not None:
            reports.append({"run": run, "keys": r.get("keys"), "compared": r.get("compared"), "passed": r.get("passed"),
                            "failed": r.get("failed"), "skipped": r.get("skipped_count"),
                            "documented_divergences": len(r.get("documented_divergences") or []),
                            "documented_residuals": len(r.get("documented_residuals") or []),
                            "effect_only": r.get("token_effect_only", 0),
                            "token_byte_differences": len(r.get("token_byte_differences") or []),
                            "requests": r.get("requests"), "seconds": round(r.get("seconds", 0), 1)})
    elif kind == "listing":
        name = rest[0]
        want = [c["rows"] for c in conversion if c["fixture"] == name]
        for server in ("java", "go"):
            rows = [l for l in listing if l["fixture"] == name and l["server"] == server]
            if len(rows) != 1 or rows[0]["exit"] != "0" or want == [] or rows[0]["rows"] != want[0]:
                missing.append(f"listing {name} {server}: no successful listing with the fixture's {want[0] if want else '?'} rows")
        d = load(P("listing", name, "diffds.json"))
        diffds[name] = d
        if not d or d.get("identical") is not True:
            missing.append(f"listing {name}: no diffds report saying identical")
    elif kind == "startup_reps":
        n = int(rest[0])
        for name in sorted({e[1] for e in expected if e[0] == "listing"}):
            for server in ("java", "go"):
                got = [s for s in startup if s["fixture"] == name and s["server"] == server
                       and s["ready_ms"].isdigit() and s["rss_kb"].isdigit()]
                if len(got) != n:
                    missing.append(f"startup {name} {server}: {len(got)} of {n} starts recorded")
fails = [l.rstrip("\n") for l in open(P("failures.txt"))] if os.path.exists(P("failures.txt")) else []
verdict = "FAIL" if fails or failures else "INCOMPLETE" if missing else "PASS"
summary = {"verdict": verdict, "mode": "probe" if probe else "full", "swath_sha": env.get("swath_sha"),
           "converter_commit": env.get("converter_commit"), "machine": env.get("gce_machine_type"), "nproc": env.get("nproc"),
           "failures": fails, "missing_evidence": missing, "conversion": conversion, "replaydiff": reports,
           "listing": listing, "diffds": diffds, "startup": startup,
           "startup_conditions": "fresh processes, OS page cache not dropped (cache-uncontrolled, generally warm); "
                                 "per fixture: Java STARTUP_REPS starts, its listing start, then Go the same way"}
json.dump(summary, open(P("summary.json"), "w"), indent=1)

md = [f"# Go and Java replay comparison: {verdict}", "",
      f"Mode {summary['mode']}; swath `{summary['swath_sha']}`, converter `{summary['converter_commit']}`; "
      f"machine {summary['machine'] or 'unknown'}, {summary['nproc']} vCPU; Java {env.get('java', '')}; "
      f"Java max heap {env.get('java_max_heap_bytes', '?')} bytes (the launcher's default).", "",
      "The Java server serves each Java-era fixture, the Go server its footer-only SeekParquet conversion "
      "(list index on). evidence-azure is not compared: the Java sort-fixture refuses its names.", ""]
if fails:
    md += ["## Failures", ""] + [f"- {f}" for f in fails] + [""]
if missing:
    md += ["## Missing evidence", ""] + [f"- {m}" for m in missing] + [""]
md += ["## Conversions", "", "| Fixture | Parts | Rows | validate exit | Parts equal to the study's staged conversion |", "|---|---|---|---|---|"]
md += [f"| {c['fixture']} | {c['parts']} | {c['rows']} | {c['validate']} | {c['study_staged_parts_equal']} |" for c in conversion]
md += ["", "## replaydiff", "", "| Run | Keys | Compared | Passed | Failed | Skipped | S3 divergences | Residuals | Token or marker effect only | Seconds |",
       "|---|---|---|---|---|---|---|---|---|---|"]
md += [f"| {r['run']} | {r['keys']} | {r['compared']} | {r['passed']} | {r['failed']} | {r['skipped']} | {r['documented_divergences']} | "
       f"{r['documented_residuals']} | {r['effect_only']} | {r['seconds']} |" for r in reports]
md += ["", "## Full listings", "",
       "RSS at ready is the listing start's at its first 200 from /healthz; VmHWM is the server's peak over that start and the listing.", "",
       "| Fixture | Server | Exit | Wall ms | Rows | RSS at ready KiB | VmHWM KiB | diffds identical |", "|---|---|---|---|---|---|---|---|"]
md += [f"| {l['fixture']} | {l['server']} | {l['exit']} | {l['list_ms']} | {l['rows']} | {l['ready_rss_kb']} | {l['server_vmhwm_kb']} | "
       f"{(diffds.get(l['fixture']) or {}).get('identical', 'n/a')} |" for l in listing]
md += ["", "## Startup", "",
       "Exec to the first 200 from /healthz, and VmRSS then. Fresh processes; the OS page cache is not dropped, so these are "
       "cache-uncontrolled, generally warm starts (the fixtures were just written, converted, hashed and read). Seq is the "
       "global start order: per fixture, Java's starts, its listing start, then Go's.", "",
       "| Fixture | Server | Rep | Seq | Ready ms | RSS at ready KiB | List index |", "|---|---|---|---|---|---|---|"]
md += [f"| {s['fixture']} | {s['server']} | {s['rep']} | {s['seq']} | {s['ready_ms']} | {s['rss_kb']} | {s['list_index']} |" for s in startup]
open(P("summary.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md))
sys.exit(0 if verdict == "PASS" else 3)
PY
}

stop_server() {
	[ -n "${pid:-}" ] || return 0
	kill -TERM "$pid" 2>/dev/null || true
	wait "$pid" 2>/dev/null || true
	pid=
}

# finish runs on every exit: it stops a running server, records an exit
# nobody recorded, writes the summary, and makes the final copy of the
# receipts. The status is the run's unless that was 0 and the verdict is not
# PASS, the summary could not be written, or the final copy failed.
finish() {
	local rc=$? s=0
	trap - EXIT
	set +e
	stop_server
	if [ "$rc" != 0 ] && [ "$failures" = 0 ]; then
		record_failure "the run ended with status $rc during: $current_step"
	fi
	printf '\n== %s summary\n' "$(date -u +%FT%TZ)"
	summarize
	s=$?
	if [ "$s" != 0 ] && [ "$s" != 3 ]; then
		echo "the summary could not be written (status $s)" >&2
		printf '# Go and Java replay comparison: INCOMPLETE\n\nThe summary could not be written (status %s); see failures.txt.\n' "$s" >"$out/summary.md"
	fi
	[ "$s" = 0 ] || [ "$rc" != 0 ] || rc=1
	if [ -n "${SYNC_TO:-}" ]; then
		if ! { mkdir -p "$SYNC_TO" && cp -r "$out/." "$SYNC_TO/"; }; then
			echo "publishing the receipts to $SYNC_TO failed" >&2
			[ "$rc" != 0 ] || rc=1
		fi
	fi
	exit "$rc"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

# ------------------------------------------------------------------- the plan
# Every compared fixture: name|bucket|region|Java-era directory|converter id.
fixtures=("synthetic-10k|bucket||$fx_root/synthetic-10k/sorted|make-fixtures/synthetic-10k/sorted")
# Every replaydiff run: fixture|route|tag|extra flags (space-separated).
replaydiff_plan=("synthetic-10k|s3||" "synthetic-10k|gcs||" "synthetic-10k|azure||-accept-segment-improvement")
if [ "$probe" = 0 ]; then
	fixtures+=(
		"synthetic-1m-sorted|bucket||$fx_root/synthetic-1m/sorted|make-fixtures/synthetic-1m/sorted"
		"synthetic-1m-rolled|bucket||$fx_root/synthetic-1m/sorted-rolled|make-fixtures/synthetic-1m/sorted-rolled"
		"evidence-gcs|bucket||$fx_root/evidence-gcs/sorted|make-fixtures/evidence-gcs/sorted"
	)
	for name in synthetic-1m-sorted synthetic-1m-rolled; do
		# A million keys: walks bounded, then one walk at the largest page
		# size to the end.
		replaydiff_plan+=(
			"$name|s3||-walks 1,7,1000 -walk-limit 400"
			"$name|s3|-full|-walks 1000 -walk-limit 0 -random 0"
			"$name|gcs||-walks 1,7,1000 -walk-limit 400"
			"$name|gcs|-full|-walks 1000 -walk-limit 0 -random 0"
			"$name|azure||-walks 1,7,5000 -walk-limit 400 -accept-segment-improvement"
			"$name|azure|-full|-walks 5000 -walk-limit 0 -random 0 -accept-segment-improvement"
		)
	done
	replaydiff_plan+=("evidence-gcs|s3||" "evidence-gcs|gcs||" "evidence-gcs|azure||-accept-segment-improvement")
	for row in "${STUDY_FIXTURES[@]}"; do
		IFS='|' read -r name region path _ _ <<<"$row"
		fixtures+=("$name|$name|$region|$work/study/$name/java-era|$path")
	done
fi
{
	for row in "${fixtures[@]}"; do
		echo "conversion ${row%%|*}"
		echo "listing ${row%%|*}"
	done
	for entry in "${replaydiff_plan[@]}"; do
		IFS='|' read -r name route tag _ <<<"$entry"
		echo "replaydiff $name-$route$tag"
	done
	echo "startup_reps $STARTUP_REPS"
} >"$out/expected.txt"

# ---------------------------------------------------------------- environment
step "environment"
{
	echo "started $(date -u +%FT%TZ)"
	echo "mode $([ "$probe" = 1 ] && echo probe || echo full)"
	echo "swath_sha $SWATH_SHA"
	echo "converter_commit $CONVERTER_COMMIT"
	echo "fixtures_src $FIXTURES_SRC"
	echo "host $(uname -a)"
	echo "nproc $(nproc)"
	grep -E '^(MemTotal|MemAvailable)' /proc/meminfo
	grep -m1 'model name' /proc/cpuinfo || true
	if mt=$(curl -fsS -m 2 -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type 2>/dev/null); then
		echo "gce_machine_type ${mt##*/}"
	fi
	echo "go $(cd "$gosrc" && go version)"
	echo "java $("$JAVA_HOME/bin/java" -version 2>&1 | tr '\n' ' ')"
	echo "java_opts SWATH_REPLAY_OPTS=${SWATH_REPLAY_OPTS:-} JAVA_OPTS=${JAVA_OPTS:-} JAVA_TOOL_OPTIONS=${JAVA_TOOL_OPTIONS:-}"
	echo "java_max_heap_bytes $("$JAVA_HOME/bin/java" -XX:+PrintFlagsFinal -version 2>/dev/null | awk '$2 == "MaxHeapSize" {print $4}')"
	echo "python $("$PYTHON" -c 'import sys, duckdb; print(sys.version.split()[0], "duckdb", duckdb.__version__)')"
	echo "list_workers $LIST_WORKERS startup_reps $STARTUP_REPS"
	echo "startup_conditions cache-uncontrolled (page cache not dropped, generally warm); per fixture Java then Go"
} >"$out/env.txt" 2>&1
cat "$out/env.txt"

# ---------------------------------------------------------------------- build
# build DIR LOG COMMAND...: one build step in DIR, appended to LOG. Any failure
# stops the run, --keep-going or not. Earlier outputs are removed first, so a
# stale binary or distribution cannot stand in for one that did not build.
build() {
	local dir=$1 log=$2
	shift 2
	echo "+ (cd $dir && $*)" >>"$log"
	(cd "$dir" && "$@") >>"$log" 2>&1 || { tail -n 50 "$log" >&2; fatal "build step failed: $* (build/${log##*/})"; }
}
step "build: the Java replay distribution, the Go binaries, the converter"
rm -rf "$bin" "$java_install"
mkdir -p "$bin"
build "$checkout" "$out/build/gradle.log" env GITHUB_SHA="${SWATH_SHA%-dirty}" ./gradlew --no-daemon :swath-replay:installDist
[ -x "$java_replay" ] || fatal "the Java replay distribution has no $java_replay"
build "$gosrc" "$out/build/go.log" env SWATH_REVISION="$SWATH_SHA" ./build.sh -o "$bin/swath" ./cmd/swath
build "$gosrc" "$out/build/go.log" env SWATH_REVISION="$SWATH_SHA" ./build.sh -o "$bin/swath-replay" ./cmd/swath-replay
build "$gosrc" "$out/build/go.log" env CGO_ENABLED=0 go build -trimpath -o "$bin/replaydiff" ./bench/replaydiff
build "$gosrc" "$out/build/go.log" env CGO_ENABLED=0 go build -trimpath -o "$bin/seekparquet" github.com/varveio/seekparquet/go/cmd/seekparquet

converter_src=${CONVERTER_SRC:-}
if [ -z "$converter_src" ]; then
	converter_src=$work/converter-src
	rm -rf "$converter_src"
	build "$work" "$out/build/converter.log" git init -q "$converter_src"
	build "$converter_src" "$out/build/converter.log" git fetch -q --depth 1 "$CONVERTER_REPO" "$CONVERTER_COMMIT"
	build "$converter_src" "$out/build/converter.log" git -c advice.detachedHead=false checkout -q FETCH_HEAD
fi
got=$(git -C "$converter_src" rev-parse HEAD 2>/dev/null) || fatal "the converter source $converter_src is not a git checkout"
[ "$got" = "$CONVERTER_COMMIT" ] || fatal "the converter source $converter_src is at $got, not $CONVERTER_COMMIT"
[ -z "$(git -C "$converter_src" status --porcelain)" ] || fatal "the converter source $converter_src has local changes"
echo "converter_src $converter_src" >>"$out/env.txt"
build "$converter_src/go" "$out/build/converter.log" env CGO_ENABLED=0 go build -trimpath -o "$bin/adoptfixture" ./bench/replay/adoptfixture
{
	for b in "$bin"/*; do
		printf '%s\t%s\n' "${b##*/}" "$(sha256sum "$b" | cut -d' ' -f1)"
	done
	printf 'java-replay-lib\t%s\n' "$(cd "$java_install/lib" && sha256sum ./*.jar | LC_ALL=C sort | sha256sum | cut -d' ' -f1)"
} >"$out/build/sha256.tsv"
for b in swath swath-replay replaydiff seekparquet adoptfixture; do
	go version -m "$bin/$b" >"$out/build/$b.buildinfo.txt"
done
sync_out

# ------------------------------------------------------------------- fixtures
step "fixtures: make-fixtures.sh (the Java sort-fixture)"
rm -rf "$fx_root"
PATH="${java_replay%/*}:$PATH" "$here/make-fixtures.sh" "$fx_root" >"$out/fixtures/make-fixtures.log" 2>&1 ||
	{ cat "$out/fixtures/make-fixtures.log" >&2; fatal "make-fixtures.sh failed (fixtures/make-fixtures.log)"; }
if [ "$probe" = 0 ]; then
	step "fixtures: the study's Java-era fixtures from $FIXTURES_SRC"
	printf 'fixture\tsource\tdigest\texpected\n' >"$out/fixtures/study.tsv"
	for row in "${STUDY_FIXTURES[@]}"; do
		IFS='|' read -r name region path digest staged <<<"$row"
		dest=$work/study/$name/java-era
		rm -rf "$dest" && mkdir -p "$dest"
		rc=0
		case $FIXTURES_SRC in
		gs://*) gcloud storage cp "$FIXTURES_SRC/$path/part-*.parquet" "$dest/" >>"$out/logs/fetch.log" 2>&1 || rc=$? ;;
		*) cp "$FIXTURES_SRC/$path"/part-*.parquet "$dest/" || rc=$? ;;
		esac
		[ "$rc" = 0 ] || fatal "fetching $FIXTURES_SRC/$path failed"
		got=$(cd "$dest" && for f in part-*.parquet; do printf '%s\t%s\t%s\n' "$f" "$(stat -c %s "$f")" "$(sha256sum "$f" | cut -d' ' -f1)"; done | LC_ALL=C sort | sha256sum | cut -d' ' -f1)
		printf '%s\t%s\t%s\t%s\n' "$name" "$FIXTURES_SRC/$path" "$got" "$digest" >>"$out/fixtures/study.tsv"
		[ "$got" = "$digest" ] || fatal "$name: the fetched parts' digest is $got, not the study's $digest"
		rc=0
		case $FIXTURES_SRC in
		gs://*) gcloud storage cp "$FIXTURES_SRC/$staged/manifest.json" "$out/fixtures/$name.study-staged-manifest.json" >>"$out/logs/fetch.log" 2>&1 || rc=$? ;;
		*) cp "$FIXTURES_SRC/$staged/manifest.json" "$out/fixtures/$name.study-staged-manifest.json" || rc=$? ;;
		esac
		[ "$rc" = 0 ] || echo "the study's staged manifest for $name could not be read; that informative comparison is left out" >&2
	done
fi
sync_out

# ----------------------------------------------------------------- conversion
# check_conversion NAME IN OUT PARTS_TSV: every part's identity. Each file's
# data extent (the bytes before its footer) is located from its own Parquet
# trailer (the footer length and the PAR1 magic), not taken from the
# converter: the two extents must be equal, equal to the converter's recorded
# data_bytes, and that whole prefix byte-identical. The manifest's sizes and
# SHA-256 must be the files', and the input digests it records the inputs'.
check_conversion() {
	"$PYTHON" -I - "$@" <<'PY'
import hashlib, json, os, sys
name, src, dst, out = sys.argv[1:5]
m = json.load(open(os.path.join(dst, "manifest.json")))
adopted = m["extensions"]["swath-replay"]["adopted_from"]["parts"]
parts = m["parts"]
problems = []
if len(adopted) != len(parts):
    problems.append(f"{len(parts)} output parts for {len(adopted)} inputs")
inputs = sorted(f for f in os.listdir(src) if f.endswith(".parquet"))
if sorted(a["input"] for a in adopted) != inputs:
    problems.append(f"the manifest's inputs {sorted(a['input'] for a in adopted)} are not the fixture's {inputs}")

def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while b := f.read(1 << 22):
            h.update(b)
    return h.hexdigest()

def extent(path):
    """The bytes before the footer, from the file's own trailer; None if it is not Parquet."""
    size = os.path.getsize(path)
    if size < 12:
        return None, None
    with open(path, "rb") as f:
        head = f.read(4)
        f.seek(size - 8)
        tail = f.read(8)
    footer = int.from_bytes(tail[:4], "little")
    n = size - 8 - footer
    if head != b"PAR1" or tail[4:] != b"PAR1" or n < 4:
        return None, None
    return n, footer

def same_prefix(a, b, n):
    with open(a, "rb") as x, open(b, "rb") as y:
        left = n
        while left:
            k = min(left, 1 << 22)
            if x.read(k) != y.read(k):
                return False
            left -= k
    return True

rows = ["input\tinput_bytes\tinput_sha256\toutput\toutput_bytes\toutput_sha256\trows\tinput_extent\toutput_extent\t"
        "input_footer_len\toutput_footer_len\tconverter_data_bytes\tprefix_identical"]
total = 0
for a, p in zip(adopted, parts):
    ip, op = os.path.join(src, a["input"]), os.path.join(dst, p["path"])
    isize, osize = os.path.getsize(ip), os.path.getsize(op)
    isha, osha = digest(ip), digest(op)
    (ie, ifl), (oe, ofl) = extent(ip), extent(op)
    same = ie is not None and ie == oe and same_prefix(ip, op, ie)
    for what, got, want in (("input bytes", isize, a["input_bytes"]), ("input sha256", isha, a["input_sha256"]),
                            ("output bytes", osize, p["bytes"]), ("output sha256", osha, p["sha256"])):
        if got != want:
            problems.append(f"{a['input']}: {what} {got}, the manifest says {want}")
    if ie is None or oe is None:
        problems.append(f"{a['input']}: no Parquet trailer (input extent {ie}, output extent {oe})")
    elif ie != oe:
        problems.append(f"{a['input']}: the data extents differ: input {ie}, output {oe}")
    elif not same:
        problems.append(f"{a['input']}: the {ie} bytes before the footers differ")
    if ie != a["data_bytes"]:
        problems.append(f"{a['input']}: the converter recorded data_bytes {a['data_bytes']}, the trailer says {ie}")
    total += p["rows"]
    rows.append("\t".join(str(v) for v in (a["input"], isize, isha, p["path"], osize, osha, p["rows"], ie, oe, ifl, ofl,
                                           a["data_bytes"], same)))
open(out, "w").write("\n".join(rows) + "\n")
print(json.dumps({"fixture": name, "parts": len(parts), "rows": total, "problems": problems}))
sys.exit(1 if problems else 0)
PY
}

step "conversion: footer-only SeekParquet (the converter at $CONVERTER_COMMIT)"
printf 'fixture\tparts\trows\tvalidate\tstudy_staged_parts_equal\n' >"$out/conversion.tsv"
for row in "${fixtures[@]}"; do
	IFS='|' read -r name bucket region src id <<<"$row"
	dst=$work/converted/$name
	rec=$out/conversion/$name
	rm -rf "$dst" && mkdir -p "$rec"
	args=(-input "$src" -output "$dst" -bucket "$bucket" -fixture "$id")
	[ -n "$region" ] && args+=(-region "$region")
	echo "adoptfixture ${args[*]}" >"$rec/command.txt"
	rc=0
	"$bin/adoptfixture" "${args[@]}" >"$rec/adopt.log" 2>&1 || rc=$?
	if [ "$rc" != 0 ]; then
		cat >"$rec/REFUSED.txt" <<EOF
The converter refused $name (exit $rc); nothing was converted and the comparison stops here.
$(cat "$rec/adopt.log")

Fallback (#353's original port, not run automatically): serve the Java-era fixture
with the Java server and re-list it with swath-go into a SeekParquet dataset:
  swath-replay serve --fixture $src --bucket $bucket --port 0
  swath list s3://$bucket --endpoint-url http://127.0.0.1:PORT --anonymous -o DIR
then compare that dataset with the Java server's listing (diffds) and record the
layout change (relisting rewrites the parts; footer adoption does not).
EOF
		cat "$rec/REFUSED.txt" >&2
		fail "the converter refused $name (conversion/$name/REFUSED.txt)"
		continue
	fi
	cp "$dst/manifest.json" "$rec/manifest.json"
	rc=0
	check=$(check_conversion "$name" "$src" "$dst" "$rec/parts.tsv") || rc=$?
	echo "$check" >"$rec/check.json"
	[ "$rc" = 0 ] || { echo "$check" >&2; fail "$name: conversion identity check failed (conversion/$name/check.json, parts.tsv)"; }
	vrc=0
	"$bin/seekparquet" validate --level full --json "$dst" >"$rec/validate.json" 2>"$rec/validate.err" || vrc=$?
	[ "$vrc" = 0 ] || fail "$name: seekparquet validate --level full exited $vrc (conversion/$name/validate.json)"
	staged=n/a
	if [ -f "$out/fixtures/$name.study-staged-manifest.json" ]; then
		staged=$("$PYTHON" -I -c 'import json, sys
a, b = (json.load(open(p))["parts"] for p in sys.argv[1:3])
print(str([(p["path"], p["bytes"], p["sha256"]) for p in a] == [(p["path"], p["bytes"], p["sha256"]) for p in b]).lower())' \
			"$dst/manifest.json" "$out/fixtures/$name.study-staged-manifest.json")
	fi
	parts=$("$PYTHON" -I -c 'import json,sys; c = json.loads(sys.argv[1]); print(c["parts"], c["rows"])' "$check")
	printf '%s\t%s\t%s\t%s\t%s\n' "$name" "${parts% *}" "${parts#* }" "$vrc" "$staged" >>"$out/conversion.tsv"
done
sync_out

# ----------------------------------------------------------------- replaydiff
fixture_field() {
	local row
	for row in "${fixtures[@]}"; do
		if [ "${row%%|*}" = "$1" ]; then
			cut -d'|' -f"$2" <<<"$row"
			return
		fi
	done
	return 1
}

step "replaydiff: S3, GCS and Azure"
printf 'fixture\troute\texit\tms\targs\n' >"$out/replaydiff.tsv"
for entry in "${replaydiff_plan[@]}"; do
	IFS='|' read -r name route tag flags <<<"$entry"
	read -ra extra <<<"$flags"
	src=$(fixture_field "$name" 4)
	base=$out/replaydiff/$name-$route$tag
	logs=$work/logs/replaydiff-$name-$route$tag
	mkdir -p "$logs"
	cmd=("$bin/replaydiff" -protocol "$route" -java "$java_replay" -go "$bin/swath-replay" -fixture "$src"
		-go-fixture "$work/converted/$name" -bucket bucket -report "$base.json" -log-dir "$logs" "${extra[@]}")
	echo "${cmd[*]}" >"$base.command.txt"
	t0=$(now_ns)
	rc=0
	"${cmd[@]}" >"$base.log" 2>&1 || rc=$?
	printf '%s\t%s\t%s\t%s\t%s\n' "$name" "$route$tag" "$rc" "$((($(now_ns) - t0) / 1000000))" "$flags" >>"$out/replaydiff.tsv"
	cp "$logs"/*.log "$out/logs/" 2>/dev/null || true
	[ "$rc" = 0 ] || { tail -n 40 "$base.log" >&2; fail "replaydiff $route on $name: exit $rc (replaydiff/$name-$route$tag.txt)"; }
	sync_out
done

# ---------------------------------------------------- listing and startup
# serve WHICH BIN FIXTURE BUCKET LOG: starts a server (S3 route, sorted
# serving, its other flags at their defaults: the Go list index on) and waits
# for a 200 from /healthz. Sets pid, endpoint, ready_ms (exec to healthy),
# rss_kb (VmRSS once healthy) and seq (the global start order).
start_seq=0
serve() {
	local which=$1 b=$2 fx=$3 bucket=$4 log=$5 t0 metrics deadline
	start_seq=$((start_seq + 1))
	seq=$start_seq
	t0=$(now_ns)
	"$b" serve --fixture "$fx" --bucket "$bucket" --host 127.0.0.1 --port 0 --metrics-port 0 --serving-mode sorted \
		>/dev/null 2>"$log" &
	pid=$!
	deadline=$(($(date +%s) + 900))
	while ! grep -q '^swath_replay endpoint=' "$log" 2>/dev/null; do
		kill -0 "$pid" 2>/dev/null || { cat "$log" >&2; pid=; return 1; }
		[ "$(date +%s)" -lt "$deadline" ] || { echo "$which: no startup line in 900 s" >&2; return 1; }
		sleep 0.01
	done
	endpoint=$(grep -m1 '^swath_replay endpoint=' "$log" | sed 's/^swath_replay endpoint=\([^ ]*\).*/\1/')
	metrics=$(grep -m1 '^swath_replay endpoint=' "$log" | sed -n 's/.* metrics_endpoint=\(http:[^ ]*\)\/metrics.*/\1/p')
	[ -n "$metrics" ] || { echo "$which: no metrics endpoint in the startup line" >&2; return 1; }
	until curl -fsS -m 5 -o /dev/null "$metrics/healthz" 2>/dev/null; do
		kill -0 "$pid" 2>/dev/null || { cat "$log" >&2; pid=; return 1; }
		[ "$(date +%s)" -lt "$deadline" ] || { echo "$which: /healthz not ready in 900 s" >&2; return 1; }
		sleep 0.01
	done
	ready_ms=$((($(now_ns) - t0) / 1000000))
	rss_kb=$(awk '/^VmRSS:/ {print $2}' "/proc/$pid/status")
	[ -n "$rss_kb" ] || { echo "$which: no VmRSS for $pid" >&2; return 1; }
}

step "listings, startup and diffds"
printf 'fixture\tserver\trep\tseq\tready_ms\trss_kb\tlist_index\tcache\n' >"$out/startup.tsv"
printf 'fixture\tserver\texit\tlist_ms\trows\tseq\tready_rss_kb\tserver_vmhwm_kb\n' >"$out/listing.tsv"
for row in "${fixtures[@]}"; do
	IFS='|' read -r name bucket region src id <<<"$row"
	dst=$work/converted/$name
	rec=$out/listing/$name
	mkdir -p "$rec"
	[ -f "$dst/manifest.json" ] || { fail "$name: no conversion to list"; continue; }
	want_rows=$("$PYTHON" -I -c 'import json,sys; print(sum(p["rows"] for p in json.load(open(sys.argv[1]))["parts"]))' "$dst/manifest.json")
	for which in java go; do
		if [ "$which" = java ]; then b=$java_replay fx=$src; else b=$bin/swath-replay fx=$dst; fi
		for rep in $(seq 1 "$STARTUP_REPS"); do
			log=$work/logs/serve-$name-$which-$rep.log
			mkdir -p "${log%/*}"
			serve "$which" "$b" "$fx" "$bucket" "$log" || { stop_server; fail "$which server did not start on $name (rep $rep)"; continue 2; }
			idx=$(sed -n 's/^swath_replay list_index state=\([^ ]*\).*/\1/p' "$log" | head -n 1)
			printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\tuncontrolled-warm\n' "$name" "$which" "$rep" "$seq" "$ready_ms" "$rss_kb" "${idx:-n/a}" >>"$out/startup.tsv"
			cp "$log" "$out/logs/"
			stop_server
		done
		log=$work/logs/serve-$name-$which-list.log
		serve "$which" "$b" "$fx" "$bucket" "$log" || { stop_server; fail "$which server did not start on $name for the listing"; continue; }
		ds=$work/listing/$name/$which
		rm -rf "$ds"
		cmd=("$bin/swath" list "s3://$bucket" --endpoint-url "$endpoint" --anonymous --workers "$LIST_WORKERS"
			-o "$ds" --report "$rec/$which.report.json")
		echo "${cmd[*]}" >"$rec/$which.command.txt"
		t0=$(now_ns)
		rc=0
		"${cmd[@]}" >"$rec/$which.log" 2>&1 || rc=$?
		ms=$((($(now_ns) - t0) / 1000000))
		hwm=$(awk '/^VmHWM:/ {print $2}' "/proc/$pid/status")
		stop_server
		cp "$log" "$out/logs/"
		rows=-1
		if [ "$rc" = 0 ]; then
			rows=$("$PYTHON" -I -c 'import json,sys; print(sum(p["rows"] for p in json.load(open(sys.argv[1]))["parts"]))' "$ds/manifest.json") || rows=-1
		fi
		printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$name" "$which" "$rc" "$ms" "$rows" "$seq" "$rss_kb" "${hwm:-n/a}" >>"$out/listing.tsv"
		[ "$rc" = 0 ] || { tail -n 20 "$rec/$which.log" >&2; fail "swath list through the $which server on $name exited $rc"; continue; }
		[ "$rows" = "$want_rows" ] || fail "$name: the listing through the $which server has $rows rows, the fixture $want_rows"
	done
	if [ -f "$work/listing/$name/java/manifest.json" ] && [ -f "$work/listing/$name/go/manifest.json" ]; then
		rc=0
		(cd "$work" && "$PYTHON" "$gosrc/bench/diffds.py" "$work/listing/$name/java" "$work/listing/$name/go") >"$rec/diffds.json" 2>"$rec/diffds.err" || rc=$?
		[ "$rc" = 0 ] || fail "$name: diffds found the listings through the two servers different, or failed (listing/$name/diffds.json, diffds.err)"
	else
		fail "$name: diffds not run: a listing is missing"
	fi
	# The listings are large: keep only the receipts.
	rm -rf "$work/listing/$name"
	sync_out
done
current_step="done"
# finish (the EXIT trap) writes the summary, makes the final copy and sets
# the status.
