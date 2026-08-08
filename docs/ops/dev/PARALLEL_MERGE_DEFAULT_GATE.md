# Parallel merge default-on validation gate

Status: **required before approving the default-on decision**. The implementation may remain
default-on in the candidate branch while this gate runs, but the PR should not merge on the old
single-run evidence alone.

This is a **manual operator runbook, not CI**. Never put the live-S3 section in a GitHub Actions
workflow, never give CI AWS credentials for it, and never make either the synthetic benchmark or
replay sweep part of `build`, PR checks, deep tests, or integration tests. The benchmark stays behind
the explicit `-Dswath.bench=on` opt-in and a named-test filter.

Do **not** run this plan on the slower development box. Run it on the same class of host used for
the 2026-08-07 measurements, or a faster dedicated host: GCP `c4a-highcpu-32` (32 physical arm64
cores, no SMT), at least 62 GiB RAM, JDK 25, `ulimit -n 65536`, and a local SSD with enough free
space for staging and final output at the same time. Do not bypass swath's disk guard; provision a
larger volume if it refuses a run. Keep the host otherwise idle and run every arm sequentially.

This gate uses all three evidence layers. The synthetic harness isolates merge scaling and performs
full-row comparisons. Replay makes the end-to-end listing deterministic and covers both server
implementations. Live S3 validates the real client, page-run staging, filesystem, and public-bucket
conditions that replay cannot reproduce.

## 1. Freeze and record the environment

Use one candidate commit for every arm. Keep all artifacts outside the repository. Run every shell
block below in the same Bash session so the exported paths and `pipefail` remain active.

```bash
cd /path/to/swath
set -o pipefail
export SWATH_REPO=$PWD
export SCRATCH_BASE=/mnt/local-ssd
export RUN_ROOT
RUN_ROOT=$(mktemp -d "$SCRATCH_BASE/swath-parallel-merge.XXXXXX")
export SWATH="$SWATH_REPO/swath-cli/build/install/swath/bin/swath"
export REPLAY="$SWATH_REPO/swath-replay-server/build/install/swath-replay-server/bin/swath-replay-server"

git status -sb
git rev-parse HEAD | tee "$RUN_ROOT/candidate-sha.txt"
java -version 2>&1 | tee "$RUN_ROOT/java.txt"
lscpu | tee "$RUN_ROOT/lscpu.txt"
free -h | tee "$RUN_ROOT/memory.txt"
df -h "$SCRATCH_BASE" | tee "$RUN_ROOT/disk.txt"
ulimit -n | tee "$RUN_ROOT/ulimit.txt"
./gradlew :swath-cli:installDist :swath-replay-server:installDist --no-daemon
```

Required fixed client settings are `-Xmx12g`, default LZ4 page-run staging, default merge budget,
default fan-in, default 256 MiB staged-input floor, and `--concurrency 256` for live S3. The only
merge knob changed between the live/replay A/B arms is
`swath.sort.merge-parallelism`: `1` for serial and absent for the candidate default. Do not set an
explicit `R=8` in the default arm; the point is to verify the shipped processor-derived default.

## 2. Functional precondition

Before collecting performance evidence, run the normal, integration, and deep correctness gates on
the candidate SHA. These are tests, not benchmark runs:

```bash
cd "$SWATH_REPO"
./gradlew build --no-daemon --stacktrace
./gradlew test -PonlyIntegration --no-daemon --stacktrace
./gradlew test -Pdeep -PtestMaxParallelForks=1 --no-daemon --stacktrace
```

All three must pass. The PR's ordinary check only runs the fast tier; do not treat green PR checks
alone as integration/deep coverage of the final SHA.

## 3. Isolated page-run merge scaling

Run three fresh JVM sweeps. This is the production `SortBuffer -> seal -> PageRunSegmentWriter ->
SortTransform` path, not legacy Parquet staging. Each sweep compares every parallel output against
the `R=1` baseline row for row.

```bash
cd "$SWATH_REPO"
for repetition in 1 2 3; do
  ./gradlew :swath-core:test \
    --tests 'io.varve.swath.sort.ParallelMergeBenchmark' \
    -Dswath.bench=on \
    -Dswath.bench.segments=64 \
    -Dswath.bench.rows=12000000 \
    -Dswath.bench.ranges=1,2,4,8 \
    -Pperf --no-daemon --rerun-tasks \
    2>&1 | tee "$RUN_ROOT/merge-harness-$repetition.log" || exit 1
  cp swath-core/build/test-results/test/TEST-io.varve.swath.sort.ParallelMergeBenchmark.xml \
    "$RUN_ROOT/merge-harness-$repetition.xml"
done
```

Accept this layer only if all three sweeps satisfy all of the following:

- no `BENCH_FULL_ROW_EXACT_FAIL`, and every `BENCH_ROW` says `full_row_exact=true`;
- `actual_ranges` is exactly 1, 2, 4, and 8 for the requested arms, with no staged-floor,
  FD, cascade, or unsplittable clamp counter;
- `cascaded_passes=0` in every arm;
- the median `R=8` merge speedup is at least `2.0x` versus `R=1`, neither `R=2` nor `R=4`
  regresses versus `R=1`, and `R=8` is not more than 10% slower than `R=4`;
- the two `R=1` measurements inside each sweep differ by at most 10%; and
- `R=8` peak heap is no more than the larger of `1.15x` the first `R=1` peak or the `R=1`
  peak plus 512 MiB.

If an arm is clamped, the harness correctly suppresses its speedup. That is a valid clamp test, but
it does not satisfy this scaling gate: fix the reference host's fd/heap setup and repeat.

## 4. Deterministic replay, both serving modes

Use immutable sorted captures of these two buckets:

- `noaa-gefs-retrospective` (about 9.9 M objects): run both `sorted` and `duckdb` serving modes;
- `pds-css-archive` (about 96 M objects): run `sorted` mode.

Use immutable, pre-existing serial captures as the fixtures; set and validate their paths before
starting this section. They may be serial live-S3 outputs from section 5 if that section was run
earlier, but this numbered procedure must not assume newly-created `$RUN_ROOT` paths already exist.
The GEFS dual-mode arms prove the same client result against the indexed sorted path and the
independent DuckDB oracle. The larger
PDS sorted arm exercises a materially larger staging/segment set without paying DuckDB's unrelated
full-fixture materialization cost.

First verify that both serving modes complete the same fixture with the same page and row counts.
`bench` does not compare keys, full rows, or ordering; the independent checks below do that:

```bash
export GEFS_FIXTURE=/absolute/path/to/serial-gefs/data
export PDS_FIXTURE=/absolute/path/to/serial-pds/data
test -d "$GEFS_FIXTURE"
test -d "$PDS_FIXTURE"

"$REPLAY" bench --fixture "$GEFS_FIXTURE" --bucket gefs-replay \
  --modes sorted,duckdb --max-keys 1000 --json "$RUN_ROOT/gefs-serving-modes.json" \
  2>&1 | tee "$RUN_ROOT/gefs-serving-modes.log" || exit 1
"$REPLAY" bench --fixture "$PDS_FIXTURE" --bucket pds-replay \
  --modes sorted --max-keys 1000 --json "$RUN_ROOT/pds-serving-mode.json" \
  2>&1 | tee "$RUN_ROOT/pds-serving-mode.log" || exit 1
```

Then run every client arm against a fresh server process, pinned to cores 0-7. This removes server
JIT/cache warming as an ordering bias. Use four Parquet connections and the same deterministic
100 ms per-request profile used by the existing GEFS CPU characterization:

```bash
run_replay_arm() {
  local fixture=$1
  local bucket=$2
  local mode=$3
  local label=$4
  local client_opts=$5
  local client_status
  local replay_pid
  local ready=0

  JAVA_TOOL_OPTIONS='-Xmx8g' taskset -c 0-7 "$REPLAY" serve \
    --fixture "$fixture" --bucket "$bucket" \
    --host 127.0.0.1 --port 19090 --serving-mode "$mode" --parquet-connections 4 \
    --inject-latency 'worker_page=100ms,pivot_probe=100ms,structure_probe=100ms' \
    >"$RUN_ROOT/replay-$label-server.log" 2>&1 &
  replay_pid=$!

  for attempt in $(seq 1 120); do
    if curl -fsS "http://127.0.0.1:19090/$bucket?list-type=2&max-keys=1&encoding-type=url" \
      >/dev/null; then
      ready=1
      break
    fi
    kill -0 "$replay_pid" 2>/dev/null || break
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    kill "$replay_pid" 2>/dev/null || true
    wait "$replay_pid" || true
    return 1
  fi

  JAVA_TOOL_OPTIONS="$client_opts" taskset -c 8-31 "$SWATH" list \
    "s3://$bucket" --endpoint-url http://127.0.0.1:19090 --force-path-style --no-sign-request \
    --format parquet --sort --concurrency 64 --restart -v \
    -o "$RUN_ROOT/replay-$label" 2>&1 | tee "$RUN_ROOT/replay-$label.log"
  client_status=$?

  kill "$replay_pid" 2>/dev/null || true
  wait "$replay_pid" || true
  return "$client_status"
}

run_replay_arm "$GEFS_FIXTURE" gefs-replay sorted gefs-sorted-r1a \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1
run_replay_arm "$GEFS_FIXTURE" gefs-replay sorted gefs-sorted-default '-Xmx12g' || exit 1
run_replay_arm "$GEFS_FIXTURE" gefs-replay sorted gefs-sorted-r1b \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1

run_replay_arm "$GEFS_FIXTURE" gefs-replay duckdb gefs-duckdb-r1a \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1
run_replay_arm "$GEFS_FIXTURE" gefs-replay duckdb gefs-duckdb-default '-Xmx12g' || exit 1
run_replay_arm "$GEFS_FIXTURE" gefs-replay duckdb gefs-duckdb-r1b \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1

run_replay_arm "$PDS_FIXTURE" pds-replay sorted pds-sorted-r1a \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1
run_replay_arm "$PDS_FIXTURE" pds-replay sorted pds-sorted-default '-Xmx12g' || exit 1
run_replay_arm "$PDS_FIXTURE" pds-replay sorted pds-sorted-r1b \
  '-Xmx12g -Dswath.sort.merge-parallelism=1' || exit 1
```

Every mode is bracketed `R=1 -> default -> R=1`. Never run two clients or two server modes
concurrently. Preserve every client `_swath_summary.json`, server log, and output dataset under
`$RUN_ROOT`.

Use the section 5 `EXCEPT ALL` query for full-row comparisons. Require zero mismatches between the
candidate and both serial brackets within each serving mode, and between corresponding GEFS
`sorted` and `duckdb` outputs. Use the section 5 physical-order query on all nine outputs and require
zero descending key transitions.

Accept replay only if every run completes, every output is accepted by a fresh replay server with
`--serving-mode sorted`, serial/default outputs are full-row equal for each fixture, the candidate
default records `SORT.merge_range_parallel` with eight effective ranges and no clamp/cascade/failure
reason, and its merge wall is no worse than the mean of its two serial brackets in either GEFS
source mode. On PDS, require at least `2.0x` merge-phase speedup and no more than 5% end-to-end wall
regression.

## 5. Live S3, three sizes

Run the following anonymous public buckets from the reference host:

| bucket | region | purpose |
| --- | --- | --- |
| `noaa-gefs-retrospective` | `us-east-1` | small/low-segment case and immutable equality anchor |
| `pds-css-archive` | `us-west-2` | medium, deep-divergence case and immutable equality anchor |
| `noaa-mrms-pds` | `us-east-1` | large/high-segment resource and scaling case; bucket is mutable |

For each bucket, run `serial A -> candidate default -> serial B`, with a fresh destination for every
arm. The following GEFS commands are the template; substitute the table's bucket/region and labels
for PDS and MRMS.

```bash
JAVA_TOOL_OPTIONS='-Xmx12g -Dswath.sort.merge-parallelism=1' "$SWATH" list \
  s3://noaa-gefs-retrospective --region us-east-1 --no-sign-request \
  --format parquet --sort --concurrency 256 --restart -v --progress-interval 30s \
  -o "$RUN_ROOT/live-gefs-r1a" 2>&1 | tee "$RUN_ROOT/live-gefs-r1a.log" || exit 1

JAVA_TOOL_OPTIONS='-Xmx12g' "$SWATH" list \
  s3://noaa-gefs-retrospective --region us-east-1 --no-sign-request \
  --format parquet --sort --concurrency 256 --restart -v --progress-interval 30s \
  -o "$RUN_ROOT/live-gefs-default" 2>&1 | tee "$RUN_ROOT/live-gefs-default.log" || exit 1

JAVA_TOOL_OPTIONS='-Xmx12g -Dswath.sort.merge-parallelism=1' "$SWATH" list \
  s3://noaa-gefs-retrospective --region us-east-1 --no-sign-request \
  --format parquet --sort --concurrency 256 --restart -v --progress-interval 30s \
  -o "$RUN_ROOT/live-gefs-r1b" 2>&1 | tee "$RUN_ROOT/live-gefs-r1b.log" || exit 1
```

Do not compare only key digests. On GEFS and PDS, use DuckDB `EXCEPT ALL` in both directions across
all columns to prove full-row multiset equality between each serial arm and the default arm:

```bash
duckdb -c "
WITH serial AS (
  SELECT * FROM read_parquet('$RUN_ROOT/live-gefs-r1a/data/*.parquet', union_by_name=true)
), candidate AS (
  SELECT * FROM read_parquet('$RUN_ROOT/live-gefs-default/data/*.parquet', union_by_name=true)
), mismatch AS (
  (SELECT * FROM serial EXCEPT ALL SELECT * FROM candidate)
  UNION ALL
  (SELECT * FROM candidate EXCEPT ALL SELECT * FROM serial)
)
SELECT count(*) AS full_row_mismatches FROM mismatch;"
```

The result must be zero. Repeat with `live-gefs-r1b`, then with the corresponding PDS paths. Also
check the physical output sequence independently rather than assuming a successful token walk proves
ordering:

```bash
duckdb -c "
WITH physical AS (
  SELECT key,
         lag(key) OVER (ORDER BY filename, file_row_number) AS previous_key
  FROM read_parquet(
    '$RUN_ROOT/live-gefs-default/data/*.parquet',
    filename=true,
    file_row_number=true,
    union_by_name=true
  )
)
SELECT count(*) AS descending_key_transitions
FROM physical
WHERE previous_key > key;"
```

The result must be zero for every serial/default GEFS and PDS output. Start the replay server with
each output under `--serving-mode sorted`; successful startup validates the global `file_index` /
`file_final` completeness proof and serving eligibility. A replay `bench` full token walk adds a
termination/page/count check, but it is not an equality or ordering oracle. MRMS mutates, so
cross-arm equality and raw object-count equality are invalid; require each arm independently to
pass the physical-order query and start successfully in sorted serving mode.

For MRMS, score normalized rates from each arm's `_swath_summary.json`, not raw wall time:

```text
merge_input_rate = $.sort.segment_bytes / ($.sort.merge_ms / 1000)
merge_object_rate = $.objects / ($.sort.merge_ms / 1000)
normalized_merge_speedup = candidate merge_input_rate / mean(serial-A, serial-B merge_input_rate)
normalized_end_to_end_ratio = candidate $.efficiency.keys_per_sec
                              / mean(serial-A, serial-B $.efficiency.keys_per_sec)
```

Require `normalized_merge_speedup >= 2.0`, candidate `merge_object_rate` at least `2.0x` the serial
bracket mean, and `normalized_end_to_end_ratio >= 0.95`. Keep raw MRMS walls descriptive only.

Accept live S3 only if:

- all nine runs exit 0 and publish `_SUCCESS`, with no orphaned range files or `_staging` directory;
- GEFS and PDS are full-row equal across serial/default arms;
- the default arm actually engages eight ranges on PDS and MRMS, with no FD, cascade,
  unsplittable, cancellation, timeout, or cleanup signal;
- default merge wall is never slower than the mean of its two serial brackets on immutable GEFS and
  PDS; PDS shows at least `2.0x` merge-phase speedup, and no immutable bucket regresses end-to-end
  wall by more than 5%; MRMS meets the normalized thresholds above;
- boundary sampling is at most 10% of parallel merge wall on PDS and MRMS;
- default peak heap is no more than the larger of `1.15x` the serial-bracket mean or that mean plus
  512 MiB, and remains below 80% of `-Xmx`;
- default `efficiency.peak_rss_bytes` is no more than the larger of `1.15x` the serial-bracket mean
  or that mean plus 1 GiB, and remains below 16 GiB; fresh JVMs make this process peak attributable
  per arm; and
- the two serial brackets are within 10% in merge wall on GEFS/PDS. If they are not, the host or
  bucket was not stable enough to score; repeat rather than selecting the favorable baseline.

## 6. Decision and evidence bundle

Keep the raw logs, XML, JSON summaries, replay reports, full-row comparison output, `lscpu`, JDK,
disk, fd limit, exact candidate SHA, and exact commands together. Add only a redacted result summary
to the PR; never commit bucket captures or scratch output.

Approve parallel merge as the default only when all validation layers pass. A correctness mismatch,
cleanup/resource failure, default arm that silently clamps on the reference host, or repeatable
end-to-end regression is a blocker. A speedup below the thresholds is not a correctness bug, but it
means the default-on cost/benefit claim has not been established; keep the feature opt-in and retain
the measurements for a later tuning pass.
