# Parallel merge default-on validation gate

Status: **required before approving the default-on decision**.

This is the minimum credible manual gate for the shipped parallel-merge default. It answers three
questions:

1. Does the default produce exactly the same sorted rows as the serial merge?
2. Is its merge speedup real on a representative production-scale listing?
3. Does default-on avoid a material heap, RSS, file-descriptor, or end-to-end regression?

The required experiment is one immutable public S3 bucket, `pds-css-archive` (about 96 million
objects and normally about 16 staging segments), run sequentially as **serial A → shipped default →
serial B**. On the designated 32-core host it is large enough to engage the shipped eight-range
default, while remaining practical to compare row-for-row.

This is a **manual operator runbook, not CI**. Never put live S3 in GitHub Actions, never give CI AWS
credentials for it, and never add this gate to `build`, PR checks, deep tests, or integration tests.
The candidate SHA's ordinary automated gates must already be green; this runbook does not duplicate
them.

Do **not** run this gate on the slower development box. Use the designated fast remote host or a
faster dedicated equivalent: GCP `c4a-highcpu-32` (32 physical arm64 cores, no SMT), at least
62 GiB RAM, JDK 25, and `ulimit -n 65536`. A local SSD is preferred, but a sufficiently provisioned
dedicated filesystem is acceptable when its storage type is stamped as a caveat. All three arms
must use that same filesystem. Keep the host idle, run one arm at a time, and do not bypass swath's
disk guard.

## 1. Freeze the candidate and environment

Use one clean candidate commit and fresh JVMs for all arms. Keep artifacts outside the repository.
Run these blocks in one Bash session:

```bash
cd /path/to/swath
set -euo pipefail

export SWATH_REPO=$PWD
# Select an existing absolute directory on the dedicated filesystem stamped below.
export SCRATCH_BASE=/absolute/path/on/chosen-filesystem
case "$SCRATCH_BASE" in
  /*) ;;
  *) printf 'SCRATCH_BASE must be an absolute path\n' >&2; false ;;
esac
test -d "$SCRATCH_BASE"
export RUN_ROOT
RUN_ROOT=$(mktemp -d "$SCRATCH_BASE/swath-parallel-default.XXXXXX")
export SWATH="$SWATH_REPO/swath-cli/build/install/swath/bin/swath"
export REPLAY="$SWATH_REPO/swath-replay-server/build/install/swath-replay-server/bin/swath-replay-server"

# This public-bucket gate must not inherit credentials, profiles, endpoints, or
# container/web-identity providers. Empty files suppress the shared-file chain.
export AWS_SHARED_CREDENTIALS_FILE="$RUN_ROOT/empty-aws-credentials"
export AWS_CONFIG_FILE="$RUN_ROOT/empty-aws-config"
: >"$AWS_SHARED_CREDENTIALS_FILE"
: >"$AWS_CONFIG_FILE"
chmod 600 "$AWS_SHARED_CREDENTIALS_FILE" "$AWS_CONFIG_FILE"
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_SECURITY_TOKEN
unset AWS_ACCESS_KEY AWS_SECRET_KEY AWS_PROFILE AWS_DEFAULT_PROFILE
unset AWS_WEB_IDENTITY_TOKEN_FILE AWS_ROLE_ARN
unset AWS_CONTAINER_CREDENTIALS_RELATIVE_URI AWS_CONTAINER_CREDENTIALS_FULL_URI
unset AWS_CONTAINER_AUTHORIZATION_TOKEN AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3
export AWS_EC2_METADATA_DISABLED=true

# Each arm supplies its one intentional JVM option source explicitly below.
unset JAVA_TOOL_OPTIONS JAVA_OPTS SWATH_OPTS _JAVA_OPTIONS JDK_JAVA_OPTIONS

# Capture the exact expanded commands without mixing tracing into swath's logs.
exec 3>"$RUN_ROOT/commands.xtrace"
export BASH_XTRACEFD=3
export PS4='+ ${BASH_SOURCE}:${LINENO}: '
set -x

git status --porcelain | tee "$RUN_ROOT/git-status.txt"
test ! -s "$RUN_ROOT/git-status.txt"
git rev-parse HEAD | tee "$RUN_ROOT/candidate-sha.txt"
java -version 2>&1 | tee "$RUN_ROOT/java.txt"
lscpu | tee "$RUN_ROOT/lscpu.txt"
free -h | tee "$RUN_ROOT/memory.txt"
df -h "$SCRATCH_BASE" | tee "$RUN_ROOT/disk.txt"
findmnt -T "$SCRATCH_BASE" -o SOURCE,FSTYPE,OPTIONS,TARGET | tee "$RUN_ROOT/storage-filesystem.txt"
lsblk -o NAME,TYPE,SIZE,ROTA,TRAN,MODEL,MOUNTPOINTS | tee "$RUN_ROOT/storage-devices.txt"
ulimit -n 65536
ulimit -n | tee "$RUN_ROOT/ulimit.txt"

# Blocking host preflight. Manual confirmations are recorded because "idle" and
# "dedicated" cannot be inferred reliably from a one-shot command.
read -r -p 'Confirm the host is idle (type yes): ' host_idle
test "$host_idle" = yes
printf '%s\n' "$host_idle" >"$RUN_ROOT/host-idle-confirmed.txt"

read -r -p 'Confirm all arms use this dedicated filesystem (type yes): ' dedicated_fs
test "$dedicated_fs" = yes
printf '%s\n' "$dedicated_fs" >"$RUN_ROOT/dedicated-filesystem-confirmed.txt"

read -r -p 'Describe the storage type/device for the evidence stamp: ' storage_type_note
test -n "$storage_type_note"
printf '%s\n' "$storage_type_note" >"$RUN_ROOT/storage-type-note.txt"

java_major=$(java -XshowSettings:properties -version 2>&1 \
  | awk -F'= ' '/java.specification.version =/ { print $2; exit }')
test "$java_major" = 25

arch=$(uname -m)
physical_cores=$(lscpu -p=CORE,SOCKET \
  | awk -F, '!/^#/ { print $1 "," $2 }' | sort -u | wc -l)
threads_per_core=$(lscpu -p=CPU,CORE \
  | awk -F, '!/^#/ { count[$2]++ } END { for (core in count) if (count[core] > max) max=count[core]; print max }')
if [ "$arch" != aarch64 ] || [ "$physical_cores" -lt 32 ] || [ "$threads_per_core" -ne 1 ]; then
  read -r -p 'Document the approved faster-equivalent host: ' host_equivalence_note
  test -n "$host_equivalence_note"
else
  host_equivalence_note='designated baseline: arm64, >=32 physical cores, no SMT'
fi
printf '%s\n' "$host_equivalence_note" >"$RUN_ROOT/host-equivalence-note.txt"

memory_kib=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)
test "$memory_kib" -ge $((62 * 1024 * 1024))
test "$(ulimit -n)" -eq 65536

# 32 GiB covers three complete PDS outputs plus one arm's observed staging and
# disk-guard headroom. A larger known output requires a correspondingly larger reservation.
required_free_kib=$((32 * 1024 * 1024))
available_kib=$(df -Pk "$SCRATCH_BASE" | awk 'NR == 2 { print $4 }')
test "$available_kib" -ge "$required_free_kib"
printf 'required_free_kib=%s available_kib=%s\n' \
  "$required_free_kib" "$available_kib" >"$RUN_ROOT/storage-preflight.txt"

./gradlew :swath-cli:installDist :swath-replay-server:installDist --no-daemon
find swath-cli/build/install/swath/lib \
     swath-replay-server/build/install/swath-replay-server/lib \
     -type f -print0 | sort -z | xargs -0 sha256sum >"$RUN_ROOT/artifacts.sha256"
```

Required fixed settings are `-Xmx12g`, default LZ4 page-run staging, default merge budget, default
fan-in, default 256 MiB parallel floor, `--concurrency 256`, anonymous S3 access, and distinct fresh
destinations. The only arm difference is `swath.sort.merge-parallelism=1` on the two serial brackets;
the default arm must leave the property absent. Do not substitute an explicit `R=8`: proving that
the processor-derived shipped default actually selects eight ranges is part of the gate.

## 2. Run serial A → shipped default → serial B

The helper samples the Java process's open descriptors once per second. The installed launch script
uses `exec`, so the background PID remains the JVM PID. It requires at least one successful sample;
an unreadable `/proc/$pid/fd` while the client is still alive fails the arm without writing a numeric
score. Shell exit or interruption kills and reaps both the client and sampler. Every arm must exit
zero, publish `_SUCCESS` and a completed summary, and leave no staging or temporary range files.

```bash
run_arm() {
  local label=$1
  local merge_property=$2
  local output="$RUN_ROOT/$label"
  local log="$RUN_ROOT/$label.log"
  local fd_sample="$RUN_ROOT/$label.peak-open-fds.txt"
  local fd_sampler_log="$RUN_ROOT/$label.fd-sampler.log"
  local leftovers="$RUN_ROOT/$label.leftovers.txt"
  local pid='' sampler_pid='' status=0 sampler_status=0
  local java_opts='-Xmx12g'

  cleanup_arm() {
    trap - EXIT INT TERM
    if [ -n "$pid" ]; then
      kill "$pid" 2>/dev/null || true
    fi
    if [ -n "$sampler_pid" ]; then
      kill "$sampler_pid" 2>/dev/null || true
    fi
    if [ -n "$pid" ]; then
      wait "$pid" 2>/dev/null || true
      pid=''
    fi
    if [ -n "$sampler_pid" ]; then
      wait "$sampler_pid" 2>/dev/null || true
      sampler_pid=''
    fi
  }

  trap 'exit_status=$?; cleanup_arm; exit "$exit_status"' EXIT
  trap 'cleanup_arm; exit 130' INT
  trap 'cleanup_arm; exit 143' TERM

  if [ -n "$merge_property" ]; then
    java_opts="$java_opts $merge_property"
  fi

  printf 'label=%q JAVA_TOOL_OPTIONS=%q output=%q\n' \
    "$label" "$java_opts" "$output" >>"$RUN_ROOT/invocations.txt"

  JAVA_TOOL_OPTIONS="$java_opts" "$SWATH" list \
    s3://pds-css-archive --region us-west-2 --no-sign-request \
    --format parquet --sort --concurrency 256 --restart -v --progress-interval 30s \
    -o "$output" >"$log" 2>&1 &
  pid=$!

  rm -f "$fd_sample" "$fd_sampler_log"
  (
    samples=0
    current_fds=0
    peak_fds=0
    while kill -0 "$pid" 2>/dev/null; do
      if current_fds=$(find "/proc/$pid/fd" -mindepth 1 -maxdepth 1 \
          -printf '.\n' 2>>"$fd_sampler_log" | wc -l); then
        samples=$((samples + 1))
      elif kill -0 "$pid" 2>/dev/null; then
        printf 'FD sampling failed while swath PID %s was alive; terminating the client\n' \
          "$pid" >>"$fd_sampler_log"
        kill -TERM "$pid" 2>>"$fd_sampler_log" || true
        exit 1
      else
        # The process exited between the loop probe and the /proc read.
        break
      fi
      if [ "$current_fds" -gt "$peak_fds" ]; then
        peak_fds=$current_fds
      fi
      sleep 1
    done
    if [ "$samples" -eq 0 ]; then
      printf 'FD sampling produced no successful samples for swath PID %s\n' \
        "$pid" >>"$fd_sampler_log"
      exit 1
    fi
    printf '%s\n' "$peak_fds" >"$fd_sample"
  ) &
  sampler_pid=$!

  if wait "$pid"; then
    status=0
  else
    status=$?
  fi
  pid=''
  if wait "$sampler_pid"; then
    sampler_status=0
  else
    sampler_status=$?
  fi
  sampler_pid=''

  if [ "$sampler_status" -ne 0 ]; then
    tail -n 100 "$fd_sampler_log"
    cleanup_arm
    return "$sampler_status"
  fi
  if [ "$status" -ne 0 ]; then
    tail -n 100 "$log"
    cleanup_arm
    return "$status"
  fi
  test -s "$fd_sample"
  test -f "$output/_SUCCESS"
  test -f "$output/_swath_summary.json"
  jq -e '.completed == true and .exit_code == 0' \
    "$output/_swath_summary.json" >/dev/null
  test ! -e "$output/_staging"
  find "$output" -type f \
    \( -name '*.pageseg' -o -name '*.prange.tmp' -o -name '*.tmp' \) \
    -print >"$leftovers"
  test ! -s "$leftovers"
  trap - EXIT INT TERM
}

run_arm live-pds-serial-a '-Dswath.sort.merge-parallelism=1'
run_arm live-pds-default ''
run_arm live-pds-serial-b '-Dswath.sort.merge-parallelism=1'
```

If any arm fails, stop. Do not keep the successful arms and rerun only the failed position: that
breaks the sequential bracket. Fix the host or candidate, choose a new `$RUN_ROOT`, and rerun all
three.

## 3. Prove engagement, stability, and resource safety

First bind the three summaries and define a reason-counter reader:

The engagement assertion requires exactly eight `merge_range_parallel` events and zero events for
`merge_range_below_staged_floor`, `merge_range_fd_limited`, `merge_range_fd_exhausted`,
`merge_range_would_cascade`, `merge_range_unsplittable`, `merge_cascade_predicted`,
`merge_fanin_clamped`, `merge_fanin_fd_clamped`, `merge_fanin_mem_clamped`, and
`merge_pass_cascaded`. The log scan accepts only `rg` status 1 (no matches); a match or an inspection
error fails the gate.

```bash
export SERIAL_A="$RUN_ROOT/live-pds-serial-a/_swath_summary.json"
export DEFAULT="$RUN_ROOT/live-pds-default/_swath_summary.json"
export SERIAL_B="$RUN_ROOT/live-pds-serial-b/_swath_summary.json"

reason_count() {
  local summary=$1
  local reason=$2
  jq -r --arg reason "$reason" '
    [.meters[]?
      | select(.name == "swath.steal_reason")
      | select(.tags.outcome == "SORT" and .tags.reason == $reason)
      | (.value // 0)]
    | add // 0' "$summary"
}

test "$(reason_count "$DEFAULT" merge_range_parallel)" -eq 8
for reason in \
  merge_range_below_staged_floor merge_range_fd_limited merge_range_fd_exhausted \
  merge_range_would_cascade merge_range_unsplittable merge_cascade_predicted \
  merge_fanin_clamped merge_fanin_fd_clamped merge_fanin_mem_clamped merge_pass_cascaded; do
  test "$(reason_count "$DEFAULT" "$reason")" -eq 0
done
log_scan_status=0
rg -n 'ERROR|Exception|Too many open files|EMFILE|sort_merge_range_(failed|cancelled)' \
  "$RUN_ROOT/live-pds-default.log" || log_scan_status=$?
test "$log_scan_status" -eq 1
```

Score only a stable bracket. All three object counts must match. The two serial merge walls must be
within 10% of their mean, and the two serial `session_duration_ms` values must independently be
within 10% of their mean. The default must achieve at least `2.0x` merge speedup against the serial
merge mean and take no more than `1.05x` the serial-session mean end-to-end wall.

The retained resource thresholds are:

- peak heap below 80% of `-Xmx12g`;
- peak RSS no more than the larger of `1.15x` the serial-bracket mean or mean + 1 GiB, and below
  16 GiB;
- sampled peak open descriptors no more than the serial-bracket mean + 256, and below 1024 (the
  +256 allowance deliberately covers eight ranges over roughly 16 segments); and
- boundary sampling no more than 10% of the default merge wall.

The score records the serial/default heap values and their delta as descriptive evidence. A
serial-relative heap increase is not by itself a gate failure; only the 80%-of-Xmx heap ceiling is
a release criterion.

The following emits the scored values and fails if any threshold is missed:

```bash
jq -se '
  all(.[];
    ([.efficiency.peak_heap_bytes, .efficiency.peak_rss_bytes]
      | all(.[]; type == "number")))
' "$SERIAL_A" "$DEFAULT" "$SERIAL_B" >/dev/null

jq -se '
  length == 3 and all(.[]; type == "number" and . > 0)
' \
  "$RUN_ROOT/live-pds-serial-a.peak-open-fds.txt" \
  "$RUN_ROOT/live-pds-default.peak-open-fds.txt" \
  "$RUN_ROOT/live-pds-serial-b.peak-open-fds.txt" >/dev/null

jq -n \
  --slurpfile a "$SERIAL_A" --slurpfile d "$DEFAULT" --slurpfile b "$SERIAL_B" \
  --argjson fd_a "$(cat "$RUN_ROOT/live-pds-serial-a.peak-open-fds.txt")" \
  --argjson fd_d "$(cat "$RUN_ROOT/live-pds-default.peak-open-fds.txt")" \
  --argjson fd_b "$(cat "$RUN_ROOT/live-pds-serial-b.peak-open-fds.txt")" '
  def mean($x; $y): ($x + $y) / 2;
  def abs: if . < 0 then -. else . end;
  def maximum($x; $y): if $x > $y then $x else $y end;
  ($a[0]) as $a0 | ($d[0]) as $d0 | ($b[0]) as $b0 |
  mean($a0.sort.merge_ms; $b0.sort.merge_ms) as $serial_merge |
  mean($a0.session_duration_ms; $b0.session_duration_ms) as $serial_session |
  mean($a0.efficiency.peak_heap_bytes; $b0.efficiency.peak_heap_bytes) as $serial_heap |
  mean($a0.efficiency.peak_rss_bytes; $b0.efficiency.peak_rss_bytes) as $serial_rss |
  mean($fd_a; $fd_b) as $serial_fds |
  {
    objects_equal: ($a0.objects == $d0.objects and $d0.objects == $b0.objects),
    serial_merge_spread: ((($a0.sort.merge_ms - $b0.sort.merge_ms) | abs) / $serial_merge),
    serial_session_spread: ((($a0.session_duration_ms - $b0.session_duration_ms) | abs) / $serial_session),
    merge_speedup: ($serial_merge / $d0.sort.merge_ms),
    serial_session_mean_ms: $serial_session,
    default_session_duration_ms: $d0.session_duration_ms,
    end_to_end_ratio: ($d0.session_duration_ms / $serial_session),
    boundary_share: ($d0.sort.merge_boundaries_ms / $d0.sort.merge_ms),
    serial_peak_heap_mean_bytes: $serial_heap,
    default_peak_heap_bytes: $d0.efficiency.peak_heap_bytes,
    default_minus_serial_heap_mean_bytes: ($d0.efficiency.peak_heap_bytes - $serial_heap),
    heap_hard_limit_bytes: (12 * 1024 * 1024 * 1024 * 0.80),
    default_peak_rss_bytes: $d0.efficiency.peak_rss_bytes,
    rss_limit_bytes: maximum($serial_rss * 1.15; $serial_rss + 1073741824),
    default_peak_open_fds: $fd_d,
    fd_limit_from_bracket: ($serial_fds + 256),
    pass: (
      $a0.objects == $d0.objects and $d0.objects == $b0.objects and
      ((($a0.sort.merge_ms - $b0.sort.merge_ms) | abs) / $serial_merge) <= 0.10 and
      ((($a0.session_duration_ms - $b0.session_duration_ms) | abs) / $serial_session) <= 0.10 and
      ($serial_merge / $d0.sort.merge_ms) >= 2.0 and
      ($d0.session_duration_ms / $serial_session) <= 1.05 and
      ($d0.sort.merge_boundaries_ms / $d0.sort.merge_ms) <= 0.10 and
      $d0.efficiency.peak_heap_bytes < (12 * 1024 * 1024 * 1024 * 0.80) and
      $d0.efficiency.peak_rss_bytes <= maximum($serial_rss * 1.15; $serial_rss + 1073741824) and
      $d0.efficiency.peak_rss_bytes < (16 * 1024 * 1024 * 1024) and
      $fd_d <= ($serial_fds + 256) and $fd_d < 1024
    )
  }' | tee "$RUN_ROOT/gate-score.json"

jq -e '.pass == true' "$RUN_ROOT/gate-score.json" >/dev/null
```

## 4. Compare the output contracts

DuckDB is used here **only as an independent Parquet output comparator**. Do not run DuckDB replay
serving mode, serving-mode timing, serial/default serving brackets, or a DuckDB token walk as part
of this gate.

Run one bidirectional, all-column `EXCEPT ALL` comparison of the default against serial A. Serial B
is intentionally only the timing-drift control, not a second output-comparison arm. A digest or row
count is not a substitute:

```bash
full_row_mismatches=$(duckdb -csv -noheader -c "
WITH serial AS (
  SELECT * FROM read_parquet(
    '$RUN_ROOT/live-pds-serial-a/data/*.parquet', union_by_name=true)
), candidate AS (
  SELECT * FROM read_parquet(
    '$RUN_ROOT/live-pds-default/data/*.parquet', union_by_name=true)
), mismatch AS (
  (SELECT * FROM serial EXCEPT ALL SELECT * FROM candidate)
  UNION ALL
  (SELECT * FROM candidate EXCEPT ALL SELECT * FROM serial)
)
SELECT count(*) FROM mismatch;")
printf '%s\n' "$full_row_mismatches" | tee "$RUN_ROOT/full-row-mismatches.txt"
test "$full_row_mismatches" -eq 0
```

Check physical key order on the default output only:

```bash
descending_key_transitions=$(duckdb -csv -noheader -c "
WITH physical AS (
  SELECT key,
         lag(key) OVER (ORDER BY filename, file_row_number) AS previous_key
  FROM read_parquet(
    '$RUN_ROOT/live-pds-default/data/*.parquet',
    filename=true,
    file_row_number=true,
    union_by_name=true
  )
)
SELECT count(*) FROM physical WHERE previous_key > key;")
printf '%s\n' "$descending_key_transitions" | tee "$RUN_ROOT/descending-transitions.txt"
test "$descending_key_transitions" -eq 0
```

Finally, perform one bounded sorted-serving startup/health check of the default output. This checks
the global `file_index` / `file_final` completeness proof and sorted-serving eligibility; it is not
a replay benchmark or full token walk. Every readiness request has explicit connect and total
timeouts:

```bash
JAVA_TOOL_OPTIONS='-Xmx4g' "$REPLAY" serve \
  --fixture "$RUN_ROOT/live-pds-default/data" --bucket pds-default-check \
  --host 127.0.0.1 --port 19090 --serving-mode sorted \
  >"$RUN_ROOT/sorted-serving-startup.log" 2>&1 &
server_pid=$!
ready=0

cleanup_server() {
  kill "$server_pid" 2>/dev/null || true
  wait "$server_pid" 2>/dev/null || true
}
trap cleanup_server EXIT

for attempt in $(seq 1 30); do
  if curl --connect-timeout 1 --max-time 2 -fsS \
    'http://127.0.0.1:19090/pds-default-check?list-type=2&max-keys=1&encoding-type=url' \
    >/dev/null; then
    ready=1
    break
  fi
  kill -0 "$server_pid" 2>/dev/null || break
  sleep 1
done

test "$ready" -eq 1
cleanup_server
trap - EXIT
```

## 5. Decision and evidence bundle

The gate passes only when every command and every threshold above passes. Preserve together:

- exact candidate SHA, clean-status record, expanded command trace, JDK/CPU/memory/disk/fd facts,
  and installed-artifact hashes;
- all three logs, output datasets, `_swath_summary.json` files, and sampled peak-FD files;
- `gate-score.json`, the full-row mismatch result, the physical-order result, and the sorted-serving
  startup log.

Add only a redacted result summary to the PR. Never commit bucket output or scratch artifacts. Any
row mismatch, physical-order failure, incomplete publication, staging residue, default clamp,
cascade/failure signal, unstable serial bracket, missed speed/resource threshold, or failed sorted
startup blocks default-on approval.

## Optional diagnostics — non-blocking

Run an optional leg only to investigate a **named question**, with owner approval. Optional legs
must never be automatically expanded into this default gate:

- `noaa-mrms-pds` (~823 M objects): large-scale soak/resource question;
- replay: deterministic reproduction of an observed failure;
- synthetic `R=1,2,4,8` sweep: range-scaling or clamp diagnosis; and
- DuckDB replay serving mode: backend-specific serving investigation.

None is required evidence for this focused default-on decision, and none may replace the live PDS
serial/default bracket or its full-row comparison.
