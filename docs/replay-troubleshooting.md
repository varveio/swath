# Troubleshooting bucket-shape pathologies with replay

This is a contributor and performance-investigation workflow. It is not needed to install,
use, or operate an ordinary swath listing.

Use this loop when a listing collapses toward one worker, stalls at a repeatable region, or
spends many probes without producing splits. The replay server fixes the keyspace and
request-cost profile so an engine change can be evaluated without paying for repeated live
bucket scans.

Use it after the JSON report and aggregate metrics have identified a repeatable
key-distribution problem. The companion [replay toolkit reference](swath-replay.md)
documents the development tool itself.

## 1. Capture the evidence

Before stopping the live run, retain its JSON report, checkpoint, exact command, version,
and—if safe—trace. The useful signals are:

- `trajectory.serial_frac`, `collapse_at_frac`, and final workers;
- average/peak in-flight versus configured concurrency;
- steals, successful splits, probe reasons, and `efficiency.steal_success_rate`;
- `slow_ranges[]`, seed decisions, tail occupancy, and open-frontier share;
- `probe_latency[]` for each call class and `shape.delimiter_fanout`.

The failure signature matters more than the percentage where it begins. Cutting off a
healthy head changes that percentage while preserving a serial tail.

## 2. Prepare the smallest faithful fixture

Sorted swath output is ready to serve. Convert a legacy capture with
[`sort-fixture`](swath-replay.md#sort-a-legacy-capture-sort-fixture).

For a long run, use `slow_ranges[].lo`/`.hi` to retain only the affected prefixes, then
sort the subset again. A simple ASCII-prefix cut can use DuckDB:

```bash
duckdb -c "copy (
  select * from read_parquet('<fixture>/data/*.parquet')
  where cast(key as varchar) like '<prefix-1>%'
     or cast(key as varchar) like '<prefix-2>%'
  order by key
) to '<cut-src>/part-00000.parquet' (format parquet, compression zstd);"

swath-replay sort-fixture \
  --capture <cut-src> --output <cut-fixture>
```

The varchar predicate is only safe for text/ASCII prefixes. For arbitrary byte keys, use
blob bounds. Keep fixtures and reports outside the repository.

## 3. Calibrate request costs

Loopback is often too fast to reproduce a timeout-driven collapse. Derive a profile from
the failed run's `probe_latency` `total` phase:

```text
worker_page=<observed mean>
pivot_probe=<observed mean>
structure slope ≈ (structure mean - worker mean) / mean prefixes per probe
```

Serve the fixture with those values:

```bash
swath-replay serve \
  --fixture <cut-fixture> --bucket <bucket> \
  --host 127.0.0.1 --port 19090 --serving-mode sorted \
  --inject-latency 'worker_page=223ms,pivot_probe=121ms,structure_probe=223ms+55ms/cp' \
  --latency-jitter 0.15 --latency-scale 20
```

Compressed time preserves relative injected costs, not absolute wall time. Confirm any
timeout- or cadence-sensitive result with `--latency-scale 1`.

## 4. Reproduce the run

Use the original engine settings and a fresh output path:

```bash
swath list s3://<bucket> \
  --endpoint-url http://127.0.0.1:19090 \
  --force-path-style --no-sign-request \
  --format parquet -o <out> --sort \
  --concurrency 64 --progress-interval 10s --restart
```

Confirm the same mechanism, not merely a similar duration:

- in-flight falls while the target remains high;
- steals/API calls climb while committed splits flatten;
- the same key region remains in `slow_ranges` or the checkpoint;
- the same seed/pivot/gate reasons dominate;
- `serial_frac` and final workers show the same terminal shape.

The checkpoint can identify surviving ranges directly:

```sql
SELECT id, parent_id, hex(range_start) AS lo, hex(range_end) AS hi,
       hex(cursor) AS cursor, status, pages_emitted, api_calls
FROM listing_node
WHERE run_id = (SELECT id FROM run_meta ORDER BY id DESC LIMIT 1)
  AND status != 'COMPLETED';
```

Walking `parent_id` toward a seed node shows whether the tail began as one oversized seed
range or emerged later from ineffective splits.

## 5. Run a controlled A/B

Hold the server process, fixture, injection profile, CLI flags, JDK, and host load fixed.
Change one engine mechanism. Run arms sequentially, each with its own clean output/checkpoint,
and verify that each built distribution contains the intended revision.

Score completion and predeclared fields such as `serial_frac`, `collapse_at_frac`, final
workers, steal success, API calls, and the specific engagement counters the change should
affect. Do not select winning metrics after seeing the result. Repeat enough times to
separate scheduler noise from a systematic change.

Check the harness itself before trusting a margin: run `bench`, warm the JVM and filesystem,
and ensure uninjected server cost is small relative to injected delay. Do not run unrelated
clients or builds on the same server during an arm.

To isolate the sorted merge after the listing decision path has already been measured, use the
[diagnostic zero-LIST merge-replay recipe](performance.md#diagnostic-zero-list-merge-replay).
That path reuses retained checkpoint-finalized page runs and deliberately removes `_SUCCESS` to
force merge-only re-entry. It is a tested diagnostic benchmark seam, not a replay-server fixture,
not a production recovery instruction, and not evidence about LIST latency or engine scheduling.

## 6. Know what replay proves

Replay can show that a deterministic keyspace and cost shape trigger the mechanism, that a
policy change changes the relevant decisions, and that sorted serving matches the DuckDB
oracle. It does not automatically reproduce absolute S3 latency, distributed service
capacity, scheduler interleavings, throttles, connection resets, or malformed responses.

Use the right layer:

- replay latency injection for request-shape timing;
- `MockPageFetcher`/`PageInterceptor` tests for deterministic 503, timeout, token, and
  crash/resume paths;
- a transport proxy for TCP latency, resets, slicing, and bandwidth constraints;
- a final live run for environment-dependent conclusions.

Common experimental failures are an older JDK on `PATH`, cold-start timings, a prefetch
window smaller than a row group, two clients contending on one server, reusing an earlier
checkpoint, connecting to the wrong port, or killing the driver itself with a broad
`pkill -f` pattern. Record PIDs and explicit ports, warm before measuring, and retain the
full configuration with the result.
