# Performance

This is the operator guide for diagnosing and sizing a real run. It also records the
project's current field evidence. The figures are observations from one machine and mostly
one run per point—not a portable performance promise or a release-candidate benchmark.

For a first listing, use [Getting started](getting-started.md). Return here after a
representative run has produced `_swath_summary.json`.

### Three diagnostic recipes

1. **Listing-bound vs. output-bound.** Rerun with `--format discard --checkpoint none
   --report discard.json` and compare `keys_per_sec` and `duration_ms` against the
   original report. A similar rate with output removed points at the listing path; a
   large gain points at the output sink. See [Find the limiting
   stage](#find-the-limiting-stage).
2. **Underfilled concurrency.** Compare `engine.avg_in_flight` against the configured
   `--concurrency` ceiling, and check `engine.splits`, steal counts, and probe reasons.
   Low in-flight utilization with idle splits/probes means another stage is limiting
   throughput, not the concurrency ceiling. See [In-flight
   utilization](#in-flight-utilization).
3. **Sorted-finalization bound.** Compare `pipeline_router_wait_ms`,
   `pipeline_plan_queue_wait_ms`, and `pipeline_encoder_read_wait_ms`, and check cascade
   pass count and encoder clamp/floor reasons. High router wait points at segment
   production (header scan or cascade passes) rather than the encoders; high plan-queue
   wait means the encoders cannot drain plans as fast as the router produces them, so
   more encoder parallelism (if heap and descriptors allow) is the lever. See [The
   sorted merge](#the-sorted-merge).

<a id="diagnosing-a-run"></a>

## Start with your own run

Run with a JSON report and compare like with like:

```bash
swath list s3://bucket/prefix/ --format parquet -o out/ --report run.json
```

Bucket shape, client location, output disk, filters, CPU architecture, and service load
all affect the result. Absolute numbers from another bucket are rarely useful; the
relationships among your own report fields are.

### In-flight utilization

`--concurrency N` is a ceiling. The achieved request concurrency is
`engine.avg_in_flight`:

```text
utilization = engine.avg_in_flight / configured concurrency
```

Use the configured ceiling, not `peak_in_flight`. High utilization means the ceiling may
be the bottleneck. Low utilization means useful ranges, CPU, or another stage could not
keep it full; raising the ceiling usually adds overhead.

For a sorted run, the report's whole-run average includes the post-listing merge, where
API in-flight is zero. Rescale it to the listing phase with:

```text
listing avg in-flight = avg_in_flight * duration_ms / listing_duration_ms
listing keys/s = (objects - recovered_objects) / (listing_duration_ms / 1000)
```

The [run-summary reference](metrics-and-observability.md#2-list_run_summary-one-line-at-run-end)
defines those clocks and resume fields.

### Find the limiting stage

Little's law gives a useful approximation:

```text
listing throughput ≈ average in-flight * page size / request latency
```

Across a concurrency sweep:

- Rising latency with rising in-flight suggests remote, network, client CPU, or queue
  saturation. Check `fetch.latency.phase`, throttling and adaptive-concurrency counters,
  connection-pool waits, and CPU.
- Flat request latency together with plateaus in in-flight requests and split count
  suggests work-supply starvation. Check
  `engine.splits`, steals, probe reasons, tail occupancy, and the shape block.
- High `queue.wait`, Parquet latency, or checkpoint waits identify a local downstream stage
  rather than the object store.
- The combination of falling utilization, falling throughput, and rising CPU per key is a
  sign of an overshot ceiling, regardless of which resource ran out first.

Treat `shape.divergence_depth_histogram`, `mass_skew_gini`, and `delimiter_fanout` as clues,
not diagnoses. Direct engagement and wait counters show what actually constrained the run.

To isolate output cost, repeat the same run with the diagnostic discard sink:

```bash
swath list s3://bucket/prefix/ --format discard --checkpoint none --report discard.json
```

Discard still performs S3 response parsing, filtering, checkpoint coordination, and row
counting; it only removes formatting and output I/O. Compare it with an otherwise identical
TSV or Parquet run. When using a replay server, retain its metrics too so a server bottleneck
is not mistaken for a client limit.

For unsorted Parquet or a parallel TSV/JSONL directory dataset, each batch is assigned
to one background writer, called a lane. Read the report's `dataset_writer` block with
`client_cost`:

- `submit_blocked_ms` is time spent waiting because the selected writer's queue was full.
  It is already included in `client_cost[].emit`; do not add it again.
- `head_of_line_blocked_ms` is the blocked time for waits that began while another writer
  had an empty queue and was waiting for work. A material value shows that assigning each
  batch to a fixed writer left capacity unused. Zero means that condition was not observed
  when a wait began.
- Per-lane rows, finalized bytes, batches, active elapsed time, queue peak, and finalize activity
  show load imbalance. `active_elapsed_ms` is elapsed service time and can overlap across lanes.
- If `submit_blocked_ms` rises and `client_cost[].writer_backpressure` follows, output is slowing
  the listing workers. A queue reaching capacity without measurable blocked time is not a
  bottleneck by itself.
- If `writer_backpressure` is high while `submit_blocked_ms` stays near zero and lanes are mostly
  idle, the workers are waiting on the consumer stage itself, not on the writers. Read
  `client_cost[].channel_receive` and `emit` per page: together they are the consumer's whole
  per-page cost, and its reciprocal is the page rate the run can reach regardless of writer count.

Do not infer writer capacity from an I/O-bound run whose lanes are mostly idle. Validate a writer
count against the fastest expected listing regime. Sustained submit blocking means output is
the bottleneck. Material head-of-line blocking means writer assignment is leaving capacity
unused. If neither appears, raising the writer count would only add memory and file-part
overhead for that workload.

Unsorted managed Parquet directory output uses `--tune parquet.writers=N`; 2–4 is the
tested range. Counts 5–64 require enough JVM heap to pass the safety check described in
the contracts. Directory TSV/JSONL uses `--text-writers N`, also in the range 2–64. Both
limit the total queued work to 256 batches. Confirm `writer_count`,
`total_queue_capacity`, rotation settings, and the Parquet memory-plan fields in
`dataset_writer` before comparing runs.

More writers do not guarantee more throughput. Because the total queue budget is fixed,
each writer gets fewer queue slots. More writers can also create more small parts, and every
part adds close, checkpoint, and digest work. For text, compare otherwise identical runs
with 2, 3, and 4 writers first; keep unsorted Parquet within its tested 2–4 range. Adopt a
higher count only when throughput improves without disproportionate growth in part count,
finalization time, `submit_blocked_ms`, or `head_of_line_blocked_ms`.

<a id="writeback-shaping"></a>

### Writeback shaping for large dataset parts

`--writeback-size SIZE` is an off-by-default experiment for TSV/JSONL/Parquet directory datasets,
including sorted Parquet final files.
It periodically forces physical bytes already emitted to each open part while keeping its format
writer and final part open. It exists to test whether bounding the dirty-page backlog
reduces the final close stall enough to support large published parts efficiently.

The cadence policy and pool hooks are format-neutral. Text, direct Parquet, and sorted-final Parquet provide narrow
transport adapters: text does not flush its compression codec, while Parquet flushes only the
bottom 4 KiB transport buffer after parquet-mr has naturally completed a row group. It never asks
Parquet to flush a row group, page, or column store, so row-group geometry and file boundaries do
not change. Sorted PageRun staging, cascade intermediates, and single-file output are not wired to
this option; the measured staging path keeps its existing strict seal-order close barrier.

This option has **no crash-recovery benefit**. A text dataset is still non-resumable, direct
Parquet still advances its checkpoint only at finalized-part boundaries, and a sorted final remains
rebuildable rather than authoritative until its durable close and publish sequence. I6 is unchanged: rows
become durable and publishable only after the part is finalized and its full
file-plus-parent barrier succeeds. A periodic force does not write a compression trailer, manifest,
Parquet footer, checkpoint record, or `_SUCCESS` marker. Positive values below `4mb` are rejected.
If row/time rotation is disabled to obtain size-only Parquet parts, the checkpoint RPO becomes the
time needed to fill/finalize that larger part; periodic writeback does not reduce it.

Benchmark it against an otherwise identical disabled arm. For a size-only comparison, explicitly
pass `--part-rotation-interval 0 --part-rotation-max-rows 0`; otherwise the default 30-second or
2-million-row trigger may rotate before the requested part size. Compare wall time,
`swath.data_sync.latency`, `.bytes`, `.residual.bytes`, format finalize latency, part count, and exact
manifest row/MD5 totals. If writeback cost does not recover the large-part throughput loss, leave it
disabled rather than treating a smaller residual as a throughput win by itself.

A local replay gate measured direct Parquet at 1.516M rows/s with 32-MiB writeback
versus 1.440M rows/s disabled across five matched runs, without a median CPU or RSS
increase. This result is host- and filesystem-specific; writeback remains off by default.

### Size CPU and memory empirically

CPU cost per million keys is:

```text
cpu_seconds / (objects / 1,000,000)
```

Measure it with your actual key lengths, filters, format, and CPU. Throughput cannot exceed
either the CPU budget or the request-latency/concurrency budget. Add headroom because
coordination cost rises with core and concurrency count.

Active page, queue, writer, and merge buffers are configuration-bounded, but the complete
process is not strictly constant-memory: unsorted Parquet retains `O(parts)` metadata and
sorted output retains `O(segments)` metadata. Larger part/segment targets reduce those
counts. The public PERF-2 gate covers 100,000 keys and requires peak heap below 1 GiB under
its default Parquet fixture; it does not establish a billion-object memory envelope.

For production sizing, sweep realistic concurrency under an explicit `-Xmx`, watch peak
heap/RSS and disk, and keep the setting below the point where utilization collapses.

<a id="retain-a-jfr-cpu-profile"></a>

### Advanced: retain a JFR CPU profile

Elapsed timers show how long work took, but not how much CPU it consumed. For CPU
attribution on Linux with JDK 25, create a Java Flight Recorder (JFR) configuration that
enables CPU-time sampling. The stock `profile.jfc` enables execution sampling but leaves
`jdk.CPUTimeSample` disabled:

```bash
jfr configure --input "$JAVA_HOME/lib/jfr/profile.jfc" --output swath-cpu-profile.jfc \
  jdk.CPUTimeSample#enabled=true
JAVA_OPTS="-XX:StartFlightRecording=filename=$PWD/swath.jfr,settings=$PWD/swath-cpu-profile.jfc,disk=true,dumponexit=true,maxsize=2g" \
  swath list s3://bucket/prefix/ --format parquet -o out/ --report run.json
```

The application launcher honors `JAVA_OPTS`. For the runnable jar or container, pass the
same option through `JAVA_TOOL_OPTIONS` or directly to `java`. `dumponexit=true` retains
the recording after a normal exit, handled signal, or uncaught Java failure. It cannot run
after `SIGKILL`, host loss, or a deliberate `Runtime.halt()`. When investigating a process
that may be forcibly halted, dump the recording first with
`jcmd <pid> JFR.dump filename=...`. Give each comparison run a different filename and keep
its JFR recording beside its JSON report.

Useful first commands are:

```bash
jfr summary swath.jfr
jfr view cpu-time-hot-methods swath.jfr
jfr view thread-cpu-load swath.jfr
jfr view gc-cpu-time swath.jfr
jfr view file-writes-by-path swath.jfr
jfr view jdk.CPUTimeSamplesLost swath.jfr
```

`cpu-time-hot-methods` uses `jdk.CPUTimeSample`. If that event is unavailable, use the
stock `profile` configuration and `jfr view hot-methods` instead, and describe the result
as execution samples rather than measured CPU time. Check `jdk.CPUTimeSamplesLost` before
comparing runs. If losses are material, adjust `jdk.CPUTimeSample#throttle` and rerun each
configuration with identical profiler settings; more frequent sampling can add overhead.

Attribute samples by both thread and stack. Unsorted Parquet writers are named
`parquet-writer-*`, and the checkpoint writer is `swath-checkpoint-writer`. Listing workers
are virtual threads, so group them by `io.varve.swath.engine`, `io.varve.swath.store`, and
AWS SDK frames. Sorted work appears under `io.varve.swath.sort` frames and `*-encoder-*`
platform threads. Oracle's
[JDK 25 troubleshooting guide](https://docs.oracle.com/en/java/javase/25/troubleshoot/troubleshooting-guide.pdf)
reports less than 2% overhead for most profiling recordings, but measure the overhead on
your workload and use the same settings for every comparison.

## Current field observations

Unless a table says otherwise, these runs used:

| Item | Value |
| --- | --- |
| Host | GCP `c4a-highcpu-32`, `us-east1-b` |
| CPU | Google Axion / ARM Neoverse-V2, 32 physical cores, no SMT |
| Memory | 62 GiB |
| Disk | 200 GB Hyperdisk Balanced persistent disk, ext4 |
| JDK / swath | Temurin 25.0.3+9 / 0.2.2-dev |
| Dates | 2026-08-07–08 |

The client was cross-cloud. For `noaa-gefs-retrospective` in AWS `us-east-1`, network RTT
was 17.9 ms and worker-page p50 total latency 119.5 ms. For `pds-css-archive` in AWS
`us-west-2`, RTT was 79.9 ms and p50 total 172.0 ms. Moving in-region would reduce the
concurrency needed for a given request rate. Sorted staging used network-attached storage,
not local SSD.

### Unsorted concurrency and memory

One unsorted Parquet sweep against `pds-css-archive` (96,022,559 objects) produced:

| `--concurrency` | keys/s | avg in-flight (utilization) | CPU-s/Mkey | peak heap | peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 165,831 | 29.9 (93%) | 7.42 | 0.92 GB | 1.27 GB |
| 64 | 327,275 | 56.9 (89%) | 7.28 | 2.24 GB | 2.51 GB |
| 128 | 511,429 | 89.0 (70%) | 7.51 | 3.24 GB | 3.52 GB |
| 256 | 655,346 | 118.4 (46%) | 8.50 | 4.80 GB | 4.92 GB |
| 512 | 519,286 | 93.2 (18%) | 11.06 | 5.25 GB | 5.66 GB |

The peak in this sweep was 655,346 keys/s at 256—not a global ceiling or recommended
default. At 512, utilization collapsed and CPU cost rose while request latency stayed near
175 ms. Split counts had plateaued around 4,900, so extra slots had little useful work.

Across the measured buckets, API calls per 1,000 objects were 1.016–1.089 against the 1.0
floor for full 1,000-key pages. That is encouraging evidence for the tested shapes, not a
claim of shape independence. There is no fixed-shape object-count sweep yet.

A separate local-replay experiment, with 100 ms injected request latency and 1–8 client
cores, observed roughly 118,000–136,000 keys/s per busy core for objects-mode Parquet.
CPU cost rose about 7–8% from one/two cores to eight. Derive an equivalent value on your
hardware rather than importing this ARM-specific constant.

The 32-concurrency run stayed below 1 GiB heap. Every run used the same 96-million-object
input, but the higher-concurrency runs used more heap. That supports the buffer-sizing
design, but it is one host, one bucket, one run per point, and unsorted output only.

### Local replay tuning takeaways

Controlled replay runs reinforce the sizing procedure above: keep the default concurrency ceiling
for an unknown endpoint, and raise it only when repeated matched runs show that more in-flight work
still buys throughput. Higher ceilings can add queueing, latency, CPU, memory, and connections after
keys/s has plateaued. The adaptive-concurrency controller reacts to explicit store stress, but it
does not automatically lower a healthy but unnecessarily high limit to find the most efficient
setting. Higher-latency endpoints may still need a larger ceiling, so no single replay
result is a universal S3 setting.

For TSV/JSONL directory output, compare the default with otherwise identical runs using
2 and 4 writers before trying expert counts. If output is the suspected bottleneck,
compare discard, a memory-backed filesystem, and the production destination to separate
listing, encoding, and storage costs. Part size is also storage-specific: compare 64,
128, and 256 MiB while watching file-finalization time, part count, submit blocking, and
head-of-line blocking. Keep the 256 MiB default unless measurements on the destination
justify an override.

## The sorted merge

Sorted output finalizes durable page-run segments through one reference-routing pipeline. The
configured `sort.merge-parallelism` value is the maximum encoder count:

```text
max(1, min(8, availableProcessors / 2))
```

The runtime can lower that count when heap or file-descriptor admission cannot support it. One
encoder is a useful baseline, but it still uses the same header cursors, router, calibrated part
plans, positional reads, and publisher:

```bash
swath list s3://bucket/prefix --format parquet --sort -o out/ \
  --tune sort.merge-parallelism=1
```

Encoder count and output-part count are independent. `final-file-bytes` is a soft target.
The router first builds one conservative plan, waits for its completed Parquet size, and sizes every
later plan from that part's encoded-to-logical ratio, so the same input and configuration produce
the same parts at any encoder count. It rolls only between complete pages or
transitive overlap components and never divides an equal-key group. The admitted
256–16,384-reference plan cap can create an earlier roll, and an overlap component wider than
that cap takes a part of its own after spilling its page references to staging.

When the segment catalog exceeds admitted cascade fan-in, `CascadePageMerger` writes bounded
page-run intermediates first. A whole-page cascade decision avoids the row heap; overlapping page
bounds use the shared bounded row merger. The post-cascade catalog is then admitted for final
encoders using its exact persisted record, decoded-payload, and key maxima.

Use the JSON `sort` block to identify the limiting stage:

| Signal | Interpretation |
| --- | --- |
| high `pipeline_header_scan_ms` | metadata reads or many surviving segments dominate; compare with cascade passes |
| high `pipeline_router_wait_ms`, low queue-wait subset | the router is waiting for segment cursor output |
| high `pipeline_plan_queue_wait_ms` | encoders cannot drain complete plans as quickly as the router produces them |
| high `pipeline_encoder_read_wait_ms` | positional frame reads and CRC/page parsing dominate encoder service |
| high `pipeline_cluster_pages` / `pipeline_cluster_rows` | page bounds overlap enough to require row merging |
| `pipeline_decoded_page_bytes_peak` near the admitted encoder lane share, or near the cascade decoded-page budget when `passes > 1` | decoded-page residency is tight in the corresponding path |
| encoder clamp or floor engagement reasons | heap or descriptor admission bound concurrency |
| cascade reasons and multiple `passes` | the source catalog did not fit one cascade pass |

Service timers sum work across concurrent cursors or encoders and can exceed merge wall time. Use
their corresponding shares as service-to-wall ratios, not percentages that must stay below one.

Benchmark `1`, the core-derived default, and at most one higher candidate on the same retained
corpus. Record merge wall, peak RSS, admitted encoders, plan-queue wait, encoder-read service,
cascade passes, output part count, and exact output checks. More encoders can improve CPU and I/O
overlap, consume more writer and reference memory, or add no value when the router or storage is
already binding.

### Diagnostic zero-LIST merge replay

This procedure isolates finalization from listing by deliberately re-entering a completed
diagnostic run. It is not an ordinary recovery operation and must not be used against a production
dataset.

Create the retained corpus once:

```bash
RUN=/tmp/swath-merge-corpus
HASHES=/tmp/swath-merge-corpus.pageseg.sha256

swath list s3://<bucket>/<prefix> --format parquet -o "$RUN" --sort \
  --tune sort.keep-staging=on

find "$RUN/_staging" -maxdepth 1 -type f -name '*.pageseg' -print0 \
  | sort -z | xargs -0 -r sha256sum > "$HASHES"
```

For each sequential benchmark arm, remove only the completion marker and resume with retention
enabled. Save the report before the next arm because publication replaces the prior finals and
report:

```bash
ARM=n1
ENCODERS=1
rm -- "$RUN/_SUCCESS"
swath resume "$RUN" \
  --tune sort.keep-staging=on \
  --tune sort.merge-parallelism="$ENCODERS"

sha256sum -c "$HASHES"
test "$(jq -r '.cost.api_calls' "$RUN/_swath_summary.json")" = 0
mkdir -p "/tmp/swath-merge-$ARM"
cp "$RUN/manifest.json" "$RUN/.swath-state.json" "$RUN/symlink.txt" \
  "$RUN/_swath_summary.json" "/tmp/swath-merge-$ARM/"
```

Hash the retained page runs after every arm. Compare manifest rows and sizes, final-part checksums,
the ordered-row result, admitted encoder count, merge wall, and peak RSS. A zero API-call count
confirms only that the invocation finalized retained staging without listing; it does not make the
corpus a general object-store replay or reproduce remote timing.

For a controlled local sweep, the opt-in `ParallelMergeBenchmark` runs the production
`SortedDatasetCoordinator`, brackets candidate encoder counts with three one-encoder samples, reverses
candidate order, applies a variance gate, and verifies every result against an independent source
oracle:

```bash
./gradlew :swath-core:test \
  --tests 'io.varve.swath.sort.finalize.ParallelMergeBenchmark' \
  -Dswath.bench=on -Pperf \
  -Dswath.bench.encoders=1,4,8 \
  -Dswath.bench.staging-dir=/path/to/_staging
```

The harness fully reads and CRC-validates its corpus before measurement, so its results are
warm-cache measurements. Its `BENCH_ROW`, `BENCH_VARIANCE`, and `BENCH_SPEEDUP` lines carry
the requested and admitted encoder counts, finalization meters, corpus identity, and output
fingerprint.

## Resume cost

No stamped resume-cost measurement exists yet. The implementation bounds replay using the
durable cursor, but this page makes no empirical claim about checkpoint-open time, re-listed
pages, or resume overhead.

## Known slow paths

- Deep-divergence keyspaces can make each useful split expensive, leaving a high
  concurrency ceiling under-filled.
- Aggressive concurrency can increase scheduling, allocation, and connection churn while
  producing no more work.
- Unsorted Parquet with very small part targets grows retained part metadata and makes the one
  terminal `O(parts)` manifest larger; per-part finalization/checkpoint/digest overhead can still
  dominate even though the manifest is no longer rewritten per part.
- Sorted output depends on staging disk, heap, and descriptor headroom. Constrained runs reduce
  cascade fan-in or encoder count, and refuse resumably when the one-encoder floor cannot fit.
- Remote throttling reduces the adaptive concurrency target and may dominate a run even
  when the engine can supply work.

## Methodology and missing evidence

Current observations are mostly n=1, from one ARM host and one cross-cloud vantage. Live
buckets can mutate between comparison runs. Runs were executed one at a time and read from their
JSON reports.

Raise this evidence to release-candidate quality with repeated runs and reported variance,
an in-region client, an x86 comparison, a fixed-shape object-count sweep, and a documented
large-scale resume measurement. Until then, use the tables to understand the diagnostic
method, not as an advertised envelope.

The separate [S3-listing comparison study](https://github.com/varveio/s3-listing-study)
publishes a methodology and tool roster; it does not currently publish comparative results.

No publishable production scaling result exists yet for the current v4 finalization pipeline.
Use the one-encoder brackets, variance gate, exact-output checks, and provenance fields above before
drawing a local conclusion.
