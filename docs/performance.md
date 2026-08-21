# Performance

This is the operator guide for diagnosing and sizing a real run. It also records the
project's current field evidence. The figures are observations from one machine and mostly
one run per point—not a portable performance promise or a release-candidate benchmark.

For a first listing, use [Getting started](getting-started.md). Return here after a
representative run has produced `_swath_summary.json`.

<a id="diagnosing-a-run"></a>

## Start with your own run

Run with a JSON report and compare like with like:

```bash
swath list s3://bucket/prefix/ --format parquet -o out/ --report run.json
```

Bucket shape, client location, output disk, filters, CPU architecture, and service load
all affect the result. Absolute numbers from another bucket are rarely useful; the
relationships among your own report fields are.

### In-flight utilisation

`--concurrency N` is a ceiling. The achieved request concurrency is
`engine.avg_in_flight`:

```text
utilisation = engine.avg_in_flight / configured concurrency
```

Use the configured ceiling, not `peak_in_flight`. High utilisation means the ceiling may
be binding. Low utilisation means useful ranges, CPU, or another stage could not keep it
full; raising the ceiling usually adds overhead.

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
  saturation. Check `fetch.latency.phase`, throttle/AIMD counters, pool pending, and CPU.
- Flat latency while in-flight and splits plateau suggests work-supply starvation. Check
  `engine.splits`, steals, probe reasons, tail occupancy, and the shape block.
- High `queue.wait`, Parquet latency, or checkpoint waits identifies a local downstream
  stage rather than the object store.
- Falling utilisation, falling throughput, and rising CPU per key is the signature of an
  overshot ceiling, regardless of which resource ran out first.

Treat `shape.divergence_depth_histogram`, `mass_skew_gini`, and `delimiter_fanout` as clues,
not diagnoses. Direct engagement and wait counters show what actually constrained the run.

### Size CPU and memory empirically

CPU cost per million keys is:

```text
cpu_seconds / (objects / 1,000,000)
```

Measure it with your actual key lengths, filters, format, and CPU. Throughput cannot exceed
either the CPU budget or the request-latency/concurrency budget. Add headroom because
coordination cost rises with core and concurrency count.

Active page, queue, writer, and merge buffers are configuration-bounded, but the complete
process is not strictly constant-memory: direct Parquet retains `O(parts)` metadata and
sorted output retains `O(segments)` metadata. Larger part/segment targets reduce those
counts. The public PERF-2 gate covers 100,000 keys and requires peak heap below 1 GiB under
its default Parquet fixture; it does not establish a billion-object memory envelope.

For production sizing, sweep realistic concurrency under an explicit `-Xmx`, watch peak
heap/RSS and disk, and keep the setting below the point where utilisation collapses.

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

| `--concurrency` | keys/s | avg in-flight (utilisation) | CPU-s/Mkey | peak heap | peak RSS |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 165,831 | 29.9 (93%) | 7.42 | 0.92 GB | 1.27 GB |
| 64 | 327,275 | 56.9 (89%) | 7.28 | 2.24 GB | 2.51 GB |
| 128 | 511,429 | 89.0 (70%) | 7.51 | 3.24 GB | 3.52 GB |
| 256 | 655,346 | 118.4 (46%) | 8.50 | 4.80 GB | 4.92 GB |
| 512 | 519,286 | 93.2 (18%) | 11.06 | 5.25 GB | 5.66 GB |

The peak in this sweep was 655,346 keys/s at 256—not a global ceiling or recommended
default. At 512, utilisation collapsed and CPU cost rose while request latency stayed near
175 ms. Split counts had plateaued around 4,900, so extra slots had little useful work.

Across the measured buckets, API calls per 1,000 objects were 1.016–1.089 against the 1.0
floor for full 1,000-key pages. That is encouraging evidence for the tested shapes, not a
claim of shape independence. There is no fixed-shape object-count sweep yet.

A separate local-replay experiment, with 100 ms injected request latency and 1–8 client
cores, observed roughly 118,000–136,000 keys/s per busy core for objects-mode Parquet.
CPU cost rose about 7–8% from one/two cores to eight. Derive an equivalent value on your
hardware rather than importing this ARM-specific constant.

The 32-concurrency arm stayed below 1 GiB heap at 96 million objects, while smaller runs at
higher concurrency used more. That supports the buffer-sizing design, but it is one host,
one bucket, one run per point, and unsorted output only.

## The sorted merge

Sorted output stages packed runs, samples boundaries, then merges contiguous key ranges.
For at least 256 MiB of staged input, the configured maximum defaults to:

```text
max(1, min(8, availableProcessors / 2))
```

The runtime may lower it for segment count, heap, fan-in, or file descriptors. Fewer than
two effective ranges uses the serial path. Set
`-Dswath.sort.merge-parallelism=1` for an explicit serial comparison. See
[configuration](configuration.md#sorted-output-jvm-properties) and
[using sorted output](usage.md#sorted-output).

The final PR #99 campaign ran serial A → default → serial B at fixed `-Xmx12g` and
`--concurrency 256`; serial values below are the bracket mean.

| Bucket | Objects | Segments | Serial merge | Default merge (`R=8`) | Merge speedup | Serial session | Default session |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `noaa-gefs-retrospective` | 9,915,173 | 2 | 15.1 s | 5.4 s | 2.81× | 26.0 s | 16.9 s |
| `pds-css-archive` | 96,022,559 | 16 | 139.5 s | 38.4 s | 3.63× | 300.5 s | 194.9 s |
| `noaa-mrms-pds` | 823.70–823.72 M | 128 | 1,123.6 s | 282.8 s | 3.97× | 1,564.6 s | 723.2 s |

The tested SHA was `2bd24c2f33df35341a497a91e24e7633a224b941`. The focused
`pds-css-archive` gate had zero bidirectional full-row `EXCEPT ALL` differences against
serial A, zero descending physical-key transitions, eight effective ranges, and no clamp,
cascade, or failure reason. Peak heap was 3.598 GiB of `-Xmx12g`.

GEFS and MRMS corroborate scale only. MRMS mutated across arms and its physical order was
not independently checked. Merge-object rate is not listing throughput; do not combine the
campaign's phase rates with the unsorted sweep.

The remaining serial fractions are boundary sampling, small/resource-limited fallbacks,
and final publication. A serial fraction becomes more visible as listing gets faster, so
judge merge share against the machine and storage, not object count alone.

## Resume cost

No stamped resume-cost measurement exists yet. The implementation bounds replay using the
durable cursor, but this page makes no empirical claim about checkpoint-open time, re-listed
pages, or resume overhead.

## Known slow paths

- Deep-divergence keyspaces can make each useful split expensive, leaving a high
  concurrency ceiling under-filled.
- Aggressive concurrency can increase scheduling, allocation, and connection churn while
  producing no more work.
- Direct Parquet with very small part targets grows manifest and part metadata; manifest
  rewrites are cumulative `O(parts²)` serialization work.
- Sorted output depends on staging disk, heap, and descriptor headroom. Small or constrained
  runs fall back to serial merge.
- Remote throttling reduces the AIMD target and may dominate a run even when the engine can
  supply work.

## Methodology and missing evidence

Current observations are mostly n=1, from one ARM host and one cross-cloud vantage. Live
buckets can mutate between arms. Runs were serial and read from their JSON reports; the
merge comparison bracketed the default arm with two serial arms on the same idle filesystem.

Raise this evidence to release-candidate quality with repeated runs and reported variance,
an in-region client, an x86 comparison, a fixed-shape object-count sweep, and a documented
large-scale resume measurement. Until then, use the tables to understand the diagnostic
method, not as an advertised envelope.

The separate [S3-listing comparison study](https://github.com/varveio/s3-listing-study)
publishes a methodology and tool roster; it does not currently publish comparative results.
