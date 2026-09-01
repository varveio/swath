# Full-scale public demonstration

This page reproduces the large public listing shown in the README. It is evidence that
swath can list a very large public bucket and resume it after an interruption; it is not
the recommended first command.

Start with [Getting started](getting-started.md) if you have not yet completed the small
public example.

## What the recording shows

The embedded README recording is run `noaa-gestofs-pds-2026-08-03-505ae26`, captured on
2026-08-03 with **swath v0.2.1** (`505ae26e6019`) against the full public bucket
`s3://noaa-gestofs-pds/`, with `--region us-east-1` and `--concurrency 128`.

It is two invocations: a `swath list` stopped by a signal, then the `swath resume` that
finished the dataset. The completed dataset holds 39,585,029 objects, confirmed in the
recording by a DuckDB count over its Parquet parts. The resume invocation reported:

- 41,582 `ListObjectsV2` attempts, including probes and retries;
- 22 Parquet parts totaling 790.0 MB;
- a peak of about 1.7 GB of resident memory; and
- 1m36s elapsed for that invocation. swath prints an elapsed time per invocation, so this
  is not a wall clock for the whole capture, and no such clock was recorded.

The interrupted first attempt reported 573,741 objects and 635 attempts of its own. The
recording does not say whether the resume invocation's totals already include them, so
neither figure is a whole-capture request count.

No run report or decision trace was retained for this capture, and the client machine,
its provider, and its region are not recorded anywhere.
[The run facts](../site/data/runs/noaa-gestofs-pds-2026-08-03-505ae26.json) list what is
known and what is not.

## The interactive trace is a separate capture

The [interactive run trace](https://swath.varve.io/runs/noaa-gestofs-pds/) visualizes run
`noaa-gestofs-pds-field-guide-trace`, a different capture of the same bucket. Read the two
as separate runs:

- it listed 39,651,850 objects, 66,821 more than the recording above;
- it began with 513 initial ranges, one of which held 68.0% of the objects the run
  returned;
- it made 2,399 splits and completed all 2,912 ranges it claimed, with none failed; and
- the report generated from its trace records a 68,592.2 ms span — about 1m 09s — between
  the trace's first and last event. The trace file itself was not kept and no run summary
  survives, so that span is the only listing duration available for this capture.

That report counts 41,420 **committed listing pages** in the trace. It is a different
metric from the 41,582 **`ListObjectsV2` attempts** the recording's resume invocation
reported: committed pages exclude probes and retries, and the two counts belong to
different runs in any case. This capture retains no API-attempt count, swath version,
commit, capture date, or command; see
[its run facts](../site/data/runs/noaa-gestofs-pds-field-guide-trace.json).

That visualization is where the skew is visible: it shows the capture's seed guesses,
live ranges, split pivots, and long tail as swath discovers the key-distribution
imbalance while listing and splits the unscanned remainder of busy ranges so idle workers
can help.

Neither capture is a portable benchmark or a promise for the current release. The bucket
continues to change, object-store latency varies, and client CPU, memory, disk, region,
and network path all affect the result.

## Before you run it

This command scans the entire public bucket. Review
[request cost](operating.md#request-cost) first. The floor is about one committed listing
page per 1,000 returned keys, and each of those pages normally costs one successful
request; probes, retries, sparse pages, and interrupted tails add further attempts.

The recording's resume invocation reported 790.0 MB written for the bucket state it
captured; your run's output size will differ as the bucket changes. The command uses more
working memory than the small getting-started example. Keep the resulting
`_swath_summary.json`; it records the actual request count, duration, throughput, and
resource evidence for your run.

## Run the full listing

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/ \
  --no-sign-request --region us-east-1 \
  --concurrency 128 \
  --format parquet -o /out/noaa-gestofs-pds
```

`--concurrency 128` is part of the recorded demonstration, not a universal
recommendation. It is an adaptive ceiling: swath starts below it, raises the live target
while the endpoint is healthy, and reduces it under service backpressure. For your own
bucket, begin with the default and increase the ceiling only when repeated comparable
runs still gain throughput. See [Choosing concurrency](configuration.md#choosing-concurrency).

## Interrupt and resume

Stop the active container with Ctrl+C. The process exits after closing what it can
safely close; finalized Parquet parts remain durable and the checkpoint stays under the
output directory.

Resume with the same mounted directory:

```bash
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  resume /out/noaa-gestofs-pds
```

swath discards any unfinished part, re-lists only the non-durable tail, and continues from
the saved range ownership. A deterministic error can recur after resume, so read the
terminal error and `_swath_summary.json` rather than treating every interrupted run as
automatically recoverable.

## Query the result

With DuckDB installed:

```bash
duckdb -c "
  SELECT count(*) AS objects
  FROM read_parquet('out/noaa-gestofs-pds/data/*.parquet')
"
```

Or use DuckDB's official container:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace duckdb/duckdb \
  -c "SELECT count(*) AS objects
      FROM read_parquet('out/noaa-gestofs-pds/data/*.parquet')"
```

There is no required compaction step. The default result is not globally ordered because
parallel workers finish independently. Use `--sort` only when a downstream consumer
requires global key order and after sizing both staging and final-output disk.

## Interpret your run

Compare the result with the recording only after accounting for:

- the current bucket object count and key distribution;
- client region and network latency to S3;
- available CPU and JVM heap;
- output filesystem performance;
- configured concurrency; and
- swath version and commit.

Use the relationships inside your own run report before relying on absolute numbers from
another machine. The [performance guide](performance.md) explains request utilization,
work-supply starvation, output backpressure, and sorted-merge sizing.

## Related material

- [Interactive trace of run `noaa-gestofs-pds-field-guide-trace`](https://swath.varve.io/runs/noaa-gestofs-pds/)
  — a separate capture of the same bucket
- Run facts:
  [`noaa-gestofs-pds-2026-08-03-505ae26`](../site/data/runs/noaa-gestofs-pds-2026-08-03-505ae26.json)
  and
  [`noaa-gestofs-pds-field-guide-trace`](../site/data/runs/noaa-gestofs-pds-field-guide-trace.json)
- [Visual field guide](https://swath.varve.io/field-guide/)
- [Getting started](getting-started.md)
- [Operating swath and request cost](operating.md)
- [Performance and resource sizing](performance.md)
