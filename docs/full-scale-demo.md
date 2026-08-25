# Full-scale public demonstration

This page reproduces the large public listing shown in the README. It is evidence that
swath can recover from severe key-distribution skew and resume a long run; it is not the
recommended first command.

Start with [Getting started](getting-started.md) if you have not yet completed the small
public example.

## What the recording shows

The embedded README recording was captured with **swath v0.2.1** against the full public bucket
`s3://noaa-gestofs-pds/` in `us-east-1`.

That observed run:

- listed 39,585,029 objects;
- made 41,582 `ListObjectsV2` calls;
- wrote 790.8 MB of Parquet;
- peaked at about 1.7 GB of resident memory; and
- began with 513 seed ranges, one of which contained 68% of the objects later found.

swath discovered the imbalance while listing and split busy remaining ranges so idle
workers could help. The [interactive run trace](https://swath.varve.io/runs/noaa-gestofs-pds/)
shows the seed guesses, live ranges, split pivots, and long-tail behavior.

These figures describe one recorded run, not a portable benchmark or a promise for the
current release. The bucket continues to change, object-store latency varies, and client
CPU, memory, disk, region, and network path all affect the result.

## Before you run it

This command scans the entire public bucket. Review
[request cost](operating.md#request-cost) first. The ideal page count is approximately
one request per 1,000 returned keys, but probes, retries, sparse pages, and interrupted
tails add overhead.

The command writes roughly a gigabyte of output for the recorded bucket state and uses
more working memory than the small getting-started example. Keep the resulting
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

- [Interactive trace for the recorded NOAA run](https://swath.varve.io/runs/noaa-gestofs-pds/)
- [Visual field guide](https://swath.varve.io/field-guide/)
- [Getting started](getting-started.md)
- [Operating swath and request cost](operating.md)
- [Performance and resource sizing](performance.md)
