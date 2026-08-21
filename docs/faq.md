# Troubleshooting and FAQ

## No AWS region

Pass `--region`, set `AWS_REGION` / `AWS_DEFAULT_REGION`, or configure a region in
the selected AWS profile. swath fails before opening a checkpoint when the SDK chain
cannot resolve one.

## Credentials could not be loaded

For a public bucket, add `--no-sign-request`. For a private bucket, verify the same
profile, environment, web identity, container role, or instance role with the AWS CLI,
then use `--profile` if needed. A container does not inherit host credentials unless you
pass or mount them; see [Credentials in Docker](operating.md#credentials-in-docker).

## Access denied

The caller needs bucket-level `s3:ListBucket` on `arn:aws:s3:::bucket`, not an object
permission on `arn:aws:s3:::bucket/*`. Requester-pays buckets also require
`--requester-pays requester`. See [Least-privilege IAM](operating.md#least-privilege-iam).

## Docker cannot write the output directory

The published image runs as non-root UID 10001. Run it as your current user:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest list s3://my-bucket/ --format parquet -o /out
```

The output path must be inside the mounted directory. See
[the platform notes in Getting started](getting-started.md) and
[Credentials in Docker](operating.md#credentials-in-docker).

## The run was interrupted

If the output is a managed Parquet dataset, resume by its output directory:

```bash
swath resume out/
```

Stdout and FILE-kind outputs are one-shot and cannot resume. Exit codes 74, 75, 124,
130, and 143 only imply resumable state when a managed output directory exists. See
[Checkpoint and resume](usage.md#checkpoint-and-resume).

## The output directory is refused

swath does not silently mix runs. A directory can be refused because it is already
complete, contains a different run identity, has an unfinished checkpoint, or contains a
managed path that is a symlink.

- Resume an interrupted matching run with `swath resume out/`.
- Replace a completed result with a fresh `list ... -o out/ --overwrite`.
- Discard an unfinished checkpoint and start again with `--restart`.
- For a foreign directory or a symlink refusal, choose a clean destination.

## Sorted output reports insufficient disk

During the final merge, compressed staging data and final Parquet coexist. Move the run
to a larger device only if you can mount or bind it at the exact absolute output path
recorded in the checkpoint; otherwise swath refuses the moved run. The simplest recovery
is to free space or expand the current volume in place, then resume. If the original path
cannot be preserved, start a fresh run at the new destination. The guard is conservative
because a new run cannot know its final object count before listing. Bypass it with
`--tune sort.ignore-disk-check=on` only after sizing independently. See
[Sorted output](usage.md#sorted-output).

## The process used more or less concurrency than requested

`--concurrency` is a ceiling, not a fixed worker count. swath starts below it, increases
after clean windows, and reduces the live target under S3 backpressure. A sparse or
poorly divisible keyspace can also leave workers idle. Start with
[Performance](performance.md#start-with-your-own-run) rather than raising the ceiling blindly.

## A `%` key fails on an S3-compatible endpoint

Some implementations echo request cursors without AWS S3's required percent-encoding.
The AWS SDK then rejects a lone or trailing `%` while decoding the response. swath never
synthesizes `%` in an internal pivot, but a real key or user prefix can still expose the
server deviation. See [S3 implementation compatibility](internals/s3-implementation-compatibility.md).

## Why is the output not sorted?

Unsorted is the faster default. Parallel ranges finish independently, so part order is
not key order. Add `--sort` when you need globally ordered Parquet, or sort with your
query engine afterward. See [Sorted output](usage.md#sorted-output).

## Why a JVM?

The engine uses JDK 25 virtual threads and final `ScopedValue` support to run many range
tasks with structured runtime context. The tradeoff is a JVM process and a heap to size,
in exchange for a concurrency substrate that does not require a callback-based engine.
Shipped artifacts use no preview features.

## Should I use S3 Inventory or S3 Metadata instead?

Yes, when a fresh table already exists and you can query it. Reading a precomputed
inventory is cheaper than any live `ListObjectsV2` scan. swath is for buckets where the
inventory is missing, stale, or controlled by somebody else.

## Where are all the flags and exit codes?

Run `swath list --help` for flags. The stable exit-code table and output contracts are in
[Using swath](usage.md#exit-codes); progress, report, and metric fields are in
[Metrics and observability](metrics-and-observability.md).
