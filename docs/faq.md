# Troubleshooting and FAQ

## Does swath read or modify object contents?

No. swath calls `ListObjectsV2` and writes the selected local output. It does not call
`GetObject`, modify objects, delete objects, or change bucket configuration.

The normal IAM permission is bucket-level `s3:ListBucket`.

## Is the result a point-in-time snapshot?

No. swath publishes the complete result of the live listing it performed, but S3 does not
provide one transaction across a long scan. Objects added, removed, or renamed during the
run can affect which live state the result observes.

`_SUCCESS` means the listing result was completely published. It does not turn a changing
bucket into a historical snapshot.

## Should I use S3 Inventory or S3 Metadata instead?

Yes, when a fresh table already exists and you can query it. Reading a precomputed
inventory is normally cheaper than any live `ListObjectsV2` scan.

Use swath when the inventory is missing, stale, inaccessible, or too slow to arrive and a
serial live listing is too slow.

## How much will a run cost?

The ideal page count is approximately one LIST request per 1,000 returned keys, plus
probes, retries, sparse pages, and any unfinished tail re-listed after interruption.

swath reports the actual request count. Calculate the bill with the provider's current
price rather than an evergreen number in the documentation. See
[Request cost](operating.md#request-cost).

## Do I need Java?

Not when using the Docker image; it includes the required runtime.

The runnable JAR, application archive, and source build require JDK 25. Shipped artifacts
do not require `--enable-preview`.

## Which object stores are supported?

AWS S3 general-purpose buckets are the supported backend.

LocalStack and MinIO are used in compatibility and integration work, but any
S3-compatible endpoint must provide globally ordered listings and correct `StartAfter`
semantics. GCS through its XML API is experimental compatibility, not a native backend.
S3 directory buckets are not supported.

See [Operating swath](operating.md#s3-compatible-endpoints).

## Can every output resume?

No. Durable resume is a managed Parquet directory feature.

Stdout, text files, TSV/JSONL directory datasets, the diagnostic discard sink, and the
legacy `.parquet`-looking one-writer layout are one-shot outputs.

## No AWS region

Pass `--region`, set `AWS_REGION` or `AWS_DEFAULT_REGION`, or configure a region in the
selected AWS profile.

swath fails before opening a checkpoint or sending a request when the SDK chain cannot
resolve a region.

## Credentials could not be loaded

For a public bucket, add `--no-sign-request`.

For a private bucket, verify the same profile, environment, web identity, container role,
or instance role with the AWS CLI, then pass `--profile` if needed.

A container does not inherit host credentials unless you pass variables or mount the
required files. See [Credentials in Docker](operating.md#credentials-in-docker).

## Access denied

The caller normally needs bucket-level `s3:ListBucket` on:

```text
arn:aws:s3:::bucket
```

This is a bucket permission, not an object permission on `arn:aws:s3:::bucket/*`.
Requester-pays buckets also require `--requester-pays requester`.

See [Least-privilege IAM](operating.md#least-privilege-iam).

## Docker cannot write the output directory

The published image runs as non-root UID 10001. On Linux or macOS, run the container as
the current user and mount the host output directory:

```bash
mkdir -p out
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/ \
  --format parquet -o /out/my-inventory
```

The output path must be inside the mounted directory.

On Windows PowerShell, omit `--user "$(id -u):$(id -g)"` and use `${PWD}` for the mount.
See the [platform notes](getting-started.md#prerequisites).

## The run was interrupted

If the output is a managed Parquet directory, resume by that directory:

```bash
swath resume out/
```

swath retains finalized parts, removes an unfinished part, and continues after the last
durable cursor.

Exit codes 74, 75, 124, 130, and 143 imply resumable state only when a managed output
directory exists. A deterministic error can recur after resume, so read the terminal
error and `_swath_summary.json`.

## The output directory is refused

swath does not silently mix or delete unrelated data. A directory can be refused because
it is already complete, contains another run identity, has an unfinished checkpoint, is
a symlinked managed path, or lacks durable evidence that swath owns it.

- Resume an interrupted matching run with `swath resume out/`.
- Replace a completed result with a fresh `list ... -o out/ --overwrite`.
- Discard an unfinished checkpoint and start again with `--restart`.
- For a foreign directory or symlink refusal, choose a clean destination.

Do not edit the checkpoint or create files that imitate swath's ownership markers.

## A `.parquet` output path became a directory

The current pre-1.0 CLI retains a legacy compatibility behavior: a destination such as
`-o inventory.parquet` selects a one-writer, non-resumable directory layout under a path
that looks like a file. It does not create one physical Parquet file.

Use a directory path instead:

```bash
swath list s3://my-bucket/ --format parquet -o inventory/
```

Then query `inventory/data/*.parquet`. See [Managed Parquet](usage.md#managed-parquet).

## Sorted output reports insufficient disk

During the final merge, compressed staging data and final Parquet coexist. The simplest
recovery is to free space or expand the current volume in place, then resume.

Moving an interrupted run works only when the exact absolute output path recorded in the
checkpoint remains available, for example through the same mount or bind path. Otherwise
swath refuses the moved run and a fresh listing is required.

`--tune sort.ignore-disk-check=on` bypasses the guard. Use it only after sizing the volume
independently. See [Sorted output](usage.md#sorted-output).

## The process used less concurrency than requested

`--concurrency` is a ceiling, not a fixed worker count. swath begins below it, increases
the live target after healthy windows, and reduces it under S3 backpressure.

A sparse or poorly divisible keyspace can also leave workers idle. Raising the ceiling
does not create useful ranges and can add CPU, memory, queueing, and connection overhead.
Start with [Performance](performance.md#start-with-your-own-run).

## A `%` key fails on an S3-compatible endpoint

Some implementations echo request cursors without the percent-encoding used by AWS S3.
The AWS SDK can then reject a lone or trailing `%` while decoding the response.

swath avoids synthesizing `%` in an internal pivot, but a real key or user prefix can
still expose the endpoint difference. See
[S3 implementation compatibility](internals/s3-implementation-compatibility.md).

## Why is the output not sorted?

Unsorted output is faster and is the default. Parallel ranges finish independently, so
part order is not key order.

Add `--sort` when a downstream consumer requires globally ordered Parquet, or sort in the
query engine afterward. See [Sorted output](usage.md#sorted-output).

## Where are all the options and exit codes?

Run:

```bash
swath list --help
swath resume --help
swath list --tune help
```

The stable exit-code table and output contracts are in
[Using swath](usage.md#exit-codes). Progress, report, and metric fields are in
[Metrics and observability](metrics-and-observability.md).

If none of the above resolves it, see
[Filing a support request](operating.md#filing-a-support-request) for what to include.
