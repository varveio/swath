# Getting started

This guide gets from a Docker image to a small queryable Parquet inventory. The public
example lists one historical day from NOAA's `noaa-gestofs-pds` bucket, so it is a quick
functional check rather than the 39.6-million-object demonstration shown in the README.

The commands below are written against swath **0.3.2**. Match your installed version with
[step 1](#1-check-the-cli), and adjust the image tag if you are running a different
release.

swath reads object metadata only. It never downloads or modifies object contents. A run is
a live listing, not a point-in-time snapshot of a bucket that changes while the scan is in
progress.

## Prerequisites

The commands below use Docker with Linux-container support. They use a Bash-compatible
shell on Linux or macOS.

On Windows PowerShell:

- create the output directory with `New-Item -ItemType Directory -Force out`;
- replace `$PWD` with `${PWD}`; and
- omit `--user "$(id -u):$(id -g)"`, because Docker Desktop makes the bind mount writable.

To use the runnable JAR or application archive instead, choose one in
[Installation](install.md), remove the Docker options, and run the `swath` arguments
directly.

## 1. Check the CLI

```bash
docker run --rm ghcr.io/varveio/swath:0.3.2 --version
```

This prints the swath version and exits without contacting object storage. The examples
below pin the `0.3.2` release tag; for reproducible automation, prefer the immutable
digest published with that release instead of a mutable tag.

## 2. Stream a few public rows

The following command lists one historical NOAA day and stops after the downstream
`head` process has read five rows:

```bash
docker run --rm ghcr.io/varveio/swath:0.3.2 \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --no-sign-request --region us-east-1 \
  --format tsv |
  head -n 5
```

The command is anonymous and needs no AWS credentials. Closing the pipe is treated as a
successful downstream stop, not as a failed listing.

Output is tab-separated, one object per line: `key`, `size`, `last_modified`, `etag`,
`storage_class`, and `row_type`. The shape looks like this (illustrative; your exact
keys, sizes, and timestamps will differ):

```text
key	size	last_modified	etag	storage_class	row_type
stofs_2d_glo.20230113/stofs_2d_glo.t00z.fields.cwl.nc	123456789	2023-01-13T02:00:00Z	a1b2c3d4e5f6	STANDARD	OBJECT
stofs_2d_glo.20230113/stofs_2d_glo.t00z.points.cwl.nc	12345678	2023-01-13T02:05:00Z	f6e5d4c3b2a1	STANDARD	OBJECT
```

If this exact public command fails, follow the reported network, region, or Docker symptom
in [Troubleshooting and FAQ](faq.md). Do not add credentials to the public example.

## 3. Create a managed Parquet dataset

Create a host directory, mount it into the container, and write the listing beneath it:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:0.3.2 \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ \
  --no-sign-request --region us-east-1 \
  --format parquet -o /out/stofs-20230113
```

A **managed Parquet dataset** is a directory of Parquet parts plus swath's manifest,
completion marker, run report, and temporary resume state. Use a directory path such as
`/out/stofs-20230113`, not a filename ending in `.parquet`.

When the command completes, the host directory contains:

```text
out/stofs-20230113/
  data/                   Parquet part files
  manifest.json           files, row counts, checksums, and dataset metadata
  .swath-state.json       internal run identity
  _swath_summary.json     machine-readable run report
  _SUCCESS                written last: the listing result is complete
  symlink.txt             part paths for engines that use a manifest list
```

Do not consume a directory dataset until `_SUCCESS` exists. During a resumable run,
swath also keeps `<output>/.swath/checkpoint.sqlite`; the live checkpoint is removed after
successful publication.

## 4. Query it with DuckDB

With the DuckDB CLI installed:

```bash
duckdb -c "
  SELECT count(*) AS objects
  FROM read_parquet('out/stofs-20230113/data/*.parquet')
"

duckdb -c "
  SELECT key, size, last_modified
  FROM read_parquet('out/stofs-20230113/data/*.parquet')
  LIMIT 5
"
```

Or use DuckDB's official container, so the entire walkthrough still needs only Docker:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace duckdb/duckdb \
  -c "SELECT count(*) AS objects
      FROM read_parquet('out/stofs-20230113/data/*.parquet')"
```

The default dataset is not globally ordered. Parallel ranges can finish in any order.
Add `--sort` only when a downstream consumer requires global key order; sorted output
needs temporary disk and a final merge. Read [Sorted output](usage.md#sorted-output)
before using it on a large bucket.

## 5. Resume a run

The output directory is the public resume handle:

```bash
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:0.3.2 \
  resume /out/stofs-20230113
```

Against the completed example, this reports that there is nothing to resume and exits
successfully.

To observe real recovery, use the optional
[full-scale demonstration](full-scale-demo.md), stop it with Ctrl+C, and pass the same
output directory to `resume`. That demonstration lists the entire `noaa-gestofs-pds`
bucket and makes tens of thousands of `ListObjectsV2` requests; it is a capability
demonstration, not an installation test, so do not run it just to confirm swath works.
swath retains finalized Parquet parts, discards an unfinished part, and continues after
the last durable cursor.

Do not edit the checkpoint or move an interrupted managed dataset. To replace a completed
dataset deliberately, start a new listing with `--overwrite`.

<a id="5-list-your-bucket"></a>

## 6. List a private bucket

Remove `--no-sign-request` and use the bucket's region. A container does not
automatically inherit credentials from the host, so forward the selected credential
source deliberately:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$PWD/out:/out" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION \
  ghcr.io/varveio/swath:0.3.2 \
  list s3://my-bucket/prefix/ \
  --region us-east-1 \
  --format parquet -o /out/my-inventory
```

For shared profiles, workload roles, web identity, requester-pays buckets, custom
endpoints, and least-privilege IAM, use
[Operating swath against object storage](operating.md). The normal S3 permission is
bucket-level `s3:ListBucket`; swath does not need permission to read object bodies.

## Cost and consistency

A live scan ideally uses approximately one `ListObjectsV2` request per 1,000 returned
keys, plus probes, retries, and any unfinished tail re-listed after interruption. swath
reports its actual request count. Pricing varies by provider, region, and time, so
calculate from the current provider price rather than from an evergreen number in these
docs.

If a fresh S3 Inventory or S3 Metadata table already exists and is accessible, query it
instead. A precomputed inventory is normally cheaper than any live scan.

Because S3 does not give a point-in-time transaction across a long listing, concurrent
bucket changes can affect which live state the result observes. `_SUCCESS` means swath
finished and published the complete result of its listing; it does not turn that listing
into a historical snapshot.

## Next steps

- Choose output forms, filters, sorting, and automation behavior in
  [Using swath](usage.md).
- Review credentials, IAM, endpoint compatibility, and request cost in
  [Operating swath](operating.md).
- Understand the range model through the
  [visual field guide](https://swath.varve.io/field-guide/).
- Go directly to [Troubleshooting and FAQ](faq.md) when a command fails.
