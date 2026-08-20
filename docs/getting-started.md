# Getting started

This guide uses Docker to make a public listing, save a managed Parquet dataset, query it,
and show how the same output directory resumes after an interruption. The example target
is an 11-object slice of the same public NOAA bucket shown in the README demo; replace it
with your own bucket after the public command works.

**Shell note:** the commands use a Bash-compatible shell on Linux or macOS. On Windows
PowerShell, create the directory with `New-Item -ItemType Directory -Force out`, replace
`$PWD` with `${PWD}`, and omit `--user "$(id -u):$(id -g)"`; Docker Desktop makes its
bind mount writable.

**Without Docker:** choose the jar or launcher in [Installation](install.md), omit the
Docker options, and run the swath arguments directly. For example, use
`swath list s3://...` and write to the host path `-o out/` instead of `/out`.

## 1. Check the CLI

Docker includes the required Java runtime:

```bash
docker run --rm ghcr.io/varveio/swath:latest --version
```

This should print the swath version and exit without contacting S3.

## 2. List a public prefix

Run a one-shot listing to stdout:

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20260803/00/ \
  --no-sign-request --region us-east-1
```

Because Docker's stdout is not a terminal, the default `auto` format resolves to TSV.
Each line describes one object. swath reads listing metadata; it never downloads object
contents.

The command is anonymous and needs no AWS credentials. If the exact command fails, use
the [troubleshooting guide](faq.md) for the reported access, network, or Docker error
instead of adding credentials to this public example.

## 3. Save and query a Parquet dataset

Create a host directory and mount it at `/out`. Running the container as your current
user avoids root-owned output on Linux:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/stofs_2d_glo.20260803/00/ \
  --no-sign-request --region us-east-1 \
  --format parquet -o /out/noaa-gestofs-sample
```

The host's `out/noaa-gestofs-sample/` directory is now a managed Parquet dataset:

```text
out/noaa-gestofs-sample/
  data/                   Parquet part files
  manifest.json           files and dataset metadata
  .swath-state.json       internal run identity
  _swath_summary.json     machine-readable run report
  _SUCCESS                written last: the dataset is complete
  symlink.txt             part paths for engines that use a manifest list
```

Read every part as one table. If the DuckDB CLI is installed:

```bash
duckdb -c "SELECT count(*) AS objects FROM read_parquet('out/noaa-gestofs-sample/data/*.parquet')"
duckdb -c "SELECT key, size FROM read_parquet('out/noaa-gestofs-sample/data/*.parquet') LIMIT 5"
```

Or use DuckDB's
[official container](https://duckdb.org/docs/current/operations_manual/duckdb_docker),
so the entire walkthrough still requires only Docker:

```bash
docker run --rm -v "$PWD:/workspace" -w /workspace duckdb/duckdb \
  -c "SELECT count(*) AS objects FROM read_parquet('out/noaa-gestofs-sample/data/*.parquet')"
```

The default dataset is not globally ordered: parallel workers finish independently. Add
`--sort` to the original listing only when downstream processing requires global key order.
Sorted output uses temporary disk space; read
[Sorted output](usage.md#sorted-output) before using it on a large bucket.

## 4. Resume an interrupted run

The output directory is also the run handle. While a listing is active, swath keeps its
checkpoint under `<output>/.swath/`. After Ctrl+C, a crash, or a stopped container, resume
the same directory:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest resume /out/noaa-gestofs-sample
```

The public sample normally completes before you can interrupt it. Running the resume command
against that completed directory is safe: swath reports that there is nothing to resume and
exits successfully. On a longer interrupted listing, the same command retains finalized parts,
discards an unfinished part, and continues after the last durable cursor.

To replace a completed dataset deliberately, run a new `list` command with `--overwrite`.
For mismatched or refused run directories, follow
[The output directory is refused](faq.md#the-output-directory-is-refused) rather than
editing the checkpoint.

## 5. Run the full demo (optional)

The README video removes the sample prefix and lists the entire NOAA bucket with 128
concurrent listings:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://noaa-gestofs-pds/ \
  --no-sign-request --region us-east-1 \
  --concurrency 128 \
  --format parquet -o /out/noaa-gestofs-pds
```

This is the exact listing command shown in the demo, wrapped in Docker. The recorded run
listed 39,585,029 objects, made 41,582 S3 API calls, wrote 790.8 MB of Parquet, and
peaked around 1.7 GB RSS. It is a real large-bucket run, so read the
[request-cost guidance](operating.md#request-cost) before reproducing it.

## 6. List your bucket

For a private bucket, remove `--no-sign-request`. A container does not automatically
inherit credentials from the host, so pass environment credentials explicitly. Replace
`us-east-1` below with the bucket's region:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --region us-east-1 --format parquet -o /out
```

For a shared profile, mount its files read-only; on AWS compute, prefer the platform's
container, task, pod, or instance role instead of copying long-lived keys. The exact
examples and limitations are in [Credentials in Docker](operating.md#credentials-in-docker).
The bucket needs only `s3:ListBucket`; the same operator guide owns IAM, endpoint,
requester-pays, and cost guidance.

## Next steps

- To choose another output, filter rows, sort, or automate exit handling, read
  [Common workflows](usage.md).
- Before a large or requester-pays listing, read
  [Credentials, IAM, endpoints, and request cost](operating.md).
- To understand why the parallel scan works, read the
  [visual field guide](https://swath.varve.io/field-guide/).
- If a command failed, go directly to [Troubleshooting and FAQ](faq.md).
