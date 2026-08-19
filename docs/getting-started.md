# Getting started

This guide takes you from no swath installation to a saved, queryable, resumable
Parquet listing. The example target is the small anonymous prefix used by swath's
release smoke test; replace it with your bucket when the flow works.

## 1. Check the CLI

Docker is the shortest path because it includes Java:

```bash
docker run --rm ghcr.io/varveio/swath:latest --version
```

If you prefer a jar or native launcher script, use the
[installation guide](install.md), then replace `docker run ...` below with `swath`.

## 2. List a public prefix

Run a one-shot listing to stdout:

```bash
docker run --rm ghcr.io/varveio/swath:latest \
  list s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/ \
  --region us-east-1 --no-sign-request
```

Because Docker's stdout is not a terminal, the default `auto` format resolves to TSV.
Each line is one object; swath reads listing metadata and never downloads the object
body.

If this fails with a region or credential error, keep the explicit
`--region us-east-1 --no-sign-request` from the example. Endpoint or network failures
are covered in [troubleshooting](faq.md).

## 3. Save a Parquet dataset

Create an output directory that the container can write as your user:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest \
  list s3://cmas-smoke-testcase/smoke_example_case/2018gg_18j/inputs/htap/ \
  --region us-east-1 --no-sign-request --format parquet -o /out
```

On success, the host's `out/` directory contains:

```text
out/
  data/                   Parquet part files
  manifest.json           files and dataset metadata
  .swath-state.json       internal run identity
  _swath_summary.json     machine-readable run report
  _SUCCESS                written last: the dataset is complete
  symlink.txt             part paths for engines that use a manifest list
```

Read all parts as one dataset. With the DuckDB CLI:

```bash
duckdb -c "SELECT count(*) AS objects FROM read_parquet('out/data/*.parquet')"
duckdb -c "SELECT key, size FROM read_parquet('out/data/*.parquet') LIMIT 5"
```

The default output is not globally ordered: parallel workers finish independently.
Add `--sort` to the original listing when downstream processing requires parts and rows
in key order. Sorted output uses temporary disk space; read
[sorted output](usage.md#sorted-output) before using it on a large bucket.

## 4. Stop and resume

A Parquet directory is both the result and the run handle. While a run is active,
swath keeps its checkpoint under `<output>/.swath/`. Press Ctrl+C during a larger run,
then resume with the same mounted directory:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  ghcr.io/varveio/swath:latest resume /out
```

Finalized parts remain durable. swath discards any unfinished part and may re-list only
the tail after the last durable cursor. On clean completion it removes the checkpoint;
resuming a completed directory is a successful no-op.

If swath says the output is complete, use `--overwrite` on a new `list` command to
replace it deliberately. If it reports that the run identity changed, resume with the
original bucket, prefix, filters, and output options, or use `--restart` to discard the
old checkpoint.

## 5. Use your bucket

For a private bucket, remove `--no-sign-request`. Outside a container, swath uses the
same credential chain as the AWS CLI. To pass environment credentials into Docker:

```bash
mkdir -p out
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD/out:/out" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN -e AWS_REGION \
  ghcr.io/varveio/swath:latest \
  list s3://my-bucket/prefix/ --format parquet -o /out
```

On AWS compute, prefer the platform's container or instance role instead of copying
long-lived keys. The target bucket needs only `s3:ListBucket`; see
[credentials, least-privilege IAM, requester-pays, and cost](operating.md).

## Next steps

- [Common workflows and output contracts](usage.md)
- [Configuration and advanced controls](configuration.md)
- [Performance and large-run sizing](performance.md)
- [Progress, reports, and metrics](metrics-and-observability.md)
- [How the work-stealing scan works](internals/overview.md)
