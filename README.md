[![CI](https://github.com/varveio/swath/actions/workflows/ci.yml/badge.svg)](https://github.com/varveio/swath/actions/workflows/ci.yml)

# swath

**Parallel listing and inventory for object stores and filesystems.**

See what's taking up space in a bucket or a directory, and save a sorted Parquet inventory you can query
without listing again. swath reads listing metadata only; it never reads, changes or deletes what it lists.

![swath du s3://1000genomes: the largest prefixes of the bucket at any depth, each under its parent with its size, object count, share of the total as a bar, and newest object](site/readme/hero.png)

Where 697 TiB sits in a public genomics bucket: 477,714 objects totaled in 3.5 seconds on a 4-vCPU cloud VM
([how this was measured](docs/internals/measurements.md#the-home-pages-lead-view-1000genomes-2026-11-16)).

## Install and try

With Docker, this lists the public bucket above from your terminal, with no account:

```bash
docker run --rm -t ghcr.io/varveio/swath:0.4.0 du s3://1000genomes --anonymous --s3-region us-east-1
```

`--anonymous` sends no credentials, and `-t` gives swath a terminal to draw the view on.

To install swath 0.4.0, run `brew install varveio/tap/swath` on macOS or Linux
([Homebrew](https://swath.varve.io/docs/install.html#homebrew)), or follow the steps for
[Linux](https://swath.varve.io/docs/install.html#linux),
[macOS](https://swath.varve.io/docs/install.html#macos) or
[Windows](https://swath.varve.io/docs/install.html#windows), or
[build it from source](https://swath.varve.io/docs/install.html#build-from-source). swath is pre-1.0, so flags
and schemas can still change between releases.

Then try it on a directory of yours, or a bucket:

```bash
swath du ~/Downloads             # replace with any directory
swath du s3://bucket/prefix/     # also gs://bucket/prefix/ or az://account/container/prefix/
```

Listing a bucket makes billable API requests. For S3, `--s3-region REGION` names the bucket's region;
[sources and credentials](https://swath.varve.io/docs/sources/) covers authentication.

## Save an inventory, query it with SQL

`swath list -o DIR` writes what it lists as a sorted Parquet dataset. Here it is on a public prefix of 5,252
objects, queried with [DuckDB](https://duckdb.org/):

```bash
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ -o noaa --anonymous --s3-region us-east-1
swath show noaa                    # confirm it says "complete" before querying
swath du --from-dataset noaa       # this inventory's sizes, without listing again

duckdb -c "SELECT regexp_extract(key, '\.(\w+)$', 1) AS ext, count(*) AS objects,
                  format_bytes(sum(size)::BIGINT) AS bytes
           FROM 'noaa/data/*.parquet' GROUP BY ext ORDER BY sum(size) DESC"
```

An inventory is the result of one live listing, not a point-in-time snapshot of a source that changes during
the run. [Query an inventory](https://swath.varve.io/docs/query.html) has more queries, pandas and Spark.

In one live run with `--workers 2048`, swath 0.4.0 wrote a sorted Parquet inventory of 1.09 billion S3 objects
in 238 seconds on one 64-vCPU VM
([how this was measured](docs/internals/measurements.md#the-same-bucket-on-64-vcpus-at-2048-workers-sentinel-cogs-2026-11-17)).

## Go further

swath lists ranges of a bucket's keys in parallel, splitting ranges further when workers become idle.
The [field guide](https://swath.varve.io/field-guide/) shows how on a recorded run.

The documentation is at [swath.varve.io](https://swath.varve.io/):

- [Start here](https://swath.varve.io/docs/start-here.html): pick a job, and when swath fits.
- [Run large listings](https://swath.varve.io/docs/running.html): a bucket of your own, start to finish,
  with stopping and resuming.
- [Sources](https://swath.varve.io/docs/sources/): Amazon S3 and S3-compatible stores, Google Cloud Storage,
  Azure Blob Storage, and local or mounted filesystems, with what each supports.

To work on swath, read the [contributor documentation](docs/internals/README.md) and
[CONTRIBUTING.md](CONTRIBUTING.md). [Apache-2.0](LICENSE); swath is by [Varve](https://varve.io).
