---
layout: home
title: swath
titleTemplate: Parallel listing and inventory for object stores and filesystems
description: swath lists object stores and filesystems in parallel, totals their sizes by directory or prefix, and saves sorted Parquet inventories you can query.

hero:
  name: swath
  text: Parallel listing and inventory for object stores and filesystems
  tagline: See what's taking up space in a bucket or a directory, and save a Parquet inventory you can query without listing again.
  actions:
    - theme: brand
      text: Start here
      link: /docs/start-here
    - theme: alt
      text: Install
      link: /docs/install
    - theme: alt
      text: GitHub
      link: https://github.com/varveio/swath

features:
  - title: Find where the space went
    details: <code>swath du</code> totals files and bytes per directory or prefix, as <code>du</code> does, for a directory or a whole bucket.
    link: /docs/first-inventory
    linkText: Your first inventory
  - title: List and export rows
    details: <code>swath list</code> prints every key with its size, time and storage metadata, in byte order, as a table, TSV or JSONL.
    link: /docs/output
    linkText: Output
  - title: Save an inventory you can query
    details: <code>swath list -o DIR</code> writes a sorted Parquet dataset with a manifest that records how complete it is. Query it with DuckDB, Python or any Parquet reader, or total it again without listing again with <code>swath du --from-dataset DIR</code> (coming in 0.4.0).
    link: /docs/dataset
    linkText: The swath dataset
---

## What swath is

swath is an open-source command-line tool that lists object stores and filesystems in parallel. It reads
listing metadata only and never changes what it lists.

A billion-object S3 bucket lists in about three minutes on one 32-vCPU VM, and becomes a sorted Parquet
inventory in about 14. [How this was measured](../docs/internals/measurements.md#a-billion-live-objects-sentinel-cogs-2026-11-07)

## Download

> [!NOTE]
> These pages describe the upcoming 0.4.0. The newest published build is swath 0.4.0-rc.2, a release
> candidate, which does not yet have everything described here, such as `swath du --from-dataset`
> ([what differs](../docs/release-candidate.md)).

Download the archive for your platform from the [releases page](https://github.com/varveio/swath/releases),
verify it and put `swath` on your `PATH`, or run the container image `ghcr.io/varveio/swath:0.4.0-rc.2`.
[Install](../docs/install.md) has the steps.

## Try it on a local directory

No account or credentials needed. Replace `~/Downloads` with any directory:

```bash
swath du -d 1 ~/Downloads                       # totals per directory, one level down
swath list ~/Downloads -o ~/downloads-inventory  # a sorted Parquet inventory
swath show ~/downloads-inventory                # what it holds and how complete it is
```

[Your first inventory](../docs/first-inventory.md) walks through the output, and
[Understand your results](../docs/results.md) says what the numbers cover.

## Try a public bucket

A public NOAA prefix of 5,252 objects, with no account: `--anonymous` sends no credentials.

```bash
swath du s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ -d 1 --anonymous --s3-region us-east-1
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 | head
```

`-d` changes the breakdown, not the scan: each `swath du` lists the whole prefix again. To explore several
depths of a big bucket, list it once with `-o DIR` and total the saved inventory with
`swath du --from-dataset DIR -d N` (coming in 0.4.0).

## List your own buckets

```bash
swath du s3://bucket/prefix/ -d 1                # totals per prefix
swath list gs://bucket/prefix/ -o inventory      # a sorted Parquet inventory
```

Listing a bucket makes billable API requests and reads metadata, not object contents; credentials come from
the provider's usual chain ([sources](../docs/sources.md)). An interrupted bucket inventory continues where it
stopped with [`swath resume DIR`](../docs/commands/swath-resume.md).

Sources: Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local or mounted
filesystems. [What each supports](../docs/sources.md)

## Documentation

- [Start here](../docs/start-here.md): pick a job, and when swath fits.
- [Sources](../docs/sources.md): URLs, credentials and what each source supports.
- [Run large listings](../docs/running.md): size, watch, stop and resume a long run.
- [Commands](../docs/commands/swath.md): every command and option.
- [Troubleshooting](../docs/troubleshooting.md): by symptom and exit code.
