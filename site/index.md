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
  - title: Save an inventory you can query
    details: <code>swath list -o DIR</code> writes a sorted Parquet dataset with a manifest that records how complete it is. Query it with DuckDB, Python or any Parquet reader, or total it without listing again with <code>swath du --from-dataset DIR</code>.
    link: /docs/dataset
    linkText: The swath dataset
  - title: List and export rows
    details: <code>swath list</code> prints every key with its size, time and storage metadata, in byte order, as a table, TSV or JSONL.
    link: /docs/output
    linkText: Output
  - title: Find where the space went
    details: <code>swath du</code> shows the largest directories, prefixes and files at any depth, or totals files and bytes per directory or prefix, as <code>du</code> does, for a directory or a whole bucket.
    link: /docs/first-inventory
    linkText: Your first inventory
---

## What swath is

swath is an open-source command-line tool that lists object stores and filesystems in parallel. It reads
listing metadata only and never changes what it lists.

In one live run with `--workers 1024`, swath 0.4.0 wrote a sorted Parquet inventory of 1.09 billion S3
objects in 289 seconds (under 5 minutes) on one 32-vCPU VM; a separate count-only run of the same bucket on
the same VM, which wrote nothing, took 188 seconds ([how many workers to use](../docs/size-a-run.md#workers)).
[How this was measured](../docs/internals/measurements.md#a-billion-live-objects-on-the-040-release-against-032-sentinel-cogs-2026-11-17)

## Download

The newest published build is swath 0.4.0, on the [releases page](https://github.com/varveio/swath/releases).
Download the archive for your platform and put `swath` on your `PATH`, or run the container image
`ghcr.io/varveio/swath:0.4.0`. [Install](../docs/install.md) has the steps.

## Try a public bucket

A public NOAA prefix of 5,252 objects, with no account: `--anonymous` sends no credentials. Start with a prefix
like this one: the recorded listing of the whole bucket, the "Save an inventory, query it with SQL" example
above, covered 37,064,981 objects in 39,016 requests.

```bash
swath du s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1
swath list s3://noaa-gestofs-pds/stofs_2d_glo.20230113/ --anonymous --s3-region us-east-1 | head
```

On a terminal, bare `swath du` draws the view at the top of this page: the largest prefixes and objects at any
depth, each under its parent with its share of the total. In a pipe it prints the total row only. `-d N`
prints the totals of every prefix down to N levels instead, a table or TSV for scripts. Each `swath du` lists
the whole prefix again: to look at a big bucket several ways, list it once with `-o DIR` and total the saved
inventory with `swath du --from-dataset DIR`.

## Try it on a local directory

No account or credentials needed. Replace `~/Downloads` with any directory:

```bash
swath du ~/Downloads                            # where the bytes are, at any depth
swath list ~/Downloads -o ~/downloads-inventory  # a sorted Parquet inventory
swath show ~/downloads-inventory                # what it holds and how complete it is
```

[Your first inventory](../docs/first-inventory.md) walks through the output, and
[Understand your results](../docs/results.md) says what the numbers cover.

## List your own buckets

```bash
swath du s3://bucket/prefix/                     # where the bytes are, at any depth
swath list gs://bucket/prefix/ -o inventory      # a sorted Parquet inventory
```

Listing a bucket makes billable API requests and reads metadata, not object contents; credentials come from
the provider's usual chain ([sources](../docs/sources/README.md)). An interrupted bucket inventory continues where it
stopped with [`swath resume DIR`](../docs/commands/swath-resume.md).

Sources: Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local or mounted
filesystems. [What each supports](../docs/sources/README.md)

## Documentation

- [Start here](../docs/start-here.md): pick a job, and when swath fits.
- [Sources](../docs/sources/README.md): URLs, credentials and what each source supports.
- [Run large listings](../docs/running.md): list a bucket of your own start to finish.
- [Commands](../docs/commands/swath.md): every command and option.
- [Troubleshooting](../docs/troubleshooting.md): by symptom and exit code.
