---
layout: home
title: swath
titleTemplate: Parallel listing and inventory for object stores and filesystems

hero:
  name: swath
  text: Parallel listing and inventory for object stores and filesystems
  tagline: List S3, Google Cloud Storage, Azure Blob and local trees quickly, see where the space went, and keep a sorted Parquet inventory.
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
    details: <code>swath du</code> totals files and bytes per directory or prefix, as <code>du</code> does, for a local tree or a whole bucket.
    link: /docs/first-inventory
    linkText: Your first inventory
  - title: List and export rows
    details: <code>swath list</code> prints every key with its size, time and storage metadata, in byte order, as a table, TSV or JSON lines.
    link: /docs/output
    linkText: Text formats
  - title: Save an inventory you can query
    details: <code>swath list -o DIR</code> writes a sorted Parquet dataset with a manifest that records how complete it is. Query it with DuckDB, Python or any Parquet reader, or total it again with <code>swath du --from-dataset DIR</code> without listing again.
    link: /docs/dataset
    linkText: The swath dataset
---

## What swath is

swath is an open-source command-line tool that lists files and objects in parallel: local and mounted
filesystems, Amazon S3 and S3-compatible stores, Google Cloud Storage, and Azure Blob Storage flat
namespaces. It reads listing metadata only and never changes what it lists.

On 2026-11-07 one run listed and counted 1,088,794,418 objects of the public `sentinel-cogs` S3 bucket in
177 seconds, and a separate run wrote them as a sorted Parquet inventory in 846 seconds, each with 1,024
workers on one 32-vCPU VM. These are individual runs, not a promise for every source or machine
([measurements](../docs/internals/measurements.md#engine-campaign-2026-11-07-prod-key-lists-through-a-fake-at-50-ms)).

## Install

These pages describe the upcoming 0.4.0. The newest published build is swath 0.4.0-rc.2, a release candidate,
which does not yet have everything described here, such as `swath du --from-dataset`. Download the archive for
your platform from the [releases page](https://github.com/varveio/swath/releases), verify it and put `swath` on
your `PATH`, or run the container image `ghcr.io/varveio/swath:0.4.0-rc.2`. [Install](../docs/install.md) has
the steps.

## Try it on a local directory

No account or credentials needed. Replace `~/Downloads` with any directory:

```bash
swath du -d 1 ~/Downloads                       # totals per directory, one level down
swath list ~/Downloads -o ~/downloads-inventory  # a sorted Parquet inventory
swath show ~/downloads-inventory                # what it holds and how complete it is
```

[Your first inventory](../docs/first-inventory.md) walks through the output, and
[Understand your results](../docs/results.md) says what the numbers cover.

## List a bucket

```bash
swath du s3://bucket/prefix/ -d 1                # totals per prefix
swath list gs://bucket/prefix/ -o inventory      # a sorted Parquet inventory
```

Listing a bucket makes billable API requests and reads metadata, not object contents; credentials come from
the provider's usual chain ([sources](../docs/sources.md)). An interrupted bucket inventory continues where it
stopped with [`swath resume DIR`](../docs/commands/swath-resume.md).

## Documentation

- [Start here](../docs/start-here.md): pick a job, and when swath fits.
- [Sources](../docs/sources.md): S3, S3-compatible stores, GCS, Azure Blob and local trees: URLs and credentials.
- [Run large listings](../docs/running.md): size, watch, stop and resume a long run.
- [Commands](../docs/commands/swath.md): every command and option.
- [Troubleshooting](../docs/troubleshooting.md): by symptom and exit code.
