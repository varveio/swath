# swath documentation

swath lists object stores and filesystems in parallel, prints size totals and text rows, and writes sorted
Parquet inventories. Releases are published on the [releases page](https://github.com/varveio/swath/releases)
([Install](install.md)).

**Start**

- [Start here](start-here.md): pick a job, and when swath fits.
- [Install](install.md): release binaries and their verification, the container image, a source build.
- [Your first inventory](first-inventory.md): total a local directory, list it, save and inspect an inventory.
- [Understand your results](results.md): scope, apparent and allocated bytes, hard links, coverage, observation
  time, live and saved results.

**Use**

- [Sources](sources/README.md): Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local
  and mounted filesystems: URLs, credentials, permissions, endpoints, requests.
- [Output](output.md): text rows (table, TSV, JSONL), `swath du` totals, reading them in DuckDB, when to write
  a dataset.
- [Total a source](totals.md): `swath du`, the view of where the bytes are, totals per prefix and totals from a
  saved dataset.
- [Filter rows](filters.md): globs, regular expressions, sizes, times and storage classes.
- [Run large listings](running.md): list a bucket of your own start to finish: the command, what it prints,
  stopping and resuming it, where the result is.
  - [Size a run](size-a-run.md): disk, memory and cost.
  - [Watch and tune a run](watch-a-run.md): progress, the signals to read, throttling, the flags to change.
  - [Stop, resume and refresh](resume-and-refresh.md): signals, `swath resume`, overwrite and seeding a refresh.
  - [List every version](all-versions.md): versions, generations, snapshots and delete markers.
- [Run on a schedule](production.md): a scheduled run under a supervisor, a dataset written to a
  bucket, preemptible machines.
- [Troubleshooting](troubleshooting.md): symptom, cause, fix; the known limits.

**Read the output**

- [Query an inventory](query.md): DuckDB, pandas and Spark, checking that an inventory is complete, verifying it.
- [The swath dataset format](dataset.md): its files, schemas,
  manifest and coverage. From Go, the [`reader`](../reader/README.md) module.

**Reference**

- Commands: [swath](commands/swath.md), [swath list](commands/swath-list.md), [swath du](commands/swath-du.md),
  [swath resume](commands/swath-resume.md), [swath show](commands/swath-show.md),
  [swath completion](commands/swath-completion.md).
- [Exit codes](reference/exit-codes.md), [Run report and progress file](reference/run-report.md),
  [Log events](reference/log-events.md).
