# swath documentation

swath lists object stores and filesystems in parallel, prints size totals and text rows, and writes sorted
Parquet inventories. Releases are published on the [releases page](https://github.com/varveio/swath/releases)
([Install](install.md)).

**Start**

- [Start here](start-here.md): pick a job, and when swath fits.
- [Install](install.md): release binaries and their verification, the container image, a source build.
- [Your first inventory](first-inventory.md): total a local directory, list it, save and inspect an inventory.
- [Understand your results](results.md): scope, sizes, coverage, observation time, live and saved results.

**Use**

- [Sources](sources/README.md): Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local
  and mounted filesystems: URLs, credentials, permissions, endpoints, requests.
- [Output](output.md): text rows (table, TSV, JSONL), the fields of `swath du`'s rows, reading them in DuckDB,
  when to write a dataset.
- [Total a source](totals.md): `swath du`, the view of where the bytes are, totals per prefix, totals from a
  saved dataset, and how sizes, hard links and unreadable entries are counted.
- [Filter rows](filters.md): globs, regular expressions, sizes, times and storage classes.
- [Run large listings](running.md): list a bucket of your own start to finish: the command, what it prints,
  stopping and resuming it, where the result is.
  - [Size a run](size-a-run.md): the disk, memory and workers a bucket needs, and what it costs.
  - [Watch and tune a run](watch-a-run.md): progress, the closing lines, throttling, memory and disk, a stuck run.
  - [Stop, resume and refresh](resume-and-refresh.md): signals, `swath resume`, overwrite and seeding a refresh.
  - [List every version](all-versions.md): versions, generations, snapshots and delete markers.
- [Run on a schedule](production.md): a scheduled run under a supervisor, a dataset written to a
  bucket, preemptible machines.
- [Troubleshooting](troubleshooting.md): symptom, cause, fix; the known limits.

**Read the output**

- [Query an inventory](query.md): DuckDB, pandas and Spark, checking that an inventory is complete, verifying it.
- [Dataset format](dataset.md): its files, schemas,
  manifest and coverage. From Go, the [`reader`](../reader/README.md) module.

**Reference**

- Commands: [swath](commands/swath.md), [swath list](commands/swath-list.md), [swath du](commands/swath-du.md),
  [swath resume](commands/swath-resume.md), [swath show](commands/swath-show.md),
  [swath completion](commands/swath-completion.md).
- [Exit codes](reference/exit-codes.md), [Run report and progress file](reference/run-report.md),
  [Log events](reference/log-events.md).
