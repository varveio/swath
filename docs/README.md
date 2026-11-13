# swath documentation

swath lists object stores and filesystems in parallel, prints size totals and text rows, and writes sorted
Parquet inventories. Release candidates for 0.4.0
are published on the [releases page](https://github.com/varveio/swath/releases) ([Install](install.md)).

**Start**

- [Start here](start-here.md): pick a job, and when swath fits.
- [Install](install.md): release binaries and their verification, the container image, a source build.
- [Differences in 0.4.0-rc.2](release-candidate.md): what the newest download does differently from these pages.
- [Your first inventory](first-inventory.md): total a local directory, list it, save and inspect an inventory.
- [Understand your results](results.md): scope, apparent and allocated bytes, hard links, coverage, observation
  time, live and saved results.

**Use**

- [Sources](sources.md): Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage, and local
  and mounted filesystems: URLs, credentials, permissions, endpoints, requests.
- [Output](output.md): text rows (table, TSV, JSONL), `swath du` totals, reading them in DuckDB, when to write
  a dataset.
- [Filters](filters.md): globs, regular expressions, sizes, times and storage classes.
- [Running swath](running.md): sizing, cost, watching, throttling, stopping and resuming, refreshing an inventory,
  versions, known limits. [Running swath in production](production.md) is the short checklist for scheduled runs.
- [Troubleshooting](troubleshooting.md): symptom, cause, fix.

**Read the output**

- [The swath dataset](dataset.md): querying an inventory, checking that it is complete, and its files, schemas,
  manifest and coverage. From Go, the [`reader`](../reader/README.md) module.

**Reference**

- Commands: [swath](commands/swath.md), [swath list](commands/swath-list.md), [swath du](commands/swath-du.md),
  [swath resume](commands/swath-resume.md), [swath show](commands/swath-show.md),
  [swath completion](commands/swath-completion.md).
- [Exit codes](reference/exit-codes.md), [Run report and progress file](reference/run-report.md),
  [Log events](reference/log-events.md).
