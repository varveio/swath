# swath documentation

swath lists local and mounted filesystems, S3, Google Cloud Storage and Azure Blob Storage flat namespaces in
parallel, prints size totals and text rows, and writes sorted Parquet inventories. Release candidates for 0.4.0
are published on the [releases page](https://github.com/varveio/swath/releases) ([install.md](install.md)).
These pages are also published at [swath.varve.io](https://swath.varve.io/docs/).

**Start**

- [start-here.md](start-here.md): pick a job, and when swath fits.
- [install.md](install.md): release binaries and their verification, the container image, a source build.
- [first-inventory.md](first-inventory.md): total a local directory, list it, save and inspect an inventory.
- [results.md](results.md): scope, apparent and allocated bytes, hard links, coverage, observation time, live
  and saved results.

**Use**

- [sources.md](sources.md): S3 and S3-compatible stores, GCS, Azure Blob and local trees: URLs, credentials,
  permissions, endpoints, requests.
- [output.md](output.md): text rows (table, TSV, JSONL), `swath du` totals, reading them in DuckDB, when to write
  a dataset.
- [filters.md](filters.md): globs, regular expressions, sizes, times and storage classes.
- [running.md](running.md): sizing, cost, watching, throttling, stopping and resuming, refreshing an inventory,
  versions, known limits. [production.md](production.md) is the short checklist for scheduled runs.
- [troubleshooting.md](troubleshooting.md): symptom, cause, fix.

**Read the output**

- [dataset.md](dataset.md): the swath dataset: its files, schemas, manifest and coverage, for anyone who reads
  one. From Go, the [`reader`](../reader/README.md) module.

**Reference**

- Commands: [swath](commands/swath.md), [swath list](commands/swath-list.md), [swath du](commands/swath-du.md),
  [swath resume](commands/swath-resume.md), [swath show](commands/swath-show.md),
  [swath completion](commands/swath-completion.md). Their synopses and option lists are generated from the
  binary and checked by `go test ./cmd/swath`.
- [Exit codes](reference/exit-codes.md), [run report and progress file](reference/run-report.md),
  [log events](reference/log-events.md).

**Contributing**: [internals/README.md](internals/README.md): the repository layout, architecture, the invariants,
providers, testing and dated measurements.
