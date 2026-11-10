# swath documentation

These pages describe the Go implementation on main, which is not released yet; the latest published release
is the Java v0.3.2 ([its docs](https://github.com/varveio/swath/tree/v0.3.2/docs)).

**Start**

- [getting-started.md](getting-started.md): build swath, total a directory, list a public prefix, write and
  query a dataset, interrupt and resume a run.
- [install.md](install.md): build from source, verify the binary, platforms.

**Use**

- [sources.md](sources.md): S3 and S3-compatible stores, GCS, Azure Blob and local trees: URLs, credentials,
  permissions, endpoints.
- [output.md](output.md): text rows (table, TSV, JSONL), `swath du` totals, reading them in DuckDB, when to write
  a dataset.
- [filters.md](filters.md): globs, regular expressions, sizes, times and storage classes.
- [dataset.md](dataset.md): the swath dataset: its schemas, manifest and coverage, for anyone who reads one.
- [running.md](running.md): sizing, watching, throttling, stopping and resuming, refreshing an inventory,
  versions, known limits. [production.md](production.md) is the short checklist for scheduled runs.
- [troubleshooting.md](troubleshooting.md): symptom, cause, fix.

**Reference**

- Commands: [swath](commands/swath.md), [swath list](commands/swath-list.md), [swath du](commands/swath-du.md),
  [swath resume](commands/swath-resume.md), [swath show](commands/swath-show.md),
  [swath completion](commands/swath-completion.md).
- [Exit codes](reference/exit-codes.md), [run report and progress file](reference/run-report.md),
  [log events](reference/log-events.md).

**Coming from Java**: [from-java.md](from-java.md) (the command line) and [upgrade.md](upgrade.md) (the dataset
and report changes).

Contributors, reviewers and readers of the Go packages: [internals/README.md](internals/README.md).
