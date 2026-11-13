# swath documentation

swath is not released yet: build it from source ([install.md](install.md)). The latest published release, v0.3.2,
has another command line and dataset format ([its docs](https://github.com/varveio/swath/tree/v0.3.2/docs);
[migrating from 0.3](upgrade.md)).

**Start**

- [getting-started.md](getting-started.md): build swath, total a directory, list a public prefix, write and
  query a dataset, interrupt and resume a run.
- [concepts.md](concepts.md): sources and scope, key order, ranges, parts and the manifest, coverage, the
  journal and resume, spill, the outcome: what each promises and what it does not.
- [install.md](install.md): build from source, verify the binary, platforms.

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

**Migrating from 0.3**: [upgrade.md](upgrade.md): the command line, the run report and the dataset.

**Contributing**: [internals/README.md](internals/README.md): architecture, the invariants, providers, testing
and dated measurements. `swath diag` and its options are in [internals/diagnostics.md](internals/diagnostics.md).

**Repository layout**

| Path | What it is |
|---|---|
| `cmd/swath` | the command |
| `cmd/swath-replay` | the replay server ([internals/swath-replay.md](internals/swath-replay.md)) |
| `internal/` | the lister's packages ([architecture](internals/architecture.md#packages)) |
| `reader/`, `objfile/` | nested modules, importable on their own ([`reader`](../reader/README.md), [`objfile`](../objfile/README.md)) |
| `docs/` | this documentation; `go test ./docs` checks the relative links of the repository's Markdown |
| `bench/`, `verify.py` | benchmarks and the independent DuckDB verifier |

The gate is `go vet ./... && go test -race ./...` at the root, and the same in `objfile/` and `reader/`
([testing](internals/testing.md)).
