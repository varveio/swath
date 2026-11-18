# Roadmap

Items here are intentions, not release commitments. Issues track actionable work.

## Planned work

### `swath inspect`

A cheap first look at a bucket or prefix: estimated scale, density, prefix structure,
and listing constraints, without performing a full inventory. The current `show`
command describes an existing dataset or interrupted run, not an unlisted source.

### Versioned-listing coverage

S3 `--all-versions`, GCS generations and Azure Blob versions, snapshots and soft-deleted blobs are implemented
in Go under their own schemas. Large versioned-bucket measurements, a live check of Azure versions and snapshots
against a writable account, and GCS soft-deleted objects remain future work.

### Supplied keyspace hints

Go already accepts `--seed-from` for a previous dataset or a cut file. Broader ways to
provide known keyspace structure still need a design that preserves disjoint ranges
and resume identity.

### Diff two listings

Compare buckets, prefixes or inventory datasets and emit missing or changed entries.
The user-facing contract and restart behavior still need design. Today comparisons
are downstream work, not a built-in swath command.

### Additional source semantics

S3 directory buckets lack the global ordering required by the range engine. Azure
hierarchical namespaces have different ordering semantics. Both need explicit support
rather than being treated as equivalent to today's supported sources.

### Resume retargeting

Move an interrupted run to a new output destination through an explicit identity-rewrite
operation. Ordinary `swath resume` keeps source and destination identity fixed.

## Library interfaces

The CLI is the product entry point. Internal engine packages are not a stable embedding
API. The [Go dataset reader](reader/README.md) and the
[object-store I/O library](objfile/README.md) document their own interfaces, and the root
module is importable by released version.

What exists: the reader module holds an experimental contract of the questions swath
answers about a listed location (`reader/listing`: Page, Lookup and Walk, an inventory's
Summarize, their evidence and error classes) and an inventory view that answers them from a dataset
(`reader/view`). The root module's package `source` answers the same contract live, from an S3 or GCS
bucket, an Azure Blob Storage container or a local directory ([the Go library](docs/library.md)). All
three may change in any release. A whole run, with its output and exit codes, is reachable only through
the CLI.
