# Roadmap

Items here are intentions, not release commitments. Issues track actionable work.

## Release transition

The latest published release is the S3-focused Java v0.3.2. The Go implementation on
`main` is unreleased. Go will replace Java, and the Java code will be removed at that
release. A copy change does not itself make that release or remove Java.

Go already implements local and mounted filesystem listings, native GCS, S3 and Azure
Blob flat namespaces, direct size summaries, sorted Parquet inventories, eligible
object-store dataset resume, S3 versions and delete markers, and GCS generations.
The [Go guide](go/docs/getting-started.md) and [command reference](go/docs/commands/swath.md) describe their limits.
These capabilities should not be listed as future backend work.

The [publication checklist](docs/go-release-copy-checklist.md) keeps installation,
repository metadata and documentation aligned with the release transition.

## Planned work

### `swath inspect`

A cheap first look at a bucket or prefix: estimated scale, density, prefix structure,
and listing constraints, without performing a full inventory. The current `show`
command describes an existing dataset or interrupted run, not an unlisted source.

### Versioned-listing coverage

S3 `--all-versions` and GCS generations are implemented in Go under their own schemas.
Large versioned-bucket measurements and GCS soft-deleted objects remain future work.
Azure version, snapshot and deleted-blob listing are not implemented.

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
API. The existing [Go dataset reader](go/reader/README.md) and
[object-store file reader](go/objfile/README.md) document their own interfaces; they are
not a promise of an embeddable listing engine. A curated engine API needs a separate
contract after the CLI and lifecycle settle.
