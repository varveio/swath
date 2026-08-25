# Roadmap

This page lists deliberately deferred work in rough priority order. Items are intentions,
not release commitments; issues track work that is actionable now.

## Current shipped scope

swath currently provides:

- parallel current-object listing for general-purpose S3 buckets;
- table, TSV, JSONL, and managed Parquet output;
- checkpoint and resume for managed Parquet directories;
- opt-in globally key-sorted Parquet;
- filters, run reports, metrics, and diagnostic traces; and
- experimental GCS access through the S3-compatible XML API.

The supported product is the CLI. Internal Java seams are not a stable third-party API.

## Planned features

### `swath inspect`

Give a cheap first look at a bucket or prefix—estimated scale, density, prefix structure,
and likely listing constraints—without performing the full inventory.

### Versioned listing

Add `--all-versions` support for object versions and delete markers. The Parquet schema
already reserves `version_id`, `is_latest`, `is_delete_marker`, and `row_type`; current
object-listing runs leave the version fields unpopulated.

### Supplied keyspace hints

Allow callers to provide known keyspace structure instead of paying for the default
shallow discovery pass. The design must preserve the same disjoint range and resume
contracts as discovered seeds.

### Diff two listings

Compare two buckets, prefixes, or inventory datasets and emit missing or changed objects.
The user-facing contract and restart behavior need design before an implementation is
committed.

### S3 directory buckets

Directory buckets do not provide the one global lexicographic order required by swath's
range-splitting engine. Supporting them needs a separate sequential continuation-token
path rather than pretending the current algorithm applies.

## Post-1.0 candidates

### Embeddable library entry point

Expose a curated, versioned Java API after the internal types and lifecycle have settled.
The current modules are implementation boundaries, not a supported SPI.

### Native GCS and additional backends

A native backend must provide or deliberately replace the global-order and
exclusive-lower-bound assumptions used by the S3 engine. The current GCS XML path remains
experimental S3-compatible access.

### Resume retargeting

Allow an interrupted run to move to a new output destination through an explicit
identity-rewrite operation. Ordinary `swath resume` intentionally keeps destination
identity fixed.

### Checked nullness

Adopt a repository-wide nullness story such as JSpecify with a static checker after the
pre-1.0 structural work settles.
