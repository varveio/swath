# reader

`github.com/varveio/swath/reader` adds swath's schema and metadata knowledge to
[SeekParquet](https://github.com/varveio/seekparquet). It opens a dataset, exposes its
metadata, checks its schema, and optionally decodes rows into concrete source types.
Reading, parallel scanning, aggregation and pagination belong to SeekParquet, reached
through `Dataset.SeekParquet()`.

This is a nested Go module. It depends on SeekParquet and the standard library, with no
cloud SDK or dependency on the swath lister or objfile. The lister binds its producer
schemas to this module's registry (`internal/schema.Bind`), so writers and decoders
share the column names, types, nullability and version roles. The
[swath dataset profile](../docs/dataset.md) owns the data contract.

From swath 0.4.0 the module is released with swath, at the same version, tagged
`reader/vX.Y.Z`: `go get github.com/varveio/swath/reader@v0.4.0`. It is at version 0, with no
API compatibility promise: a minor release may change its API.

## Opening and metadata

`Open(ctx, src, opt)` takes `seekparquet.Source` and `seekparquet.OpenOptions`;
`OpenDir(ctx, path, opt)` opens a local directory. Both read the manifest alone,
resolve its registered schema id and parse `extensions.swath`. A present extension
must have integer `version: 1`: one without a `version`, or with any other, is a `SemanticError` of
`KindSchema` ("unsupported swath metadata version"), and a member of the wrong JSON type one of
`KindMetadata`. Members swath does not define are ignored, and `Metadata.Manifest` keeps the manifest as
SeekParquet read it, so an informative member can be added under `version` 1. An absent extension leaves
`Metadata.Swath` false: `Open` alone does not prove that swath wrote a dataset. Unknown schema ids are refused.

`Dataset.Metadata()` returns a copy of the parsed publication time, source, scope,
coverage (its filters as `Filter`s), what the listing collected (`Collected`), committed object bytes and, for a local source, the filesystem's space. Missing members remain nil: unknown is distinct
from zero, false or empty. The application decides whether the source, scope, filters,
exclusions, completeness and freshness suit its purpose.

`Dataset.SeekParquet()` is borrowed; `Dataset.Close()` closes it and its part handles.
A caller-supplied Source must stay usable until Close and remains caller-owned.
Use SeekParquet's types and errors directly; this module does not re-export them.

## Schema checks and concrete decoders

`Dataset.CheckSchema(ctx)` checks the manifest's key column and uniqueness against
its schema spec, verifies every part through SeekParquet, then checks each part's
columns from the verified footer. Registered columns must be top-level and
non-repeated, with the declared physical/logical type and requiredness, and every part
has every registered column. Unknown extra columns are ignored. This checks
neither every row's values nor part digests.

`Schema(id)` returns a copy of a schema spec; `SchemaIDs()` lists the registered ids.
`SchemaSpec.Column(name)` looks up a column, and `ColumnNames()` gives the projection
for its concrete decoder:

| Schema | Decoder | Result |
|---|---|---|
| `swath/s3-objects/v1` | `DecodeS3Object` | `S3Object` |
| `swath/s3-versions/v1` | `DecodeS3Version` | `S3Version` |
| `swath/gcs-objects/v1` | `DecodeGCSObject` | `GCSObject` |
| `swath/gcs-generations/v1` | `DecodeGCSGeneration` | `GCSGeneration` |
| `swath/azure-objects/v1` | `DecodeAzureObject` | `AzureObject` |
| `swath/azure-versions/v1` | `DecodeAzureVersion` | `AzureVersion` |
| `swath/local-entries/v1` | `DecodeLocalEntry` | `LocalEntry` |

Each decoder takes a `seekparquet.Row` projecting its schema's columns. Nullable
fields become pointers; null becomes nil. Required fields
remain values; times are UTC. Byte fields share the owning row's memory. Every
result embeds `Position` (key and equal-key offset); `Position.Cursor()` returns an
owning `*seekparquet.Cursor` for continuation. Concrete decoders preserve source
fields, including local kind/link target and Azure storage class (its access tier).

An unprojected column, forbidden absence or wrong value type returns `ErrSchema`;
a null required value returns `ErrRow`. Decoders do not impose application rules
such as nonnegative size, accepted timestamp years, local kind selection or class
mapping. `SemanticError` identifies its `Kind`, column and, when decoding, position.
`ErrSchema`, `ErrMetadata` and `ErrRow` are `errors.Is` targets. `IsRefusal` recognizes
semantic errors and SeekParquet refusals; other errors retain their original identity.

## Reading through SeekParquet

Use `Dataset.SeekParquet()` for `Rows`, `List`, `Plan` and `Plan.Scan`,
[`Plan.ScanParallel`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Plan.ScanParallel),
[`Dataset.Aggregate`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Aggregate),
`AggregateRows` and
[`Dataset.Page`](https://pkg.go.dev/github.com/varveio/seekparquet/go#Dataset.Page).
Project named columns explicitly. Borrowed batches avoid row materialization;
concrete decoders are optional helpers when owning Go values are useful.
[example_test.go](example_test.go) opens and checks a local dataset, scans it with
`DecodeLocalEntry`, and lists it by delimiter; `go test ./...` runs both examples.

Aggregations use named reductions such as `CountRows`, `Sum(size)` and
`Max(last_modified)`. Counts include every selected row, including directories and
versions. `Sum` skips null/absent values; request `CountNulls` and `CountAbsent` if
missing values must make an application total unknown. `AggregateOptions.CheckKeys`
requests selected-key validation. `ValueBudgetBytes` is explicit: zero allows no
value I/O, -1 is unlimited, and a positive budget bounds unique compressed measure
and filter extents, including dictionaries. For browsing, request `Page` with count and size reductions and
a caller-selected budget such as 4 MiB. Inspect each result's status before its value.
The SeekParquet API owns execution, borrowed lifetimes, cancellation and exactness.

`Page` reuses the row positions a listing has already established for complete-prefix
reductions: an unfiltered count adds no value reads, while an unknown boundary, a clipped
prefix or an unchecked contributing row count leaves the result unknown. Value-only reads do
not certify the keys they did not read; request `CheckKeys` when that matters. For a local
dataset, every count includes a directory's own row (a prefix `d/` includes its row `d/` once;
the root has no row), and `Sum(size)` without a kind filter gives entry bytes, directories'
own `st_size` included; select `kind` through SeekParquet's `BatchFilter` for files-only totals.
Under `--metadata names` sizes are null, so check the null and absent counters before calling
a sum complete.

A row's offset is its zero-based position among the rows of its key. A row cursor
`(key, offset)` continues after that row: at offset + 1 of the same key, then at the next key
([SeekParquet PAGE-1](https://github.com/varveio/seekparquet/blob/go/v0.2.0/spec/format-1.md#page-1)).
A cursor is always a row's: `Cursor{Key: k}` is the cursor after row 0 of `k`, not "after key
`k`". To skip every row of `k`, use an exclusive key bound (`seekparquet.Span` with `Start` = `k`
and `StartExclusive`), which also skips the rest of a versions group. An offset is valid only
for the same immutable dataset and is not a durable version id.

Storage owns caching and physical concurrency. To bypass objfile's byte cache for a
bulk operation, pass `objfile.WithCacheMode(ctx, objfile.CacheBypass)` to both planning
and execution, including exact-row reads. Browse calls use the policy supplied by
their caller. SeekParquet's `ReadStats` measures logical work; storage observers
measure physical requests.

### Version roles

`VersionSpec`, carried by `SchemaSpec.Versions`, is the public description of a versions schema's column roles.
Generic version consumers, including the lister's text output, version counts and per-prefix totals, use these
roles. The typed reader decoders still read their schema's named columns. The zero value means the rows are not
versions (every current-object schema). Otherwise the schema's keys repeat (`UniqueKeys` false), `ID` and `Latest` are set:

| Role | Column | Meaning |
|---|---|---|
| `ID` | STRING, or signed INT64 read in decimal; nullable | the version's identifier |
| `Latest` | required BOOLEAN | true on the key's latest row |
| `DeleteMarker` | required BOOLEAN; optional role | true on a delete marker (no object and no bytes); a schema without the role has no delete markers |

`swath/s3-versions/v1` declares `{ID: "version_id", Latest: "is_latest", DeleteMarker: "is_delete_marker"}` and
`swath/gcs-generations/v1` `{ID: "generation", Latest: "is_latest"}`. A row is current when it is latest and
not a delete marker, noncurrent when neither. `swath/azure-versions/v1` declares `{ID: "version_id", Latest:
"is_latest"}` and includes versions, snapshots and soft-deleted blobs; interpret its additional fields with the
Azure versions schema in the [dataset profile](../docs/dataset.md#schema-registry).

## The listing contract

The package [`github.com/varveio/swath/reader/listing`](listing/doc.go) holds the contract of the four questions
swath answers about a listed location, from a live source or from an opened inventory: Page (the next children
of a prefix), Lookup (what is at exactly one key), Walk (every entry under a prefix) and Summarize (count, bytes,
space on disk and newest change under each of several targets). It defines their requests and results, the
common `Entry`, the `Evidence` every answer carries (its origin, acquisition interval, scope, unlocated gaps and
whether the call completed), the `Unread` extents a call did not read, and the error classes with `Retryable`.
It holds types only, imports nothing beyond the standard library, and its API is experimental: it may change in
any release.

## The inventory view

The package [`github.com/varveio/swath/reader/view`](view/doc.go) answers those four questions from an opened
dataset. `view.Open(d)` reads nothing; it refuses a dataset of versions, whose keys repeat, and one whose manifest
records no scope, with `listing.ErrUnsupported`. `Page` is SeekParquet's delimited `Dataset.Page`, with opaque
tokens bound to the manifest digest, the prefix and the kinds the child totals count, and optional child totals
within a budget; `Lookup` reads the key's row; `Walk` is a range scan, in parallel shards when unbudgeted, or a
level-by-level listing to a depth; `Summarize` reads the entries at its targets' keys, then aggregates its subtree
targets' rows in one `AggregateRows` call, checking their key order, both within one entry, value-byte and time
budget. An object store's prefix may be any key prefix; a local one is empty or a directory key, and a local
inventory has no row for its root.

Every answer's `Evidence` comes from the manifest and never widens it: the SHA-256 of the manifest's bytes (`sha256:` and its hex), the
listing's interval and publication time, its scope, and its count of unreadable entries as `Unlocated` only when
the listing reached the end of its scope with no filter and no exclusion. A key is absent only inside the scope
and when `Unlocated` is 0, never at or under a local entry of unknown kind (`listing.KindUnknown`, which may be a
directory the listing did not read); a question outside the scope is unknown (`ErrOutOfScope`). A budget that stops a walk
or a summary reports the keys it did not reach as `Unread` intervals of class `budget`; the listing's own gaps
stay unlocated. The view checks every value it reads (a missing column, a negative size, a local kind swath does
not write), not every part's schema: an application admitting an inventory runs `Dataset.CheckSchema` first.
[view/example_test.go](view/example_test.go) pages, looks up and summarizes the photos dataset.
