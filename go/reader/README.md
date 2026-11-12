# reader

`github.com/varveio/swath/go/reader` adds swath's schema and metadata knowledge to
[SeekParquet](https://github.com/varveio/seekparquet). It opens a dataset, exposes its
metadata, checks its schema, and optionally decodes rows into concrete source types.
Reading, parallel scanning, aggregation and pagination belong to SeekParquet, reached
through `Dataset.SeekParquet()`.

This is a nested Go module. It depends on SeekParquet and the standard library, with no
cloud SDK or dependency on the swath lister or objfile. The lister binds its producer
schemas to this module's registry (`internal/schema.Bind`), so writers and decoders
share the column names, types, nullability and version roles. The
[swath dataset profile](../docs/dataset.md) owns the data contract.

## Opening and metadata

`Open(ctx, src, opt)` takes `seekparquet.Source` and `seekparquet.OpenOptions`;
`OpenDir(ctx, path, opt)` opens a local directory. Both read the manifest alone,
resolve its registered schema id and parse `extensions.swath`. A present extension
must have integer `version: 1`; malformed members return a `SemanticError`.
An absent extension leaves `Metadata.Swath` false. Unknown schema ids are refused.

`Dataset.Metadata()` returns a copy of the parsed publication time, source, scope,
coverage and committed object bytes. Missing members remain nil: unknown is distinct
from zero, false or empty. The application decides whether the source, scope, filters,
exclusions, completeness and freshness suit its purpose.

`Dataset.SeekParquet()` is borrowed; `Dataset.Close()` closes it and its part handles.
A caller-supplied Source must stay usable until Close and remains caller-owned.
Use SeekParquet's types and errors directly; this module does not re-export them.

## Schema checks and concrete decoders

`Dataset.CheckSchema(ctx)` checks the manifest's key column and uniqueness against
its schema spec, verifies every part through SeekParquet, then checks each part's
columns from the verified footer. Registered columns must be top-level and
non-repeated, with the declared physical/logical type and requiredness; only columns
marked `MayBeAbsent` may be missing. Unknown extra columns are ignored. This checks
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
| `swath/azblob-objects/v1` | `DecodeAzblobObject` | `AzblobObject` |
| `swath/azblob-versions/v1` | `DecodeAzblobVersion` | `AzblobVersion` |
| `swath/local-entries/v1` | `DecodeLocalEntry` | `LocalEntry` |

Each decoder takes a `seekparquet.Row` projecting its schema's columns. Nullable
fields become pointers; null or permitted absence becomes nil. Required fields
remain values; times are UTC. Byte fields share the owning row's memory. Every
result embeds `Position` (key and equal-key offset); `Position.Cursor()` returns an
owning `*seekparquet.Cursor` for continuation. Concrete decoders preserve source
fields, including local kind/link target and Azure access tier.

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
and filter extents. For browsing, request `Page` with count and size reductions and
a caller-selected budget such as 4 MiB. Inspect each result's status before its value.
The SeekParquet API owns execution, borrowed lifetimes, cancellation and exactness.

Storage owns caching and physical concurrency. To bypass objfile's byte cache for a
bulk operation, pass `objfile.WithCacheMode(ctx, objfile.CacheBypass)` to both planning
and execution, including exact-row reads. Browse calls use the policy supplied by
their caller. SeekParquet's `ReadStats` measures logical work; storage observers
measure physical requests.

### Version roles

`VersionSpec`, carried by `SchemaSpec.Versions`, is the public description of a versions schema's column roles.
Generic version consumers, including the lister's text output, version counts and per-prefix totals, use these
roles. The typed reader decoders still read their schema's named columns. The zero value means the rows are not
versions (every current-object schema). Otherwise the schema's keys repeat (`UniqueKeys` false), `ID` is set, and exactly one of `Latest` and `DeletedAt` is:

| Role | Column | Meaning |
|---|---|---|
| `ID` | STRING, or signed INT64 read in decimal; nullable | the version's identifier |
| `Latest` | required BOOLEAN | true on the key's latest row |
| `DeletedAt` | nullable timestamp | the alternative to `Latest`: null on the latest (live) row; any value, zero included, is not latest |
| `DeleteMarker` | required BOOLEAN; optional role | true on a delete marker (no object and no bytes); a schema without the role has no delete markers |

`swath/s3-versions/v1` declares `{ID: "version_id", Latest: "is_latest", DeleteMarker: "is_delete_marker"}` and
`swath/gcs-generations/v1` `{ID: "generation", DeletedAt: "time_deleted"}`. A row is current when it is latest and
not a delete marker, noncurrent when neither. `swath/azblob-versions/v1` declares `{ID: "version_id", Latest:
"is_latest"}` and includes versions, snapshots and soft-deleted blobs; interpret its additional fields with the
Azure versions schema in the [dataset profile](../docs/dataset.md#schema-registry).
