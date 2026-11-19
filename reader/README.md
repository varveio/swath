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

From swath 0.4.0 the module is tagged `reader/vX.Y.Z`, with a version of its own:
`go get github.com/varveio/swath/reader@v0.4.0`. It is at version 0, with no
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

The package [`github.com/varveio/swath/reader/listing`](listing/doc.go) holds the contract of the questions swath
answers about a listed location, from a live source (package `github.com/varveio/swath/source` in the root
module, [the Go library](../docs/library.md)) or from an opened inventory: Page (the next children of a
prefix), Lookup (what is at exactly one key) and Walk (every entry under a prefix), and an inventory's Summarize
(count, bytes, space on disk and newest change under each of several targets). It defines their requests and results, the
common `Entry`, the `Evidence` every answer carries (its acquisition interval, scope and unlocated gaps), the
`Unread` places a live walk could not read, and the error classes. No call has a time limit of its own: a context
deadline bounds it.
It holds types only, imports nothing beyond the standard library, and its API is experimental: it may change in
any release.

## The inventory view

The package [`github.com/varveio/swath/reader/view`](view/doc.go) answers those questions from an opened
dataset. `view.OpenDir(ctx, dir)` opens a local dataset directory, checks its parts' schemas and returns the
inventory, which `Close` closes; `view.Open(d)` takes a `Dataset` the caller opened and owns, and reads nothing. Both
refuse a dataset of versions, whose keys repeat, and one whose manifest records no scope, with
`listing.ErrUnsupported`. `Page` is SeekParquet's delimited `Dataset.Page`, with opaque tokens bound to the manifest
digest and the prefix; `Lookup` reads the key's row; `Walk` is a range scan, in parallel shards unless `Workers` is 1;
`Summarize` reads the entries at its targets' keys, then aggregates its subtree targets' rows in one
`AggregateRows` call, checking their key order: to size a page's child folders, summarize its `KindDir` and
`KindPrefix` entries as subtree targets. A walk or summary sets no limit: a caller stops it by cancelling
its context. An object store's prefix may be any key prefix; a local one is empty or a
directory key, and a local inventory has no row for its root.

Every answer's `Evidence` comes from the manifest and never widens it: the listing's interval, its scope, and its
count of unreadable entries as `Unlocated` only when
the listing reached the end of its scope with no filter and no exclusion. A key is absent only inside the scope
and when `Unlocated` is 0, never at or under a local entry of unknown kind (`listing.KindUnknown`, which may be a
directory the listing did not read), and never below a symbolic link, which no listing follows (unknown, with
`ErrNotDirectory`); a question outside the scope is unknown (`ErrOutOfScope`). The listing's own gaps stay
unlocated: a walk hands on no `Unread`, and a summary target is `Complete` only when `Unlocated` is 0 and its
key's state is known. The view checks every value it reads (a missing column, a negative size, a local kind swath
does not write), not every part's schema: an application admitting an inventory runs `Dataset.CheckSchema` first.
A call that fails returns an error of one of the contract's classes with its cause wrapped: data the reader refuses
is `ErrInvalidPage`, a refused access `ErrAuth`, and any other failure to read the dataset, a missing file of it
included, `ErrUnavailable`. A context's error is returned as it is and matches no class; an error returned by a
walk's own callback is returned as it is, so it need not match one.
[view/example_test.go](view/example_test.go) pages, looks up and summarizes the photos dataset.

## The du tally

The package [`github.com/varveio/swath/reader/du`](du/doc.go) sums a walk's entries per requested key prefix by
du's rules: space on disk with directories' own blocks, and a file with several hard links once within each
prefix that holds any of its names. It lists nothing and imports only `reader/listing` and the standard library:
a caller hands `Tally.Add` each batch of a Walk, live or from an inventory, in any order, and reads
`Tally.Result` at any time. Its API is experimental: it may change in any release. `swath du` itself does not
use it: the command keeps its own tally, which sums every depth of one walk and counts a hard-linked file once
per run, under its smallest name.

Each prefix (`""` or a key ending in `/`) is summed as if it were the only one requested, and an entry counts in
every prefix its key begins with, its own entry included. A local file, symbolic link or other entry counts only
with its `AllocatedBytes` and its identity (`Links` 1, or above 1 with `Device` and `Inode`); without them it adds
1 to `AllocatedMissing` (checked first) or `Unidentified` and to no other figure except `Symlinks`, which counts
every symbolic link's name. A directory adds its own blocks, an object its size and time and no space on disk;
common prefixes and entries of unknown kind are ignored. The missing counters and `Unidentified` count names: each
name of a hard-linked file without a size or a time adds to `SizeMissing` or `NewestMissing`, though only one adds
the file's figures. `Add`
takes a whole slice or none of it: an entry of no kind or with a negative figure, a sum past int64
(`ErrOverflow`) or more hard-linked files than `Options.MaxInodes` (`ErrInodeLimit`) fails it, and every later
`Add` fails with the same error.

For every prefix, nested ones included, `Allocated` is a lower bound of `du -sB1` of the prefix on an acyclic
traversal scope, also after a walk stopped early; `Files` and `Size` never exceed their values over every entry
under the prefix, and `Newest` is never later than its value there. The missing counters are diagnostics, not
bounds. The walk covered a prefix when it returned no error, an inventory's walk reported
`WalkSummary.Complete`, and no `Unread` holds the prefix or lies under it (an `Unread` elsewhere leaves it
covered). Where the walk covered a prefix and its allocation evidence is complete (`AllocatedMissing` and
`Unidentified` are 0), `Allocated` equals what `du -sB1` reports for that directory, with three differences:

- A directory on another device is listed and not entered: its own blocks count and its contents do not. GNU
  `du -s` enters such a directory, and `du -sx` leaves out the mount point's own blocks.
- The prefix's own directory entry counts only when it is fed. A live walk of a non-empty prefix hands it on; a
  walk of the root does not, so a caller feeds the root's entry, a `KindDir` at key `""`, itself. An inventory
  has no row for its source root, so a root summed from one leaves out the root's own blocks.
- A same-device bind mount of an ancestor is a cycle: swath's walk enters it as an ordinary directory and counts
  it again, while GNU du detects the cycle and skips it, so on such a tree `Allocated` can exceed du's figure.

Every equality and bound against du holds on acyclic traversal scopes only, and on a tree that does not change
during the walk.
