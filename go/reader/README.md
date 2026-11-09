# reader

`github.com/varveio/swath/go/reader` is what a reader of swath datasets needs to know about swath on top of the
[SeekParquet](https://github.com/varveio/seekparquet) module: the schema ids and their columns, the manifest's
`extensions.swath`, each schema's rows as typed values decoded from a `seekparquet.Row`, and a `Dataset` that
opens a dataset, checks its schema, accepts it as a whole-source listing, streams its current objects and totals
them under paths and prefixes.

It is a nested Go module. It depends on the SeekParquet module (and through it parquet-go) and the standard
library only: no cloud SDK, and not the swath lister (`github.com/varveio/swath/go`), which imports it instead.
The lister's producers bind their schemas to the specs here (`internal/schema.Bind`), so what swath writes and
what a reader decodes are described once.

## API

```go
type SchemaID string // S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, LocalV1
type ColumnType uint8 // TypeString, TypeInt32, TypeInt64, TypeTimestampMicros, TypeBool, TypeUint32, TypeUint64, TypeBinary
type ColumnSpec struct { Name string; Type ColumnType; Required, MayBeAbsent bool }
type SchemaSpec struct { ID SchemaID; Key ColumnSpec; UniqueKeys bool; MaxKeyBytes int; Columns []ColumnSpec }
func Schema(id SchemaID) (SchemaSpec, bool)       // a copy; false for an id swath does not write
func (SchemaSpec) Column(name string) (ColumnSpec, bool)
func (SchemaSpec) ColumnNames() []string          // the projection the decoders read

const Extension = "swath"
type Metadata struct { Manifest seekparquet.Metadata; SchemaID SchemaID; Swath bool
	PublishedAt *time.Time; Source *SourceInfo; Scope *Scope; Coverage *Coverage }
func ParseMetadata(m seekparquet.Metadata) (Metadata, error)

type Position struct { Key []byte; Offset int64 } // embedded in every row
func (Position) Cursor() *seekparquet.Cursor

type Object, S3Object, S3Version, GCSObject, GCSGeneration, LocalEntry
func DecodeObject(id SchemaID, row seekparquet.Row) (Object, error) // s3-objects/v1, gcs-objects/v1, swath-local/v1
func DecodeS3Object(row seekparquet.Row) (S3Object, error)
func DecodeS3Version(row seekparquet.Row) (S3Version, error)
func DecodeGCSObject(row seekparquet.Row) (GCSObject, error)
func DecodeGCSGeneration(row seekparquet.Row) (GCSGeneration, error)
func DecodeLocalEntry(row seekparquet.Row) (LocalEntry, error)

func Open(ctx context.Context, src seekparquet.Source, opt seekparquet.Options) (*Dataset, error)
func OpenDir(ctx context.Context, path string, opt seekparquet.Options) (*Dataset, error)
func (*Dataset) Metadata() Metadata
func (*Dataset) SeekParquet() *seekparquet.Dataset // borrowed: the Dataset closes it
func (*Dataset) CheckSchema(ctx context.Context) error
func (*Dataset) AcceptWholeSource(kind, container string) error
func (*Dataset) Stream(ctx context.Context, fn func(Object) error) error
func (*Dataset) StreamStats() StreamStats // { Requests, Objects int64; seekparquet.ReadStats }
func (*Dataset) Totals(ctx context.Context, paths []string) ([]PathTotals, error)
func (*Dataset) Aggregates(ctx context.Context, prefixes [][]byte, opt AggregateOptions) ([]PrefixAggregate, error)
func (*Dataset) Close() error

type SemanticError struct { Kind Kind; Column string; Row *Position; Err error }
const KindSchema, KindCoverage, KindRow Kind = "schema", "coverage", "row"
var ErrSchema, ErrCoverage, ErrRow *SemanticError // errors.Is targets, by kind

type PathTotals struct { Count, Bytes int64; NewestModified *time.Time; Exact *Object }
type SumMode int // SumsOff, SumsWithinBudget, SumsAlways
const DefaultSumBudget = 4 << 20
type AggregateOptions struct { Counts bool; Sums SumMode; SumBudgetBytes int64 }
type PrefixAggregate struct { Count, Bytes *int64 } // nil: not requested or not known exactly
```

A reader of current objects opens the dataset, admits it once and streams it:

```go
d, err := reader.OpenDir(ctx, dir, seekparquet.Options{}) // or reader.Open over a seekparquet.Source (objfile)
defer d.Close()
err = d.CheckSchema(ctx)                 // at admission, not per read
err = d.AcceptWholeSource("s3", "bucket") // the whole bucket, nothing skipped or filtered
err = d.Stream(ctx, func(o reader.Object) error { /* ... */ return nil })
```

A reader of anything else (versions, a key range, a listing by delimiter) queries the module's dataset and
decodes its rows, projecting `SchemaSpec.ColumnNames()`:

```go
spec, ok := reader.Schema(d.Metadata().SchemaID) // S3VersionsV1, say
rows, err := d.SeekParquet().Range(ctx, seekparquet.Range{Prefix: []byte("logs/"), Limit: 1000, Columns: spec.ColumnNames()})
for _, r := range rows {
	v, err := reader.DecodeS3Version(r)
	// ...
}
// the next page: Range{..., Cursor: last.Cursor()}
```

## What is checked, and when

- **`Open`, `OpenDir`.** The module reads and checks the manifest and opens no part
  (`seekparquet.OpenDataset`, `OpenDir`); its refusals are returned unchanged. `extensions.swath` is parsed
  (a member of the wrong JSON type is a `KindCoverage` error) and the schema id must be one swath writes (else
  `KindSchema`). A manifest without the extension opens; `AcceptWholeSource` refuses it.
- **`CheckSchema`** (at admission). The manifest's key column and key uniqueness are the spec's, so a versions
  dataset (keys repeat) never passes as current objects. Then `seekparquet.Dataset.Verify` checks every part
  (size, footer, stamps with the schema id, key column, each row group's first key page), and each part's columns
  are read from the footer the module checked and reads the part through (`seekparquet.Dataset.Columns`, legacy
  converted types folded into logical ones), so a file put at a part's path later is never what is checked:
  every spec column top-level and non-repeated, of the spec's physical and
  logical type (strings STRING, binary unannotated, times TIMESTAMP(MICROS, UTC), unsigned integers INT(n,
  false), signed ones INT(n, true) or unannotated), REQUIRED where the spec requires a value, present unless the
  spec lets a part lack it (`time_created`). Other columns are ignored. A disagreement is `KindSchema` naming the
  column and the part. It reads nothing beyond Verify's reads.
- **`AcceptWholeSource(kind, container)`.** The listing covers the whole of that source: `extensions.swath`
  present, `source.kind` and `source.container` equal to the arguments (a filesystem has no container: pass
  `""`), `scope.kind` `whole`, `coverage.complete` true, `coverage.stop_reason` `completed`,
  `coverage.skipped_entries` 0 and `coverage.filters` present and empty. An absent member refuses. The first
  failure is `KindCoverage` with `Column` naming the member (`coverage.filters`, say). Exclusions (what a
  listing leaves out by design, such as an output directory inside a listed tree) and `coverage.snapshot` (a
  listing is never a point-in-time snapshot) do not refuse.
- **`Stream`.** Current objects only: a versions schema is refused (`KindSchema`) before any read. Every object
  in dataset order through `seekparquet.Range`, 1,024 rows a request, each page after the previous page's last
  row (`Cursor`), projecting `size`, `last_modified` and, for S3 and Cloud Storage, `etag` and
  `storage_class`. The module checks every key page it decodes (order, bounds, page index). Each object must
  have a size, at least 0, and a modification time, when it has one, in the years 1 to 9999; else `KindRow`
  naming the column and the row. The first error ends the stream: `fn`'s and the module's
  (`*seekparquet.RefusalError`, storage) unchanged, and the context's, checked before each object.
  Each `Object` owns its key, a copy, so `fn` may keep or change it. `StreamStats` sums every Stream's
  requests, objects handed to `fn` (counted before the call) and `ReadStats`.
- **`Totals(paths)`.** Current objects only, as `Stream`. A path is taken verbatim: `""` selects every object,
  a path ending in `/` every object whose key begins with it (an object whose key is the path included), any
  other path only the object whose key equals it. Paths may repeat, nest and overlap; each total is independent.
  The rows of all paths are planned at once (`seekparquet.Dataset.PlanRanges`, projecting `size` and
  `last_modified`), which checks every part holding them, and scanned once, so a row several paths select is
  decoded once. `Count` is the plan's count of the path's range, exact once its parts are checked; `Bytes` the
  sum of the sizes; `NewestModified` the latest time, nil when the path selects nothing or any selected object
  has no time; `Exact` the object whose key is the path (never for `""`), with its ETag and storage class read by
  a plan of those objects alone. Each object is checked as `Stream` checks it (`KindRow`), and sizes whose sum
  overflows an int64 fail with `KindRow` naming `size` and no row. Totals is exact or fails: the module's
  refusals and the Source's errors unchanged, the context's error, no totals.
- **`Aggregates(prefixes, opt)`.** Current objects only. A prefix covers every object whose key begins with it,
  the one whose key is the prefix included; prefixes may repeat, nest and overlap. Nothing is read unless
  `Counts` or `Sums` is set. A count plans the prefix's range without columns, decoding the key pages at its
  ends and no value page, and opening no part between them: it is nil when a part holding the rows has not
  been checked by an earlier read (`CheckSchema`, `Stream`, a sum), since its row count is then the manifest's
  alone. Only a prefix with a known count is summed (a count of 0 sums to 0 without a read); a sum plans the
  rows projecting `size` and scans them once. `SumsAlways` sums every such prefix. `SumsWithinBudget` takes
  them in ascending key order and sums one when the compressed size-page bytes it adds to those already
  selected (`RangePlan.ProjectedBytes`: the pages holding the rows and each chunk's dictionary, an extent
  shared with a prefix already selected counted once) fit what remains of `SumBudgetBytes` (0:
  `DefaultSumBudget`); a prefix inside one already selected adds nothing. Footers, key pages and page indexes
  read to plan are not charged; a remaining budget below the smallest possible page plans nothing more, and a
  prefix whose size chunk has no OffsetIndex, so that its cost cannot be proven, is not summed under a budget.
  `Bytes` is nil for a prefix not summed and for a sum that overflows an int64; `Count` is reported only with
  `Counts`. A budget with another mode, a negative budget or an unknown mode wraps
  `seekparquet.ErrInvalidRequest`, before any read. A failure returns no aggregates.
- **Not checked.** The parts' SHA-256 digests: `seekparquet.Validate` (or `ValidateDir`) at `LevelDigest`
  compares them, and every part swath writes declares one. Rows no request decodes, and columns a request does
  not project (CHECK-4); `seekparquet.Validate` at `LevelProfile` decodes every key, at `LevelFull` every value.

## Rows

A nullable column is a pointer, nil for null; a required column (`is_latest`, `is_delete_marker`, `kind`) is a
value. Times are UTC `time.Time`s of the stored microseconds. Unsigned columns (`mode`, `uid`, `gid`, `nlink`,
`inode`, `dev`) are `uint32` and `uint64` over their full range. Keys and link targets are bytes, and need not
be UTF-8 for `swath-local/v1`. Byte values share the `seekparquet.Row`'s memory.

Every row keeps its key and its `Offset` in its group of equal keys, so a page that ends inside a group
continues from the row (`Cursor`). Rows of one key stay separate rows in file order: nothing collects a key's
versions, whose group can be longer than any page. An S3 delete marker's `Size` is nil, never 0; a GCS
generation is live when `TimeDeleted` is nil, and there is no `IsLatest` for it.

`Object` is the small projection a reader of current objects needs (key, size, modification time, ETag,
storage class), from `swath/s3-objects/v1`, `swath/gcs-objects/v1` and `swath-local/v1` (no ETag or storage
class): a versions row is not a current object, and `DecodeObject` refuses its schema id.

## Errors

A decoder fails with a `SemanticError` naming the column and the row: `KindSchema` when the row lacks a column
the schema id defines (its part lacks it, or the read did not project it) or holds a value of another type,
`KindRow` for a null in a required column. `time_created` is the one column a part may lack (it was added to
`swath/gcs-objects/v1` later); it reads as nil there. Columns a schema id does not define are ignored.

`ParseMetadata` reads every member of `extensions.swath` as a pointer (a slice: nil) that is nil when absent, so
a missing `coverage.complete` is never read as false; a member of the wrong JSON type is a `KindCoverage`
error. A manifest without the extension parses with `Swath` false. Unknown members (an older build's
`part_md5`) are ignored. `AcceptWholeSource`'s errors are `KindCoverage` with `Column` naming the member of
`extensions.swath` at fault; `CheckSchema`'s are `KindSchema` naming the column, with the part in the message.

## What it is not

Opening, validation (`seekparquet.Validate`), cursors, range and delimiter queries, page decoding and refusal
codes are the SeekParquet module's; storage is objfile's or the caller's. There is no publication, no provider
client, and no grouping of versions and no streaming or totals of them yet.

## Tests

`go test -race ./...` in this directory: the decoders on hand-built rows (nulls, binary keys, unsigned bounds,
absent, unprojected and mistyped columns), `ParseMetadata` on present, absent and malformed members, and
`TestNoCloudSDK`, which runs `go list -deps -test ./...` and `go list -m all` and fails on any `aws`,
`cloud.google.com` or `azure` path. The lister's `internal/listing` `TestReader*` tests decode datasets its own
writer wrote for all five schemas: every column, nulls, keys that are not UTF-8, unsigned bounds, a part
without `time_created` and one without `md5`, and S3 versions read in pages of 1, 4, 7 and 100 rows over
several parts, continuing inside key groups by `Offset`.

`dataset_test.go` writes datasets with the module's writer (`seekparquet/go/local`) to test `AcceptWholeSource`
on every member it requires, present and absent; `Open` on an unknown schema id; `CheckSchema` on key
uniqueness, the key column, an absent column, a column of another logical type, and a later part lacking a
column the module accepts; `Stream`'s order and values over pages, nulls, its row checks, cancellation, `fn`'s
error, a module refusal returned unchanged, and the refusal of a versions schema; and a part with corrupted
bytes caught by digest validation. The lister's `TestReaderStreams*` tests admit and stream datasets its own
writer wrote: S3 objects over three parts, GCS objects with and without `time_created`, filesystem entries
(one without a size refused), and both versions schemas admitted and refused by `Stream`.

`totals_test.go` and `aggregates_test.go` port varve's `internal/swath` totals and aggregates tests, each
naming the one it ports, over datasets of several parts cut inside prefixes, read through a `Source` that
records every open and read: `TestTotalsMatchStream` is the oracle, totals of root, nested, overlapping, exact,
repeated and absent paths equal to those computed from a full `Stream`; the selection of `a`, `a/`, `a/b` and
`ab/x`; missing times; overflow; Source failures and cancellation returned unchanged; nested paths reading
exactly root's size and time pages; the aggregates oracle on fresh and checked datasets; counts reading no value
page and opening no part between a prefix's ends, and nil across an unchecked part with a false manifest row
count; sums publishing nothing on failure; budgets read within, a budget of exactly the prefixes' projected bytes
summing them all (shared pages and dictionaries charged once), and a one-byte budget reading nothing beyond the
counts. varve's tests of its own page-index parsing are the module's now; `TestTotalsIgnoreManifestBoundExcludingKey`
shows the module's refusal of a part beyond its manifest bound returned unchanged.
