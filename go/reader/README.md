# reader

`github.com/varveio/swath/go/reader` is what a reader of swath datasets needs to know about swath on top of the
[SeekParquet](https://github.com/varveio/seekparquet) module: the schema ids and their columns, the manifest's
`extensions.swath`, each schema's rows as typed values decoded from a `seekparquet.Row`, and a `Dataset` that
opens a dataset, checks its schema, streams, scans and lists its current objects (or a filesystem's entries) and
totals them under paths and prefixes.

The reader opens and reads; it does not judge a dataset. `Metadata()` hands on what the manifest records about
the listing (source, scope, coverage with its filters such as `one_filesystem`, publication time), and the
application decides what to do with it: whether a partial or filtered listing will do, whether it is of the
source expected, and whether a dataset opened again is the one seen before. Part digests are for checking a
dataset after it is copied or moved (a planned `swath verify` command, #391), not for reading it.

The swath dataset profile this module implements (what a swath dataset is, the schema ids and their row
contracts, `extensions.swath`, key order, evolution) is defined in
[../docs/dataset.md](../docs/dataset.md). This README is the API.

It is a nested Go module. It depends on the SeekParquet module (and through it parquet-go) and the standard
library only: no cloud SDK, and not the swath lister (`github.com/varveio/swath/go`), which imports it instead.
The lister's producers bind their schemas to the specs here (`internal/schema.Bind`), so what swath writes and
what a reader decodes are described once.

## API

```go
type SchemaID string // S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, LocalV1
type ColumnType uint8 // TypeString, TypeInt32, TypeInt64, TypeTimestampMicros, TypeBool, TypeUint32, TypeUint64, TypeBinary
type ColumnSpec struct { Name string; Type ColumnType; Required, MayBeAbsent bool }
type SchemaSpec struct { ID SchemaID; Key ColumnSpec; UniqueKeys bool; MaxKeyBytes int; Columns []ColumnSpec; Versions VersionSpec }
type VersionSpec struct { ID, Latest, DeleteMarker, DeletedAt string } // a versions schema's role columns; zero: not versions
func Schema(id SchemaID) (SchemaSpec, bool)       // a copy; false for an id swath does not write
func (SchemaSpec) Column(name string) (ColumnSpec, bool)
func (SchemaSpec) ColumnNames() []string          // the projection the decoders read

const Extension = "swath"
const MetadataVersion = 1                          // extensions.swath.version this module reads
type Metadata struct { Manifest seekparquet.Metadata; SchemaID SchemaID; Swath bool
	PublishedAt *time.Time; Source *SourceInfo; Scope *Scope; Coverage *Coverage }
func ParseMetadata(m seekparquet.Metadata) (Metadata, error)

// The module's types and errors, as aliases (the module's own types: errors.Is and errors.As match either name).
type Source, SourceFile, ContextBinder, Options, Limits, ReadStats, Cursor, RefusalError, UnsupportedError = seekparquet.…
var ErrClosed, ErrInvalidRequest, ErrLimitExceeded = seekparquet.…

type Position struct { Key []byte; Offset int64 } // embedded in every row
func (Position) Cursor() *Cursor

type Object, S3Object, S3Version, GCSObject, GCSGeneration, AzblobObject, LocalEntry
func DecodeObject(id SchemaID, row seekparquet.Row) (Object, error) // s3-objects/v1, gcs-objects/v1, azblob-objects/v1, swath/local-entries/v1
func DecodeS3Object(row seekparquet.Row) (S3Object, error)
func DecodeS3Version(row seekparquet.Row) (S3Version, error)
func DecodeGCSObject(row seekparquet.Row) (GCSObject, error)
func DecodeAzblobObject(row seekparquet.Row) (AzblobObject, error)
func DecodeGCSGeneration(row seekparquet.Row) (GCSGeneration, error)
func DecodeLocalEntry(row seekparquet.Row) (LocalEntry, error)

func Open(ctx context.Context, src Source, opt Options) (*Dataset, error)
func OpenDir(ctx context.Context, path string, opt Options) (*Dataset, error)
func (*Dataset) Metadata() Metadata
func (*Dataset) SeekParquet() *seekparquet.Dataset // borrowed: the Dataset closes it
func (*Dataset) CheckSchema(ctx context.Context) error
func (*Dataset) Stream(ctx context.Context, fn func(Object) error) error
func (*Dataset) StreamLocal(ctx context.Context, fn func(LocalEntry) error) error // swath/local-entries/v1 only
func (*Dataset) StreamStats() StreamStats // { Requests, Objects int64; ReadStats }
type ScanRequest struct { Prefix []byte; Workers int; Stats *ReadStats } // Workers <= 0: 1
func (*Dataset) Scan(ctx context.Context, req ScanRequest, fn func(batch []Object) error) error
type ListRequest struct { Prefix []byte; Delimiter byte; Cursor *Cursor; Limit int; Stats *ReadStats }
type Entry struct { Object *Object; Local *LocalEntry; CommonPrefix []byte } // an object or entry, a prefix, or a prefix with its exact-key row
func (Entry) Cursor() *Cursor
func (*Dataset) List(ctx context.Context, req ListRequest) ([]Entry, error)
func (*Dataset) Totals(ctx context.Context, paths []string) ([]PathTotals, error)
func (*Dataset) Aggregates(ctx context.Context, prefixes [][]byte, opt AggregateOptions) ([]PrefixAggregate, error)
type PageRequest struct { ListRequest; Aggregates AggregateOptions }
type PageEntry struct { Entry; Aggregate *PrefixAggregate } // nil for an ordinary object
type Page struct { Entries []PageEntry; Next *Cursor }       // Next nil on the last page
func (*Dataset) Page(ctx context.Context, req PageRequest) (Page, error)
func (*Dataset) Close() error

type SemanticError struct { Kind Kind; Column string; Row *Position; Err error }
const KindSchema, KindMetadata, KindRow Kind = "schema", "metadata", "row"
var ErrSchema, ErrMetadata, ErrRow *SemanticError // errors.Is targets, by kind
func IsRefusal(err error) bool

type PathTotals struct { Count int64; Bytes *int64; NewestModified *time.Time; Exact *Object } // Bytes nil: an entry without a size
type SumMode int // SumsOff, SumsWithinBudget, SumsAlways
const DefaultSumBudget = 4 << 20
type AggregateOptions struct { Counts bool; Sums SumMode; SumBudgetBytes int64 }
type PrefixAggregate struct { Count, Bytes *int64 } // nil: not requested or not known exactly

```

A reader of current objects opens the dataset, decides from its metadata whether it will do, and streams it:

```go
d, err := reader.OpenDir(ctx, dir, reader.Options{}) // or reader.Open over a reader.Source (objfile)
defer d.Close()
m := d.Metadata()                        // m.Source, m.Scope, m.Coverage: the application's to judge
err = d.CheckSchema(ctx)                 // once, before reading, to check every part
err = d.Stream(ctx, func(o reader.Object) error { /* ... */ return nil })
entries, err := d.List(ctx, reader.ListRequest{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000})
// the next page: ListRequest{..., Cursor: entries[len(entries)-1].Cursor()}
err = d.Scan(ctx, reader.ScanRequest{Prefix: []byte("logs/"), Workers: runtime.GOMAXPROCS(0)},
	func(batch []reader.Object) error { /* called concurrently; do not keep batch */ return nil })
```

A filesystem's entries (`swath/local-entries/v1`) stream as `LocalEntry`, every column with its nulls, and list
as `Entry.Local`, a directory's prefix `d/` paired with its own row:

```go
err = d.StreamLocal(ctx, func(e reader.LocalEntry) error { /* e.Kind, e.Size (nil under --metadata names), e.LinkTarget */ return nil })
```

A browsing reader pages with each prefix's totals:

```go
page, err := d.Page(ctx, reader.PageRequest{
	ListRequest: reader.ListRequest{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000},
	Aggregates:  reader.AggregateOptions{Counts: true, Sums: reader.SumsWithinBudget},
})
// the next page: PageRequest{..., Cursor: page.Next}, while page.Next != nil
if reader.IsRefusal(err) { /* the data is refused as it is */ }
```

A count across a part no read has checked is unknown (nil); run `CheckSchema` first to make the first page's
counts exact.

A reader of anything else (versions, a key range, other columns) queries the module's dataset and decodes its
rows, projecting `SchemaSpec.ColumnNames()`:

```go
spec, ok := reader.Schema(d.Metadata().SchemaID) // S3VersionsV1, say
rows, err := d.SeekParquet().Range(ctx, seekparquet.Range{Prefix: []byte("logs/"), Limit: 1000, Columns: spec.ColumnNames()})
for _, r := range rows {
	v, err := reader.DecodeS3Version(r)
	// ...
}
// the next page: Range{..., Cursor: last.Cursor()}
```


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
not a delete marker, noncurrent when neither. Azure Blob supports current objects only; there is no registered
Azure versions schema.

## What is checked, and when

- **`Open`, `OpenDir`.** The module reads and checks the manifest and opens no part
  (`seekparquet.OpenDataset`, `OpenDir`); its refusals are returned unchanged. `extensions.swath` is parsed:
  its `version` must be the integer 1 (`MetadataVersion`), else, absent included, `KindSchema` ("unsupported
  swath metadata version"), before any other member is read; a member of the wrong JSON type is a
  `KindMetadata` error. Every other member is optional: a manifest without `published_at`, or without the
  extension, opens. The schema id must be one swath writes (else `KindSchema`). A generic SeekParquet read (`seekparquet.OpenDataset`)
  does not read the extension and is unaffected.
- **`CheckSchema`** (on request, before reads). The manifest's key column and key uniqueness are the spec's, so a versions
  dataset (keys repeat) never passes as current objects. Then `seekparquet.Dataset.Verify` checks every part
  (size, footer, stamps with the schema id, key column, each row group's first key page), and each part's columns
  are read from the footer the module checked and reads the part through (`seekparquet.Dataset.Columns`, legacy
  converted types folded into logical ones), so a file put at a part's path later is never what is checked:
  every spec column top-level and non-repeated, of the spec's physical and
  logical type (strings STRING, binary unannotated, times TIMESTAMP(MICROS, UTC), unsigned integers INT(n,
  false), signed ones INT(n, true) or unannotated), REQUIRED where the spec requires a value, present unless the
  spec lets a part lack it (`time_created`). Other columns are ignored. A disagreement is `KindSchema` naming the
  column and the part. It reads nothing beyond Verify's reads.
- **`Stream`.** Current objects only: a versions schema, and a filesystem's entries (`StreamLocal`'s), are
  refused (`KindSchema`) before any read. Every object
  in dataset order through `seekparquet.Range`, 1,024 rows a request, each page after the previous page's last
  row (`Cursor`), projecting `size`, `last_modified` and, for S3 and Cloud Storage, `etag` and
  `storage_class`. The module checks every key page it decodes (order, bounds, page index). Each object must
  have a size, at least 0, and a modification time, when it has one, in the years 1 to 9999; else `KindRow`
  naming the column and the row. The first error ends the stream: `fn`'s and the module's
  (`*seekparquet.RefusalError`, storage) unchanged, and the context's, checked before each object.
  Each `Object` owns its key, a copy, so `fn` may keep or change it. `StreamStats` sums every Stream's
  (and StreamLocal's) requests, objects handed to `fn` (counted before the call) and `ReadStats`.
- **`StreamLocal`.** `swath/local-entries/v1` only (any other schema id is `KindSchema`, before any read). Every
  entry as `Stream` reads objects, projecting every column of the schema, each `LocalEntry` owning its key.
  Nulls are kept: under `--metadata names` the stat columns, `Size` and `LastModified` included, are nil, and a
  target is nil but for a symlink listed with targets. `Kind` must be one of the schema's eight values
  (`UNKNOWN` included), a size, when present, at least 0 and a modification time, when present, in the years 1
  to 9999 (`KindRow`). Its errors are `Stream`'s.
- **`Scan(req)`.** Current objects only, refused as `Stream` refuses, before any read; `Stats` reset first. One
  plan of `Prefix`'s rows (`seekparquet.Dataset.PlanRanges`, projecting the columns `Object` holds, which checks
  every part holding them), divided into at most `Workers` shards (`RangePlan.Split`: consecutive rows cut at row
  group boundaries, balanced by projected bytes; `Workers` 0 or less is 1, and a plan with fewer row groups has
  fewer shards), each scanned by its own goroutine. Each object is decoded and checked as `Stream` checks it and
  handed to `fn` once, in batches of at most 1,024: a shard's batches in ascending key order, one at a time,
  shards' batches concurrently and in no set order, so `fn` must be safe for concurrent use. A batch's slice is
  reused once `fn` returns: `fn` must not keep it (an `Object` copied out of it owns its memory). The first
  error cancels the other shards and is returned: `fn`'s and the module's unchanged, a row's `KindRow`, the
  context's (checked before each batch). Batches already handed on stay handed on; a failed scan is no result.
  `Stats` sums the plan's, the split's and every shard scan's `ReadStats`, on failure too, with Page's exception:
  a plan (`PlanRanges`) or split (`Split`) that fails returns no statistics, so its own reads are missing, while
  every completed plan, split and shard scan is counted. The context is checked before planning, before each
  batch and after the shards end: a scan whose context ended never succeeds, even one that read nothing. `Scan`
  does not add to `StreamStats`.
- **`List(req)`.** Current objects only, as `Stream`: a versions schema is refused (`KindSchema`) before any
  read. One request of the module's delimited listing (`seekparquet.Dataset.List`), which routes it, skips each
  common prefix's keys and rolls them up (a key whose bytes after `Prefix` contain `Delimiter` yields the prefix
  up to and including the first one; `Delimiter` 0 is the NUL byte, a delimiter like any other, not "no
  rollup"), projecting the columns `Object` holds, or, for `swath/local-entries/v1`, every column of `LocalEntry`.
  Entries are in ascending byte order, at most `Limit`; each is an `Object` (a `Local` entry for a filesystem) or
  a `CommonPrefix`, owning its bytes. A `CommonPrefix` also has `Object` (`Local`) set to the row stored at the
  prefix's exact key (an S3-style `a/` folder object under its children, or a directory's own row `d/`, so an
  empty directory lists as its prefix with its row; no directory metadata is made up for a prefix), from
  the module's exact row (`seekparquet.Entry.Exact`, spec LIST-8) in the same request, when that object is the
  first key the listing reached under the prefix. That holds without a cursor and after every cursor a page
  returned: a prefix cursor resumes after all of its prefix's keys, and an object's row cursor never lies inside
  a prefix of the same listing. Only a cursor made up inside a prefix's keys (a row cursor, or a prefix cursor
  for a deeper prefix such as `a/b/` under `a/`) skips the object, and the prefix then comes without it. `Entry.Cursor` is the continuation after the entry: the row cursor (key and offset)
  after an object, the prefix cursor after a common prefix (its exact-key object included), so the next page
  starts after every key that rolls up to it; repeating the request with each page's
  last cursor lists every entry once; a cursor before `Prefix`'s keys lists from the first of them, one after
  them lists nothing. Each object is checked as `Stream` checks it, each entry as `StreamLocal` does; a row
  that fails is the `KindRow` error and no entries. The module's errors are returned unchanged: `*RefusalError`, `ErrClosed`
  after `Close`, `ErrInvalidRequest` (a `Limit` that is not positive, a malformed cursor), the Source's and the
  context's. `Stats`, when set, is reset when the request starts (a refusal before any read leaves it zero) and receives
  the request's `ReadStats`, whether it succeeds or fails; `List` does
  not add to `StreamStats`.
- **`Totals(paths)`.** Current objects only, as `Stream`. A path is taken verbatim: `""` selects every object,
  a path ending in `/` every object whose key begins with it (an object whose key is the path included), any
  other path only the object whose key equals it. Paths may repeat, nest and overlap; each total is independent.
  The rows of all paths are planned at once (`seekparquet.Dataset.PlanRanges`, projecting `size` and
  `last_modified`), which checks every part holding them, and scanned once, so a row several paths select is
  decoded once. `Count` is the plan's count of the path's range, exact once its parts are checked; `Bytes` the
  sum of the sizes (nil when a selected filesystem entry has none); `NewestModified` the latest time, nil when the path selects nothing or any selected object
  has no time; `Exact` the object whose key is the path (never for `""`), with its ETag and storage class read by
  a plan of those objects alone. Each object is checked as `Stream` checks it (`KindRow`), and sizes whose sum
  overflows an int64 fail with `KindRow` naming `size` and no row. Totals is exact or fails: the module's
  refusals and the Source's errors unchanged, the context's error, no totals. **Local totals**
  (`swath/local-entries/v1`): an object is an entry of any kind; `Count` includes a directory's own row `d/` for
  the path `d/`, once, and `""` counts the entries below the root, which has no row; `Bytes` is the entries'
  summed `st_size`, directories' own sizes included: entry bytes, not the files' content bytes and not disk usage
  (`allocated_bytes`). An entry without a size (`--metadata names`) makes `Bytes` nil, never 0, and an entry
  without a time `NewestModified` nil. `Exact` holds the entry's key, size and time.
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
  read to plan are not charged; a prefix whose pages are all charged already adds nothing, a budget below the
  smallest possible page plans nothing, and a prefix whose size chunk has no OffsetIndex, so that its cost
  cannot be proven, is not summed under a budget.
  `Bytes` is nil for a prefix not summed, for a sum over a filesystem entry without a size, and for a sum that
  overflows an int64; a filesystem's counts and sums are the local totals of `Totals`. `Count` is reported only
  with `Counts`. A budget with another mode, a negative budget or an unknown mode wraps
  `seekparquet.ErrInvalidRequest`, before any read. A failure returns no aggregates. Both calls fail first on a
  context already ended (its error) or a closed `Dataset` (`seekparquet.ErrClosed`), whatever the request.
- **`Page(req)`.** `List` with one entry beyond `req.Limit`, trimmed to `Limit`; `Next` is the last returned
  entry's `Cursor()` only when that extra entry existed, so the last page has `Next` nil and is never followed by
  an empty one. Then one `Aggregates` call with `req.Aggregates` for exactly the common prefixes returned (a prefix
  carrying its exact-key object included; an entry beyond the limit is never aggregated): each prefix entry's
  `Aggregate` is its `PrefixAggregate`, whose fields are nil as `Aggregates` leaves them (not requested, not
  known exactly, not within the sum budget), and an ordinary object's `Aggregate` is nil. The budget is
  `AggregateOptions`', not a bound on the page's reads. A `Limit` that is not positive or is `math.MaxInt`, and
  an invalid `AggregateOptions`, wrap `ErrInvalidRequest` before any read. Otherwise the errors are `List`'s and
  `Aggregates`'; a context that ends between the two is its error. No entries come with an error. `Stats` is
  reset at the start and receives the listing's and the aggregation's statistics, on failure too, except that
  an aggregation plan (`seekparquet.Dataset.PlanRanges`) that fails while planning returns no statistics: its own
  planning reads are missing, while the listing's and every completed plan's and scan's are counted.
- **Not checked.** The parts' digests, by any call: they are for checking a dataset after it is copied or moved.
  Whether the listing's source, scope and coverage suit the application: that is the caller's. Rows no request
  decodes, and columns a request does not project (CHECK-4); `seekparquet.Validate` at `LevelProfile` decodes every key, at `LevelFull`
  every value.

## Rows

A nullable column is a pointer, nil for null; a required column (`is_latest`, `is_delete_marker`, `kind`) is a
value. Times are UTC `time.Time`s of the stored microseconds. Unsigned columns (`mode`, `uid`, `gid`, `nlink`,
`inode`, `dev`) are `uint32` and `uint64` over their full range. Keys and link targets are bytes, and need not
be UTF-8 for `swath/local-entries/v1`. Byte values share the `seekparquet.Row`'s memory.

Every row keeps its key and its `Offset` in its group of equal keys, so a page that ends inside a group
continues from the row (`Cursor`). Rows of one key stay separate rows in file order: nothing collects a key's
versions, whose group can be longer than any page. An S3 delete marker's `Size` is nil, never 0; a GCS
generation is live when `TimeDeleted` is nil, and there is no `IsLatest` for it.

`Object` is the small projection a reader of current objects needs (key, size, modification time, ETag,
storage class; an Azure blob's access tier), from `swath/s3-objects/v1`, `swath/gcs-objects/v1` and
`swath/azblob-objects/v1`: a versions row is not a current object, and `DecodeObject` refuses its schema id.
`DecodeObject` also projects a `swath/local-entries/v1` row (no ETag or storage class), as `Totals` reports an
exact entry, while `StreamLocal` and `List` hand on a filesystem's entries whole, as `LocalEntry`. A local key
is the path relative to the root as bytes, unnormalized, a directory's ending in `/`; a link target is bytes,
written with `/` separators by the portable engine on Windows (`filepath.ToSlash`).

## Errors

A decoder fails with a `SemanticError` naming the column and the row: `KindSchema` when the row lacks a column
the schema id defines (its part lacks it, or the read did not project it) or holds a value of another type,
`KindRow` for a null in a required column. `time_created` is the one column a part may lack (it was added to
`swath/gcs-objects/v1` later); it reads as nil there. Columns a schema id does not define are ignored.

`ParseMetadata` first requires `extensions.swath.version` to be 1 (`KindSchema` otherwise): the version covers
the extension's own contract, while a change to a schema's rows takes a new schema id. It reads every other
member of `extensions.swath` as a pointer (a slice: nil) that is nil when absent, so a missing
`coverage.complete` is never read as false; a member of the wrong JSON type is a `KindMetadata` error. A
manifest without the extension parses with `Swath` false. Unknown members are ignored. `CheckSchema`'s errors
are `KindSchema` naming the column, with the part in the message.

`IsRefusal(err)` partitions the errors: true when `err` wraps a `*SemanticError` or a `*RefusalError`, data
that is refused as it is. It is false for everything else: an
`*UnsupportedError` (a valid dataset using a feature the module does not read; inspect it with `errors.As`, since
unsupported is not malformed), `ErrClosed`, `ErrInvalidRequest`, `ErrLimitExceeded`, the context's errors, and the
filesystem's and storage's. False does not mean retryable. The reader does not import objfile, so objfile's
`ErrObjectChanged` and `ErrInvalidSnapshot` are the caller's to classify.

## What it is not

Opening, validation (`seekparquet.Validate`), cursors, range and delimiter queries, page decoding and refusal
codes are the SeekParquet module's (this package adds swath's checks around them and names the module's types by
alias); storage is objfile's or the caller's. There is no publication, no provider
client, and no grouping of versions and no streaming or totals of them yet.

## Tests

`go test -race ./...` in this directory: the decoders on hand-built rows (nulls, binary keys, unsigned bounds,
absent, unprojected and mistyped columns), `ParseMetadata` on present, absent and malformed members and on unsupported versions, and
`TestNoCloudSDK`, which runs `go list -deps -test ./...` and `go list -m all` and fails on any `aws`,
`cloud.google.com` or `azure` path. The lister's `internal/listing` `TestReader*` tests decode datasets its own
writer wrote for all five schemas: every column, nulls, keys that are not UTF-8, unsigned bounds, a part
without `time_created` and one without `md5`, and S3 versions read in pages of 1, 4, 7 and 100 rows over
several parts, continuing inside key groups by `Offset`.

`dataset_test.go` writes datasets with the module's writer (`seekparquet/go/local`) to test `Open` on a partial,
filtered (`one_filesystem`), prefix-scoped listing without `published_at` (two parts, the manifest alone opened,
the coverage handed on by `Metadata`) and on an unknown schema id; `CheckSchema` on key
uniqueness, the key column, an absent column, a column of another logical type, and a later part lacking a
column the module accepts; `Stream`'s order and values over pages, nulls, its row checks, cancellation, `fn`'s
error, a module refusal returned unchanged, and the refusal of a versions schema.

`local_test.go` writes a filesystem tree (directories with their own rows, an empty one, a name that is not
UTF-8, a symlink with its target, a FIFO) over two parts, with every stat column and as `--metadata names`
writes it: `StreamLocal` decodes every column and null; `Stream` and `Scan` refuse the entries and `StreamLocal`
objects, before any read; `Page` lists it by `/` in pages of 1, 2 and 100, each directory paired with its row and
each prefix's totals nil-summed under `names`; `Totals` match the `StreamLocal` oracle, a directory counted once
with its own row and entry bytes nil, never 0, without sizes; a kind the schema does not define fails
`StreamLocal` and `List` (`KindRow`). `scan_test.go` checks `Scan` against `Stream` for
0, 1, 2, 7 and 64 workers and several prefixes (each object once, each batch ascending), its statistics against
a plan, split and shard scans made by hand, a failing row, `fn`'s error stopping each shard at its first batch,
cancellation (by `fn` on the last batch, and before a scan of an empty dataset: never a success), a Source
failure while planning (returned unchanged, the failed plan's reads absent from `Stats`) and a versions refusal
before any read; `BenchmarkScan` tallies 300,000 objects with one worker and
`GOMAXPROCS`. `TestAzblobTotals` totals an Azure dataset against `Stream` (an exact blob's access tier as its
storage class) and its prefixes' aggregates.

`list_test.go` lists keys holding `a`, `a/`, `a/b`, `a/b0` and `ab/x` over three parts against an oracle, by
several prefixes, in pages of 1, 2, 3 and 100 entries continuing after objects and prefixes; checks an entry's
values, that changing a returned key or prefix changes neither its cursor nor the next page, cursors before and
after a prefix's keys, a NUL delimiter, the refusal of a versions schema before any read (statistics reset), a row check failing the listing with its statistics kept, a missing part's
refusal returned unchanged, and cancellation, a limit of 0 and a closed `Dataset`. The lister's
`TestReaderStreams*` tests open, check and stream datasets its own writer wrote, with the metadata it recorded: S3 objects over three parts, GCS objects with and without `time_created`, filesystem entries
through `StreamLocal` (kinds, a link target, one without a size kept nil; `Stream` refuses them), and both
versions schemas opened and refused by `Stream`.

`errors_test.go` tables `IsRefusal`. `page_test.go` pages `list_test.go`'s keys in pages of 1, 2, 3 and 100 against the oracle with each
prefix's totals against `Stream`'s, `Next` nil only on the last page; checks that a page whose extra entry is a
prefix reads exactly what its listing reads, nil sums under a one-byte budget, statistics holding the
aggregation, a cancellation at the listing's last read (the context's error, no entries, the listing's
statistics), and invalid requests before any read.

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
