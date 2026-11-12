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
dataset after it is copied or moved (a planned `swath verify` command, [#391](https://github.com/varveio/swath/issues/391), not yet implemented), not for reading it.

The swath dataset profile this module implements (what a swath dataset is, the schema ids and their row
contracts, `extensions.swath`, key order, evolution) is defined in
[../docs/dataset.md](../docs/dataset.md). This README is the API.

It is a nested Go module. It depends on the SeekParquet module (and through it parquet-go) and the standard
library only: no cloud SDK, and not the swath lister (`github.com/varveio/swath/go`), which imports it instead.
The lister's producers bind their schemas to the specs here (`internal/schema.Bind`), so what swath writes and
what a reader decodes are described once.

## API

```go
type SchemaID string // S3ObjectsV1, S3VersionsV1, GCSObjectsV1, GCSGenerationsV1, AzblobObjectsV1, AzblobVersionsV1, LocalV1
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
func DecodeAzblobVersion(row seekparquet.Row) (AzblobVersion, error)
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
type ObjectFields uint8 // optional fields; key and size always read and checked
const (FieldLastModified ObjectFields = 1 << iota; FieldETag; FieldStorageClass)
const AllObjectFields = FieldLastModified | FieldETag | FieldStorageClass
type BatchScanRequest struct { Prefix []byte; Workers int; Stats *ReadStats; Fields ObjectFields }
func (*Dataset) ScanBatches(ctx context.Context, req BatchScanRequest, fn func(*ObjectBatch) error) error
func (*ObjectBatch) Fields() ObjectFields
func (*ObjectBatch) Len() int
func (*ObjectBatch) Key(i int) []byte
func (*ObjectBatch) Size(i int) int64
func (*ObjectBatch) LastModified(i int) (micros int64, ok bool)
func (*ObjectBatch) ETag(i int) ([]byte, bool)
func (*ObjectBatch) StorageClass(i int) ([]byte, bool) // access_tier on Azure
func (*ObjectBatch) Cursor(i int) *Cursor // owning copy
type ListRequest struct { Prefix []byte; Delimiter byte; Cursor *Cursor; Limit int; Stats *ReadStats; ReadAhead int }
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

A reader opens the dataset, decides from its metadata whether it will do, checks it and reads it.
[example_test.go](example_test.go) does that over a small committed dataset (`testdata/photos`), and lists it
by delimiter a page at a time; `go test` runs both.

For a whole-run tally without constructing Objects, select only the optional fields it needs:

```go
err = d.ScanBatches(ctx, reader.BatchScanRequest{
	Workers: runtime.GOMAXPROCS(0),
	Fields: reader.FieldLastModified | reader.FieldStorageClass,
}, func(b *reader.ObjectBatch) error {
	for i := 0; i < b.Len(); i++ {
		key, size := b.Key(i), b.Size(i)
		micros, hasTime := b.LastModified(i)
		class, hasClass := b.StorageClass(i)
		// Tally here; copy key/class bytes if keeping them after this callback.
		_, _, _, _, _, _ = key, size, micros, hasTime, class, hasClass
	}
	return nil
})
```

Zero `Fields` selects key and size only; `AllObjectFields` adds last_modified, etag and storage class.
`Fields()` reports that selection. Optional accessors return `ok == false` for null, and panic for an
unrequested field; empty bytes with `ok == true` are an empty value. Every indexed accessor panics outside
`[0, Len())`. Keys and byte fields borrow decoder memory: the batch and its values must not escape the
callback, including through a goroutine. Copy bytes explicitly to keep them; `Cursor(i)` owns its key.
There is no Object materialization on this path. Existing `Scan` and `Stream` still return complete,
owning Objects and take no field selection.

A filesystem's entries (`swath/local-entries/v1`) stream as `LocalEntry`, every column with its nulls, and list
as `Entry.Local`, a directory's prefix `d/` paired with its own row:

```go
err = d.StreamLocal(ctx, func(e reader.LocalEntry) error { /* e.Kind, e.Size (nil under --metadata names), e.LinkTarget */ return nil })
```

A browsing reader pages with each prefix's totals:

```go
page, err := d.Page(ctx, reader.PageRequest{
	ListRequest: reader.ListRequest{Prefix: []byte("logs/"), Delimiter: '/', Limit: 1000,
		OmitFields: reader.FieldETag | reader.FieldStorageClass},
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
q := seekparquet.Query{Selection: seekparquet.Selection{Span: seekparquet.PrefixSpan([]byte("logs/"))},
	Limit: 1000, Columns: spec.ColumnNames()}
rows, err := d.SeekParquet().Rows(ctx, q, nil)
for _, r := range rows {
	v, err := reader.DecodeS3Version(r)
	// ...
}
// the next page: q.Cursor = last.Cursor()
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
not a delete marker, noncurrent when neither. `swath/azblob-versions/v1` declares `{ID: "version_id", Latest:
"is_latest"}` and includes versions, snapshots and soft-deleted blobs; interpret its additional fields with the
Azure versions schema in the [dataset profile](../docs/dataset.md#schema-registry).

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
  spec lets a part lack it (`ColumnSpec.MayBeAbsent`: GCS `time_created` and the later Azure current-object
  properties). Other columns are ignored. A disagreement is `KindSchema` naming the
  column and the part. It reads nothing beyond Verify's reads.
- **`Stream`.** Current objects only: a versions schema, and a filesystem's entries (`StreamLocal`'s), are
  refused (`KindSchema`) before any read. Every object in dataset order in one ordered pass of borrowed
  batches (`seekparquet.Dataset.Scan`, no plan and no limit), projecting `size`, `last_modified` and, for S3,
  Cloud Storage and Azure, `etag` and the storage class (Azure's `access_tier`). The pass opens each part, row group
  and projected column chunk when it reaches it, so a part is checked then, not before the first object. The
  module checks every key page it decodes (order, bounds, page index). Each row is decoded by the schema's
  row decoder and checked: a size, at least 0, and a modification time, when it has one, in the
  years 1 to 9999; else `KindRow` naming the column and the row (the error owns its key). The first error ends
  the stream: `fn`'s and a row's unchanged, the module's (`*seekparquet.RefusalError`, storage, `ErrClosed`)
  unchanged, and the context's, checked before each object (so a dataset with no objects streams
  successfully even with an ended context). Objects already handed on stay handed on: every
  object before a failing row, and every object of the pages before a failing part, page or read. Each
  `Object` owns its memory (its key a copy), so `fn` may keep or change it. `fn` runs inside the read, as
  `ScanBatches`' callback does: `Close` from another goroutine waits for the running `fn` to return (if it
  returns nil, the rest of its batch is still handed on, then the stream ends with `ErrClosed`), and `fn` must not call
  `Close` or wait for a goroutine that does. `StreamStats` sums every Stream's (and StreamLocal's) requests
  (`Dataset.Scan` calls: one for each stream that reaches the read, whatever it ends in: an empty dataset, a
  context ended before it starts, a failure; none for a refusal before the read), objects handed to `fn` (counted
  before the call) and `ReadStats` (the reads', failures included). A stream's fields are added together, once,
  when it returns: `StreamStats` holds nothing of a stream still running.
- **`StreamLocal`.** `swath/local-entries/v1` only (any other schema id is `KindSchema`, before any read). Every
  entry as `Stream` reads objects, projecting every column of the schema, each `LocalEntry` owning its key.
  Nulls are kept: under `--metadata names` the stat columns, `Size` and `LastModified` included, are nil, and a
  target is nil but for a symlink listed with targets. `Kind` must be one of the schema's eight values
  (`UNKNOWN` included), a size, when present, at least 0 and a modification time, when present, in the years 1
  to 9999 (`KindRow`). Its errors are `Stream`'s.
- **`Scan(req)`.** Current objects only, refused as `Stream` refuses, before any read; `Stats` reset first. One
  plan of `Prefix`'s rows (`seekparquet.Dataset.Plan`, projecting the columns `Object` holds, which checks
  every part holding them), divided into shards (`Plan.Split`: consecutive rows cut at row group boundaries,
  balanced by projected bytes): one for one worker, otherwise at most eight a worker (`shardsPerWorker`), and a
  plan with fewer row groups has fewer shards. `Workers` goroutines (0 or less is 1) each take the next shard not
  yet taken until none is left, so a worker whose shards go fast takes more of them instead of waiting on a slow
  one; at most `Workers` calls of `fn` run at once. A worker keeps its batch slice from shard to shard; a shard's
  SeekParquet page buffers live while it is scanned, so at most `Workers` sets at once. Each object is decoded and checked as `Stream` checks it and
  handed to `fn` once, in batches of at most 1,024: a shard's batches in ascending key order, one at a time,
  shards' batches concurrently and in no set order, so `fn` must be safe for concurrent use. A batch's slice is
  reused once `fn` returns: `fn` must not keep it (an `Object` copied out of it owns its memory). The first
  error cancels the other shards and is returned: `fn`'s (`seekparquet.Stop` included: no error of `fn` ends
  a scan successfully) and the module's unchanged, a row's `KindRow`, the
  context's (checked before each batch). Each shard's scan checks its own keys' order; when every shard has
  succeeded, each shard's first key must be above the last key of the shard before it (`shardOrder`), or the
  scan fails with SeekParquet's `key_order` refusal naming that key (at no part or row group), after every
  batch was handed on: a key out of order where a row group begins, which no single shard sees, is refused
  as one pass refuses it. `ScanBatches` and `Totals` share this check. Batches already handed on stay handed
  on; a failed scan is no result.
  `Stats` sums the plan's, the split's and every shard scan's `ReadStats`, on failure too, a failed plan's or
  split's own reads included. The context is checked before planning, before each
  batch and after the shards end: a scan whose context ended never succeeds, even one that read nothing. `Scan`
  does not add to `StreamStats`.
- **`ScanBatches(req)`.** The same current-object scope, parallel ordering, cancellation, error propagation
  and statistics as `Scan`, with a separate field selection. Unknown field bits are `ErrInvalidRequest`
  before planning. Size is always projected and checked present and non-negative. Only requested optional
  fields are projected and checked against swath's schema; requested `last_modified` values must be in years
  1..9999. Other optional fields, including owners and checksums, are not read or checked against swath's
  schema. SeekParquet's structural checks still apply, including cross-part physical column consistency.
  The manifest's key declaration and each selected part's key and projected columns are checked against the
  swath schema, including logical types and all-null columns. **This is stricter than `Scan`**, whose
  existing row decoder remains unchanged: `Scan` accepts an all-null INT64 `etag`, an INT64 `last_modified`
  without its timestamp annotation, a BYTE_ARRAY key without STRING, and a current-object manifest declaring
  `key_unique: false`; `ScanBatches` refuses these with `KindSchema` (for optional fields, when selected).
  A selected column absent from its part is `KindSchema`: none of Object's selectable columns is absence-tolerant. The absence-tolerant columns (GCS's `time_created`, the later Azure
  properties) are outside this projection. Part schema errors name the part path and have `Row == nil`, independent of
  which shard encounters the part first; row errors identify the bad key and offset, owning their key bytes.
  Checks run by column before the callback, so with several invalid rows the error and earlier delivered rows may differ from `Scan`.
  Each borrowed batch stays within one row group, key page, page of every projected column and selected span;
  it may be shorter, with no fixed size. Concurrent calls and shards have independent borrowed storage.
  The callback must not call `Close` or wait for a goroutine that does, since `Close` waits for active scans.
  Return an error or cancel first, then close after the scan returns. If another goroutine calls `Close`,
  a later part's schema check may return `ErrClosed` while `Close` waits for active callbacks to finish.
  Like `Scan`, this adds no `StreamStats`.
- **`List(req)`.** Current objects only, as `Stream`: a versions schema is refused (`KindSchema`) before any
  read. One request of the module's delimited listing (`seekparquet.Dataset.List`), which routes it, skips each
  common prefix's keys and rolls them up (a key whose bytes after `Prefix` contain `Delimiter` yields the prefix
  up to and including the first one; `Delimiter` 0 is the NUL byte, a delimiter like any other, not "no
  rollup"), projecting the columns `Object` holds, or, for `swath/local-entries/v1`, every column of `LocalEntry`.
  Entries are in ascending byte order, at most `Limit`; each is an `Object` (a `Local` entry for a filesystem) or
  a `CommonPrefix`, owning its bytes. A `CommonPrefix` also has `Object` (`Local`) set to the row stored at the
  prefix's exact key (an S3-style `a/` folder object under its children, or a directory's own row `d/`, so an
  empty directory lists as its prefix with its row; no directory metadata is made up for a prefix), from
  the module's exact row (`seekparquet.ListEntry.Exact`, spec LIST-8) in the same request, when that object is the
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
  not add to `StreamStats`. `ReadAhead` (default 0, none; a value above 64 reads as 64; negative is `ErrInvalidRequest`) lets the
  module read up to that many key pages ahead concurrently (`seekparquet.ListQuery.ReadAhead`): it pays where a
  read waits on a round trip (an object store), not on a local file, and the entries are the same either way;
  `Page` passes it on.
- **`Totals(paths)`.** Current objects only, as `Stream`. A path is taken verbatim: `""` selects every object,
  a path ending in `/` every object whose key begins with it (an object whose key is the path included), any
  other path only the object whose key equals it. Paths may repeat, nest and overlap; each total is independent.
  The rows of all paths are planned at once (`seekparquet.Dataset.Plan` of the paths not inside another,
  projecting `size` and `last_modified`), which checks every part holding them, and scanned once as `Scan` scans,
  in shards read by `runtime.GOMAXPROCS(0)` goroutines over borrowed batches, so a row several paths select is
  read once; the shards' totals are added up when all have succeeded, their keys in order across shards (as
  `Scan` checks them). `Count` is the number of the path's rows the scan read: the plan's count of its range on
  a dataset its reads find in key order, and a `key_order` refusal on one they do not; `Bytes` the
  sum of the sizes (nil when a selected filesystem entry has none); `NewestModified` the latest time, nil when the path selects nothing or any selected object
  has no time; `Exact` the object whose key is the path (never for `""`), with its ETag and storage class read by
  a plan of those objects alone. Each object is checked as `Stream` checks it (`KindRow`; a failing row is
  decoded again as `Stream` decodes it, for the same error), and sizes whose sum overflows an int64 fail with
  `KindRow` naming `size` and no row. The first error of any shard stops the others: with several failing rows,
  which one is reported is unspecified, as with `Scan`. Totals is exact or fails: the module's
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
  rows projecting `size` and scans their values once (`Plan.ScanValues`), without reading keys to route
  the sizes. A sum's row error names the dataset ordinal rather than a key, which it does not read. `SumsAlways` sums every such prefix. `SumsWithinBudget` takes
  them in ascending key order and sums one when the compressed size-page bytes it adds to those already
  selected (`Plan.ProjectedBytes`: the pages holding the rows and each chunk's dictionary, an extent
  shared with a prefix already selected counted once) fit what remains of `SumBudgetBytes` (0:
  `DefaultSumBudget`); a prefix inside one already selected adds nothing. Footers, key pages and page indexes
  read to plan are not charged; a prefix whose pages are all charged already adds nothing, a budget below the
  smallest possible page plans nothing, and a prefix whose size chunk has no OffsetIndex, so that its cost
  cannot be proven, is not summed under a budget. Candidates are planned lazily, in listing order and
  part by part; once a candidate's cost cannot fit, its later parts' size pages are not discovered.
  `Bytes` is nil for a prefix not summed, for a sum over a filesystem entry without a size, and for a sum that
  overflows an int64; a filesystem's counts and sums are the local totals of `Totals`. `Count` is reported only
  with `Counts`. A budget with another mode, a negative budget or an unknown mode wraps
  `seekparquet.ErrInvalidRequest`, before any read. A failure returns no aggregates. Both calls fail first on a
  context already ended (its error) or a closed `Dataset` (`seekparquet.ErrClosed`), whatever the request.
- **`Page(req)`.** `List` with one entry beyond `req.Limit`, trimmed to `Limit`; `Next` is the last returned
  entry's `Cursor()` only when that extra entry existed, so the last page has `Next` nil and is never followed by
  an empty one. The listing's row spans supply counts through metadata-only `PlanRows`, with identical
  opens and reads whether counts are on or off. Unknown boundaries and a cursor made up inside a prefix
  leave that prefix's totals nil; Page never seeks keys to complete a count. Budgeted sums use `SelectRows`
  to charge each shared size page and dictionary once, then `ScanValues` to read the accepted union.
  Only returned prefixes are aggregated (a prefix carrying its exact-key object included; an entry beyond
  the limit is never aggregated): each prefix entry's
  `Aggregate` is its `PrefixAggregate`, whose fields are nil as `Aggregates` leaves them (not requested, not
  known exactly, not within the sum budget), and an ordinary object's `Aggregate` is nil. The budget is
  `AggregateOptions`', not a bound on the page's reads. A `Limit` that is not positive or is `math.MaxInt`, and
  an invalid `AggregateOptions`, wrap `ErrInvalidRequest` before any read. Otherwise the errors are `List`'s and
  `Aggregates`'; a context that ends between the two is its error. No entries come with an error. `Stats` is
  reset at the start and receives the listing's and the aggregation's statistics, on failure too, a failed
  plan's included.
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
generation is live when `TimeDeleted` is nil, and there is no `IsLatest` for it. An Azure entry is its name's current blob when `IsLatest`; a snapshot has no `VersionID`, and a soft-deleted entry keeps its `Size`.

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
`KindRow` for a null in a required column. A part may omit columns whose `ColumnSpec.MayBeAbsent` is true; those columns decode as nil. This includes GCS
`time_created` and the later Azure current-object properties. Columns a schema id does not define are ignored.

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

`stream_test.go` holds the one-pass `Stream` to the paged `Stream` it replaced (`streamPages`, kept as the
oracle): the same objects in the same order over the batch scan's, the scan's and a generated fixture (S3, GCS
and Azure; parts, row groups, 31- to 1,024-row pages), in one unlimited request (`StreamStats` holding nothing of
the stream while it runs); one request whose `ReadStats` equal one `Rows` of every row; an error decoded from a real batch keeps its
kind, column, key and offset when the batch's borrowed keys are overwritten after it; the same row errors (kind,
column, key) after the same objects, deep in a later page, row group or part, and a part lacking a column;
a missing part refused after the objects before it; a Source failure at reads spread over the pass returned unchanged after a
prefix of the objects; cancellation by `fn` at each page, row group and part edge (the context's error, as
before, even at the last object); `StreamStats` for an empty dataset, a context ended before the stream, and
refusals; `Close` during `fn` (waiting for it; `fn`'s error unchanged, else `ErrClosed`); and `StreamLocal`
against the paged oracle, owning its keys and link targets.

`local_test.go` writes a filesystem tree (directories with their own rows, an empty one, a name that is not
UTF-8, a symlink with its target, a FIFO) over two parts, with every stat column and as `--metadata names`
writes it: `StreamLocal` decodes every column and null; `Stream` and `Scan` refuse the entries and `StreamLocal`
objects, before any read; `Page` lists it by `/` in pages of 1, 2 and 100, each directory paired with its row and
each prefix's totals nil-summed under `names`; `Totals` match the `StreamLocal` oracle, a directory counted once
with its own row and entry bytes nil, never 0, without sizes; a kind the schema does not define fails
`StreamLocal` and `List` (`KindRow`). `scan_test.go` checks `Scan` against `Stream` for
0, 1, 2, 7, 64 and `math.MaxInt` workers and several prefixes (each object once, each batch ascending), its
statistics against a plan, split and shard scans made by hand (1, 8 and 16 shards a worker), a failing row,
`fn`'s error stopping each shard at its first batch, cancellation (by `fn` on the last batch, and before a scan
of an empty dataset: never a success), a Source failure while planning (returned unchanged, the failed plan's
reads, the part it opened and the read that failed, in `Stats`), a versions refusal before any read, and
(`TestScanCallbackStopIsAnError`) `fn` returning `seekparquet.Stop`, which `Scan`, `ScanBatches` and `Stream`
return unchanged after at most one call a worker, with one shard and with several. `TestScanShardsPerWorker` runs `Scan` and
`ScanBatches` at 1, 2, 4, 8 and 16 shards a worker and 1, 2 and 7 workers: Stream's objects, each batch
ascending, no more calls of `fn` at once than workers, a bad row's position, and `fn`'s error after at most one
call a worker. `TestScanSlowShardDoesNotHoldTheRest` holds the first key's batch until the other worker has
handed on three quarters of the objects, which it cannot with one shard a worker (half wait behind the held
batch). `BenchmarkScan` tallies 300,000 objects with one worker and
`GOMAXPROCS`. `TestAzblobTotals` totals an Azure dataset against `Stream` (an exact blob's access tier as its
storage class) and its prefixes' aggregates.

`list_test.go` lists keys holding `a`, `a/`, `a/b`, `a/b0` and `ab/x` over three parts against an oracle, by
several prefixes, in pages of 1, 2, 3 and 100 entries continuing after objects and prefixes; checks an entry's
values, that changing a returned key or prefix changes neither its cursor nor the next page, cursors before and
after a prefix's keys, a NUL delimiter, the refusal of a versions schema before any read (statistics reset), a row check failing the listing with its statistics kept, a missing part's
refusal returned unchanged, and cancellation, a limit of 0 and a closed `Dataset`; `TestListReadAhead` lists 40
prefixes of five keys in pages of two rows with `ReadAhead` 0, 1, 4, 64 and 1,000, in one request and in pages
of three continued by their cursors, and through `Page` (its entries and totals equal to those without): the
same entries, pages read ahead exactly when it is positive, and a negative one refused before any read. The lister's
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
exactly root's size and time pages; `TestTotalsMatchSerial`, sharded totals at 0 to 16 workers equal to
`serialTotals` (Totals before it read in shards, kept as the oracle) over S3, GCS and Azure datasets, filesystem
entries with and without a stat, an empty dataset and keys nested seven deep over many row groups and parts, for
root, nested, repeated, exact, absent and no paths; `TestTotalsShardErrors`, one bad row (a negative size, a
time past 9999, a null size) giving the oracle's error at every worker count, several giving one of theirs (the
oracle's, the first in key order, at one worker), and a part lacking `last_modified` failing with `KindSchema`;
`TestTotalsCancelledDuringScan`, a context cancelled at a read inside the scan at every worker count;
`scan_test.go`'s `TestShardBoundaryOrder` (a key below the last of the row group before it, or repeating it,
written by parquet-go past what `CheckSchema` decodes: `Scan`, `ScanBatches` and `Totals` refuse it at 1, 2, 4
and 8 workers with `Stream`'s and the serial Totals' code and key), `TestShardBoundaryRepeatedKeys` (a group of
equal keys across row groups under non-unique keys is read, not refused) and `TestShardOrder`; the aggregates oracle on fresh and checked datasets; counts reading no value
page and opening no part between a prefix's ends, and nil across an unchecked part with a false manifest row
count; sums publishing nothing on failure; budgets read within, a budget of exactly the prefixes' projected bytes
summing them all (shared pages and dictionaries charged once), and a one-byte budget reading nothing beyond the
counts. varve's tests of its own page-index parsing are the module's now; `TestTotalsIgnoreManifestBoundExcludingKey`
shows the module's refusal of a part beyond its manifest bound returned unchanged.

`batch_test.go` compares every optional-field combination with `Scan` for S3, GCS and Azure across pages,
row groups, parts, prefixes and worker counts. It checks null/empty values, absent and all-null wrong-typed
columns, time bounds, owned error positions and cursors, cancellation, callback/source errors, stats and
concurrent storage isolation. Local and versions schemas remain refused. The batch allocation guard and
4M-row benchmarks are described in [reader benchmarks](../docs/internals/testing.md#reader-benchmarks).
