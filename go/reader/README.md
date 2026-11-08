# reader

`github.com/varveio/swath/go/reader` is what a reader of swath datasets needs to know about swath on top of the
[SeekParquet](https://github.com/varveio/seekparquet) module: the schema ids and their columns, the manifest's
`extensions.swath`, and each schema's rows as typed values decoded from a `seekparquet.Row`.

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
func DecodeObject(id SchemaID, row seekparquet.Row) (Object, error) // s3-objects/v1 or gcs-objects/v1 only
func DecodeS3Object(row seekparquet.Row) (S3Object, error)
func DecodeS3Version(row seekparquet.Row) (S3Version, error)
func DecodeGCSObject(row seekparquet.Row) (GCSObject, error)
func DecodeGCSGeneration(row seekparquet.Row) (GCSGeneration, error)
func DecodeLocalEntry(row seekparquet.Row) (LocalEntry, error)

type SemanticError struct { Kind Kind; Column string; Row *Position; Err error }
const KindSchema, KindCoverage, KindRow Kind = "schema", "coverage", "row"
var ErrSchema, ErrCoverage, ErrRow *SemanticError // errors.Is targets, by kind
```

A reader opens the dataset with the module (`seekparquet.OpenDir`, or `OpenDataset` over objfile), parses its
metadata, picks the decoder by `Metadata.SchemaID`, and reads rows projecting `SchemaSpec.ColumnNames()`:

```go
d, err := seekparquet.OpenDir(ctx, dir, seekparquet.Options{})
meta, err := reader.ParseMetadata(d.Metadata())
spec, ok := reader.Schema(meta.SchemaID) // S3VersionsV1, say
rows, err := d.Range(ctx, seekparquet.Range{Prefix: []byte("logs/"), Limit: 1000, Columns: spec.ColumnNames()})
for _, r := range rows {
	v, err := reader.DecodeS3Version(r)
	// ...
}
// the next page: Range{..., Cursor: last.Cursor()}
```

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
storage class), from `swath/s3-objects/v1` and `swath/gcs-objects/v1` only: a versions row is not a current
object, and `DecodeObject` refuses its schema id.

## Errors

A decoder fails with a `SemanticError` naming the column and the row: `KindSchema` when the row lacks a column
the schema id defines (its part lacks it, or the read did not project it) or holds a value of another type,
`KindRow` for a null in a required column. `time_created` is the one column a part may lack (it was added to
`swath/gcs-objects/v1` later); it reads as nil there. Columns a schema id does not define are ignored.

`ParseMetadata` reads every member of `extensions.swath` as a pointer (a slice: nil) that is nil when absent, so
a missing `coverage.complete` is never read as false; a member of the wrong JSON type is a `KindCoverage`
error. A manifest without the extension parses with `Swath` false. Unknown members (an older build's
`part_md5`) are ignored.

## What it is not

It reads no bytes of a dataset. Opening, validation (`seekparquet.Validate`), cursors, range and delimiter
queries, page decoding and refusal codes are the SeekParquet module's; storage is objfile's or the caller's.
There is no publication, no provider client, no accepting or refusing a dataset by its coverage, and no
grouping of versions. Accepting a whole-source dataset, schema checks at admission and current-object
streaming come next (plan step 8b), on these types.

## Tests

`go test -race ./...` in this directory: the decoders on hand-built rows (nulls, binary keys, unsigned bounds,
absent, unprojected and mistyped columns), `ParseMetadata` on present, absent and malformed members, and
`TestNoCloudSDK`, which runs `go list -deps -test ./...` and `go list -m all` and fails on any `aws`,
`cloud.google.com` or `azure` path. The lister's `internal/listing` `TestReader*` tests decode datasets its own
writer wrote for all five schemas: every column, nulls, keys that are not UTF-8, unsigned bounds, a part
without `time_created` and one without `md5`, and S3 versions read in pages of 1, 4, 7 and 100 rows over
several parts, continuing inside key groups by `Offset`.
