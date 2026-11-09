// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"bytes"
	"errors"
	"time"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
)

// The decoded rows. A nullable column is a pointer, nil for null (or for
// a column a part may lack and does); a required column is a value. Times
// are UTC. Byte values share the seekparquet.Row's memory. Each row keeps
// its Position, so a query continues after it (Position.Cursor), and rows
// of one key stay separate rows, in file order: nothing groups them.

// Position is a row's place in its dataset: its key, and its Offset in its
// group of equal keys (always 0 where keys are unique).
type Position struct {
	Key    []byte
	Offset int64
}

// Cursor returns the cursor after this row, owning its key bytes.
func (p Position) Cursor() *Cursor {
	return &Cursor{Key: bytes.Clone(p.Key), Offset: p.Offset}
}

// Object is a current object, or a filesystem entry, as a reader of any
// source's current objects sees it: an S3, Cloud Storage, Azure Blob or
// filesystem row (DecodeObject). An Azure blob's StorageClass is its
// access tier; ETag and StorageClass are nil for a filesystem entry.
type Object struct {
	Position
	Size         *int64
	LastModified *time.Time
	ETag         *string
	StorageClass *string
}

// S3Object is a swath/s3-objects/v1 row: one current S3 object.
type S3Object struct {
	Position
	Size              *int64
	LastModified      *time.Time
	ETag              *string // without the quotes S3 sends; a multipart "hex-N" kept
	StorageClass      *string
	OwnerID           *string
	OwnerDisplayName  *string
	ChecksumAlgorithm *string
	ChecksumType      *string
}

// S3Version is a swath/s3-versions/v1 row: a version or a delete marker.
// A delete marker's Size, ETag, StorageClass and checksums are nil. The
// first row of a key's group is the latest (IsLatest).
type S3Version struct {
	S3Object
	VersionID      *string // verbatim: "null" is a real version id
	IsLatest       bool
	IsDeleteMarker bool
}

// GCSObject is a swath/gcs-objects/v1 row: one live Cloud Storage object.
type GCSObject struct {
	Position
	Size           *int64
	LastModified   *time.Time // updated
	ETag           *string    // Cloud Storage's etag, not a content hash
	Generation     *int64
	Metageneration *int64
	StorageClass   *string
	MD5            *string // base64, as listed
	CRC32C         *string // base64, as listed
	ContentType    *string
	TimeCreated    *time.Time // nil also where the part lacks the column
}

// GCSGeneration is a swath/gcs-generations/v1 row: one generation, live
// or noncurrent. Rows of a name ascend by generation; the live one has no
// TimeDeleted, and a name may have none.
type GCSGeneration struct {
	GCSObject
	TimeDeleted *time.Time
}

// AzblobObject is a swath/azblob-objects/v1 row: one current Azure blob.
type AzblobObject struct {
	Position
	Size         *int64
	LastModified *time.Time // Last-Modified, 1 s resolution; moves on metadata writes
	ETag         *string    // opaque, not a content hash
	ContentMD5   *string    // base64, as listed
	ContentType  *string
	BlobType     *string // BlockBlob, PageBlob or AppendBlob
	AccessTier   *string
	CreationTime *time.Time
}

// LocalEntry is a swath-local/v1 row: one filesystem entry below the root.
// Key is the path's bytes, which need not be UTF-8.
type LocalEntry struct {
	Position
	Size              *int64
	LastModified      *time.Time
	Kind              string // FILE, DIR, SYMLINK, FIFO, SOCKET, CHAR_DEVICE, BLOCK_DEVICE, UNKNOWN
	Mode              *uint32
	UID               *uint32
	GID               *uint32
	Nlink             *uint32
	Inode             *uint64
	Dev               *uint64
	AllocatedBytes    *int64
	MetadataChangedAt *time.Time // ctime
	Atime             *time.Time
	BirthTime         *time.Time
	LinkTarget        *[]byte
}

// The decoders read a row of a range or listing that projected the
// schema's columns (SchemaSpec.ColumnNames). A column the row does not
// carry, a part lacking a column that every part has, or a value of
// another type is a SemanticError of KindSchema; a null in a required
// column is one of KindRow.

// DecodeObject decodes a current object of swath/s3-objects/v1,
// swath/gcs-objects/v1 or swath/azblob-objects/v1, or an entry of
// swath-local/v1, the schema id of the row's dataset. Every other id is a
// KindSchema error: a versions row is not a current object.
func DecodeObject(id SchemaID, row seekparquet.Row) (Object, error) {
	columns := objectColumns(id)
	if columns == nil {
		return Object{}, semantic(KindSchema, "", nil, "schema id %q holds no current objects", id)
	}
	d := decoder{spec: schemas[id], row: &row}
	o := Object{Position: d.position(), Size: d.int64("size"), LastModified: d.time("last_modified")}
	if len(columns) > 2 {
		o.ETag, o.StorageClass = d.string("etag"), d.string(columns[3])
	}
	return o, d.err
}

// DecodeAzblobObject decodes a swath/azblob-objects/v1 row.
func DecodeAzblobObject(row seekparquet.Row) (AzblobObject, error) {
	d := decoder{spec: schemas[AzblobObjectsV1], row: &row}
	o := AzblobObject{Position: d.position(), Size: d.int64("size"), LastModified: d.time("last_modified"),
		ETag: d.string("etag"), ContentMD5: d.string("content_md5"), ContentType: d.string("content_type"),
		BlobType: d.string("blob_type"), AccessTier: d.string("access_tier"), CreationTime: d.time("creation_time")}
	return o, d.err
}

// objectColumns returns the columns DecodeObject reads for a schema id,
// nil for one that holds no current objects; the fourth, where there is
// one, is the storage class.
func objectColumns(id SchemaID) []string {
	switch id {
	case S3ObjectsV1, GCSObjectsV1:
		return []string{"size", "last_modified", "etag", "storage_class"}
	case AzblobObjectsV1:
		return []string{"size", "last_modified", "etag", "access_tier"} // the tier is its storage class
	case LocalV1:
		return []string{"size", "last_modified"}
	}
	return nil
}

// DecodeS3Object decodes a swath/s3-objects/v1 row.
func DecodeS3Object(row seekparquet.Row) (S3Object, error) {
	d := decoder{spec: schemas[S3ObjectsV1], row: &row}
	o := d.s3Object()
	return o, d.err
}

// DecodeS3Version decodes a swath/s3-versions/v1 row.
func DecodeS3Version(row seekparquet.Row) (S3Version, error) {
	d := decoder{spec: schemas[S3VersionsV1], row: &row}
	v := S3Version{S3Object: d.s3Object(), VersionID: d.string("version_id"),
		IsLatest: d.required("is_latest").Boolean(), IsDeleteMarker: d.required("is_delete_marker").Boolean()}
	return v, d.err
}

// DecodeGCSObject decodes a swath/gcs-objects/v1 row.
func DecodeGCSObject(row seekparquet.Row) (GCSObject, error) {
	d := decoder{spec: schemas[GCSObjectsV1], row: &row}
	o := d.gcsObject()
	return o, d.err
}

// DecodeGCSGeneration decodes a swath/gcs-generations/v1 row.
func DecodeGCSGeneration(row seekparquet.Row) (GCSGeneration, error) {
	d := decoder{spec: schemas[GCSGenerationsV1], row: &row}
	g := GCSGeneration{GCSObject: d.gcsObject(), TimeDeleted: d.time("time_deleted")}
	return g, d.err
}

// DecodeLocalEntry decodes a swath-local/v1 row.
func DecodeLocalEntry(row seekparquet.Row) (LocalEntry, error) {
	d := decoder{spec: schemas[LocalV1], row: &row}
	e := LocalEntry{Position: d.position(), Size: d.int64("size"), LastModified: d.time("last_modified"),
		Kind: string(d.required("kind").ByteArray()), Mode: d.uint32("mode"), UID: d.uint32("uid"), GID: d.uint32("gid"),
		Nlink: d.uint32("nlink"), Inode: d.uint64("inode"), Dev: d.uint64("dev"), AllocatedBytes: d.int64("allocated_bytes"),
		MetadataChangedAt: d.time("metadata_changed_at"), Atime: d.time("atime"), BirthTime: d.time("birth_time"),
		LinkTarget: d.bytes("link_target")}
	return e, d.err
}

// decoder reads one row's columns of one schema; after the first error
// every read returns its zero value and err holds the error.
type decoder struct {
	spec SchemaSpec
	row  *seekparquet.Row
	err  error
}

func (d *decoder) position() Position { return Position{Key: d.row.Key, Offset: d.row.Offset} }

func (d *decoder) s3Object() S3Object {
	return S3Object{Position: d.position(), Size: d.int64("size"), LastModified: d.time("last_modified"),
		ETag: d.string("etag"), StorageClass: d.string("storage_class"), OwnerID: d.string("owner_id"),
		OwnerDisplayName: d.string("owner_display_name"), ChecksumAlgorithm: d.string("checksum_algorithm"),
		ChecksumType: d.string("checksum_type")}
}

func (d *decoder) gcsObject() GCSObject {
	return GCSObject{Position: d.position(), Size: d.int64("size"), LastModified: d.time("last_modified"),
		ETag: d.string("etag"), Generation: d.int64("generation"), Metageneration: d.int64("metageneration"),
		StorageClass: d.string("storage_class"), MD5: d.string("md5"), CRC32C: d.string("crc32c"),
		ContentType: d.string("content_type"), TimeCreated: d.time("time_created")}
}

// kinds is the parquet-go kind of each column type's values.
var kinds = [...]parquet.Kind{
	TypeString: parquet.ByteArray, TypeInt32: parquet.Int32, TypeInt64: parquet.Int64, TypeTimestampMicros: parquet.Int64,
	TypeBool: parquet.Boolean, TypeUint32: parquet.Int32, TypeUint64: parquet.Int64, TypeBinary: parquet.ByteArray,
}

var errNotProjected = errors.New("not projected: read the schema's columns (SchemaSpec.ColumnNames)")

// value returns the named column's value, ok false for a null. The name is
// the schema's: a name it lacks is this package's bug.
func (d *decoder) value(name string) (v seekparquet.Value, ok bool) {
	if d.err != nil {
		return v, false
	}
	spec, found := d.spec.Column(name)
	if !found {
		panic("reader: " + string(d.spec.ID) + " has no column " + name)
	}
	v, found = d.row.Value(name)
	switch {
	case !found:
		d.err = &SemanticError{Kind: KindSchema, Column: name, Row: d.at(), Err: errNotProjected}
	case v.Absent && !spec.MayBeAbsent:
		d.err = semantic(KindSchema, name, d.at(), "absent from the row's part")
	case v.IsNull():
		if spec.Required {
			d.err = semantic(KindRow, name, d.at(), "null in a required column")
		}
		return v, false
	case v.Kind() != kinds[spec.Type]:
		d.err = semantic(KindSchema, name, d.at(), "a %s value, the schema's type is %s", v.Kind(), kinds[spec.Type])
	default:
		return v, true
	}
	return v, false
}

func (d *decoder) at() *Position {
	p := d.position()
	return &p
}

// required reads a required column: the zero value only with d.err set.
func (d *decoder) required(name string) seekparquet.Value {
	v, _ := d.value(name)
	return v
}

func (d *decoder) int64(name string) *int64 {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	n := v.Int64()
	return &n
}

func (d *decoder) uint32(name string) *uint32 {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	n := v.Uint32()
	return &n
}

func (d *decoder) uint64(name string) *uint64 {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	n := v.Uint64()
	return &n
}

func (d *decoder) time(name string) *time.Time {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	t := time.UnixMicro(v.Int64()).UTC()
	return &t
}

func (d *decoder) string(name string) *string {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	s := string(v.ByteArray())
	return &s
}

func (d *decoder) bytes(name string) *[]byte {
	v, ok := d.value(name)
	if !ok {
		return nil
	}
	b := v.ByteArray()
	return &b
}
