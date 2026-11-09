// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import "slices"

// SchemaID is a dataset's schema id: the manifest's schema_id and every
// part footer's seekparquet.schema_id.
type SchemaID string

// The schema ids swath writes.
const (
	S3ObjectsV1      SchemaID = "swath/s3-objects/v1"      // S3 current objects
	S3VersionsV1     SchemaID = "swath/s3-versions/v1"     // S3 versions and delete markers (--all-versions)
	GCSObjectsV1     SchemaID = "swath/gcs-objects/v1"     // Cloud Storage live objects
	GCSGenerationsV1 SchemaID = "swath/gcs-generations/v1" // Cloud Storage generations (--all-versions)
	AzblobObjectsV1  SchemaID = "swath/azblob-objects/v1"  // Azure Blob current blobs
	LocalV1          SchemaID = "swath/local-entries/v1"   // a filesystem tree
)

// ColumnType is a column's physical and logical Parquet type.
type ColumnType uint8

const (
	TypeString          ColumnType = iota // BYTE_ARRAY (STRING): UTF-8
	TypeInt32                             // INT32, signed
	TypeInt64                             // INT64, signed
	TypeTimestampMicros                   // INT64 TIMESTAMP(MICROS, UTC)
	TypeBool                              // BOOLEAN
	TypeUint32                            // INT32 INT(32, unsigned)
	TypeUint64                            // INT64 INT(64, unsigned)
	TypeBinary                            // BYTE_ARRAY, unannotated: arbitrary bytes
)

// ColumnSpec is one column of a schema.
type ColumnSpec struct {
	Name string
	Type ColumnType
	// Required: the column is never null.
	Required bool
	// MayBeAbsent: a part may lack the column (it was added to the schema
	// id later), which reads as null. Every other column is in every part.
	MayBeAbsent bool
}

// SchemaSpec is a schema id's key and columns.
type SchemaSpec struct {
	ID SchemaID
	// Key is the key column: "key", required, TypeString where the source
	// guarantees UTF-8, else TypeBinary.
	Key ColumnSpec
	// UniqueKeys: every key is unique. False for the versions schemas,
	// whose keys repeat on consecutive rows, in the service's order within
	// the key, and never across parts.
	UniqueKeys bool
	// MaxKeyBytes is the longest key, 0 for no bound.
	MaxKeyBytes int
	// Columns are the columns after the key, in order.
	Columns []ColumnSpec
	// Versions names the columns that carry a versions schema's roles; the
	// zero value for a schema whose rows are not versions.
	Versions VersionSpec
}

// VersionSpec names the columns of a versions schema (a key's versions as
// consecutive rows) by role, so a consumer interprets the rows of any such
// schema from its spec instead of from the column names. ID and exactly
// one of Latest and DeletedAt are set; a versions schema has repeating keys
// (UniqueKeys false).
type VersionSpec struct {
	// ID is the version's identifier: a string column, or a signed INT64
	// column whose value is the identifier in decimal. Null on a row
	// without one.
	ID string
	// Latest is a required boolean column: true on the key's latest row.
	Latest string
	// DeleteMarker, if set, is a required boolean column: true on a row
	// that is a delete marker (no object, no bytes) rather than a version
	// of the object. A schema without it has no delete markers.
	DeleteMarker string
	// DeletedAt is the alternative to Latest: a nullable timestamp
	// column, null on the latest (live) row. A row with a value, even a
	// zero one, is not the latest.
	DeletedAt string
}

// Column returns the named column (the key included).
func (s SchemaSpec) Column(name string) (ColumnSpec, bool) {
	if name == s.Key.Name {
		return s.Key, true
	}
	i := slices.IndexFunc(s.Columns, func(c ColumnSpec) bool { return c.Name == name })
	if i < 0 {
		return ColumnSpec{}, false
	}
	return s.Columns[i], true
}

// ColumnNames returns the names of the columns after the key, in order:
// the projection a decoder of the schema reads.
func (s SchemaSpec) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}

// Schema returns the spec of a schema id swath writes, and false for any
// other id.
func Schema(id SchemaID) (SchemaSpec, bool) {
	s, ok := schemas[id]
	if !ok {
		return SchemaSpec{}, false
	}
	s.Columns = slices.Clone(s.Columns)
	return s, true
}

// maxObjectKeyBytes is the longest S3 key and Cloud Storage object name;
// maxBlobNameBytes the longest Azure blob name (1,024 UTF-16 code units).
const maxObjectKeyBytes, maxBlobNameBytes = 1024, 3072

var (
	textKey   = ColumnSpec{Name: "key", Type: TypeString, Required: true}
	binaryKey = ColumnSpec{Name: "key", Type: TypeBinary, Required: true}

	// s3Columns: every S3 column is nullable. etag and storage_class are
	// null when S3 omits them, the owner columns unless the listing asked
	// for owners, the checksum columns when S3 omits them.
	s3Columns = []ColumnSpec{
		{Name: "size", Type: TypeInt64},
		{Name: "last_modified", Type: TypeTimestampMicros},
		{Name: "etag", Type: TypeString},
		{Name: "storage_class", Type: TypeString},
		{Name: "owner_id", Type: TypeString},
		{Name: "owner_display_name", Type: TypeString},
		{Name: "checksum_algorithm", Type: TypeString},
		{Name: "checksum_type", Type: TypeString},
	}
	gcsColumns = []ColumnSpec{
		{Name: "size", Type: TypeInt64},
		{Name: "last_modified", Type: TypeTimestampMicros},
		{Name: "etag", Type: TypeString},
		{Name: "generation", Type: TypeInt64},
		{Name: "metageneration", Type: TypeInt64},
		{Name: "storage_class", Type: TypeString},
		{Name: "md5", Type: TypeString},
		{Name: "crc32c", Type: TypeString},
		{Name: "content_type", Type: TypeString},
		{Name: "time_created", Type: TypeTimestampMicros, MayBeAbsent: true},
	}
)

var schemas = map[SchemaID]SchemaSpec{
	S3ObjectsV1: {ID: S3ObjectsV1, Key: textKey, UniqueKeys: true, MaxKeyBytes: maxObjectKeyBytes, Columns: s3Columns},
	// The s3-objects/v1 columns, size null on delete markers, then the
	// version's identity.
	S3VersionsV1: {ID: S3VersionsV1, Key: textKey, MaxKeyBytes: maxObjectKeyBytes, Columns: concat(s3Columns,
		ColumnSpec{Name: "version_id", Type: TypeString},
		ColumnSpec{Name: "is_latest", Type: TypeBool, Required: true},
		ColumnSpec{Name: "is_delete_marker", Type: TypeBool, Required: true}),
		Versions: VersionSpec{ID: "version_id", Latest: "is_latest", DeleteMarker: "is_delete_marker"}},
	GCSObjectsV1: {ID: GCSObjectsV1, Key: textKey, UniqueKeys: true, MaxKeyBytes: maxObjectKeyBytes, Columns: gcsColumns},
	// The gcs-objects/v1 columns, then time_deleted: null for the live
	// generation.
	GCSGenerationsV1: {ID: GCSGenerationsV1, Key: textKey, MaxKeyBytes: maxObjectKeyBytes, Columns: concat(gcsColumns,
		ColumnSpec{Name: "time_deleted", Type: TypeTimestampMicros}),
		Versions: VersionSpec{ID: "generation", DeletedAt: "time_deleted"}},
	// Every column is nullable: content_md5 and access_tier where Azure
	// lists none, the others where a listing omits them.
	AzblobObjectsV1: {ID: AzblobObjectsV1, Key: textKey, UniqueKeys: true, MaxKeyBytes: maxBlobNameBytes, Columns: []ColumnSpec{
		{Name: "size", Type: TypeInt64},
		{Name: "last_modified", Type: TypeTimestampMicros},
		{Name: "etag", Type: TypeString},
		{Name: "content_md5", Type: TypeString},
		{Name: "content_type", Type: TypeString},
		{Name: "blob_type", Type: TypeString},
		{Name: "access_tier", Type: TypeString},
		{Name: "creation_time", Type: TypeTimestampMicros},
	}},
	// Names are bytes; identifiers are unsigned; allocated_bytes is
	// st_blocks × 512 and metadata_changed_at is ctime.
	LocalV1: {ID: LocalV1, Key: binaryKey, UniqueKeys: true, Columns: []ColumnSpec{
		{Name: "size", Type: TypeInt64},
		{Name: "last_modified", Type: TypeTimestampMicros},
		{Name: "kind", Type: TypeString, Required: true},
		{Name: "mode", Type: TypeUint32},
		{Name: "uid", Type: TypeUint32},
		{Name: "gid", Type: TypeUint32},
		{Name: "nlink", Type: TypeUint32},
		{Name: "inode", Type: TypeUint64},
		{Name: "dev", Type: TypeUint64},
		{Name: "allocated_bytes", Type: TypeInt64},
		{Name: "metadata_changed_at", Type: TypeTimestampMicros},
		{Name: "atime", Type: TypeTimestampMicros},
		{Name: "birth_time", Type: TypeTimestampMicros},
		{Name: "link_target", Type: TypeBinary},
	}},
}

func concat(cols []ColumnSpec, more ...ColumnSpec) []ColumnSpec {
	return append(slices.Clone(cols), more...)
}
