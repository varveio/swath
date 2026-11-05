// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

// Local is schema swath-local/v1: one row per entry below the root,
// directories included, each with a named kind. key, size and last_modified
// match the S3 schema. Identifiers (mode, uid, gid, nlink, inode, dev) are
// unsigned; allocated_bytes is st_blocks × 512; metadata_changed_at is ctime.
var Local = Schema{
	Name: "swath_listing",
	ID:   "swath-local/v1",
	Columns: []Column{
		{Spec: Spec{"key", String, true}, Str: func(r *Row) (string, bool) { return r.Key, true }},
		int64Col("size", HasSize, func(r *Row) int64 { return r.Size }),
		timeCol("last_modified", HasMtime, func(r *Row) int64 { return r.Mtime }),
		{Spec: Spec{"kind", String, true}, Str: func(r *Row) (string, bool) { return r.Kind.String(), true }},
		uint32Col("mode", HasMode, func(r *Row) uint32 { return r.Mode }),
		uint32Col("uid", HasUID, func(r *Row) uint32 { return r.UID }),
		uint32Col("gid", HasGID, func(r *Row) uint32 { return r.GID }),
		uint32Col("nlink", HasNlink, func(r *Row) uint32 { return r.Nlink }),
		uint64Col("inode", HasInode, func(r *Row) int64 { return r.Inode }),
		uint64Col("dev", HasDev, func(r *Row) int64 { return r.Dev }),
		int64Col("allocated_bytes", HasBlocks, func(r *Row) int64 { return r.Blocks }),
		timeCol("metadata_changed_at", HasCtime, func(r *Row) int64 { return r.Ctime }),
		timeCol("atime", HasAtime, func(r *Row) int64 { return r.Atime }),
		timeCol("birth_time", HasBtime, func(r *Row) int64 { return r.Btime }),
		{Spec: Spec{"link_target", String, false}, Str: func(r *Row) (string, bool) { return r.LinkTarget, r.Has(HasLink) }},
	},
}

func int64Col(name string, p Presence, get func(*Row) int64) Column {
	return Column{Spec: Spec{name, Int64, false}, Int: func(r *Row) (int64, bool) { return get(r), r.Has(p) }}
}

func timeCol(name string, p Presence, get func(*Row) int64) Column {
	return Column{Spec: Spec{name, TimestampMicros, false}, Int: func(r *Row) (int64, bool) { return get(r), r.Has(p) }}
}

// uint32Col and uint64Col carry the unsigned value's bits in Int.
func uint32Col(name string, p Presence, get func(*Row) uint32) Column {
	return Column{Spec: Spec{name, Uint32, false}, Int: func(r *Row) (int64, bool) { return int64(int32(get(r))), r.Has(p) }}
}

func uint64Col(name string, p Presence, get func(*Row) int64) Column {
	return Column{Spec: Spec{name, Uint64, false}, Int: func(r *Row) (int64, bool) { return get(r), r.Has(p) }}
}

// S3 is the current-object schema: every row is a current object, so the
// schema has no version columns. Nullability is fixed at run start: the
// owner columns are null unless -fetch-owner, the checksum columns are
// null when S3 omits them, etag and storage_class when it does.
var S3 = Schema{
	Name:        "swath_listing",
	ID:          "swath/s3-objects/v1",
	MaxKeyBytes: 1024,
	Columns: []Column{
		{Spec: Spec{"key", String, true}, Str: func(r *Row) (string, bool) { return r.Key, true }},
		int64Col("size", HasSize, func(r *Row) int64 { return r.Size }),
		timeCol("last_modified", HasMtime, func(r *Row) int64 { return r.Mtime }),
		objectCol("etag", func(o *ObjectMeta) string { return o.ETag }),
		objectCol("storage_class", func(o *ObjectMeta) string { return o.StorageClass }),
		objectCol("owner_id", func(o *ObjectMeta) string { return o.OwnerID }),
		objectCol("owner_display_name", func(o *ObjectMeta) string { return o.OwnerDisplayName }),
		objectCol("checksum_algorithm", func(o *ObjectMeta) string { return o.ChecksumAlgorithm }),
		objectCol("checksum_type", func(o *ObjectMeta) string { return o.ChecksumType }),
	},
}

func objectCol(name string, get func(*ObjectMeta) string) Column {
	return Column{Spec: Spec{name, String, false}, Str: func(r *Row) (string, bool) {
		if r.Object == nil {
			return "", false
		}
		v := get(r.Object)
		return v, v != ""
	}}
}
