// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

// SortOrder is Swath's sort stamp: unsigned key bytes, then version_id (null
// first), then row_type rank. Local keys are unique, so it reduces to
// unsigned key-byte order.
const SortOrder = "key_bytes_unsigned,version_id_null_first,row_type_rank"

// Local is schema swath-local/v0. key, size and last_modified are identical
// to Swath's canonical schema, so a reader that needs only them works on both.
var Local = Schema{
	Name:      "swath_listing",
	ID:        "swath-local/v0",
	SortOrder: SortOrder,
	Columns: []Column{
		{Spec: Spec{"key", String, true}, Str: func(r *Row) (string, bool) { return r.Key, true }},
		int64Col("size", HasSize, func(r *Row) int64 { return r.Size }),
		timeCol("last_modified", HasMtime, func(r *Row) int64 { return r.Mtime }),
		{Spec: Spec{"kind", String, true}, Str: func(r *Row) (string, bool) { return r.Kind.String(), true }},
		int32Col("mode", HasMode, func(r *Row) uint32 { return r.Mode }),
		int32Col("uid", HasUID, func(r *Row) uint32 { return r.UID }),
		int32Col("gid", HasGID, func(r *Row) uint32 { return r.GID }),
		int32Col("nlink", HasNlink, func(r *Row) uint32 { return r.Nlink }),
		int64Col("inode", HasInode, func(r *Row) int64 { return r.Inode }),
		int64Col("dev", HasDev, func(r *Row) int64 { return r.Dev }),
		int64Col("blocks", HasBlocks, func(r *Row) int64 { return r.Blocks }),
		timeCol("ctime", HasCtime, func(r *Row) int64 { return r.Ctime }),
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

func int32Col(name string, p Presence, get func(*Row) uint32) Column {
	return Column{Spec: Spec{name, Int32, false}, Int: func(r *Row) (int64, bool) { return int64(int32(get(r))), r.Has(p) }}
}

// SwathS3 is Swath's canonical S3 schema (docs/internals/contracts.md §4) as
// column specs only. It is not bound to Row; it shows the same model
// describes it.
var SwathS3 = []Spec{
	{"key", String, true},
	{"size", Int64, false},
	{"last_modified", TimestampMicros, false},
	{"etag", String, false},
	{"storage_class", String, false},
	{"version_id", String, false},
	{"is_latest", Bool, false},
	{"is_delete_marker", Bool, true},
	{"owner_id", String, false},
	{"owner_display_name", String, false},
	{"checksum_algorithm", String, false},
	{"checksum_type", String, false},
	{"row_type", String, true},
}
