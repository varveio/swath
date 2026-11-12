// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

// localRow is a swath/local-entries/v1 row as swath writes it.
type localRow struct {
	Key               []byte  `parquet:"key"`
	Size              *int64  `parquet:"size,optional"`
	LastModified      *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	Kind              string  `parquet:"kind"`
	Mode              *uint32 `parquet:"mode,optional"`
	UID               *uint32 `parquet:"uid,optional"`
	GID               *uint32 `parquet:"gid,optional"`
	Nlink             *uint32 `parquet:"nlink,optional"`
	Inode             *uint64 `parquet:"inode,optional"`
	Dev               *uint64 `parquet:"dev,optional"`
	AllocatedBytes    *int64  `parquet:"allocated_bytes,optional"`
	MetadataChangedAt *int64  `parquet:"metadata_changed_at,optional,timestamp(microsecond:utc)"`
	Atime             *int64  `parquet:"atime,optional,timestamp(microsecond:utc)"`
	BirthTime         *int64  `parquet:"birth_time,optional,timestamp(microsecond:utc)"`
	LinkTarget        []byte  `parquet:"link_target,optional"`
}

type treeEntry struct {
	key, kind string
	size      int64
	target    string
}

// localTree is a tree as swath lists it: directory rows ending in "/" (d/e/
// empty), a name that is not UTF-8, a symlink, a FIFO and nested
// directories, in key order.
var localTree = []treeEntry{
	{"a.txt", "FILE", 3, ""},
	{"d/", "DIR", 4096, ""},
	{"d/e/", "DIR", 4096, ""},
	{"d/link", "SYMLINK", 8, "../a.txt"},
	{"d/\xff", "FILE", 0, ""},
	{"fifo", "FIFO", 0, ""},
	{"z/", "DIR", 4096, ""},
	{"z/y/", "DIR", 4096, ""},
	{"z/y/f", "FILE", 7, ""},
}
