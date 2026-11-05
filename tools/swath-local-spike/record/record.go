// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package record defines the swath-local/v0 row: one row per filesystem entry
// below the listed root, keyed by its '/'-separated path relative to the root.
package record

import "strings"

// SchemaID names the row schema. It is stamped into manifest.json.
const SchemaID = "swath-local/v0"

// Kind is the file type. Its String form is the `kind` column value.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindFile
	KindDir
	KindSymlink
	KindFIFO
	KindSocket
	KindCharDevice
	KindBlockDevice
)

var kindNames = [...]string{"UNKNOWN", "FILE", "DIR", "SYMLINK", "FIFO", "SOCKET", "CHAR_DEVICE", "BLOCK_DEVICE"}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "UNKNOWN"
}

// ParseKind is the inverse of String; unknown text maps to KindUnknown.
func ParseKind(s string) Kind {
	for i, n := range kindNames {
		if n == s {
			return Kind(i)
		}
	}
	return KindUnknown
}

// Presence bits mark which optional columns hold a value (unset = null).
type Presence uint16

const (
	HasSize Presence = 1 << iota
	HasMtime
	HasMode
	HasUID
	HasGID
	HasNlink
	HasInode
	HasDev
	HasBlocks
	HasCtime
	HasAtime
	HasBtime
	HasLink

	// HasStat is everything one stat(2)/fstatat(2) supplies.
	HasStat = HasSize | HasMtime | HasMode | HasUID | HasGID | HasNlink | HasInode | HasDev | HasBlocks | HasCtime | HasAtime
)

// Row is a compact, value-typed record (~120 bytes plus key/target bytes) so
// that multi-million-row sort buffers stay cheap. Integer columns that are
// unsigned in the kernel (uid, gid, nlink, mode, inode, dev) are stored as
// their bit pattern; Parquet INT32/INT64 columns hold the same bits.
type Row struct {
	Key        string
	LinkTarget string
	Size       int64 // st_size (apparent bytes)
	Mtime      int64 // UTC epoch microseconds
	Ctime      int64
	Atime      int64
	Btime      int64
	Inode      int64
	Dev        int64
	Blocks     int64 // st_blocks*512 (allocated bytes)
	Mode       uint32
	UID        uint32
	GID        uint32
	Nlink      uint32
	Present    Presence
	Kind       Kind
}

// Has reports whether every bit in p is present.
func (r *Row) Has(p Presence) bool { return r.Present&p == p }

// Compare orders rows by key bytes, unsigned (Go string comparison is bytewise).
func Compare(a, b *Row) int { return strings.Compare(a.Key, b.Key) }

// EstimatedBytes is the in-memory footprint estimate used for byte-bounded sort buffers.
func (r *Row) EstimatedBytes() int64 {
	return 120 + int64(len(r.Key)) + int64(len(r.LinkTarget))
}

// Micros converts seconds+nanoseconds to epoch microseconds (floor).
func Micros(sec, nsec int64) int64 {
	return sec*1_000_000 + nsec/1000
}
