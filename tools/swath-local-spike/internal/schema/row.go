// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

// Kind is a file type; its String form is the kind column value.
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

// Presence bits mark which optional values a Row holds (unset = null).
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

	// HasStat is everything one stat(2) supplies.
	HasStat = HasSize | HasMtime | HasMode | HasUID | HasGID | HasNlink | HasInode | HasDev | HasBlocks | HasCtime | HasAtime
)

// Row is one listed entry. It is a value type (about 120 bytes plus the key
// and link target) so that large buffers of rows stay cheap. Kernel values
// that are unsigned (uid, gid, nlink, mode, inode, dev) are stored as their
// bit pattern; the Parquet INT32/INT64 columns hold the same bits.
type Row struct {
	Key        string
	LinkTarget string
	Size       int64 // apparent bytes
	Mtime      int64 // UTC epoch microseconds
	Ctime      int64
	Atime      int64
	Btime      int64
	Inode      int64
	Dev        int64
	Blocks     int64 // allocated bytes (st_blocks*512)
	Mode       uint32
	UID        uint32
	GID        uint32
	Nlink      uint32
	Present    Presence
	Kind       Kind
	Object     *ObjectMeta // object-store fields; nil for filesystem rows
}

// ObjectMeta holds the object-store columns of a row. Empty strings are null.
type ObjectMeta struct {
	ETag              string // quotes stripped; multipart "hex-N" kept
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string
	ChecksumType      string
}

// Has reports whether every bit in p is present.
func (r *Row) Has(p Presence) bool { return r.Present&p == p }

// Micros converts seconds and nanoseconds (0 <= nsec < 1e9) to epoch
// microseconds, truncating the sub-microsecond part.
func Micros(sec, nsec int64) int64 { return sec*1_000_000 + nsec/1000 }
