// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package fs

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
)

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

// Presence bits mark which optional values an Entry holds (unset = null).
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

// Entry is a filesystem row's payload. Kernel values that are unsigned
// (uid, gid, nlink, mode, inode, dev) are stored as their bit pattern; the
// Parquet INT32/INT64 columns hold the same bits.
type Entry struct {
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
}

// Row is one listed filesystem entry.
type Row = schema.Row[Entry]

// Has reports whether every bit in p is present.
func (e *Entry) Has(p Presence) bool { return e.Present&p == p }

// Schema is swath-local/v1: one row per entry below the root, directories
// included, each with a named kind. size and last_modified match the S3
// schema. Names are bytes (a POSIX name need not be UTF-8): key and
// link_target are unannotated BYTE_ARRAY, written exactly as the kernel
// returned them. Identifiers (mode, uid, gid, nlink, inode, dev) are
// unsigned; allocated_bytes is st_blocks × 512; metadata_changed_at is ctime.
var Schema = schema.Schema[Entry]{
	Info: schema.Info{Name: "swath_listing", ID: "swath-local/v1", Key: schema.Binary},
	Columns: []schema.Column[Entry]{
		int64Col("size", HasSize, func(e *Entry) int64 { return e.Size }),
		timeCol("last_modified", HasMtime, func(e *Entry) int64 { return e.Mtime }),
		{Spec: schema.Spec{Name: "kind", Type: schema.String, Required: true}, Str: func(e *Entry) (string, bool) { return e.Kind.String(), true }},
		uint32Col("mode", HasMode, func(e *Entry) uint32 { return e.Mode }),
		uint32Col("uid", HasUID, func(e *Entry) uint32 { return e.UID }),
		uint32Col("gid", HasGID, func(e *Entry) uint32 { return e.GID }),
		uint32Col("nlink", HasNlink, func(e *Entry) uint32 { return e.Nlink }),
		uint64Col("inode", HasInode, func(e *Entry) int64 { return e.Inode }),
		uint64Col("dev", HasDev, func(e *Entry) int64 { return e.Dev }),
		int64Col("allocated_bytes", HasBlocks, func(e *Entry) int64 { return e.Blocks }),
		timeCol("metadata_changed_at", HasCtime, func(e *Entry) int64 { return e.Ctime }),
		timeCol("atime", HasAtime, func(e *Entry) int64 { return e.Atime }),
		timeCol("birth_time", HasBtime, func(e *Entry) int64 { return e.Btime }),
		{Spec: schema.Spec{Name: "link_target", Type: schema.Binary}, Str: func(e *Entry) (string, bool) { return e.LinkTarget, e.Has(HasLink) }},
	},
	Bytes: entryBytes,
	Heap:  func(e *Entry) int64 { return int64(len(e.LinkTarget)) },
	Codec: entryCodec{},
}

func int64Col(name string, p Presence, get func(*Entry) int64) schema.Column[Entry] {
	return schema.Column[Entry]{Spec: schema.Spec{Name: name, Type: schema.Int64}, Int: func(e *Entry) (int64, bool) { return get(e), e.Has(p) }}
}

func timeCol(name string, p Presence, get func(*Entry) int64) schema.Column[Entry] {
	return schema.Column[Entry]{Spec: schema.Spec{Name: name, Type: schema.TimestampMicros}, Int: func(e *Entry) (int64, bool) { return get(e), e.Has(p) }}
}

// uint32Col and uint64Col carry the unsigned value's bits in Int.
func uint32Col(name string, p Presence, get func(*Entry) uint32) schema.Column[Entry] {
	return schema.Column[Entry]{Spec: schema.Spec{Name: name, Type: schema.Uint32}, Int: func(e *Entry) (int64, bool) { return int64(int32(get(e))), e.Has(p) }}
}

func uint64Col(name string, p Presence, get func(*Entry) int64) schema.Column[Entry] {
	return schema.Column[Entry]{Spec: schema.Spec{Name: name, Type: schema.Uint64}, Int: func(e *Entry) (int64, bool) { return get(e), e.Has(p) }}
}

// entryBytes is an entry's logical size: the kind name, the link target
// and 8 or 4 bytes per present integer.
func entryBytes(e *Entry) int64 {
	n := int64(len(e.LinkTarget) + len(e.Kind.String()))
	for _, p := range [...]Presence{HasSize, HasMtime, HasInode, HasDev, HasBlocks, HasCtime, HasAtime, HasBtime} {
		if e.Has(p) {
			n += 8
		}
	}
	for _, p := range [...]Presence{HasMode, HasUID, HasGID, HasNlink} {
		if e.Has(p) {
			n += 4
		}
	}
	return n
}

// entryCodec is the spill encoding of an Entry: the presence bits, the
// kind, the link target if present and each present value as a varint.
type entryCodec struct{}

var int64Fields = [...]Presence{HasSize, HasMtime, HasCtime, HasAtime, HasBtime, HasInode, HasDev, HasBlocks}

func int64Ptrs(e *Entry) [8]*int64 {
	return [8]*int64{&e.Size, &e.Mtime, &e.Ctime, &e.Atime, &e.Btime, &e.Inode, &e.Dev, &e.Blocks}
}

var uint32Fields = [...]Presence{HasMode, HasUID, HasGID, HasNlink}

func uint32Ptrs(e *Entry) [4]*uint32 { return [4]*uint32{&e.Mode, &e.UID, &e.GID, &e.Nlink} }

func (entryCodec) Append(dst []byte, e *Entry) []byte {
	dst = binary.AppendUvarint(dst, uint64(e.Present))
	dst = append(dst, byte(e.Kind))
	if e.Has(HasLink) {
		dst = schema.AppendString(dst, e.LinkTarget)
	}
	for i, p := range int64Ptrs(e) {
		if e.Has(int64Fields[i]) {
			dst = binary.AppendVarint(dst, *p)
		}
	}
	for i, p := range uint32Ptrs(e) {
		if e.Has(uint32Fields[i]) {
			dst = binary.AppendUvarint(dst, uint64(*p))
		}
	}
	return dst
}

func (entryCodec) Decode(d *schema.Decoder, e *Entry) {
	e.Present = Presence(d.Uvarint())
	e.Kind = Kind(d.Byte())
	if e.Has(HasLink) {
		e.LinkTarget = d.Str()
	}
	for i, p := range int64Ptrs(e) {
		if e.Has(int64Fields[i]) {
			*p = d.Varint()
		}
	}
	for i, p := range uint32Ptrs(e) {
		if e.Has(uint32Fields[i]) {
			*p = uint32(d.Uvarint())
		}
	}
}
