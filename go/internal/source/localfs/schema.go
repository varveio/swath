// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package localfs

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/reader"
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
var Schema = schema.Bind(reader.LocalV1, schema.Schema[Entry]{
	Info: schema.Info{Name: "swath_listing"},
	Columns: []schema.Column[Entry]{
		intCol(HasSize, func(e *Entry) int64 { return e.Size }),
		intCol(HasMtime, func(e *Entry) int64 { return e.Mtime }),
		{Str: func(e *Entry) (string, bool) { return e.Kind.String(), true }},
		uint32Col(HasMode, func(e *Entry) uint32 { return e.Mode }),
		uint32Col(HasUID, func(e *Entry) uint32 { return e.UID }),
		uint32Col(HasGID, func(e *Entry) uint32 { return e.GID }),
		uint32Col(HasNlink, func(e *Entry) uint32 { return e.Nlink }),
		intCol(HasInode, func(e *Entry) int64 { return e.Inode }),
		intCol(HasDev, func(e *Entry) int64 { return e.Dev }),
		intCol(HasBlocks, func(e *Entry) int64 { return e.Blocks }),
		intCol(HasCtime, func(e *Entry) int64 { return e.Ctime }),
		intCol(HasAtime, func(e *Entry) int64 { return e.Atime }),
		intCol(HasBtime, func(e *Entry) int64 { return e.Btime }),
		{Spec: schema.Spec{MaxBytes: pathMax}, Str: func(e *Entry) (string, bool) { return e.LinkTarget, e.Has(HasLink) }},
	},
	Bytes: entryBytes,
	Heap:  func(e *Entry) int64 { return int64(len(e.LinkTarget)) },
	Codec: entryCodec{},
})

// pathMax is the longest link target (Linux PATH_MAX).
const pathMax = 4096

// intCol reads an integer or time present with p; uint32Col carries an
// unsigned value's bits in Int, as the Uint64 columns' int64 fields do.
func intCol(p Presence, get func(*Entry) int64) schema.Column[Entry] {
	return schema.Column[Entry]{Int: func(e *Entry) (int64, bool) { return get(e), e.Has(p) }}
}

func uint32Col(p Presence, get func(*Entry) uint32) schema.Column[Entry] {
	return schema.Column[Entry]{Int: func(e *Entry) (int64, bool) { return int64(int32(get(e))), e.Has(p) }}
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
