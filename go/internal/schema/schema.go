// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package schema describes listing rows and their Parquet columns as data.
// It is provider-neutral: a producer (a source provider) defines its row
// payload type P and a Schema[P] binding P's fields to columns, next to its
// code; the pipeline is generic over P and never names a producer's type.
package schema

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unsafe"

	"github.com/varveio/swath/go/reader"
)

// Type is a column's physical and logical type: the reader module's, which
// the schema ids are described in.
type Type = reader.ColumnType

const (
	String          = reader.TypeString          // BYTE_ARRAY (STRING): UTF-8 the schema guarantees
	Int32           = reader.TypeInt32           // INT32, unannotated
	Int64           = reader.TypeInt64           // INT64, unannotated
	TimestampMicros = reader.TypeTimestampMicros // INT64 TIMESTAMP(MICROS, UTC)
	Bool            = reader.TypeBool            // BOOLEAN
	Uint32          = reader.TypeUint32          // INT32 INT(32, unsigned)
	Uint64          = reader.TypeUint64          // INT64 INT(64, unsigned)
	Binary          = reader.TypeBinary          // BYTE_ARRAY, unannotated: arbitrary bytes
)

// Spec is a column's name, type and nullability.
type Spec struct {
	Name     string
	Type     Type
	Required bool
	// MaxBytes is a string or binary column's longest value, when it can
	// be long (a symlink target): the writer's admission prices its pages
	// at it (parquetout.EncoderBytes), and the writer refuses a longer
	// value (ErrValueTooLong), as it refuses a key over MaxKeyBytes.
	// 0: short values, not checked.
	MaxBytes int
}

// Row is one listed entry: its key and the producer's typed payload P.
// Keys are bytes compared unsigned (Go string order).
type Row[P any] struct {
	Key  string
	Data P
}

// Column binds a Spec to an accessor of the payload. Exactly one accessor
// is set, matching the type; ok=false means null. Unsigned values carry
// their bit pattern in Int.
type Column[P any] struct {
	Spec
	Str  func(p *P) (v string, ok bool)
	Int  func(p *P) (v int64, ok bool)
	Bool func(p *P) (v bool, ok bool)
	// Delta writes a byte-array column DELTA_BYTE_ARRAY instead of
	// dictionary-then-PLAIN (unique values such as ETags).
	Delta bool
}

// NonEmpty makes an optional string accessor: an empty value is null.
func NonEmpty[P any](get func(*P) string) func(*P) (string, bool) {
	return func(p *P) (string, bool) {
		v := get(p)
		return v, v != ""
	}
}

// Info is what the pipeline needs to know about a schema without its row
// type: the names it stamps and the key rules it enforces.
type Info struct {
	Name string // Parquet message name
	ID   string // the manifest's schema_id and the footer's swath.schema_id
	// Key is the key column's type: String where the producer guarantees
	// UTF-8, else Binary. The key column is always "key", required.
	Key Type
	// MaxKeyBytes caps key length (0: no cap). S3 keys are at most 1,024
	// bytes; filesystem paths are not capped, and their column-index
	// bounds are truncated (conservative) instead.
	MaxKeyBytes int
	// QuotedETag: the etag column holds an HTTP entity tag stored without
	// its surrounding quotes (S3); text output restores them, so a row
	// shows the ETag exactly as the API returned it.
	QuotedETag bool
	// Groups: rows may repeat a key, as consecutive rows forming one key
	// group in the producer's within-key order (a versions listing). False
	// for current-object schemas: every key is unique and strictly
	// ascending. Grouping is the ordering property only: a versions
	// schema groups, but grouped rows need not be versions.
	Groups bool
	// Versions names the columns carrying a versions schema's roles
	// (version ID, latest or deleted-at, delete marker), the reader
	// spec's; zero for rows that are not versions. Consumers interpret
	// the rows through BindVersions, never by column name.
	Versions reader.VersionSpec
}

// Schema is a producer's row schema: the key column, then Columns in
// order, each read from the payload P.
type Schema[P any] struct {
	Info
	Columns []Column[P]
	// Bytes is the payload's logical size: the uncompressed bytes of its
	// column values. It must depend only on the row, never on timing: the
	// part router cuts parts by it, so the boundaries are deterministic.
	Bytes func(p *P) int64
	// Heap is the memory the payload references beyond its own value (its
	// strings), for the memory budget. It is at least the bytes of the
	// payload's strings: spill bounds a decoded frame's storage by it.
	Heap func(p *P) int64
	// Codec serializes payloads for spill files, private to one run.
	Codec Codec[P]
	// Container, if set, tells a row that names a container of other rows
	// (a local directory), not an object: swath du's per-prefix totals
	// list its prefixes and count nothing for it (package totals).
	Container func(p *P) bool
}

// Codec encodes a payload for a spill frame and decodes it again. Decode
// may return strings that view src, which the caller keeps unmodified.
type Codec[P any] interface {
	Append(dst []byte, p *P) []byte
	Decode(d *Decoder, p *P)
}

// DeltaCodec is a Codec that can also encode a payload against the
// previous row's, for spill's delta frames (rows sorted by key, whose
// neighbours share times, classes and owners). prev is the previous row's
// payload in the same frame, or a zero P for a frame's first row: the
// state is the two arguments, never the codec, so one codec serves every
// frame and goroutine at once. DecodeDelta's strings may be prev's (a
// repeat) or the decoder's (an owning decoder copies them), never views of
// the source. A producer without one gets its Codec in delta frames too
// (keys are prefix-coded by the frame either way).
type DeltaCodec[P any] interface {
	Codec[P]
	AppendDelta(dst []byte, prev, p *P) []byte
	DecodeDelta(d *Decoder, prev, p *P)
}

// Bind returns s as the schema id's: its ID, key type, key cap, grouping
// and version roles, and its columns' names, types and nullability are the reader
// module's spec of id, so producers and readers share one description.
// s.Columns are the spec's columns in order, each with the accessor its
// type takes (Str for strings and binary, Int for integers and times, Bool)
// and the producer's own encoding settings (Delta, MaxBytes). Bind panics
// when they do not match the spec: a producer's bug, found when its
// package loads.
func Bind[P any](id reader.SchemaID, s Schema[P]) Schema[P] {
	spec, ok := reader.Schema(id)
	if !ok || len(spec.Columns) != len(s.Columns) {
		panic(fmt.Sprintf("schema %s: %d columns, the spec has %d", id, len(s.Columns), len(spec.Columns)))
	}
	s.ID, s.Key, s.MaxKeyBytes, s.Groups, s.Versions = string(id), spec.Key.Type, spec.MaxKeyBytes, !spec.UniqueKeys, spec.Versions
	s.Columns = append([]Column[P](nil), s.Columns...)
	for i, c := range spec.Columns {
		col := &s.Columns[i]
		col.Name, col.Type, col.Required = c.Name, c.Type, c.Required
		var takes bool
		switch c.Type {
		case String, Binary:
			takes = col.Str != nil && col.Int == nil && col.Bool == nil
		case Bool:
			takes = col.Bool != nil && col.Str == nil && col.Int == nil
		default:
			takes = col.Int != nil && col.Str == nil && col.Bool == nil
		}
		if !takes {
			panic(fmt.Sprintf("schema %s: column %s: its accessor does not read its type", id, c.Name))
		}
	}
	if _, err := BindVersions(&s); err != nil {
		panic(err.Error())
	}
	return s
}

// ErrKeyTooLong reports a key over the schema's MaxKeyBytes.
var ErrKeyTooLong = errors.New("key longer than the schema allows")

// ErrValueTooLong reports a value over its column's Spec.MaxBytes.
var ErrValueTooLong = errors.New("value longer than the schema allows")

// Specs returns the column specs in order, the key first.
func (s *Schema[P]) Specs() []Spec {
	out := make([]Spec, 0, len(s.Columns)+1)
	out = append(out, Spec{Name: "key", Type: s.Key, Required: true})
	for _, c := range s.Columns {
		out = append(out, c.Spec)
	}
	return out
}

// RowBytes is the memory a row holds: the row value, its key and the
// payload's Heap (strings may view a shared page arena; counting them
// approximates the arena they keep alive).
func (s *Schema[P]) RowBytes(r *Row[P]) int64 {
	return int64(unsafe.Sizeof(*r)) + int64(len(r.Key)) + s.Heap(&r.Data)
}

// RowsBytes sums RowBytes.
func (s *Schema[P]) RowsBytes(rows []Row[P]) int64 {
	var n int64
	for i := range rows {
		n += s.RowBytes(&rows[i])
	}
	return n
}

// Detach returns copies of rows that share no memory with them: a slice
// of exactly len(rows), and every key and payload string re-pointed into
// one private buffer sized to them, written and read back through the
// payload's Codec (the spill encoding, so it round-trips every field). A
// caller detaches a few rows from a large page so the page's row array and
// the arena its strings view can be collected. Without a Codec only the
// keys are cloned.
func (s *Schema[P]) Detach(rows []Row[P]) []Row[P] {
	out := make([]Row[P], len(rows))
	if s.Codec == nil {
		for i := range rows {
			out[i] = Row[P]{Key: strings.Clone(rows[i].Key), Data: rows[i].Data}
		}
		return out
	}
	var buf []byte
	for i := range rows {
		buf = AppendString(buf, rows[i].Key)
		buf = s.Codec.Append(buf, &rows[i].Data)
	}
	buf = bytes.Clone(buf) // exact size: append's growth slack is not kept
	d := NewDecoder(buf)
	for i := range out {
		out[i].Key = d.Str()
		s.Codec.Decode(d, &out[i].Data)
	}
	if d.Err() != nil || d.Len() != 0 {
		panic("schema: payload codec does not round-trip")
	}
	return out
}

// LogicalBytes is a row's deterministic uncompressed size: its key and
// payload column values (Bytes).
func (s *Schema[P]) LogicalBytes(r *Row[P]) int64 {
	return int64(len(r.Key)) + s.Bytes(&r.Data)
}

// Micros converts seconds and nanoseconds (0 <= nsec < 1e9) to epoch
// microseconds, truncating the sub-microsecond part.
func Micros(sec, nsec int64) int64 { return sec*1_000_000 + nsec/1000 }
