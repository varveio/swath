// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package schema describes listing rows and their Parquet columns as data.
// It is provider-neutral: a producer (a source provider) defines its row
// payload type P and a Schema[P] binding P's fields to columns, next to its
// code; the pipeline is generic over P and never names a producer's type.
package schema

import (
	"errors"
	"unsafe"
)

// Type is a column's physical and logical type.
type Type uint8

const (
	String          Type = iota // BYTE_ARRAY (STRING): UTF-8 the schema guarantees
	Int32                       // INT32, unannotated
	Int64                       // INT64, unannotated
	TimestampMicros             // INT64 TIMESTAMP(MICROS, UTC)
	Bool                        // BOOLEAN
	Uint32                      // INT32 INT(32, unsigned)
	Uint64                      // INT64 INT(64, unsigned)
	Binary                      // BYTE_ARRAY, unannotated: arbitrary bytes
)

// Spec is a column's name, type and nullability.
type Spec struct {
	Name     string
	Type     Type
	Required bool
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
	// strings), for the memory budget.
	Heap func(p *P) int64
	// Codec serializes payloads for spill files, private to one run.
	Codec Codec[P]
}

// Codec encodes a payload for a spill frame and decodes it again. Decode
// may return strings that view src, which the caller keeps unmodified.
type Codec[P any] interface {
	Append(dst []byte, p *P) []byte
	Decode(d *Decoder, p *P)
}

// ErrKeyTooLong reports a key over the schema's MaxKeyBytes.
var ErrKeyTooLong = errors.New("key longer than the schema allows")

// Specs returns the column specs in order, the key first.
func (s *Schema[P]) Specs() []Spec {
	out := make([]Spec, 0, len(s.Columns)+1)
	out = append(out, Spec{"key", s.Key, true})
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

// LogicalBytes is a row's deterministic uncompressed size: its key and
// payload column values (Bytes).
func (s *Schema[P]) LogicalBytes(r *Row[P]) int64 {
	return int64(len(r.Key)) + s.Bytes(&r.Data)
}

// Micros converts seconds and nanoseconds (0 <= nsec < 1e9) to epoch
// microseconds, truncating the sub-microsecond part.
func Micros(sec, nsec int64) int64 { return sec*1_000_000 + nsec/1000 }
