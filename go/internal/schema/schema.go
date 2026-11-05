// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package schema describes listing rows and their Parquet columns as data.
package schema

import (
	"errors"
)

// Type is a column's physical and logical type.
type Type uint8

const (
	String          Type = iota // BYTE_ARRAY (STRING)
	Int32                       // INT32, unannotated
	Int64                       // INT64, unannotated
	TimestampMicros             // INT64 TIMESTAMP(MICROS, UTC)
	Bool                        // BOOLEAN
	Uint32                      // INT32 INT(32, unsigned)
	Uint64                      // INT64 INT(64, unsigned)
)

// Spec is a column's name, type and nullability.
type Spec struct {
	Name     string
	Type     Type
	Required bool
}

// Column binds a Spec to a Row accessor. Exactly one accessor is set,
// matching the type; ok=false means null.
type Column struct {
	Spec
	Str  func(r *Row) (v string, ok bool)
	Int  func(r *Row) (v int64, ok bool)
	Bool func(r *Row) (v bool, ok bool)
}

// Schema is a named, ordered column list.
type Schema struct {
	Name string // Parquet message name
	ID   string // stamped into manifest.json as schemaId
	// MaxKeyBytes caps key length (0: no cap). S3 keys are at most 1,024
	// bytes (contracts §6); filesystem paths are not capped, and their
	// column-index bounds are truncated (conservative) instead.
	MaxKeyBytes int
	Columns     []Column
}

// ErrKeyTooLong reports a key over the schema's MaxKeyBytes.
var ErrKeyTooLong = errors.New("key longer than the schema allows")

// Specs returns the column specs in order.
func (s *Schema) Specs() []Spec {
	out := make([]Spec, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = c.Spec
	}
	return out
}
