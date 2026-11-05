// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package schema describes listing rows and their Parquet columns as data.
package schema

import (
	"fmt"
	"strings"
)

// Type is a column's physical and logical type.
type Type uint8

const (
	String          Type = iota // BYTE_ARRAY (STRING)
	Int32                       // INT32, unannotated
	Int64                       // INT64, unannotated
	TimestampMicros             // INT64 TIMESTAMP(MICROS, UTC)
	Bool                        // BOOLEAN
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
	Str func(r *Row) (v string, ok bool)
	Int func(r *Row) (v int64, ok bool)
}

// Schema is a named, ordered column list.
type Schema struct {
	Name      string // Parquet message name
	ID        string // stamped into manifest.json as schemaId
	SortOrder string // swath.sort.order footer value
	Columns   []Column
}

// Specs returns the column specs in order.
func (s *Schema) Specs() []Spec {
	out := make([]Spec, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = c.Spec
	}
	return out
}

// Message renders the parquet-mr MessageType.toString() form that Swath
// stores as manifest fileSchema.
func Message(name string, specs []Spec) string {
	var b strings.Builder
	fmt.Fprintf(&b, "message %s {\n", name)
	for _, s := range specs {
		rep := "optional"
		if s.Required {
			rep = "required"
		}
		var typ string
		switch s.Type {
		case String:
			typ = "binary " + s.Name + " (STRING)"
		case Int32:
			typ = "int32 " + s.Name
		case Int64:
			typ = "int64 " + s.Name
		case TimestampMicros:
			typ = "int64 " + s.Name + " (TIMESTAMP(MICROS,true))"
		case Bool:
			typ = "boolean " + s.Name
		}
		fmt.Fprintf(&b, "  %s %s;\n", rep, typ)
	}
	b.WriteString("}\n")
	return b.String()
}

// Message renders this schema's message string.
func (s *Schema) Message() string { return Message(s.Name, s.Specs()) }
