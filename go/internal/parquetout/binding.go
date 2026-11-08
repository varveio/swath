// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package parquet writes listing rows as Swath-layout sorted Parquet parts:
// the file writer, the schema binding and the part router.
package parquetout

import (
	"fmt"
	"reflect"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"

	"github.com/varveio/swath/go/internal/schema"
)

// nodeOf builds the Parquet schema for s: the key column, then s.Columns in
// declaration order. The key column is delta-encoded, and integer columns
// carry the standard INT annotation (signed or not).
func nodeOf[P any](s *schema.Schema[P]) *pq.Schema {
	specs := s.Specs()
	delta := make([]bool, len(specs))
	fields := make([]reflect.StructField, len(specs))
	for i, c := range specs {
		var t reflect.Type
		tag := c.Name
		switch c.Type {
		case schema.String:
			t = reflect.TypeFor[string]()
		case schema.Int32:
			t = reflect.TypeFor[int32]()
		case schema.Int64:
			t = reflect.TypeFor[int64]()
		case schema.Uint32:
			t = reflect.TypeFor[uint32]()
		case schema.Uint64:
			t = reflect.TypeFor[uint64]()
		case schema.TimestampMicros:
			t, tag = reflect.TypeFor[int64](), tag+",timestamp(microsecond:utc)"
		case schema.Bool:
			t = reflect.TypeFor[bool]()
		case schema.Binary:
			t = reflect.TypeFor[[]byte]()
		}
		if i == 0 {
			tag += ",delta"
		} else {
			delta[i] = s.Columns[i-1].Delta
		}
		if !c.Required {
			t = reflect.PointerTo(t)
			tag = insertOptional(tag)
		}
		fields[i] = reflect.StructField{Name: fmt.Sprintf("C%d", i), Type: t, Tag: reflect.StructTag(fmt.Sprintf("parquet:%q", tag))}
	}
	node := pq.SchemaOf(reflect.New(reflect.StructOf(fields)).Interface())
	return pq.NewSchema(s.Name, root{node, delta})
}

// insertOptional adds "optional" right after the column name in a tag.
func insertOptional(tag string) string {
	for i := range tag {
		if tag[i] == ',' {
			return tag[:i] + ",optional" + tag[i:]
		}
	}
	return tag + ",optional"
}

// root sets the encoding of the fields with schema.Column.Delta; the
// module's part writer marks the key column's index ascending.
type root struct {
	pq.Node
	delta []bool // per field: DELTA_BYTE_ARRAY (schema.Column.Delta)
}

func (r root) Fields() []pq.Field {
	fields := append([]pq.Field(nil), r.Node.Fields()...)
	for i := range fields {
		if r.delta[i] {
			fields[i] = encoded{fields[i], &pq.DeltaByteArray}
		}
	}
	return fields
}

// encoded overrides a field's encoding.
type encoded struct {
	pq.Field
	enc encoding.Encoding
}

func (f encoded) Encoding() encoding.Encoding { return f.enc }

// fill writes rows into dst, one parquet row each of s's columns (the
// key, then s.Columns), a column at a time: the single row-to-column
// mapping, driven by the schema's accessors. Byte values view the rows'
// strings.
func fill[P any](s *schema.Schema[P], dst []pq.Row, rows []schema.Row[P]) {
	for i := range rows {
		dst[i][0] = pq.ByteArrayValue(unsafeBytes(rows[i].Key)).Level(0, 0, 0)
	}
	for c := range s.Columns {
		fillColumn(&s.Columns[c], c+1, dst, rows)
	}
}

// fillColumn writes column col (schema column c) of rows into dst.
func fillColumn[P any](c *schema.Column[P], col int, dst []pq.Row, rows []schema.Row[P]) {
	def := 1
	if c.Required {
		def = 0
	}
	switch {
	case c.Str != nil:
		for i := range rows {
			if v, ok := c.Str(&rows[i].Data); ok {
				dst[i][col] = pq.ByteArrayValue(unsafeBytes(v)).Level(0, def, col)
			} else {
				dst[i][col] = pq.NullValue().Level(0, 0, col)
			}
		}
	case c.Type == schema.Int32 || c.Type == schema.Uint32:
		for i := range rows {
			if v, ok := c.Int(&rows[i].Data); ok {
				dst[i][col] = pq.Int32Value(int32(v)).Level(0, def, col)
			} else {
				dst[i][col] = pq.NullValue().Level(0, 0, col)
			}
		}
	case c.Bool != nil:
		for i := range rows {
			if v, ok := c.Bool(&rows[i].Data); ok {
				dst[i][col] = pq.BooleanValue(v).Level(0, def, col)
			} else {
				dst[i][col] = pq.NullValue().Level(0, 0, col)
			}
		}
	default:
		for i := range rows {
			if v, ok := c.Int(&rows[i].Data); ok {
				dst[i][col] = pq.Int64Value(v).Level(0, def, col)
			} else {
				dst[i][col] = pq.NullValue().Level(0, 0, col)
			}
		}
	}
}
