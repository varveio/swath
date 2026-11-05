// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package parquet writes listing rows as Swath-layout sorted Parquet parts:
// the file writer, the schema binding and the part router.
package parquet

import (
	"fmt"
	"reflect"

	pq "github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/format"

	"github.com/varveio/swath/go/internal/schema"
)

// nodeOf builds the Parquet schema for s. Column order is the declaration
// order; the key column is delta-encoded with an ascending single-page index,
// and integer columns are bare (no INT annotation), like Swath's size column.
func nodeOf(s *schema.Schema) *pq.Schema {
	fields := make([]reflect.StructField, len(s.Columns))
	for i, c := range s.Columns {
		var t reflect.Type
		tag := c.Name
		switch c.Type {
		case schema.String:
			t = reflect.TypeFor[string]()
		case schema.Int32:
			t = reflect.TypeFor[int32]()
		case schema.Int64:
			t = reflect.TypeFor[int64]()
		case schema.TimestampMicros:
			t, tag = reflect.TypeFor[int64](), tag+",timestamp(microsecond:utc)"
		case schema.Bool:
			t = reflect.TypeFor[bool]()
		}
		if i == 0 {
			tag += ",delta"
		}
		if !c.Required {
			t = reflect.PointerTo(t)
			tag = insertOptional(tag)
		}
		fields[i] = reflect.StructField{Name: fmt.Sprintf("C%d", i), Type: t, Tag: reflect.StructTag(fmt.Sprintf("parquet:%q", tag))}
	}
	node := pq.SchemaOf(reflect.New(reflect.StructOf(fields)).Interface())
	return pq.NewSchema(s.Name, root{node, s})
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

type root struct {
	pq.Node
	s *schema.Schema
}

func (r root) Fields() []pq.Field {
	fields := append([]pq.Field(nil), r.Node.Fields()...)
	for i, f := range fields {
		switch r.s.Columns[i].Type {
		case schema.Int64:
			fields[i] = bareInt{f, pq.Int64Type}
		case schema.Int32:
			fields[i] = bareInt{f, pq.Int32Type}
		}
		if i == 0 {
			fields[i] = ascendingKey{f}
		}
		if deltaBytes[r.s.Columns[i].Name] {
			fields[i] = encoded{fields[i], &pq.DeltaByteArray}
		}
	}
	return fields
}

// deltaBytes are byte-array columns written DELTA_BYTE_ARRAY instead of
// dictionary-then-PLAIN: ETags are unique hex, so the dictionary never pays
// and parquet-mr (Java swath) falls back to DELTA_BYTE_ARRAY for them.
var deltaBytes = map[string]bool{"etag": true}

// encoded overrides a field's encoding.
type encoded struct {
	pq.Field
	enc encoding.Encoding
}

func (f encoded) Encoding() encoding.Encoding { return f.enc }

// ascendingKey stamps BoundaryOrder ASCENDING on a one-page column index
// (accepted by older Java readers); multi-page indexes are untouched.
type ascendingKey struct{ pq.Field }

func (f ascendingKey) Type() pq.Type { return ascendingType{f.Field.Type()} }

type ascendingType struct{ pq.Type }

func (t ascendingType) NewColumnIndexer(sizeLimit int) pq.ColumnIndexer {
	return ascendingIndexer{t.Type.NewColumnIndexer(sizeLimit)}
}

type ascendingIndexer struct{ pq.ColumnIndexer }

func (ix ascendingIndexer) ColumnIndex() format.ColumnIndex {
	index := ix.ColumnIndexer.ColumnIndex()
	if len(index.NullPages) == 1 {
		index.BoundaryOrder = format.Ascending
	}
	return index
}

// bareInt removes parquet-go's INT(n,true) annotation.
type bareInt struct {
	pq.Field
	typ pq.Type
}

func (f bareInt) Type() pq.Type { return plainInt{f.typ} }

type plainInt struct{ pq.Type }

func (plainInt) LogicalType() *format.LogicalType { return nil }

// fill writes column c of rows into dst as parquet values: the single
// row-to-column mapping, driven by the schema's accessors.
func fill(c *schema.Column, col int, dst []pq.Value, rows []schema.Row) {
	def := 1
	if c.Required {
		def = 0
	}
	switch {
	case c.Str != nil:
		for i := range rows {
			if v, ok := c.Str(&rows[i]); ok {
				dst[i] = pq.ByteArrayValue(unsafeBytes(v)).Level(0, def, col)
			} else {
				dst[i] = pq.NullValue().Level(0, 0, col)
			}
		}
	case c.Type == schema.Int32:
		for i := range rows {
			if v, ok := c.Int(&rows[i]); ok {
				dst[i] = pq.Int32Value(int32(v)).Level(0, def, col)
			} else {
				dst[i] = pq.NullValue().Level(0, 0, col)
			}
		}
	case c.Bool != nil:
		for i := range rows {
			if v, ok := c.Bool(&rows[i]); ok {
				dst[i] = pq.BooleanValue(v).Level(0, def, col)
			} else {
				dst[i] = pq.NullValue().Level(0, 0, col)
			}
		}
	default:
		for i := range rows {
			if v, ok := c.Int(&rows[i]); ok {
				dst[i] = pq.Int64Value(v).Level(0, def, col)
			} else {
				dst[i] = pq.NullValue().Level(0, 0, col)
			}
		}
	}
}
