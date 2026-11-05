// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package pqout

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// schemaRow declares the swath-local/v0 column order and types. It is used
// only to build the schema; rows are written through the public column-values
// API (see values.go), never through the reflective struct path.
//
// key/size/last_modified are identical to Swath's canonical schema (contracts §4).
type schemaRow struct {
	Key          string  `parquet:"key"`
	Size         *int64  `parquet:"size,optional"`
	LastModified *int64  `parquet:"last_modified,optional,timestamp(microsecond:utc)"`
	Kind         string  `parquet:"kind"`
	Mode         *int32  `parquet:"mode,optional"`
	UID          *int32  `parquet:"uid,optional"`
	GID          *int32  `parquet:"gid,optional"`
	Nlink        *int32  `parquet:"nlink,optional"`
	Inode        *int64  `parquet:"inode,optional"`
	Dev          *int64  `parquet:"dev,optional"`
	Blocks       *int64  `parquet:"blocks,optional"`
	Ctime        *int64  `parquet:"ctime,optional,timestamp(microsecond:utc)"`
	Atime        *int64  `parquet:"atime,optional,timestamp(microsecond:utc)"`
	BirthTime    *int64  `parquet:"birth_time,optional,timestamp(microsecond:utc)"`
	LinkTarget   *string `parquet:"link_target,optional"`
}

// Leaf column positions (flat schema, declaration order).
const (
	colKey = iota
	colSize
	colMtime
	colKind
	colMode
	colUID
	colGID
	colNlink
	colInode
	colDev
	colBlocks
	colCtime
	colAtime
	colBtime
	colLink
	columnCount
)

// SchemaName matches Swath's MessageType name.
const SchemaName = "swath_listing"

// ColumnNames in order.
var ColumnNames = []string{"key", "size", "last_modified", "kind", "mode", "uid", "gid", "nlink",
	"inode", "dev", "blocks", "ctime", "atime", "birth_time", "link_target"}

// Schema returns the writer schema: delta-encoded key with a single-page
// ascending column-index adapter, and bare (unannotated) INT32/INT64 integer
// columns, matching Swath's unannotated `size`.
func Schema() *parquet.Schema {
	tag := reflect.StructTag(`parquet:"key,delta"`)
	root := parquet.SchemaOf(new(schemaRow), parquet.StructTag(tag, "Key"))
	return parquet.NewSchema(SchemaName, localRoot{root})
}

// MessageString renders the schema in parquet-mr MessageType.toString() form,
// for manifest.json's fileSchema (Swath stores the Java MessageType string).
func MessageString() string {
	var b strings.Builder
	fmt.Fprintf(&b, "message %s {\n", SchemaName)
	lines := []string{
		"required binary key (STRING)",
		"optional int64 size",
		"optional int64 last_modified (TIMESTAMP(MICROS,true))",
		"required binary kind (STRING)",
		"optional int32 mode",
		"optional int32 uid",
		"optional int32 gid",
		"optional int32 nlink",
		"optional int64 inode",
		"optional int64 dev",
		"optional int64 blocks",
		"optional int64 ctime (TIMESTAMP(MICROS,true))",
		"optional int64 atime (TIMESTAMP(MICROS,true))",
		"optional int64 birth_time (TIMESTAMP(MICROS,true))",
		"optional binary link_target (STRING)",
	}
	for _, l := range lines {
		fmt.Fprintf(&b, "  %s;\n", l)
	}
	b.WriteString("}\n")
	return b.String()
}

type localRoot struct{ parquet.Node }

func (root localRoot) Fields() []parquet.Field {
	fields := append([]parquet.Field(nil), root.Node.Fields()...)
	for i, f := range fields {
		switch f.Name() {
		case "key":
			fields[i] = ascendingKeyField{f}
		case "size", "inode", "dev", "blocks":
			fields[i] = bareIntField{f, parquet.Int64Type}
		case "mode", "uid", "gid", "nlink":
			fields[i] = bareIntField{f, parquet.Int32Type}
		}
	}
	return fields
}

// ascendingKeyField: with one page per column chunk, stamp BoundaryOrder
// ASCENDING (accepted by older Java readers); multi-page indexes are untouched.
type ascendingKeyField struct{ parquet.Field }

func (f ascendingKeyField) Type() parquet.Type { return ascendingType{f.Field.Type()} }

type ascendingType struct{ parquet.Type }

func (t ascendingType) NewColumnIndexer(sizeLimit int) parquet.ColumnIndexer {
	return ascendingIndexer{t.Type.NewColumnIndexer(sizeLimit)}
}

type ascendingIndexer struct{ parquet.ColumnIndexer }

func (ix ascendingIndexer) ColumnIndex() format.ColumnIndex {
	index := ix.ColumnIndexer.ColumnIndex()
	if len(index.NullPages) == 1 {
		index.BoundaryOrder = format.Ascending
	}
	return index
}

type bareIntField struct {
	parquet.Field
	typ parquet.Type
}

func (f bareIntField) Type() parquet.Type { return bareInt{f.typ} }

type bareInt struct{ parquet.Type }

func (bareInt) LogicalType() *format.LogicalType { return nil }
