// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"encoding/binary"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/reader"
)

// Object is one Cloud Storage object's listed metadata (objects.list).
type Object struct {
	Size           int64
	Mtime          int64 // updated, UTC epoch microseconds
	TimeCreated    int64 // timeCreated, UTC epoch microseconds; 0 when not listed
	ETag           string
	Generation     int64
	Metageneration int64
	StorageClass   string
	MD5            string // base64, as listed
	CRC32C         string // base64, as listed
	ContentType    string
}

// Row is one listed object.
type Row = schema.Row[Object]

// Schema is the GCS listing schema. Object names are at most 1,024 bytes
// of UTF-8.
var Schema = schema.Bind(reader.GCSObjectsV1, schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing", StorageClass: "storage_class"},
	Columns: []schema.Column[Object]{
		{Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Int: func(o *Object) (int64, bool) { return o.Mtime, o.Mtime != 0 }},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Int: func(o *Object) (int64, bool) { return o.Generation, o.Generation != 0 }},
		{Int: func(o *Object) (int64, bool) { return o.Metageneration, o.Metageneration != 0 }},
		{Str: schema.NonEmpty(func(o *Object) string { return o.StorageClass })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.MD5 })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.CRC32C })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ContentType })},
		// time_created is an optional column of the schema id, placed
		// last: parts written before the lister recorded it lack it, so a
		// reader may read it where present but not require it
		// (docs/dataset.md, "Schema registry").
		{Int: func(o *Object) (int64, bool) { return o.TimeCreated, o.TimeCreated != 0 }},
	},
	Bytes: func(o *Object) int64 { return 40 + objectHeap(o) }, // five integers of 8 bytes
	Heap:  objectHeap,
	Codec: objectCodec{},
})

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.MD5) + len(o.CRC32C) + len(o.ContentType))
}

// objectCodec is the spill encoding of an Object (private to one run).
type objectCodec struct{}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	for _, v := range [...]int64{o.Size, o.Mtime, o.Generation, o.Metageneration, o.TimeCreated} {
		dst = binary.AppendVarint(dst, v)
	}
	for _, s := range [...]string{o.ETag, o.StorageClass, o.MD5, o.CRC32C, o.ContentType} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	o.Size, o.Mtime, o.Generation, o.Metageneration, o.TimeCreated = d.Varint(), d.Varint(), d.Varint(), d.Varint(), d.Varint()
	o.ETag, o.StorageClass, o.MD5, o.CRC32C, o.ContentType = d.Str(), d.Str(), d.Str(), d.Str(), d.Str()
}
