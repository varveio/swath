// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
)

// Object is one Cloud Storage object's listed metadata (objects.list).
type Object struct {
	Size           int64
	Mtime          int64 // updated, UTC epoch microseconds
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
var Schema = schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing", ID: "swath/gcs-objects/v1", Key: schema.String, MaxKeyBytes: 1024},
	Columns: []schema.Column[Object]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(o *Object) (int64, bool) { return o.Mtime, o.Mtime != 0 }},
		{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Spec: schema.Spec{Name: "generation", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Generation, o.Generation != 0 }},
		{Spec: schema.Spec{Name: "metageneration", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Metageneration, o.Metageneration != 0 }},
		{Spec: schema.Spec{Name: "storage_class", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.StorageClass })},
		{Spec: schema.Spec{Name: "md5", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.MD5 })},
		{Spec: schema.Spec{Name: "crc32c", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.CRC32C })},
		{Spec: schema.Spec{Name: "content_type", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ContentType })},
	},
	Bytes: func(o *Object) int64 { return 32 + objectHeap(o) },
	Heap:  objectHeap,
	Codec: objectCodec{},
}

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.MD5) + len(o.CRC32C) + len(o.ContentType))
}

func nonEmpty(get func(*Object) string) func(*Object) (string, bool) {
	return func(o *Object) (string, bool) {
		v := get(o)
		return v, v != ""
	}
}

// objectCodec is the spill encoding of an Object (private to one run).
type objectCodec struct{}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	for _, v := range [...]int64{o.Size, o.Mtime, o.Generation, o.Metageneration} {
		dst = binary.AppendVarint(dst, v)
	}
	for _, s := range [...]string{o.ETag, o.StorageClass, o.MD5, o.CRC32C, o.ContentType} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	o.Size, o.Mtime, o.Generation, o.Metageneration = d.Varint(), d.Varint(), d.Varint(), d.Varint()
	o.ETag, o.StorageClass, o.MD5, o.CRC32C, o.ContentType = d.Str(), d.Str(), d.Str(), d.Str(), d.Str()
}
