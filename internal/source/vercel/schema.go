// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package vercel

import (
	"encoding/binary"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/reader"
)

// Object is the metadata of one current Vercel Blob object.
type Object struct {
	Size       int64
	UploadedAt int64  // UTC epoch microseconds from the list API, persisted as created_at
	ETag       string // opaque service tag, including its quotes if present
}

// Schema records the upload time of the current blob as created_at.
var Schema = schema.Bind(reader.VercelObjectsV1, schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing"},
	Columns: []schema.Column[Object]{
		{Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Int: func(o *Object) (int64, bool) { return o.UploadedAt, true }},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
	},
	Bytes: func(o *Object) int64 { return 16 + objectHeap(o) },
	Heap:  objectHeap,
	Codec: objectCodec{},
})

func objectHeap(o *Object) int64 { return int64(len(o.ETag)) }

type objectCodec struct{}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	dst = binary.AppendVarint(dst, o.Size)
	dst = binary.AppendVarint(dst, o.UploadedAt)
	return schema.AppendString(dst, o.ETag)
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	o.Size, o.UploadedAt, o.ETag = d.Varint(), d.Varint(), d.Str()
}
