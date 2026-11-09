// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/reader"
)

// Object is one blob's listed properties (List Blobs).
type Object struct {
	Size         int64  // Content-Length
	Mtime        int64  // Last-Modified, UTC epoch microseconds (1 s resolution on the wire); 0 when absent
	CreationTime int64  // Creation-Time, UTC epoch microseconds; 0 when absent
	ETag         string // opaque, as listed
	ContentMD5   string // base64, as listed; "" when absent
	ContentType  string
	BlobType     string // BlockBlob, PageBlob or AppendBlob
	AccessTier   string // "" when absent
}

// Row is one listed blob.
type Row = schema.Row[Object]

// MaxKeyBytes is the longest blob name in UTF-8 bytes: 1,024 UTF-16 code
// units, each at most three bytes (a four-byte scalar is two units).
const MaxKeyBytes = 3 * maxNameUnits

// Schema is the Azure Blob listing schema.
var Schema = schema.Bind(reader.AzblobObjectsV1, schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing"},
	Columns: []schema.Column[Object]{
		{Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Int: func(o *Object) (int64, bool) { return o.Mtime, o.Mtime != 0 }},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ContentMD5 })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ContentType })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.BlobType })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.AccessTier })},
		{Int: func(o *Object) (int64, bool) { return o.CreationTime, o.CreationTime != 0 }},
	},
	Bytes: func(o *Object) int64 { return 24 + objectHeap(o) }, // three integers of 8 bytes
	Heap:  objectHeap,
	Codec: objectCodec{},
})

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.ContentMD5) + len(o.ContentType) + len(o.BlobType) + len(o.AccessTier))
}

// objectCodec is the spill encoding of an Object (private to one run).
type objectCodec struct{}

func objectInts(o *Object) [3]*int64 { return [3]*int64{&o.Size, &o.Mtime, &o.CreationTime} }

func objectStrings(o *Object) [5]*string {
	return [5]*string{&o.ETag, &o.ContentMD5, &o.ContentType, &o.BlobType, &o.AccessTier}
}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	for _, v := range objectInts(o) {
		dst = binary.AppendVarint(dst, *v)
	}
	for _, s := range objectStrings(o) {
		dst = schema.AppendString(dst, *s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	for _, v := range objectInts(o) {
		*v = d.Varint()
	}
	for _, s := range objectStrings(o) {
		*s = d.Str()
	}
}

func (objectCodec) AppendDelta(dst []byte, prev, o *Object) []byte {
	pi, ci := objectInts(prev), objectInts(o)
	ps, cs := objectStrings(prev), objectStrings(o)
	return schema.AppendDelta(dst, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
}

func (objectCodec) DecodeDelta(d *schema.Decoder, prev, o *Object) {
	pi, ci := objectInts(prev), objectInts(o)
	ps, cs := objectStrings(prev), objectStrings(o)
	schema.DecodeDelta(d, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
}

var _ schema.DeltaCodec[Object] = objectCodec{}
