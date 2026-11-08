// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
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
var Schema = schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing", ID: "swath/azblob-objects/v1", Key: schema.String, MaxKeyBytes: MaxKeyBytes},
	Columns: []schema.Column[Object]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(o *Object) (int64, bool) { return o.Mtime, o.Mtime != 0 }},
		{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Spec: schema.Spec{Name: "content_md5", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ContentMD5 })},
		{Spec: schema.Spec{Name: "content_type", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ContentType })},
		{Spec: schema.Spec{Name: "blob_type", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.BlobType })},
		{Spec: schema.Spec{Name: "access_tier", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.AccessTier })},
		{Spec: schema.Spec{Name: "creation_time", Type: schema.TimestampMicros}, Int: func(o *Object) (int64, bool) { return o.CreationTime, o.CreationTime != 0 }},
	},
	Bytes: func(o *Object) int64 { return 24 + objectHeap(o) }, // three integers of 8 bytes
	Heap:  objectHeap,
	Codec: objectCodec{},
}

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.ContentMD5) + len(o.ContentType) + len(o.BlobType) + len(o.AccessTier))
}

func nonEmpty(get func(*Object) string) func(*Object) (string, bool) {
	return func(o *Object) (string, bool) {
		v := get(o)
		return v, v != ""
	}
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

// The delta encoding of an Object against the previous row's (spill's
// delta frames, where neighbours share their content type, blob type and
// tier and have close times): a flags byte, each integer as a signed
// varint of its wrapping difference from the previous row's, then each
// string unless it repeats the previous row's (bit i of the flags: string
// i of objectStrings), as gcs.GenerationSchema's codec.
const objectFlags = 1<<5 - 1 // one bit per string

func (objectCodec) AppendDelta(dst []byte, prev, o *Object) []byte {
	at := len(dst)
	dst = append(dst, 0)
	pi, ci := objectInts(prev), objectInts(o)
	for i := range ci {
		dst = binary.AppendVarint(dst, int64(uint64(*ci[i])-uint64(*pi[i])))
	}
	var flags byte
	ps, cs := objectStrings(prev), objectStrings(o)
	for i := range cs {
		if *cs[i] == *ps[i] {
			flags |= 1 << i
		} else {
			dst = schema.AppendString(dst, *cs[i])
		}
	}
	dst[at] = flags
	return dst
}

func (objectCodec) DecodeDelta(d *schema.Decoder, prev, o *Object) {
	flags := d.Byte()
	if flags&^objectFlags != 0 {
		d.Fixed(-1) // an unknown flag: malformed
		return
	}
	pi, ci := objectInts(prev), objectInts(o)
	for i := range ci {
		*ci[i] = int64(uint64(*pi[i]) + uint64(d.Varint()))
	}
	ps, cs := objectStrings(prev), objectStrings(o)
	for i := range cs {
		if flags&(1<<i) != 0 {
			*cs[i] = *ps[i]
		} else {
			*cs[i] = d.Str()
		}
	}
}

var _ schema.DeltaCodec[Object] = objectCodec{}
