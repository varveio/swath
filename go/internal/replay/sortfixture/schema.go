// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortfixture

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
)

// Object is a sorted capture row's payload: one current object. Nil size or
// time and empty strings are nulls.
type Object struct {
	Size              *int64
	Mtime             *int64 // UTC epoch microseconds
	ETag              string
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string
	ChecksumType      string
}

// newSchema returns the replay-owned current-object schema with a key cap
// of keyLimit bytes: the nine columns of the S3 current-object schema, so
// readers project it as they project S3 listings, under its own id because
// a converted capture is not a listing by the Go tool.
func newSchema(keyLimit int) *schema.Schema[Object] {
	text := func(get func(*Object) string) func(*Object) (string, bool) {
		return func(o *Object) (string, bool) {
			v := get(o)
			return v, v != ""
		}
	}
	number := func(get func(*Object) *int64) func(*Object) (int64, bool) {
		return func(o *Object) (int64, bool) {
			if v := get(o); v != nil {
				return *v, true
			}
			return 0, false
		}
	}
	return &schema.Schema[Object]{
		Info: schema.Info{Name: "swath_listing", ID: SchemaID, Key: schema.String, MaxKeyBytes: keyLimit, QuotedETag: true},
		Columns: []schema.Column[Object]{
			{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: number(func(o *Object) *int64 { return o.Size })},
			{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: number(func(o *Object) *int64 { return o.Mtime })},
			// ETags are unique hex: a dictionary never pays.
			{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: text(func(o *Object) string { return o.ETag }), Delta: true},
			{Spec: schema.Spec{Name: "storage_class", Type: schema.String}, Str: text(func(o *Object) string { return o.StorageClass })},
			{Spec: schema.Spec{Name: "owner_id", Type: schema.String}, Str: text(func(o *Object) string { return o.OwnerID })},
			{Spec: schema.Spec{Name: "owner_display_name", Type: schema.String}, Str: text(func(o *Object) string { return o.OwnerDisplayName })},
			{Spec: schema.Spec{Name: "checksum_algorithm", Type: schema.String}, Str: text(func(o *Object) string { return o.ChecksumAlgorithm })},
			{Spec: schema.Spec{Name: "checksum_type", Type: schema.String}, Str: text(func(o *Object) string { return o.ChecksumType })},
		},
		Bytes: func(o *Object) int64 { return 16 + objectHeap(o) },
		Heap:  objectHeap,
		Codec: objectCodec{},
	}
}

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.OwnerID) + len(o.OwnerDisplayName) + len(o.ChecksumAlgorithm) + len(o.ChecksumType))
}

// objectCodec is the spill encoding of an Object (the writer does not spill
// a sorted batch; the schema requires one): presence and value of size and
// time, then the six strings.
type objectCodec struct{}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	for _, v := range [...]*int64{o.Size, o.Mtime} {
		if v == nil {
			dst = append(dst, 0)
		} else {
			dst = binary.AppendVarint(append(dst, 1), *v)
		}
	}
	for _, s := range [...]string{o.ETag, o.StorageClass, o.OwnerID, o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	for _, v := range [...]**int64{&o.Size, &o.Mtime} {
		*v = nil
		if d.Byte() == 1 {
			x := d.Varint()
			*v = &x
		}
	}
	o.ETag, o.StorageClass, o.OwnerID = d.Str(), d.Str(), d.Str()
	o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType = d.Str(), d.Str(), d.Str()
}
