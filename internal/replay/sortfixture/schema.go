// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package sortfixture

import (
	"encoding/binary"

	"github.com/varveio/swath/internal/schema"
)

// Object is a sorted capture row's payload: one current object. A size or
// time without its Has flag is null; a string is null unless its bit is
// set in Present, so an empty string stays distinct from a null one (the
// replay server renders <ETag>&quot;&quot;</ETag> for one and no ETag for
// the other). It holds no pointer to a separate allocation: a capture is
// held whole in memory.
type Object struct {
	Size              int64
	Mtime             int64 // UTC epoch microseconds
	HasSize, HasMtime bool
	Present           Strings
	ETag              string
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string
	ChecksumType      string
}

// Strings is the set of an Object's string fields that are present (not
// null).
type Strings uint8

// The string fields of an Object.
const (
	HasETag Strings = 1 << iota
	HasStorageClass
	HasOwnerID
	HasOwnerDisplayName
	HasChecksumAlgorithm
	HasChecksumType
)

// newSchema returns the replay-owned current-object schema with a key cap
// of keyLimit bytes: the nine columns of the S3 current-object schema, so
// readers project it as they project S3 listings, under its own id because
// a converted capture is not a listing by the Go tool.
func newSchema(keyLimit int) *schema.Schema[Object] {
	text := func(bit Strings, get func(*Object) string) func(*Object) (string, bool) {
		return func(o *Object) (string, bool) { return get(o), o.Present&bit != 0 }
	}
	number := func(get func(*Object) (int64, bool)) func(*Object) (int64, bool) { return get }
	return &schema.Schema[Object]{
		Info: schema.Info{Name: "swath_listing", ID: SchemaID, Key: schema.String, MaxKeyBytes: keyLimit},
		Columns: []schema.Column[Object]{
			{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: number(func(o *Object) (int64, bool) { return o.Size, o.HasSize })},
			{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: number(func(o *Object) (int64, bool) { return o.Mtime, o.HasMtime })},
			// ETags are unique hex: a dictionary never pays.
			{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: text(HasETag, func(o *Object) string { return o.ETag }), Delta: true},
			{Spec: schema.Spec{Name: "storage_class", Type: schema.String}, Str: text(HasStorageClass, func(o *Object) string { return o.StorageClass })},
			{Spec: schema.Spec{Name: "owner_id", Type: schema.String}, Str: text(HasOwnerID, func(o *Object) string { return o.OwnerID })},
			{Spec: schema.Spec{Name: "owner_display_name", Type: schema.String}, Str: text(HasOwnerDisplayName, func(o *Object) string { return o.OwnerDisplayName })},
			{Spec: schema.Spec{Name: "checksum_algorithm", Type: schema.String}, Str: text(HasChecksumAlgorithm, func(o *Object) string { return o.ChecksumAlgorithm })},
			{Spec: schema.Spec{Name: "checksum_type", Type: schema.String}, Str: text(HasChecksumType, func(o *Object) string { return o.ChecksumType })},
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
	for _, v := range [...]struct {
		n   int64
		has bool
	}{{o.Size, o.HasSize}, {o.Mtime, o.HasMtime}} {
		if !v.has {
			dst = append(dst, 0)
		} else {
			dst = binary.AppendVarint(append(dst, 1), v.n)
		}
	}
	dst = append(dst, byte(o.Present))
	for _, s := range [...]string{o.ETag, o.StorageClass, o.OwnerID, o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	o.Size, o.HasSize, o.Mtime, o.HasMtime = 0, false, 0, false
	if d.Byte() == 1 {
		o.Size, o.HasSize = d.Varint(), true
	}
	if d.Byte() == 1 {
		o.Mtime, o.HasMtime = d.Varint(), true
	}
	o.Present = Strings(d.Byte())
	o.ETag, o.StorageClass, o.OwnerID = d.Str(), d.Str(), d.Str()
	o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType = d.Str(), d.Str(), d.Str()
}
