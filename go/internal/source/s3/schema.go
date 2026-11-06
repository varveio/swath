// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
)

// Object is an S3 row's payload: one current object. Empty strings are
// null.
type Object struct {
	Size              int64
	Mtime             int64  // UTC epoch microseconds
	ETag              string // quotes stripped; multipart "hex-N" kept
	StorageClass      string
	OwnerID           string
	OwnerDisplayName  string
	ChecksumAlgorithm string
	ChecksumType      string
}

// Row is one listed object.
type Row = schema.Row[Object]

// Schema is the current-object schema: every row is a current object, so
// the schema has no version columns. Nullability is fixed at run start:
// the owner columns are null unless -fetch-owner, the checksum columns are
// null when S3 omits them, etag and storage_class when it does.
var Schema = schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing", ID: "swath/s3-objects/v1", Key: schema.String, MaxKeyBytes: 1024, QuotedETag: true},
	Columns: []schema.Column[Object]{
		{Spec: schema.Spec{Name: "size", Type: schema.Int64}, Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Spec: schema.Spec{Name: "last_modified", Type: schema.TimestampMicros}, Int: func(o *Object) (int64, bool) { return o.Mtime, true }},
		// ETags are unique hex: a dictionary never pays.
		{Spec: schema.Spec{Name: "etag", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Spec: schema.Spec{Name: "storage_class", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.StorageClass })},
		{Spec: schema.Spec{Name: "owner_id", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.OwnerID })},
		{Spec: schema.Spec{Name: "owner_display_name", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.OwnerDisplayName })},
		{Spec: schema.Spec{Name: "checksum_algorithm", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ChecksumAlgorithm })},
		{Spec: schema.Spec{Name: "checksum_type", Type: schema.String}, Str: nonEmpty(func(o *Object) string { return o.ChecksumType })},
	},
	Bytes: func(o *Object) int64 { return 16 + objectHeap(o) },
	Heap:  objectHeap,
	Codec: objectCodec{},
}

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.OwnerID) + len(o.OwnerDisplayName) + len(o.ChecksumAlgorithm) + len(o.ChecksumType))
}

func nonEmpty(get func(*Object) string) func(*Object) (string, bool) {
	return func(o *Object) (string, bool) {
		v := get(o)
		return v, v != ""
	}
}

// objectCodec is the spill encoding of an Object: size and mtime as
// varints, then the six strings.
type objectCodec struct{}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	dst = binary.AppendVarint(binary.AppendVarint(dst, o.Size), o.Mtime)
	for _, s := range [...]string{o.ETag, o.StorageClass, o.OwnerID, o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType} {
		dst = schema.AppendString(dst, s)
	}
	return dst
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	o.Size, o.Mtime = d.Varint(), d.Varint()
	o.ETag, o.StorageClass, o.OwnerID = d.Str(), d.Str(), d.Str()
	o.OwnerDisplayName, o.ChecksumAlgorithm, o.ChecksumType = d.Str(), d.Str(), d.Str()
}
