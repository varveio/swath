// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"encoding/binary"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/internal/source/azblob/client"
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
	// Props are the other properties, as the client read them, with
	// Metadata, Tags and LegalHold set to their empty value where the run
	// asked for them and the blob has none (fill).
	client.Props
}

// Row is one listed blob.
type Row = schema.Row[Object]

// MaxKeyBytes is the longest blob name in UTF-8 bytes: 1,024 UTF-16 code
// units, each at most three bytes (a four-byte scalar is two units).
const MaxKeyBytes = 3 * maxNameUnits

// Schema is the Azure Blob listing schema.
var Schema = schema.Bind(reader.AzblobObjectsV1, schema.Schema[Object]{
	Info:    schema.Info{Name: "swath_listing", StorageClass: "access_tier"}, // the access tier is its storage class
	Columns: objectColumns,
	Bytes:   objectBytes,
	Heap:    objectHeap,
	Codec:   objectCodec{},
})

// objectColumns are swath/azblob-objects/v1's, in order. The columns from
// access_tier_inferred on were added to the schema id later
// (reader.ColumnSpec.MayBeAbsent).
var objectColumns = []schema.Column[Object]{
	{Int: func(o *Object) (int64, bool) { return o.Size, true }},
	{Int: func(o *Object) (int64, bool) { return o.Mtime, o.Mtime != 0 }},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ContentMD5 })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ContentType })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.BlobType })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.AccessTier })},
	{Int: func(o *Object) (int64, bool) { return o.CreationTime, o.CreationTime != 0 }},
	{Bool: func(o *Object) (bool, bool) { return o.AccessTierInferred.Get() }},
	{Int: func(o *Object) (int64, bool) { return o.AccessTierChangeTime, o.AccessTierChangeTime != 0 }},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ArchiveStatus })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.RehydratePriority })},
	{Int: func(o *Object) (int64, bool) { return o.LastAccessTime, o.LastAccessTime != 0 }},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ContentEncoding })},
	{Bool: func(o *Object) (bool, bool) { return o.ServerEncrypted.Get() }},
	{Str: schema.NonEmpty(func(o *Object) string { return o.EncryptionScope })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.CustomerProvidedKeySHA256 })},
	{Str: schema.NonEmpty(func(o *Object) string { return o.LeaseState })},
	{Int: func(o *Object) (int64, bool) { return o.TagCount, o.TagCount != 0 }},
	// Metadata and tags are mostly distinct per blob: a dictionary rarely pays.
	{Str: schema.NonEmpty(func(o *Object) string { return o.Metadata }), Delta: true},
	{Str: schema.NonEmpty(func(o *Object) string { return o.Tags }), Delta: true},
	{Int: func(o *Object) (int64, bool) { return o.ImmutabilityUntil, o.ImmutabilityUntil != 0 }},
	{Str: schema.NonEmpty(func(o *Object) string { return o.ImmutabilityMode })},
	{Bool: func(o *Object) (bool, bool) { return o.LegalHold.Get() }},
}

// objectBytes is the logical size: 8 bytes per integer column with a
// value (the first three always count), 1 per boolean with one, and the
// strings. A listing whose blobs carry none of the later properties sizes
// its rows as before they were added, so its parts are cut alike.
func objectBytes(o *Object) int64 {
	n := 24 + objectHeap(o)
	for _, v := range objectProps(o).ints() {
		if *v != 0 {
			n += 8
		}
	}
	for _, t := range objectTris(o) {
		if *t != client.Unset {
			n++
		}
	}
	return n
}

func objectHeap(o *Object) int64 {
	n := int64(len(o.ETag) + len(o.ContentMD5) + len(o.ContentType) + len(o.BlobType) + len(o.AccessTier))
	for _, s := range objectProps(o).strings() {
		n += int64(len(*s))
	}
	return n
}

// objectCodec is the spill encoding of an Object (private to one run): the
// first eight columns' fields, the properties' integers and strings, then
// one byte packing the three optional booleans. Its delta form encodes each
// group against the previous row's (schema.AppendDelta, eight strings at
// most per group).
type objectCodec struct{}

func objectInts(o *Object) [3]*int64 { return [3]*int64{&o.Size, &o.Mtime, &o.CreationTime} }

func objectStrings(o *Object) [5]*string {
	return [5]*string{&o.ETag, &o.ContentMD5, &o.ContentType, &o.BlobType, &o.AccessTier}
}

// propFields are the properties' integers and strings, in spill order.
type propFields struct {
	i [4]*int64
	s [8]*string
	t *string // the ninth string: its own delta group
}

func objectProps(o *Object) propFields {
	p := &o.Props
	return propFields{
		i: [4]*int64{&p.AccessTierChangeTime, &p.LastAccessTime, &p.TagCount, &p.ImmutabilityUntil},
		s: [8]*string{&p.ArchiveStatus, &p.RehydratePriority, &p.ContentEncoding, &p.EncryptionScope,
			&p.CustomerProvidedKeySHA256, &p.LeaseState, &p.ImmutabilityMode, &p.Metadata},
		t: &p.Tags,
	}
}

func (f propFields) ints() []*int64     { return f.i[:] }
func (f propFields) strings() []*string { return append(f.s[:], f.t) }

func objectTris(o *Object) [3]*client.Tri {
	return [3]*client.Tri{&o.AccessTierInferred, &o.ServerEncrypted, &o.LegalHold}
}

// packTris is the three booleans in one byte, two bits each; unpackTris
// reads it, marking d malformed for a value no Tri has.
func packTris(o *Object) byte {
	var b byte
	for i, t := range objectTris(o) {
		b |= byte(*t) << (2 * i)
	}
	return b
}

func unpackTris(d *schema.Decoder, o *Object, b byte) {
	for i, t := range objectTris(o) {
		v := client.Tri(b >> (2 * i) & 3)
		if v > client.True {
			d.Fixed(-1)
			return
		}
		*t = v
	}
	if b>>6 != 0 {
		d.Fixed(-1)
	}
}

func (objectCodec) Append(dst []byte, o *Object) []byte {
	for _, v := range objectInts(o) {
		dst = binary.AppendVarint(dst, *v)
	}
	for _, s := range objectStrings(o) {
		dst = schema.AppendString(dst, *s)
	}
	f := objectProps(o)
	for _, v := range f.ints() {
		dst = binary.AppendVarint(dst, *v)
	}
	for _, s := range f.strings() {
		dst = schema.AppendString(dst, *s)
	}
	return append(dst, packTris(o))
}

func (objectCodec) Decode(d *schema.Decoder, o *Object) {
	for _, v := range objectInts(o) {
		*v = d.Varint()
	}
	for _, s := range objectStrings(o) {
		*s = d.Str()
	}
	f := objectProps(o)
	for _, v := range f.ints() {
		*v = d.Varint()
	}
	for _, s := range f.strings() {
		*s = d.Str()
	}
	unpackTris(d, o, d.Byte())
}

func (objectCodec) AppendDelta(dst []byte, prev, o *Object) []byte {
	pi, ci := objectInts(prev), objectInts(o)
	ps, cs := objectStrings(prev), objectStrings(o)
	dst = schema.AppendDelta(dst, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
	pf, cf := objectProps(prev), objectProps(o)
	dst = schema.AppendDelta(dst, schema.DeltaFields{Ints: pf.i[:], Strings: pf.s[:]}, schema.DeltaFields{Ints: cf.i[:], Strings: cf.s[:]})
	dst = schema.AppendDelta(dst, schema.DeltaFields{Strings: []*string{pf.t}}, schema.DeltaFields{Strings: []*string{cf.t}})
	return append(dst, packTris(o))
}

func (objectCodec) DecodeDelta(d *schema.Decoder, prev, o *Object) {
	pi, ci := objectInts(prev), objectInts(o)
	ps, cs := objectStrings(prev), objectStrings(o)
	schema.DecodeDelta(d, schema.DeltaFields{Ints: pi[:], Strings: ps[:]}, schema.DeltaFields{Ints: ci[:], Strings: cs[:]})
	pf, cf := objectProps(prev), objectProps(o)
	schema.DecodeDelta(d, schema.DeltaFields{Ints: pf.i[:], Strings: pf.s[:]}, schema.DeltaFields{Ints: cf.i[:], Strings: cf.s[:]})
	schema.DecodeDelta(d, schema.DeltaFields{Strings: []*string{pf.t}}, schema.DeltaFields{Strings: []*string{cf.t}})
	unpackTris(d, o, d.Byte())
}

var _ schema.DeltaCodec[Object] = objectCodec{}
