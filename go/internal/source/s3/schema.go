// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/varveio/swath/go/internal/schema"
	"github.com/varveio/swath/go/reader"
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
// the owner columns are null unless --s3-fetch-owner, the checksum columns are
// null when S3 omits them, etag and storage_class when it does.
var Schema = schema.Bind(reader.S3ObjectsV1, schema.Schema[Object]{
	Info: schema.Info{Name: "swath_listing", StorageClass: "storage_class"},
	Columns: []schema.Column[Object]{
		{Int: func(o *Object) (int64, bool) { return o.Size, true }},
		{Int: func(o *Object) (int64, bool) { return o.Mtime, true }},
		// ETags are unique hex: a dictionary never pays.
		{Str: schema.NonEmpty(func(o *Object) string { return o.ETag }), Delta: true},
		{Str: schema.NonEmpty(func(o *Object) string { return o.StorageClass })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.OwnerID })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.OwnerDisplayName })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ChecksumAlgorithm })},
		{Str: schema.NonEmpty(func(o *Object) string { return o.ChecksumType })},
	},
	Bytes: func(o *Object) int64 { return 16 + objectHeap(o) },
	Heap:  objectHeap,
	Codec: objectCodec{},
})

func objectHeap(o *Object) int64 {
	return int64(len(o.ETag) + len(o.StorageClass) + len(o.OwnerID) + len(o.OwnerDisplayName) + len(o.ChecksumAlgorithm) + len(o.ChecksumType))
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

// The delta encoding of an Object against the previous row's (spill's delta
// frames): a flags byte, the size as a signed varint (as in Append), the
// time as a signed varint of its difference from the previous time (zigzag:
// neighbours' times cluster, so the difference is small either way) or,
// when the difference overflows, the time itself; the ETag; then each other
// string unless it repeats the previous row's.
const (
	deltaTimeAbs   = 1 << 0 // the time itself, not a difference (int64 overflow)
	deltaETagShift = 1      // two bits, the ETag's form (etag*)
	deltaETagMask  = 3 << deltaETagShift
	// deltaSame + i: string i of sameStrings repeats the previous row's.
	deltaSame = 3
)

// The ETag's forms. A raw ETag is 16 bytes standing for exactly 32
// lowercase hex digits (an MD5, how S3 spells a single-part ETag once its
// quotes are stripped, which is how Object carries it): it round-trips
// byte for byte. Any other spelling (uppercase hex, multipart "hex-N",
// quoted, empty) is the string itself.
const (
	etagString = iota
	etagSame
	etagRaw
)

// sameStrings are the strings after the ETag, in order, each with a repeat
// flag (bits deltaSame..deltaSame+4).
func sameStrings(o *Object) [5]*string {
	return [5]*string{&o.StorageClass, &o.OwnerID, &o.OwnerDisplayName, &o.ChecksumAlgorithm, &o.ChecksumType}
}

func (objectCodec) AppendDelta(dst []byte, prev, o *Object) []byte {
	at := len(dst)
	dst = append(dst, 0)
	var flags byte
	dst = binary.AppendVarint(dst, o.Size)
	if d := o.Mtime - prev.Mtime; (o.Mtime^prev.Mtime)&(o.Mtime^d) < 0 {
		flags |= deltaTimeAbs
		dst = binary.AppendVarint(dst, o.Mtime)
	} else {
		dst = binary.AppendVarint(dst, d)
	}
	switch {
	case o.ETag == prev.ETag:
		flags |= etagSame << deltaETagShift
	default:
		if out, ok := appendRawETag(dst, o.ETag); ok {
			flags |= etagRaw << deltaETagShift
			dst = out
		} else {
			dst = schema.AppendString(dst, o.ETag)
		}
	}
	ps, cur := sameStrings(prev), sameStrings(o)
	for i := range cur {
		if *cur[i] == *ps[i] {
			flags |= 1 << (deltaSame + i)
		} else {
			dst = schema.AppendString(dst, *cur[i])
		}
	}
	dst[at] = flags
	return dst
}

func (objectCodec) DecodeDelta(d *schema.Decoder, prev, o *Object) {
	flags := d.Byte()
	o.Size = d.Varint()
	if t := d.Varint(); flags&deltaTimeAbs != 0 {
		o.Mtime = t
	} else {
		o.Mtime = prev.Mtime + t // wraps only on a malformed frame, which its CRC guards
	}
	switch (flags & deltaETagMask) >> deltaETagShift {
	case etagString:
		o.ETag = d.Str()
	case etagSame:
		o.ETag = prev.ETag
	case etagRaw:
		raw := d.Fixed(md5Bytes)
		b := d.Alloc(2 * md5Bytes)
		if d.Err() != nil {
			return
		}
		hex.Encode(b, raw)
		o.ETag = schema.View(b)
	default:
		d.Fixed(-1) // an unknown form: malformed
		return
	}
	ps, cur := sameStrings(prev), sameStrings(o)
	for i := range cur {
		if flags&(1<<(deltaSame+i)) != 0 {
			*cur[i] = *ps[i]
		} else {
			*cur[i] = d.Str()
		}
	}
}

// md5Bytes is an MD5's size: a single-part ETag is its 32 hex digits.
const md5Bytes = 16

// appendRawETag appends etag's 16 raw bytes when it is exactly 32
// lowercase hex digits (the spelling hex.Encode gives back), else returns
// dst unchanged and false. One pass through a table: the check is on
// every spilled row.
func appendRawETag(dst []byte, etag string) ([]byte, bool) {
	if len(etag) != 2*md5Bytes {
		return dst, false
	}
	var raw [md5Bytes]byte
	var bad byte
	for i := range raw {
		hi, lo := lowerHex[etag[2*i]], lowerHex[etag[2*i+1]]
		bad |= hi | lo
		raw[i] = hi<<4 | lo&0x0f
	}
	if bad&0xf0 != 0 {
		return dst, false
	}
	return append(dst, raw[:]...), true
}

// lowerHex maps a lowercase hex digit to its value and every other byte to
// 0xff (its high bits mark it invalid).
var lowerHex = func() (t [256]byte) {
	for i := range t {
		t[i] = 0xff
	}
	for i, c := range "0123456789abcdef" {
		t[c] = byte(i)
	}
	return t
}()

var _ schema.DeltaCodec[Object] = objectCodec{}
