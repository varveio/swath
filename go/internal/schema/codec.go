// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import (
	"encoding/binary"
	"errors"
	"unsafe"
)

// AppendString appends s length-prefixed (a uvarint, then the bytes).
func AppendString(dst []byte, s string) []byte {
	return append(binary.AppendUvarint(dst, uint64(len(s))), s...)
}

// ErrCodec reports a malformed encoding.
var ErrCodec = errors.New("spill frame: malformed rows")

// ErrDecodeCap reports a frame whose decoded rows would own more bytes
// than its bound: a few encoded bytes can stand for a long key (a shared
// prefix) or a longer string (a raw ETag), so the encoded size does not
// bound the decoded memory and the owning decoder charges every byte.
var ErrDecodeCap = errors.New("spill frame: decodes past its bound")

// Decoder reads what AppendString and the binary varint appenders wrote.
// After the first malformed value every read returns zero and Err is set.
//
// A plain decoder (NewDecoder) returns strings that view the source. An
// owning decoder (NewOwningDecoder) copies them into storage of its own,
// carved from chunks no other decoder sees and never written again once
// handed out, so the source can be reused as soon as decoding ends; every
// byte it hands out counts against its budget.
type Decoder struct {
	b      []byte
	err    error
	owning bool
	budget int64  // bytes the owning decoder may still hand out
	chunk  []byte // the free tail of the current storage chunk
}

// NewDecoder reads src; its strings view src.
func NewDecoder(src []byte) *Decoder { return &Decoder{b: src} }

// NewOwningDecoder reads src; its strings and Alloc's storage are owned,
// at most budget bytes in all (past it: ErrDecodeCap).
func NewOwningDecoder(src []byte, budget int64) *Decoder {
	return &Decoder{b: src, owning: true, budget: budget}
}

// ownedChunk is the owning decoder's storage chunk: large enough that a
// frame of rows takes a few allocations, small enough that the unused tail
// of the last one is noise against the frame. A request larger than a chunk
// gets storage of its own size.
const ownedChunk = 64 << 10

// Alloc returns n bytes of the owning decoder's storage for the caller to
// fill before taking a string of it (View); the bytes must not be written
// after that. A plain decoder cannot allocate: it fails with ErrCodec.
func (d *Decoder) Alloc(n int) []byte {
	if d.err != nil {
		return nil
	}
	if !d.owning || n < 0 {
		d.fail()
		return nil
	}
	if int64(n) > d.budget {
		d.err, d.b = ErrDecodeCap, nil
		return nil
	}
	d.budget -= int64(n)
	if n == 0 {
		return nil
	}
	if n > len(d.chunk) {
		// A new chunk, never larger than what the budget still allows: a
		// frame's storage stays within its bound.
		size := int64(max(n, ownedChunk))
		size = min(size, int64(n)+d.budget)
		d.chunk = make([]byte, size)
	}
	b := d.chunk[:n:n]
	d.chunk = d.chunk[n:]
	return b
}

// View is b as a string, without a copy: b must never be written again.
func View(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}

// Fixed reads n bytes; they view the source, so a caller keeps a copy
// (Alloc) of what outlives decoding.
func (d *Decoder) Fixed(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.b) {
		d.fail()
		return nil
	}
	b := d.b[:n:n]
	d.b = d.b[n:]
	return b
}

// Err is the first decoding error.
func (d *Decoder) Err() error { return d.err }

// Len is the number of bytes left.
func (d *Decoder) Len() int { return len(d.b) }

func (d *Decoder) fail() {
	if d.err == nil {
		d.err = ErrCodec
	}
	d.b = nil
}

// Uvarint reads an unsigned varint.
func (d *Decoder) Uvarint() uint64 {
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// Varint reads a signed varint.
func (d *Decoder) Varint() int64 {
	v, n := binary.Varint(d.b)
	if n <= 0 {
		d.fail()
		return 0
	}
	d.b = d.b[n:]
	return v
}

// Byte reads one byte.
func (d *Decoder) Byte() byte {
	if len(d.b) == 0 {
		d.fail()
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

// Str reads a length-prefixed string: it views the source bytes, or for an
// owning decoder is a copy in its storage.
func (d *Decoder) Str() string {
	n := d.Uvarint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.fail()
		return ""
	}
	if n == 0 {
		return ""
	}
	src := d.b[:n]
	d.b = d.b[n:]
	if !d.owning {
		return unsafe.String(&src[0], int(n))
	}
	b := d.Alloc(int(n))
	if d.err != nil {
		return ""
	}
	copy(b, src)
	return View(b)
}
