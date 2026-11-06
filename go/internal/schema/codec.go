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

// Decoder reads what AppendString and the binary varint appenders wrote.
// After the first malformed value every read returns zero and Err is set.
type Decoder struct {
	b   []byte
	err error
}

// NewDecoder reads src.
func NewDecoder(src []byte) *Decoder { return &Decoder{b: src} }

// Err is the first decoding error.
func (d *Decoder) Err() error { return d.err }

// Len is the number of bytes left.
func (d *Decoder) Len() int { return len(d.b) }

func (d *Decoder) fail() {
	d.err, d.b = ErrCodec, nil
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

// Str reads a length-prefixed string that views the source bytes.
func (d *Decoder) Str() string {
	n := d.Uvarint()
	if d.err != nil || n > uint64(len(d.b)) {
		d.fail()
		return ""
	}
	if n == 0 {
		return ""
	}
	s := unsafe.String(&d.b[0], int(n))
	d.b = d.b[n:]
	return s
}
