// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package schema

import "encoding/binary"

// DeltaFields selects a payload's integers and strings in spill order.
// Previous and current lists must have equal lengths, with at most eight
// strings and a non-nil pointer for every field.
type DeltaFields struct {
	Ints    []*int64
	Strings []*string
}

// AppendDelta appends a string-repeat mask, wrapping signed integer
// differences, then length-prefixed strings that differ from prev. Integer
// differences use int64(uint64(cur)-uint64(prev)); bit i of the mask means
// string i repeats, otherwise its raw bytes follow a uvarint length.
func AppendDelta(dst []byte, prev, cur DeltaFields) []byte {
	at := len(dst)
	dst = append(dst, 0)
	for i, v := range cur.Ints {
		dst = binary.AppendVarint(dst, int64(uint64(*v)-uint64(*prev.Ints[i])))
	}
	var flags byte
	for i, s := range cur.Strings {
		if *s == *prev.Strings[i] {
			flags |= 1 << i
		} else {
			dst = AppendString(dst, *s)
		}
	}
	dst[at] = flags
	return dst
}

// DecodeDelta reads AppendDelta's encoding. Repeated strings share prev's
// storage; new strings use d's storage and its owning budget. Integers are
// reconstructed by unsigned wrapping addition. Unknown mask bits and
// malformed values fail through d.Err().
func DecodeDelta(d *Decoder, prev, cur DeltaFields) {
	flags := d.Byte()
	known := byte((1 << len(cur.Strings)) - 1)
	if flags&^known != 0 {
		d.fail()
		return
	}
	for i, v := range cur.Ints {
		*v = int64(uint64(*prev.Ints[i]) + uint64(d.Varint()))
	}
	for i, s := range cur.Strings {
		if flags&(1<<i) != 0 {
			*s = *prev.Strings[i]
		} else {
			*s = d.Str()
		}
	}
}
