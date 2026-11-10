// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

// A minimal Thrift compact-protocol splicer. The footer is not decoded
// into parquet-go's format.FileMetaData and encoded again: that round trip
// drops optional fields the Java writer set to zero (parquet-mr 1.15 writes
// size_statistics.unencoded_byte_array_data_bytes = 0 for all-null columns,
// which format.SizeStatistics cannot tell from absent) and any field
// parquet-go does not model. Instead the footer is split into raw fields,
// the few that change are rebuilt from fields encoded with parquet-go's
// thrift codec, and every other field is copied byte for byte.

// Compact-protocol type ids.
const (
	tStop   = 0
	tTrue   = 1
	tFalse  = 2
	tByte   = 3
	tI16    = 4
	tI32    = 5
	tI64    = 6
	tDouble = 7
	tBinary = 8
	tList   = 9
	tSet    = 10
	tMap    = 11
	tStruct = 12
)

// maxDepth bounds nesting; a Parquet footer nests a handful of levels.
const maxDepth = 64

var errThrift = errors.New("malformed thrift compact encoding")

// field is one raw struct field: its id, its compact type and the bytes of
// its value (empty for a boolean field, whose value is its type).
type field struct {
	id  int16
	typ byte
	val []byte
}

func uvarint(b []byte) (uint64, int, error) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, 0, errThrift
	}
	return v, n, nil
}

// valueLen returns the length of the value of type typ at the start of b.
// elem is true inside a list, set or map, where a boolean takes one byte.
func valueLen(b []byte, typ byte, elem bool, depth int) (int, error) {
	if depth > maxDepth {
		return 0, fmt.Errorf("%w: nested deeper than %d", errThrift, maxDepth)
	}
	switch typ {
	case tTrue, tFalse:
		if elem {
			if len(b) < 1 {
				return 0, errThrift
			}
			return 1, nil
		}
		return 0, nil
	case tByte:
		if len(b) < 1 {
			return 0, errThrift
		}
		return 1, nil
	case tI16, tI32, tI64:
		_, n, err := uvarint(b)
		return n, err
	case tDouble:
		if len(b) < 8 {
			return 0, errThrift
		}
		return 8, nil
	case tBinary:
		l, n, err := uvarint(b)
		if err != nil || l > uint64(len(b)-n) {
			return 0, errThrift
		}
		return n + int(l), nil
	case tList, tSet:
		_, _, n, err := splitList(b, depth)
		return n, err
	case tMap:
		size, off, err := uvarint(b)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			return off, nil
		}
		if off >= len(b) || size > uint64(len(b)) {
			return 0, errThrift
		}
		kt, vt := b[off]>>4, b[off]&15
		off++
		for range size {
			for _, t := range [2]byte{kt, vt} {
				n, err := valueLen(b[off:], t, true, depth+1)
				if err != nil {
					return 0, err
				}
				off += n
			}
		}
		return off, nil
	case tStruct:
		_, n, err := splitStruct(b, depth+1)
		return n, err
	}
	return 0, fmt.Errorf("%w: type %d", errThrift, typ)
}

// splitStruct splits the struct at the start of b into its fields and
// returns the bytes it takes, its stop byte included.
func splitStruct(b []byte, depth int) ([]field, int, error) {
	var fields []field
	var last int16
	i := 0
	for {
		if i >= len(b) {
			return nil, 0, errThrift
		}
		h := b[i]
		i++
		if h == tStop {
			return fields, i, nil
		}
		typ, delta := h&15, int16(h>>4)
		id := last + delta
		if delta == 0 {
			v, n, err := uvarint(b[i:])
			if err != nil {
				return nil, 0, err
			}
			i += n
			id = int16(int64(v>>1) ^ -int64(v&1))
		}
		n, err := valueLen(b[i:], typ, false, depth)
		if err != nil {
			return nil, 0, err
		}
		fields = append(fields, field{id: id, typ: typ, val: b[i : i+n]})
		i += n
		last = id
	}
}

// splitList splits the list (or set) at the start of b into its element
// type and the raw bytes of each element, and returns the bytes it takes.
func splitList(b []byte, depth int) (byte, [][]byte, int, error) {
	if len(b) < 1 {
		return 0, nil, 0, errThrift
	}
	size, et, off := uint64(b[0]>>4), b[0]&15, 1
	if size == 15 {
		v, n, err := uvarint(b[1:])
		if err != nil {
			return 0, nil, 0, err
		}
		size, off = v, 1+n
	}
	if size > uint64(len(b)) {
		return 0, nil, 0, errThrift
	}
	elems := make([][]byte, 0, size)
	for range size {
		n, err := valueLen(b[off:], et, true, depth+1)
		if err != nil {
			return 0, nil, 0, err
		}
		elems = append(elems, b[off:off+n])
		off += n
	}
	return et, elems, off, nil
}

// joinStruct encodes fields, in the order given, as a struct.
func joinStruct(fields []field) []byte {
	var out []byte
	var last int16
	for _, f := range fields {
		if d := int(f.id) - int(last); d > 0 && d <= 15 {
			out = append(out, byte(d<<4)|f.typ)
		} else {
			out = append(out, f.typ)
			out = binary.AppendUvarint(out, uint64((int64(f.id)<<1)^(int64(f.id)>>63)))
		}
		out = append(out, f.val...)
		last = f.id
	}
	return append(out, tStop)
}

// joinList encodes elements of type et as a list.
func joinList(et byte, elems [][]byte) []byte {
	var out []byte
	if len(elems) < 15 {
		out = append(out, byte(len(elems)<<4)|et)
	} else {
		out = binary.AppendUvarint(append(out, 0xF0|et), uint64(len(elems)))
	}
	for _, e := range elems {
		out = append(out, e...)
	}
	return out
}

// lookup returns the field with id, if present.
func lookup(fields []field, id int16) (field, bool) {
	for _, f := range fields {
		if f.id == id {
			return f, true
		}
	}
	return field{}, false
}

// insert adds f before the first field with a larger id, keeping ids
// ascending; a field with the same id is an error.
func insert(fields []field, f field) ([]field, error) {
	for i, g := range fields {
		if g.id == f.id {
			return nil, fmt.Errorf("field %d is already present", f.id)
		}
		if g.id > f.id {
			return append(fields[:i:i], append([]field{f}, fields[i:]...)...), nil
		}
	}
	return append(fields, f), nil
}

// flatten lists every leaf of the struct b as path=value, lists as
// path[i] with path.len, for comparing two footers field by field.
func flatten(b []byte) (map[string]string, error) {
	out := map[string]string{}
	if _, err := flattenStruct(b, "", out, 0); err != nil {
		return nil, err
	}
	return out, nil
}

func flattenStruct(b []byte, path string, out map[string]string, depth int) (int, error) {
	fields, n, err := splitStruct(b, depth)
	if err != nil {
		return 0, err
	}
	out[path+"{}"] = ""
	for _, f := range fields {
		if err := flattenValue(f.val, f.typ, fmt.Sprintf("%s.%d", path, f.id), out, depth+1); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func flattenValue(b []byte, typ byte, path string, out map[string]string, depth int) error {
	if depth > maxDepth {
		return errThrift
	}
	switch typ {
	case tTrue, tFalse:
		out[path] = fmt.Sprintf("bool:%d", typ)
		if len(b) == 1 { // a list element
			out[path] = fmt.Sprintf("bool:%d", b[0])
		}
	case tList, tSet:
		et, elems, _, err := splitList(b, depth)
		if err != nil {
			return err
		}
		out[path+".len"] = fmt.Sprintf("%d/%d", len(elems), et)
		for i, e := range elems {
			if err := flattenValue(e, et, fmt.Sprintf("%s[%d]", path, i), out, depth+1); err != nil {
				return err
			}
		}
	case tStruct:
		if _, err := flattenStruct(b, path, out, depth); err != nil {
			return err
		}
	default:
		out[path] = fmt.Sprintf("%d:%x", typ, b)
	}
	return nil
}

// footerDiff returns the paths whose values differ between old and new
// (removed or changed) and the paths only new has.
func footerDiff(old, new []byte) (changed, added []string, err error) {
	a, err := flatten(old)
	if err != nil {
		return nil, nil, fmt.Errorf("old footer: %w", err)
	}
	b, err := flatten(new)
	if err != nil {
		return nil, nil, fmt.Errorf("new footer: %w", err)
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			changed = append(changed, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			added = append(added, k)
		}
	}
	return changed, added, nil
}

// hasAnyPrefix reports whether s begins with one of prefixes.
func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
