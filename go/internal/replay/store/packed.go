// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"
)

// packed holds a window's rows column by column in arrays without
// pointers: one byte arena for the keys, one string per text field, and
// the integers as they are. A cache of a million rows held as Objects is
// millions of small heap objects the collector marks on every cycle; held
// packed it is a few dozen arrays per window it never scans. A page is
// unpacked into Objects that point into these arrays when it is served.
type packed struct {
	n      int
	keys   []byte
	keyEnd []uint32
	ints   [2]packedInts // Size, LastModified (epoch microseconds)
	texts  [6]packedText // ETag, StorageClass, OwnerID, OwnerDisplayName, ChecksumAlgorithm, ChecksumType
}

type packedInts struct {
	present bool // some row holds a value
	values  []int64
	null    []bool
}

type packedText struct {
	present bool
	arena   string
	end     []uint32
	null    []bool
}

// textFields are the Object fields packed as text, in packed.texts order.
func textFields(o *Object) [6]**string {
	return [6]**string{&o.ETag, &o.StorageClass, &o.OwnerID, &o.OwnerDisplayName, &o.ChecksumAlgorithm, &o.ChecksumType}
}

// packer is the sortedparquet.ColumnSink that packs a read for the
// projection: the Objects it unpacks are those objectProjection.object
// makes of the same rows (a column the read did not project, or a null
// value, is a nil field). Offsets are uint32 to keep a window's index
// arrays half the size of int offsets; the keys and any one text field
// can still outgrow them (long keys or text values, a read past the
// window size), so the packer stops at limit bytes per arena and reports
// errPackedOverflow, and the read is served unpacked instead.
type packer struct {
	out packed
	// slots maps each read column to its packed field: 0 and 1 the ints,
	// 2 to 7 the texts.
	slots []int
	texts [6]strings.Builder
	// limit bounds each arena (at most maxPackedArena); overflow reports
	// a row was refused, and stop (when set) ends the read early.
	limit    int
	overflow bool
	stop     func()
}

// maxPackedArena is the most bytes a uint32 offset addresses.
const maxPackedArena = math.MaxUint32

// errPackedOverflow is a read whose keys or one text field exceed the
// packer's arena limit.
var errPackedOverflow = errors.New("packed arena limit exceeded")

// packer returns a packer for a read of columns (p.columns of a
// projection) expected to return about n rows, its arenas bounded by
// limit bytes (maxPackedArena when limit is not in (0, maxPackedArena]).
func (p objectProjection) packer(columns []string, n, limit int) *packer {
	if limit <= 0 || limit > maxPackedArena {
		limit = maxPackedArena
	}
	k := &packer{slots: make([]int, len(columns)), limit: limit}
	fields := [8]string{p.size, p.lastModified, p.etag, p.storageClass, p.ownerID, p.ownerDisplayName, p.checksumAlgorithm, p.checksumType}
	for c, name := range columns {
		k.slots[c] = slices.Index(fields[:], name)
	}
	k.out.keyEnd = make([]uint32, 0, n)
	for _, slot := range k.slots {
		switch {
		case slot < 0:
		case slot < 2:
			col := &k.out.ints[slot]
			col.present, col.values, col.null = true, make([]int64, 0, n), make([]bool, 0, n)
		default:
			col := &k.out.texts[slot-2]
			col.present, col.end, col.null = true, make([]uint32, 0, n), make([]bool, 0, n)
		}
	}
	return k
}

// fits reports whether an arena of have bytes takes add more, and marks
// the read overflowed when not.
func (k *packer) fits(have, add int) bool {
	if k.overflow || add > k.limit-have {
		if !k.overflow && k.stop != nil {
			k.stop()
		}
		k.overflow = true
		return false
	}
	return true
}

func (k *packer) Keys(keys [][]byte) {
	for _, key := range keys {
		if !k.fits(len(k.out.keys), len(key)) {
			return
		}
		k.out.keys = append(k.out.keys, key...)
		k.out.keyEnd = append(k.out.keyEnd, uint32(len(k.out.keys)))
	}
	k.out.n += len(keys)
}

func (k *packer) Values(c int, values []parquet.Value) {
	switch slot := k.slots[c]; {
	case slot < 0 || k.overflow:
	case slot < 2:
		col := &k.out.ints[slot]
		for _, v := range values {
			col.values = append(col.values, v.Int64())
			col.null = append(col.null, v.IsNull())
		}
	default:
		col, arena := &k.out.texts[slot-2], &k.texts[slot-2]
		for _, v := range values {
			if !k.fits(arena.Len(), len(v.ByteArray())) {
				return
			}
			arena.Write(v.ByteArray())
			col.end = append(col.end, uint32(arena.Len()))
			col.null = append(col.null, v.IsNull())
		}
	}
}

// Missing nulls the column for the run's rows: every row before them has
// a value or a null already.
func (k *packer) Missing(c int) {
	switch slot := k.slots[c]; {
	case slot < 0 || k.overflow:
	case slot < 2:
		col := &k.out.ints[slot]
		for len(col.null) < k.out.n {
			col.values = append(col.values, 0)
			col.null = append(col.null, true)
		}
	default:
		col, arena := &k.out.texts[slot-2], &k.texts[slot-2]
		for len(col.null) < k.out.n {
			col.end = append(col.end, uint32(arena.Len()))
			col.null = append(col.null, true)
		}
	}
}

// packed returns the rows packed, or errPackedOverflow.
func (k *packer) packed() (packed, error) {
	if k.overflow {
		return packed{}, errPackedOverflow
	}
	for f := range k.texts {
		k.out.texts[f].arena = k.texts[f].String()
	}
	return k.out, nil
}

func (p *packed) len() int { return p.n }

// key returns row i's key, capped so an append cannot reach the next.
func (p *packed) key(i int) []byte {
	start := uint32(0)
	if i > 0 {
		start = p.keyEnd[i-1]
	}
	end := p.keyEnd[i]
	return p.keys[start:end:end]
}

// objects appends rows [start, end) as Objects to dst. The keys and sizes
// point into the window; the text and time values into arrays allocated
// for the page.
func (p *packed) objects(dst []Object, start, end int) []Object {
	n := end - start
	if n <= 0 {
		return dst
	}
	var times []time.Time
	if p.ints[1].present {
		times = make([]time.Time, n)
	}
	texts := 0
	for f := range p.texts {
		if p.texts[f].present {
			texts++
		}
	}
	strs := make([]string, 0, texts*n)
	for i := start; i < end; i++ {
		o := Object{Key: p.key(i)}
		if c := &p.ints[0]; c.present && !c.null[i] {
			o.Size = &c.values[i]
		}
		if c := &p.ints[1]; c.present && !c.null[i] {
			t := &times[i-start]
			*t = time.UnixMicro(c.values[i]).UTC()
			o.LastModified = t
		}
		fields := textFields(&o)
		for f := range p.texts {
			c := &p.texts[f]
			if !c.present || c.null[i] {
				continue
			}
			from := uint32(0)
			if i > 0 {
				from = c.end[i-1]
			}
			strs = append(strs, c.arena[from:c.end[i]])
			*fields[f] = &strs[len(strs)-1]
		}
		dst = append(dst, o)
	}
	return dst
}
