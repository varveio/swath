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
	texts  [8]packedText // ETag, StorageClass, OwnerID, OwnerDisplayName, ChecksumAlgorithm, ChecksumType, ContentType, MD5
}

type packedInts struct {
	present bool // some row holds a value
	values  []int64
	null    []bool
}

type packedText struct {
	present bool
	// fixed is a column whose every row holds arena (a placeholder), kept
	// once: end and null are unused.
	fixed bool
	arena string
	end   []uint32
	null  []bool
}

// textFields are the Object fields packed as text, in packed.texts order.
func textFields(o *Object) [8]**string {
	return [8]**string{&o.ETag, &o.StorageClass, &o.OwnerID, &o.OwnerDisplayName, &o.ChecksumAlgorithm, &o.ChecksumType, &o.ContentType, &o.MD5}
}

// packer is the legacy.ColumnSink that packs a read for the
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
	// 2 to 8 the texts before MD5, 9 a GCS recording's MD5 (md5), 10 its
	// creation time (created).
	slots []int
	texts [7]strings.Builder
	// md5 holds the MD5 column of a projection with one, from which
	// packed derives the ETags (gcsETag) and, when md5Field is set, the
	// MD5 field.
	md5      packedText
	md5Text  strings.Builder
	md5Field bool
	// created holds the creation time column of a projection with
	// Created, whose non-null values packed puts over the LastModified
	// column's.
	created packedInts
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

// packer returns a packer for a read of columns (p.columns of proj)
// expected to return about n rows, its arenas bounded by limit bytes
// (maxPackedArena when limit is not in (0, maxPackedArena]).
func (p objectProjection) packer(columns []string, proj Projection, n, limit int) *packer {
	if limit <= 0 || limit > maxPackedArena {
		limit = maxPackedArena
	}
	k := &packer{slots: make([]int, len(columns)), limit: limit, md5Field: proj.Content}
	fields := [11]string{p.size, p.lastModified, p.etag, p.storageClass, p.ownerID, p.ownerDisplayName, p.checksumAlgorithm, p.checksumType,
		p.contentType, p.md5, p.timeCreated}
	for c, name := range columns {
		k.slots[c] = slices.Index(fields[:], name)
	}
	k.out.keyEnd = make([]uint32, 0, n)
	for _, slot := range k.slots {
		switch col := k.ints(slot); {
		case slot < 0:
		case col != nil:
			col.present, col.values, col.null = true, make([]int64, 0, n), make([]bool, 0, n)
		default:
			col, _ := k.text(slot)
			col.present, col.end, col.null = true, make([]uint32, 0, n), make([]bool, 0, n)
		}
	}
	for f, value := range [...]string{p.fixedETag, p.fixedStorageClass} { // texts 0 and 1
		if value != "" {
			k.out.texts[f] = packedText{present: true, fixed: true, arena: value}
		}
	}
	return k
}

// ints returns an integer slot's column, nil for any other slot.
func (k *packer) ints(slot int) *packedInts {
	switch slot {
	case 0, 1:
		return &k.out.ints[slot]
	case 10:
		return &k.created
	}
	return nil
}

// text returns a text slot's column and arena.
func (k *packer) text(slot int) (*packedText, *strings.Builder) {
	if slot == 9 {
		return &k.md5, &k.md5Text
	}
	return &k.out.texts[slot-2], &k.texts[slot-2]
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
	switch slot, col := k.slots[c], k.ints(k.slots[c]); {
	case slot < 0 || k.overflow:
	case col != nil:
		for _, v := range values {
			col.values = append(col.values, v.Int64())
			col.null = append(col.null, v.IsNull())
		}
	default:
		col, arena := k.text(slot)
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
	switch slot, col := k.slots[c], k.ints(k.slots[c]); {
	case slot < 0 || k.overflow:
	case col != nil:
		for len(col.null) < k.out.n {
			col.values = append(col.values, 0)
			col.null = append(col.null, true)
		}
	default:
		col, arena := k.text(slot)
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
		if !k.out.texts[f].fixed {
			k.out.texts[f].arena = k.texts[f].String()
		}
	}
	if k.md5.present {
		k.md5.arena = k.md5Text.String()
		etags, ok := gcsETags(&k.md5, &k.out.texts[0], k.out.n, k.limit)
		if !ok {
			k.overflow = true
			return packed{}, errPackedOverflow
		}
		k.out.texts[0] = etags
		if k.md5Field {
			k.out.texts[7] = k.md5
		}
	}
	if k.created.present {
		lastModified := &k.out.ints[1]
		if !lastModified.present {
			*lastModified = packedInts{present: true, values: make([]int64, k.out.n), null: make([]bool, k.out.n)}
			for i := range lastModified.null {
				lastModified.null[i] = true
			}
		}
		for i, null := range k.created.null {
			if !null {
				lastModified.values[i], lastModified.null[i] = k.created.values[i], false
			}
		}
	}
	return k.out, nil
}

// gcsETags is the ETag column of n packed rows of a GCS recording, each
// row's gcsETag of its MD5 and etag, or false past limit bytes.
func gcsETags(md5, etag *packedText, n, limit int) (packedText, bool) {
	out := packedText{present: true, end: make([]uint32, 0, n), null: make([]bool, n)}
	var arena []byte
	for i := range n {
		arena = gcsETag(arena, md5.value(i), etag.value(i))
		if len(arena) > limit {
			return packedText{}, false
		}
		out.end = append(out.end, uint32(len(arena)))
	}
	out.arena = string(arena)
	return out, true
}

// value returns row i's value, "" when the column is absent or the row
// holds a null.
func (c *packedText) value(i int) string {
	switch {
	case !c.present:
		return ""
	case c.fixed:
		return c.arena
	case c.null[i]:
		return ""
	}
	from := uint32(0)
	if i > 0 {
		from = c.end[i-1]
	}
	return c.arena[from:c.end[i]]
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
			if !c.present || !c.fixed && c.null[i] {
				continue
			}
			strs = append(strs, c.value(i))
			*fields[f] = &strs[len(strs)-1]
		}
		dst = append(dst, o)
	}
	return dst
}
