// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package view

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	seekparquet "github.com/varveio/seekparquet/go"

	"github.com/varveio/swath/reader"
	"github.com/varveio/swath/reader/listing"
)

// rowShape is how the rows of one schema become Entries: the columns the
// view projects, and whether they are a local tree's.
type rowShape struct {
	local   bool
	columns []string
}

// The projections. Every object schema has size, last_modified and
// storage_class; a local tree's entries are its own rows.
var (
	objectColumns = []string{"size", "last_modified", "storage_class"}
	localColumns  = []string{"size", "last_modified", "kind", "nlink", "inode", "dev", "allocated_bytes", "link_target"}
)

// The projections' slots.
const (
	slotSize = iota
	slotModified
	slotClass // an object's
)

const (
	slotKind = iota + 2 // a local entry's
	slotLinks
	slotInode
	slotDevice
	slotAllocated
	slotLinkTarget
)

func shapeOf(id reader.SchemaID) rowShape {
	if id == reader.LocalV1 {
		return rowShape{local: true, columns: localColumns}
	}
	return rowShape{columns: objectColumns}
}

// localKinds maps the kinds swath writes for a local entry to an Entry's.
var localKinds = map[string]listing.Kind{
	"file":         listing.KindFile,
	"dir":          listing.KindDir,
	"symlink":      listing.KindSymlink,
	"fifo":         listing.KindOther,
	"socket":       listing.KindOther,
	"char_device":  listing.KindOther,
	"block_device": listing.KindOther,
	"unknown":      listing.KindUnknown,
}

// errNULKey refuses a row whose key holds a NUL, which no listing key
// holds: an object store permits one, so swath may have listed it.
var errNULKey = wrap(listing.ErrInvalidPage, "a key holds a NUL byte")

// decoder reads rows into Entries. With a slab, an Entry's values live in
// it until the slab is reset; without one, each is allocated, the
// caller's to keep.
type decoder struct {
	rows rowShape
	slab *slab
}

// row is one row to decode: its key, its offset in its key's group, and
// the values of the projection's slots, from an owning row or from row
// index of a batch's column views.
type row struct {
	key    []byte
	offset int64
	owned  *seekparquet.Row
	views  []seekparquet.ColumnView
	index  int
}

// ownedRow is a row SeekParquet returned, which owns its key and values.
func ownedRow(r *seekparquet.Row) row {
	return row{key: r.Key, offset: r.Offset, owned: r}
}

func (r row) value(slot int) seekparquet.Value {
	if r.owned != nil {
		return r.owned.Values[slot]
	}
	return r.views[slot].Value(r.index)
}

// entry reads one row. The Entry's Key is r's key, not a copy.
func (d *decoder) entry(r row) (listing.Entry, error) {
	if bytes.IndexByte(r.key, 0) >= 0 {
		return listing.Entry{}, errNULKey
	}
	e := listing.Entry{Key: r.key, Kind: listing.KindObject}
	var err error
	if e.Size, err = d.size(r, slotSize); err != nil {
		return listing.Entry{}, err
	}
	if e.LastModified, err = d.time(r, slotModified); err != nil {
		return listing.Entry{}, err
	}
	if !d.rows.local {
		if e.StorageClass, err = d.class(r); err != nil {
			return listing.Entry{}, err
		}
		return e, nil
	}

	if e.Kind, err = d.kind(r); err != nil {
		return listing.Entry{}, err
	}
	if e.Links, err = d.links(r); err != nil {
		return listing.Entry{}, err
	}
	if e.Inode, err = d.word(r, slotInode); err != nil {
		return listing.Entry{}, err
	}
	if e.Device, err = d.word(r, slotDevice); err != nil {
		return listing.Entry{}, err
	}
	if e.AllocatedBytes, err = d.size(r, slotAllocated); err != nil {
		return listing.Entry{}, err
	}
	if e.LinkTarget, err = d.linkTarget(r); err != nil {
		return listing.Entry{}, err
	}
	return e, nil
}

// value returns a slot's value, ok false for a null. A part lacking the
// column, or holding it with another physical type, is a schema fault; a
// null in a required column a row fault.
func (d *decoder) value(r row, slot int, want seekparquet.Kind, required bool) (seekparquet.Value, bool, error) {
	v := r.value(slot)
	switch {
	case v.Absent():
		return v, false, d.fault(r, reader.KindSchema, slot, "absent from the row's part")
	case v.IsNull() && required:
		return v, false, d.fault(r, reader.KindRow, slot, "null in a required column")
	case v.IsNull():
		return v, false, nil
	case v.Kind() != want:
		return v, false, d.fault(r, reader.KindSchema, slot, fmt.Sprintf("a %s value, the schema's type is %s", v.Kind(), want))
	}
	return v, true, nil
}

func (d *decoder) fault(r row, kind reader.Kind, slot int, reason string) error {
	return &reader.SemanticError{Kind: kind, Column: d.rows.columns[slot],
		Row: &reader.Position{Key: bytes.Clone(r.key), Offset: r.offset}, Err: errors.New(reason)}
}

// size reads a byte count, which is never negative.
func (d *decoder) size(r row, slot int) (*int64, error) {
	v, ok, err := d.value(r, slot, seekparquet.KindInt64, false)
	if !ok {
		return nil, err
	}
	if v.Int64() < 0 {
		return nil, d.fault(r, reader.KindRow, slot, "negative")
	}
	return d.slab.int64(v.Int64()), nil
}

func (d *decoder) time(r row, slot int) (*time.Time, error) {
	v, ok, err := d.value(r, slot, seekparquet.KindInt64, false)
	if !ok {
		return nil, err
	}
	return d.slab.time(time.UnixMicro(v.Int64()).UTC()), nil
}

func (d *decoder) class(r row) (*string, error) {
	v, ok, err := d.value(r, slotClass, seekparquet.KindByteArray, false)
	if !ok {
		return nil, err
	}
	return d.slab.string(v.Bytes()), nil
}

// kind reads a local entry's kind: one swath writes, and a directory's
// exactly when its key ends in "/".
func (d *decoder) kind(r row) (listing.Kind, error) {
	v, _, err := d.value(r, slotKind, seekparquet.KindByteArray, true)
	if err != nil {
		return 0, err
	}
	kind, ok := localKinds[string(v.Bytes())]
	if !ok {
		return 0, d.fault(r, reader.KindRow, slotKind, "not a kind swath writes")
	}
	if dir := len(r.key) > 0 && r.key[len(r.key)-1] == '/'; dir != (kind == listing.KindDir) {
		return 0, d.fault(r, reader.KindRow, slotKind, "a directory's key ends in /, and only a directory's")
	}
	return kind, nil
}

func (d *decoder) links(r row) (*uint32, error) {
	v, ok, err := d.value(r, slotLinks, seekparquet.KindInt32, false)
	if !ok {
		return nil, err
	}
	return d.slab.uint32(v.Uint32()), nil
}

// linkTarget reads a symbolic link's target as recorded: nil when the
// listing did not record one. With a slab it is the row's own bytes,
// valid as long as its key; without one, a copy.
func (d *decoder) linkTarget(r row) ([]byte, error) {
	v, ok, err := d.value(r, slotLinkTarget, seekparquet.KindByteArray, false)
	if !ok {
		return nil, err
	}
	if d.slab != nil {
		return v.Bytes(), nil
	}
	return bytes.Clone(v.Bytes()), nil
}

func (d *decoder) word(r row, slot int) (*uint64, error) {
	v, ok, err := d.value(r, slot, seekparquet.KindInt64, false)
	if !ok {
		return nil, err
	}
	return d.slab.uint64(v.Uint64()), nil
}

// slab holds the values of one batch's Entries, so that a walk allocates
// per batch instead of per value. A nil slab allocates each value.
type slab struct {
	ints    []int64
	words   []uint64
	links   []uint32
	times   []time.Time
	classes map[string]*string
}

// reset makes room for the values of n rows, dropping the last batch's:
// the room is reserved before any pointer into it is taken, so no append
// moves a value an Entry points to.
func (s *slab) reset(n int) {
	s.ints = room(s.ints, 2*n)
	s.words = room(s.words, 2*n)
	s.links = room(s.links, n)
	s.times = room(s.times, n)
}

func room[T any](s []T, n int) []T {
	if cap(s) < n {
		return make([]T, 0, n)
	}
	return s[:0]
}

func (s *slab) int64(v int64) *int64 {
	if s == nil {
		return new(v)
	}
	s.ints = append(s.ints, v)
	return &s.ints[len(s.ints)-1]
}

func (s *slab) uint64(v uint64) *uint64 {
	if s == nil {
		return new(v)
	}
	s.words = append(s.words, v)
	return &s.words[len(s.words)-1]
}

func (s *slab) uint32(v uint32) *uint32 {
	if s == nil {
		return new(v)
	}
	s.links = append(s.links, v)
	return &s.links[len(s.links)-1]
}

func (s *slab) time(v time.Time) *time.Time {
	if s == nil {
		return new(v)
	}
	s.times = append(s.times, v)
	return &s.times[len(s.times)-1]
}

// string returns b as a string; a slab keeps one copy of each storage
// class it meets, of which a listing has few.
func (s *slab) string(b []byte) *string {
	if s == nil {
		v := string(b)
		return &v
	}
	if p, ok := s.classes[string(b)]; ok {
		return p
	}
	if s.classes == nil {
		s.classes = map[string]*string{}
	}
	v := string(b)
	s.classes[v] = &v
	return &v
}

// batchEntries decodes a borrowed batch, appending to entries[:0]. Their
// keys are the batch's, valid until its callback returns; their values
// are the slab's, valid until it is reset, or allocated without one.
func (d *decoder) batchEntries(b *seekparquet.Batch, entries []listing.Entry) ([]listing.Entry, error) {
	if d.slab != nil {
		d.slab.reset(b.Len())
	}
	views := make([]seekparquet.ColumnView, len(d.rows.columns))
	for slot := range views {
		views[slot] = b.Column(slot)
	}
	entries = entries[:0]
	for i := range b.Len() {
		e, err := d.entry(row{key: b.Key(i), offset: b.Offset(i), views: views, index: i})
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// cloneEntry is a deep copy of e.
func cloneEntry(e *listing.Entry) *listing.Entry {
	if e == nil {
		return nil
	}
	c := *e
	c.Key = bytes.Clone(e.Key)
	c.Size, c.AllocatedBytes, c.LastModified, c.StorageClass = clone(e.Size), clone(e.AllocatedBytes), clone(e.LastModified), clone(e.StorageClass)
	c.Device, c.Inode, c.Links = clone(e.Device), clone(e.Inode), clone(e.Links)
	c.LinkTarget = bytes.Clone(e.LinkTarget)
	return &c
}
