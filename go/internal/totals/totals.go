// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package totals sums a listing per key prefix, as du sums a tree per
// directory (swath du --depth N). Depth counts from Options.Origin, the
// prefix the source names (s3://bucket/logs/ lists keys under logs/): a
// prefix of depth k is a key up to and including the k-th '/' after the
// origin, and the origin itself, depth 0, is the total. Each prefix holds
// the objects under it, the directories below it (a local tree's), their
// bytes and the newest modification time among them, a parent including
// its children. A key with fewer than N '/' counts in the prefixes it has.
//
// Rows arrive concurrently from independent ranges (order.CountWithSink's
// sink): each goroutine sums its batch into a Batch, which merges into the
// shared tally under one lock per batch. Nothing holds a row: a prefix is
// copied when it first enters the tally. Memory is the distinct prefixes,
// and Options.MaxPrefixes caps them: past it a merge fails with a
// LimitError instead of growing without bound (a flat bucket of millions of
// single-level "directories" at --depth 1).
//
// With Options.LinksOnce, an object with more than one hard link (Row.Link)
// is counted once per (device, inode), as du does by default: its object
// and bytes go to the prefixes of the smallest key among its links (byte
// order), so the result does not depend on which link a worker saw first.
// Only those inodes are remembered, and Options.MaxInodes caps them.
package totals

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
)

// DefaultMaxPrefixes is the cap on distinct prefixes when Options gives
// none: about 150 MB of tally at 60-byte prefixes.
const DefaultMaxPrefixes = 1_000_000

// DefaultMaxInodes is the cap on remembered hard-linked inodes when
// Options gives none: about 100 MB at 60-byte keys.
const DefaultMaxInodes = 1_000_000

// The bytes columns a tally sums (Options.BytesColumn): the apparent size,
// or a local entry's allocated bytes (st_blocks × 512, du's default).
const (
	Size           = "size"
	AllocatedBytes = "allocated_bytes"
)

// Options shape a tally.
type Options struct {
	// Depth is the deepest prefix level summed: 0 is the total alone.
	Depth int
	// Origin is the prefix depth counts from, the total's prefix: what the
	// source names up to its last '/' ("" for a whole bucket or a local
	// tree, whose keys are relative to it). A key outside it counts in the
	// total alone.
	Origin string
	// MaxPrefixes caps the distinct prefixes held, the total included
	// (0: DefaultMaxPrefixes).
	MaxPrefixes int
	// MaxInodes caps the hard-linked inodes LinksOnce remembers (0:
	// DefaultMaxInodes).
	MaxInodes int
	// BytesColumn names the column the bytes are read from (the registry
	// binds it; "" is Size). It is reported, not interpreted, here.
	BytesColumn string
	// LinksOnce counts an object with several hard links once (Row.Link).
	LinksOnce bool
}

// prefixes yields key's prefixes from the origin (depth 0) down to Depth,
// each a view of key (or the origin, for a key outside it).
func (o *Options) prefixes(key string) iter.Seq2[int, string] {
	return func(yield func(int, string) bool) {
		end := len(o.Origin)
		if !strings.HasPrefix(key, o.Origin) {
			key = o.Origin
		}
		for k := 0; k <= o.Depth; k++ {
			if k > 0 {
				i := strings.IndexByte(key[end:], '/')
				if i < 0 {
					return // a key with fewer components counts where it has them
				}
				end += i + 1
			}
			if !yield(k, key[:end]) {
				return
			}
		}
	}
}

// Row is what a row contributes.
type Row struct {
	Key string
	// Object is false for a row that is not an object (a local directory,
	// an S3 delete marker): its prefixes are listed, nothing is counted.
	Object bool
	// Dir marks a directory (not an object): it counts in its parents'
	// Dirs, never in its own prefix's.
	Dir      bool
	Bytes    int64 // valid with HasBytes
	HasBytes bool
	Mtime    int64 // epoch microseconds; valid with HasMtime
	HasMtime bool
	// Link marks an object with more than one hard link, identified by
	// its device and inode; with Options.LinksOnce it counts once.
	Link       bool
	Dev, Inode int64
}

// Prefix is one prefix's totals.
type Prefix struct {
	Prefix  string
	Objects int64
	Dirs    int64 // directories below the prefix (a local tree's)
	Bytes   int64 // the bytes of the objects that had a size: the whole size when Unsized is 0
	// Unsized is the objects under the prefix with no value in the bytes
	// column (the names metadata tier): Bytes leaves them out.
	Unsized int64
	Newest  int64 // epoch microseconds; valid with HasNewest
	// HasNewest is false when no object under the prefix has a time (no
	// object at all, or a metadata tier that reads none).
	HasNewest bool
}

// Sized reports whether Bytes is the prefix's whole size: every object
// under it had a value in the bytes column.
func (p *Prefix) Sized() bool { return p.Unsized == 0 }

// Stats is the tally's account for the run report (output.totals).
type Stats struct {
	Depth       int    `json:"depth"`
	Prefixes    int64  `json:"prefixes"`     // distinct prefixes, the total included: the rows printed
	MaxPrefixes int64  `json:"max_prefixes"` // the cap in effect
	BytesColumn string `json:"bytes_column"` // the column summed: size, or allocated_bytes
	Objects     int64  `json:"objects"`      // objects counted in the total
	Bytes       int64  `json:"bytes"`        // their bytes
	Dirs        int64  `json:"dirs"`         // directories counted in the total (a local tree's)
	// NotObjects counts rows that are not objects (directories, delete
	// markers): listed under their prefixes, not counted as objects.
	NotObjects int64 `json:"not_objects"`
	// Unsized counts objects without a value in the bytes column (the
	// names tier): counted with no bytes, so the sizes are unknown.
	Unsized int64 `json:"unsized"`
	// LinksOnce: objects with several hard links were counted once;
	// LinkedRows is the rows with more than one link, Inodes the distinct
	// inodes among them (each one object), MaxInodes their cap.
	LinksOnce  bool  `json:"links_once"`
	LinkedRows int64 `json:"linked_rows"`
	Inodes     int64 `json:"inodes"`
	MaxInodes  int64 `json:"max_inodes"`
}

// LimitError is a tally that would hold more than its cap of prefixes,
// or of hard-linked inodes (Inodes).
type LimitError struct {
	Depth, Max int
	Inodes     bool
}

func (e *LimitError) Error() string {
	if e.Inodes {
		return fmt.Sprintf("totals: more than %d distinct hard-linked inodes to count once", e.Max)
	}
	return fmt.Sprintf("totals: more than %d distinct prefixes at depth %d", e.Max, e.Depth)
}

// Class names the failure for the run's outcome (its cause).
func (e *LimitError) Class() string {
	if e.Inodes {
		return "inode_limit"
	}
	return "prefix_limit"
}

// inode identifies a hard-linked file.
type inode struct{ dev, ino int64 }

// linked is a hard-linked inode's owner: the smallest key among its links
// seen, and the bytes it counts once.
type linked struct {
	key   string
	bytes int64
	sized bool
}

// cell is one prefix's sums.
type cell struct {
	objects, dirs, bytes, unsized, newest int64
	hasNewest                             bool
}

func (c *cell) add(o *cell) {
	c.objects += o.objects
	c.dirs += o.dirs
	c.bytes += o.bytes
	c.unsized += o.unsized
	if o.hasNewest && (!c.hasNewest || o.newest > c.newest) {
		c.newest, c.hasNewest = o.newest, true
	}
}

// Tally is the shared sums, safe for concurrent use through Batches.
type Tally struct {
	opt  Options
	pool sync.Pool // *Batch

	mu    sync.Mutex
	cells map[string]*cell
	links map[inode]*linked // LinksOnce: the hard-linked inodes, until finish adds them
	done  bool              // finish has added the links
	dirs  bool              // the source has directories (Dirs)
	err   error             // the first LimitError: every later merge returns it
	notObjects,
	linkedRows int64
}

// New returns an empty tally.
func New(opt Options) *Tally {
	if opt.MaxPrefixes <= 0 {
		opt.MaxPrefixes = DefaultMaxPrefixes
	}
	if opt.MaxInodes <= 0 {
		opt.MaxInodes = DefaultMaxInodes
	}
	if opt.BytesColumn == "" {
		opt.BytesColumn = Size
	}
	t := &Tally{opt: opt, cells: map[string]*cell{}, links: map[inode]*linked{}}
	t.pool.New = func() any { return &Batch{t: t} }
	return t
}

// Options is the tally's options, defaults filled.
func (t *Tally) Options() Options { return t.opt }

// SetDirs records that the source's rows include directories (a local
// tree's), so its totals carry Dirs; the binding to a schema calls it,
// before the first Batch.
func (t *Tally) SetDirs() {
	t.mu.Lock()
	t.dirs = true
	t.mu.Unlock()
}

// Dirs reports whether the source's rows include directories (SetDirs).
func (t *Tally) Dirs() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dirs
}

// Batch sums one goroutine's rows before they merge into the tally. Rows
// in a range ascend, so consecutive rows share their prefixes: a batch
// keeps one entry per run of equal prefix at each level, and merges them
// under one lock. Its prefixes view the rows' keys until Flush, which
// copies the new ones and drops the views.
type Batch struct {
	t    *Tally
	ents []entry // the prefixes touched, in order
	// cur is, per level reached so far, the index in ents of its current
	// prefix (-1: none); it grows with the keys' depth, never past it.
	cur []int
	// links are the batch's hard-linked objects (LinksOnce), their keys
	// views until Flush.
	links      []linkRow
	notObjects int64
}

type linkRow struct {
	id    inode
	key   string
	bytes int64
	sized bool
}

type entry struct {
	prefix string
	c      cell
}

// Batch takes a batch to sum rows into; Flush returns it.
func (t *Tally) Batch() *Batch { return t.pool.Get().(*Batch) }

func (b *Batch) reset() {
	clear(b.ents) // drop the views of the rows' keys
	b.ents = b.ents[:0]
	clear(b.links)
	b.links = b.links[:0]
	for i := range b.cur {
		b.cur[i] = -1
	}
	b.notObjects = 0
}

// Add sums one row into the batch.
func (b *Batch) Add(r *Row) {
	var c cell
	switch {
	case r.Object:
		c.objects = 1
		if r.HasBytes {
			c.bytes = r.Bytes
		} else {
			c.unsized = 1
		}
		c.newest, c.hasNewest = r.Mtime, r.HasMtime
		if r.Link && b.t.opt.LinksOnce {
			// The object and its bytes are added once, at finish, under
			// the smallest of its keys; the time is every link's (an
			// inode has one), so it applies here.
			b.links = append(b.links, linkRow{id: inode{r.Dev, r.Inode}, key: r.Key, bytes: c.bytes, sized: r.HasBytes})
			c.objects, c.bytes, c.unsized = 0, 0, 0
		}
	case r.Dir:
		b.notObjects++
		c.dirs = 1
	default:
		b.notObjects++
	}
	for k, p := range b.t.opt.prefixes(r.Key) {
		if k == len(b.cur) {
			b.cur = append(b.cur, -1)
		}
		j := b.cur[k]
		if j < 0 || b.ents[j].prefix != p {
			b.ents = append(b.ents, entry{prefix: p})
			j = len(b.ents) - 1
			b.cur[k] = j
		}
		if r.Dir && p == r.Key {
			break // a directory lists its own prefix (an empty one shows 0) and counts in its parents'
		}
		b.ents[j].c.add(&c)
	}
}

// Discard drops the batch unmerged and returns it to the tally.
func (b *Batch) Discard() {
	b.reset()
	b.t.pool.Put(b)
}

// Flush merges the batch into the tally and returns the batch to it; the
// batch must not be used again. Its error is a LimitError: the merge that
// would pass a cap, and every merge after it, fails (the run stops).
func (b *Batch) Flush() error {
	t := b.t
	t.mu.Lock()
	err := t.err
	for i := range b.ents {
		if err != nil {
			break
		}
		e := &b.ents[i]
		c := t.cells[e.prefix]
		if c == nil {
			if len(t.cells) >= t.opt.MaxPrefixes {
				err = &LimitError{Depth: t.opt.Depth, Max: t.opt.MaxPrefixes}
				t.err = err
				break
			}
			c = &cell{}
			t.cells[strings.Clone(e.prefix)] = c
		}
		c.add(&e.c)
	}
	for i := range b.links {
		if err != nil {
			break
		}
		l := &b.links[i]
		m := t.links[l.id]
		switch {
		case m == nil && len(t.links) >= t.opt.MaxInodes:
			err = &LimitError{Depth: t.opt.Depth, Max: t.opt.MaxInodes, Inodes: true}
			t.err = err
		case m == nil:
			t.links[l.id] = &linked{key: strings.Clone(l.key), bytes: l.bytes, sized: l.sized}
		case l.key < m.key:
			m.key, m.bytes, m.sized = strings.Clone(l.key), l.bytes, l.sized
		}
	}
	t.linkedRows += int64(len(b.links))
	t.notObjects += b.notObjects
	t.mu.Unlock()
	b.reset()
	t.pool.Put(b)
	return err
}

// finish adds each hard-linked inode once, to the prefixes of its
// smallest key: those cells exist, as that row's time was added under
// them. It runs once, after the last Flush (t.mu held).
func (t *Tally) finish() {
	if t.done {
		return
	}
	t.done = true
	for _, l := range t.links {
		add := cell{objects: 1, bytes: l.bytes}
		if !l.sized {
			add.unsized = 1
		}
		for _, p := range t.opt.prefixes(l.key) {
			if c := t.cells[p]; c != nil { // nil only after a LimitError
				c.add(&add)
			}
		}
	}
}

// Result is every prefix's totals in byte order of the prefix, so the
// total (the origin) comes first and a parent before its children. Call
// it once the rows have all been flushed; an empty tally still has its
// total.
func (t *Tally) Result() []Prefix {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finish()
	out := make([]Prefix, 0, max(1, len(t.cells)))
	if t.cells[t.opt.Origin] == nil {
		out = append(out, Prefix{Prefix: t.opt.Origin})
	}
	for p, c := range t.cells {
		out = append(out, Prefix{Prefix: p, Objects: c.objects, Dirs: c.dirs, Bytes: c.bytes, Unsized: c.unsized, Newest: c.newest, HasNewest: c.hasNewest})
	}
	slices.SortFunc(out, func(a, b Prefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
	return out
}

// Stats is the tally's account; like Result, call it after the last
// Flush.
func (t *Tally) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finish()
	s := Stats{Depth: t.opt.Depth, Prefixes: int64(max(1, len(t.cells))), MaxPrefixes: int64(t.opt.MaxPrefixes), BytesColumn: t.opt.BytesColumn,
		NotObjects: t.notObjects, LinksOnce: t.opt.LinksOnce, LinkedRows: t.linkedRows, Inodes: int64(len(t.links)), MaxInodes: int64(t.opt.MaxInodes)}
	if c := t.cells[t.opt.Origin]; c != nil {
		s.Objects, s.Bytes, s.Dirs, s.Unsized = c.objects, c.bytes, c.dirs, c.unsized
	}
	return s
}
