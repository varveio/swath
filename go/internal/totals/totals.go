// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package totals sums a listing per key prefix, as du sums a tree per
// directory (swath count --depth N). A prefix of depth k is a key up to and
// including its k-th '/'; the empty prefix, depth 0, is the total. Each
// prefix holds the objects under it, their bytes and the newest
// modification time among them, a parent including its children. A key
// with fewer than N '/' counts in the prefixes it has.
//
// Rows arrive concurrently from independent ranges (order.CountWithSink's
// sink): each goroutine sums its batch into a Batch, which merges into the
// shared tally under one lock per batch. Nothing holds a row: a prefix is
// copied when it first enters the tally. Memory is the distinct prefixes,
// and Options.MaxPrefixes caps them: past it a merge fails with a
// LimitError instead of growing without bound (a flat bucket of millions of
// single-level "directories" at --depth 1).
package totals

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"sync"
)

// DefaultMaxPrefixes is the cap on distinct prefixes when Options gives
// none: about 150 MB of tally at 60-byte prefixes.
const DefaultMaxPrefixes = 1_000_000

// Options shape a tally.
type Options struct {
	// Depth is the deepest prefix level summed: 0 is the total alone.
	Depth int
	// MaxPrefixes caps the distinct prefixes held, the total included
	// (0: DefaultMaxPrefixes).
	MaxPrefixes int
	// BytesColumn names the column the bytes are read from (the registry
	// binds it; "" is size). It is reported, not interpreted, here.
	BytesColumn string
}

// Row is what a row contributes.
type Row struct {
	Key string
	// Object is false for a row that is not an object (a local directory,
	// an S3 delete marker): its prefixes are listed, nothing is counted.
	Object   bool
	Bytes    int64 // valid with HasBytes
	HasBytes bool
	Mtime    int64 // epoch microseconds; valid with HasMtime
	HasMtime bool
}

// Prefix is one prefix's totals.
type Prefix struct {
	Prefix  string
	Objects int64
	Bytes   int64
	Newest  int64 // epoch microseconds; valid with HasNewest
	// HasNewest is false when no object under the prefix has a time (no
	// object at all, or a metadata tier that reads none).
	HasNewest bool
}

// Stats is the tally's account for the run report (output.totals).
type Stats struct {
	Depth       int    `json:"depth"`
	Prefixes    int64  `json:"prefixes"`     // distinct prefixes, the total ("") included: the rows printed
	MaxPrefixes int64  `json:"max_prefixes"` // the cap in effect
	BytesColumn string `json:"bytes_column"` // the column summed: size, or allocated_bytes
	Objects     int64  `json:"objects"`      // objects counted in the total
	Bytes       int64  `json:"bytes"`        // their bytes
	// NotObjects counts rows that are not objects (directories, delete
	// markers): listed under their prefixes, not counted.
	NotObjects int64 `json:"not_objects"`
	// Unsized counts objects without a value in the bytes column (a
	// delete marker's size, the names tier): counted with no bytes.
	Unsized int64 `json:"unsized"`
}

// LimitError is a tally that would hold more than its cap of prefixes.
type LimitError struct{ Depth, Max int }

func (e *LimitError) Error() string {
	return fmt.Sprintf("totals: more than %d distinct prefixes at depth %d", e.Max, e.Depth)
}

// Class names the failure for the run's outcome (its cause).
func (e *LimitError) Class() string { return "prefix_limit" }

// cell is one prefix's sums.
type cell struct {
	objects, bytes, newest int64
	hasNewest              bool
}

func (c *cell) add(o *cell) {
	c.objects += o.objects
	c.bytes += o.bytes
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
	err   error // the first LimitError: every later merge returns it
	notObjects,
	unsized int64
}

// New returns an empty tally.
func New(opt Options) *Tally {
	if opt.MaxPrefixes <= 0 {
		opt.MaxPrefixes = DefaultMaxPrefixes
	}
	if opt.BytesColumn == "" {
		opt.BytesColumn = "size"
	}
	t := &Tally{opt: opt, cells: map[string]*cell{}}
	t.pool.New = func() any {
		b := &Batch{t: t, cur: make([]int, opt.Depth+1)}
		b.reset()
		return b
	}
	return t
}

// Options is the tally's options, defaults filled.
func (t *Tally) Options() Options { return t.opt }

// Batch sums one goroutine's rows before they merge into the tally. Rows
// in a range ascend, so consecutive rows share their prefixes: a batch
// keeps one entry per run of equal prefix at each level, and merges them
// under one lock. Its prefixes view the rows' keys until Flush, which
// copies the new ones and drops the views.
type Batch struct {
	t    *Tally
	ents []entry // the prefixes touched, in order
	cur  []int   // per level, the index in ents of its current prefix (-1: none)
	notObjects,
	unsized int64
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
	for i := range b.cur {
		b.cur[i] = -1
	}
	b.notObjects, b.unsized = 0, 0
}

// Add sums one row into the batch.
func (b *Batch) Add(r *Row) {
	var c cell
	if r.Object {
		c.objects = 1
		if r.HasBytes {
			c.bytes = r.Bytes
		} else {
			b.unsized++
		}
		c.newest, c.hasNewest = r.Mtime, r.HasMtime
	} else {
		b.notObjects++
	}
	key, end := r.Key, 0
	for k := range b.cur {
		if k > 0 {
			i := strings.IndexByte(key[end:], '/')
			if i < 0 {
				break // a key with fewer components counts where it has them
			}
			end += i + 1
		}
		p := key[:end]
		j := b.cur[k]
		if j < 0 || b.ents[j].prefix != p {
			b.ents = append(b.ents, entry{prefix: p})
			j = len(b.ents) - 1
			b.cur[k] = j
		}
		b.ents[j].c.add(&c)
	}
}

// Flush merges the batch into the tally and returns the batch to it; the
// batch must not be used again. Its error is a LimitError: the merge that
// would pass the cap, and every merge after it, fails (the run stops).
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
	t.notObjects += b.notObjects
	t.unsized += b.unsized
	t.mu.Unlock()
	b.reset()
	t.pool.Put(b)
	return err
}

// Result is every prefix's totals in byte order of the prefix, so the
// total (the empty prefix) comes first and a parent before its children.
// Call it once the rows have all been flushed; an empty tally still has
// its total.
func (t *Tally) Result() []Prefix {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Prefix, 0, max(1, len(t.cells)))
	if t.cells[""] == nil {
		out = append(out, Prefix{})
	}
	for p, c := range t.cells {
		out = append(out, Prefix{Prefix: p, Objects: c.objects, Bytes: c.bytes, Newest: c.newest, HasNewest: c.hasNewest})
	}
	slices.SortFunc(out, func(a, b Prefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
	return out
}

// Stats is the tally's account so far.
func (t *Tally) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := Stats{Depth: t.opt.Depth, Prefixes: int64(max(1, len(t.cells))), MaxPrefixes: int64(t.opt.MaxPrefixes), BytesColumn: t.opt.BytesColumn,
		NotObjects: t.notObjects, Unsized: t.unsized}
	if c := t.cells[""]; c != nil {
		s.Objects, s.Bytes = c.objects, c.bytes
	}
	return s
}
