// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package totals sums a listing per key prefix, as du sums a tree per
// directory (swath du --depth N). Depth counts from Options.Origin, the
// prefix the source names (s3://bucket/logs/ lists keys under logs/): a
// prefix of depth k is a key up to and including the k-th '/' after the
// origin, and the origin itself, depth 0, is the total. Each prefix holds
// the objects under it, the directories below it (a local tree's), their
// two byte figures (the apparent size and a local file's allocated bytes,
// each with the objects that had no value for it) and the newest
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
//
// With Options.LinksOnce, an object with more than one hard link (Row.Link)
// is counted once per (device, inode), as du does by default: its object
// and both byte figures go to the prefixes of the smallest key among its
// links (byte order), so the result does not depend on which link a worker
// saw first.
// Only those inodes are remembered, and Options.MaxInodes caps them.
package totals

import (
	"cmp"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/varveio/swath/internal/jsontime"
)

// DefaultMaxPrefixes is the cap on distinct prefixes when Options gives
// none: about 150 MB of tally at 60-byte prefixes.
const DefaultMaxPrefixes = 1_000_000

// DefaultMaxInodes is the cap on remembered hard-linked inodes when
// Options gives none: about 100 MB at 60-byte keys.
const DefaultMaxInodes = 1_000_000

// The byte figures a tally sums, by their columns' names: the apparent
// size, and a local entry's allocated bytes (st_blocks × 512, du's
// default). Options.Primary names one of them.
const (
	Size           = "size"
	AllocatedBytes = "allocated_bytes"
)

// Why the primary figure is the one it is (Options.Reason, the report's
// primary_reason). A local tree leads with its allocated bytes, as du does,
// wherever its listing reads them; the others say why it does not.
const (
	ReasonFlag        = "flag"         // --allocated or --apparent-size
	ReasonDefault     = "default"      // a local tree whose listing reads blocks: allocated bytes
	ReasonObjectStore = "object_store" // objects have no allocation: size
	ReasonNamesTier   = "names_tier"   // --metadata names reads no stat: size
	ReasonPlatform    = "platform"     // a platform whose listing reads no blocks (Windows): size
	ReasonDataset     = "dataset"      // a saved dataset without allocated bytes for its files: size
)

// NoAllocatedError is a count by allocated bytes that met an object or a
// directory without them (the registry's binding): the run fails rather
// than count none.
type NoAllocatedError struct{ Key string }

func (e *NoAllocatedError) Error() string {
	return fmt.Sprintf("per-prefix totals: %q has no %s (a metadata tier or platform that does not read it)", e.Key, AllocatedBytes)
}

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
	// Primary names the figure that orders prefixes by size (SortBySize)
	// and that a count may not lack (the registry's binding): Size ("") or
	// AllocatedBytes. Both figures are summed whichever it is.
	Primary string
	// Reason says why Primary is the one it is (ReasonFlag and the others).
	Reason string
	// Lenient counts an entry without allocated bytes as missing rather
	// than fail the count by them (the registry's binding): a listing whose
	// platform may leave them out of some entries (statx), led by them by
	// default. The figure is then a lower bound beside its missing count.
	Lenient bool
	// LinksOnce counts an object with several hard links once (Row.Link).
	LinksOnce bool
	// View also builds the adaptive view (view.go) from the rows given to
	// ViewRuns; MaxPrefixes then bounds its nodes rather than fail.
	View bool
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
	// Dirs, never in its own prefix's. Its own allocated bytes count in its
	// prefix and its parents', as du -s counts a directory's blocks; its
	// size counts nowhere.
	Dir          bool
	Size         int64 // valid with HasSize
	HasSize      bool
	Allocated    int64 // allocated bytes; valid with HasAllocated
	HasAllocated bool
	Mtime        int64 // epoch microseconds; valid with HasMtime
	HasMtime     bool
	// Link marks an object with more than one hard link, identified by
	// its device and inode; with Options.LinksOnce it counts once.
	Link       bool
	Dev, Inode int64
	// Unidentified marks an object whose links LinksOnce cannot tell
	// apart (no link count, device or inode: a tree listed on Windows): it
	// counts under its name, and the tally no longer claims LinksOnce.
	Unidentified bool
}

// Prefix is one prefix's totals.
type Prefix struct {
	Prefix string
	// Depth is the prefix's level below the origin: 0 for the total (the
	// origin itself), else the '/' after the origin it ends at.
	Depth   int
	Objects int64
	Dirs    int64 // directories below the prefix (a local tree's)
	// Size is the apparent bytes of the objects that had a size, and
	// SizeMissing the objects that had none (the names metadata tier):
	// Size is the whole size when it is 0. HasSize is false when the
	// figure is unknown: every object under the prefix lacked one (an
	// empty prefix's is a known 0).
	Size        int64
	SizeMissing int64
	HasSize     bool
	// Allocated, AllocatedMissing and HasAllocated are the same for the
	// allocated bytes, counted over the entries: the files' and the
	// directories' own (Row.Dir), so AllocatedMissing counts directories
	// without them too, and the figure is known where any entry had one.
	// HasAllocated is false throughout for a source without them
	// (Tally.SetAllocated).
	Allocated        int64
	AllocatedMissing int64
	HasAllocated     bool
	Newest           int64 // epoch microseconds; valid with HasNewest
	// HasNewest is false when no object under the prefix has a time (no
	// object at all, or a metadata tier that reads none).
	HasNewest bool
	// Unreadable: the prefix is a directory the source could not read
	// (Tally.Unreadable); its figures leave out what it holds.
	Unreadable bool
}

// Figure is the prefix's bytes in the figure named (Size or
// AllocatedBytes), and false when that figure is unknown.
func (p *Prefix) Figure(name string) (int64, bool) {
	if name == AllocatedBytes {
		return p.Allocated, p.HasAllocated
	}
	return p.Size, p.HasSize
}

// known reports whether a figure with missing objects of p's is known:
// some object had a value, or there is no object (0).
func (p *Prefix) known(missing int64) bool { return p.Objects == 0 || missing < p.Objects }

// Stats is the tally's account for the run report (output.totals): the
// total's row under the rows' names, and the tally's own counts.
type Stats struct {
	Depth       int    `json:"depth"`
	Prefixes    int64  `json:"prefixes"`     // distinct prefixes, the total included: the rows printed
	MaxPrefixes int64  `json:"max_prefixes"` // the cap in effect
	Primary     string `json:"primary"`      // the figure --sort size orders by: size, or allocated_bytes
	// PrimaryReason says why: flag, default, object_store, names_tier,
	// platform or dataset (Options.Reason).
	PrimaryReason string `json:"primary_reason"`
	Objects       int64  `json:"objects"` // objects counted in the total
	Dirs          int64  `json:"dirs"`    // directories counted in the total (a local tree's)
	// Size and AllocatedBytes are the total's figures, nil when unknown
	// (Prefix.HasSize, HasAllocated); the missing counts are the objects
	// without a value for each (for the allocated bytes, the entries:
	// directories too).
	Size                  *int64 `json:"size"`
	SizeMissing           int64  `json:"size_missing"`
	AllocatedBytes        *int64 `json:"allocated_bytes"`
	AllocatedBytesMissing int64  `json:"allocated_bytes_missing"`
	// MaxLastModified is the newest time among the objects counted, nil
	// when none had one.
	MaxLastModified *jsontime.Time `json:"max_last_modified"`
	// NotObjects counts rows that are not objects (directories, delete
	// markers): listed under their prefixes, not counted as objects.
	NotObjects int64 `json:"not_objects"`
	// LinksOnce: objects with several hard links were counted once
	// (Tally.LinksOnce); LinkedRows is the rows with more than one link,
	// Inodes the distinct inodes among them (each one object), MaxInodes
	// their cap.
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
	key string
	c   cell // the object and its byte figures
}

// cell is one prefix's sums.
type cell struct {
	objects, dirs, size, sizeMissing, alloc, allocMissing, newest int64
	allocKnown                                                    int64 // the entries (files, directories' own) with allocated bytes
	hasNewest                                                     bool
}

func (c *cell) add(o *cell) {
	c.objects += o.objects
	c.dirs += o.dirs
	c.size += o.size
	c.sizeMissing += o.sizeMissing
	c.alloc += o.alloc
	c.allocMissing += o.allocMissing
	c.allocKnown += o.allocKnown
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
	alloc bool              // the source has allocated bytes (Allocated)
	err   error             // the first LimitError: every later merge returns it
	view  *view             // Options.View
	// unreadable is the prefixes the source could not read (Unreadable),
	// those deep enough to be rows.
	unreadable map[string]bool
	notObjects,
	linkedRows,
	unidentified int64
}

// New returns an empty tally.
func New(opt Options) *Tally {
	if opt.MaxPrefixes <= 0 {
		opt.MaxPrefixes = DefaultMaxPrefixes
	}
	if opt.MaxInodes <= 0 {
		opt.MaxInodes = DefaultMaxInodes
	}
	if opt.Primary == "" {
		opt.Primary = Size
	}
	t := &Tally{opt: opt, cells: map[string]*cell{}, links: map[inode]*linked{}}
	if opt.View {
		t.view = newView(&opt)
	}
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

// SetAllocated records that the source's rows carry allocated bytes (a
// local tree's), so its totals have that figure; without it every
// prefix's is unknown. The binding to a schema calls it, before the first
// Batch.
func (t *Tally) SetAllocated() {
	t.mu.Lock()
	t.alloc = true
	t.mu.Unlock()
}

// Allocated reports whether the source's rows carry allocated bytes
// (SetAllocated).
func (t *Tally) Allocated() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.alloc
}

// LinksOnce reports whether objects with several hard links were counted
// once: Options.LinksOnce, which a live run and a dataset's totals both set
// from whether the listing reads links (a local tree, not at the names
// tier, without --count-links), unless an object's links could not be
// told apart (Row.Unidentified). Call it after the last Flush.
func (t *Tally) LinksOnce() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.opt.LinksOnce && t.unidentified == 0
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
	links        []linkRow
	notObjects   int64
	unidentified int64
}

type linkRow struct {
	id  inode
	key string
	c   cell
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
	b.notObjects, b.unidentified = 0, 0
}

// Add sums one row into the batch.
func (b *Batch) Add(r *Row) {
	var c cell
	switch {
	case r.Object:
		c.objects = 1
		if r.HasSize {
			c.size = r.Size
		} else {
			c.sizeMissing = 1
		}
		if r.HasAllocated {
			c.alloc, c.allocKnown = r.Allocated, 1
		} else {
			c.allocMissing = 1
		}
		c.newest, c.hasNewest = r.Mtime, r.HasMtime
		if r.Unidentified {
			b.unidentified++
		}
		if r.Link && b.t.opt.LinksOnce {
			// The object, its figures and its time are added once, at
			// finish, under the smallest of its keys: a name not counted
			// adds no time either.
			b.links = append(b.links, linkRow{id: inode{r.Dev, r.Inode}, key: r.Key, c: c})
			c = cell{}
		}
	case r.Dir:
		b.notObjects++
		c.dirs = 1
		if r.HasAllocated {
			c.alloc, c.allocKnown = r.Allocated, 1
		} else {
			c.allocMissing = 1
		}
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
			// A directory lists its own prefix (an empty one shows 0) and
			// counts in its parents'; its own blocks count in both.
			own := &b.ents[j].c
			own.alloc, own.allocKnown, own.allocMissing = own.alloc+c.alloc, own.allocKnown+c.allocKnown, own.allocMissing+c.allocMissing
			break
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
			t.links[l.id] = &linked{key: strings.Clone(l.key), c: l.c}
		case l.key < m.key:
			m.key, m.c = strings.Clone(l.key), l.c
		}
	}
	t.linkedRows += int64(len(b.links))
	t.notObjects += b.notObjects
	t.unidentified += b.unidentified
	t.mu.Unlock()
	b.reset()
	t.pool.Put(b)
	return err
}

// finish adds each hard-linked inode once, to the prefixes of its
// smallest key: those cells exist, as that row's prefixes were added. It
// runs once, after the last Flush (t.mu held).
func (t *Tally) finish() {
	if t.done {
		return
	}
	t.done = true
	for _, l := range t.links {
		for _, p := range t.opt.prefixes(l.key) {
			if c := t.cells[p]; c != nil { // nil only after a LimitError
				c.add(&l.c)
			}
		}
	}
}

// Result is every prefix's totals in byte order of the prefix, so the
// total (the origin, Depth 0) comes first and a parent before its
// children. Call
// it once the rows have all been flushed; an empty tally still has its
// total.
func (t *Tally) Result() []Prefix {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finish()
	out := make([]Prefix, 0, max(1, len(t.cells)))
	if t.cells[t.opt.Origin] == nil {
		out = append(out, t.prefix(t.opt.Origin, &cell{})) // depth 0
	}
	for p, c := range t.cells {
		out = append(out, t.prefix(p, c))
	}
	slices.SortFunc(out, func(a, b Prefix) int { return cmp.Compare(a.Prefix, b.Prefix) })
	return out
}

// prefix is cell c's row, for prefix p.
func (t *Tally) prefix(p string, c *cell) Prefix {
	r := Prefix{Prefix: p, Depth: strings.Count(p[len(t.opt.Origin):], "/"), Objects: c.objects, Dirs: c.dirs,
		Size: c.size, SizeMissing: c.sizeMissing, Allocated: c.alloc, AllocatedMissing: c.allocMissing, Newest: c.newest, HasNewest: c.hasNewest}
	r.HasSize = r.known(r.SizeMissing)
	// Directories' own blocks count too: the figure is known where any
	// entry under the prefix had one, or none lacked it.
	r.HasAllocated = t.alloc && (c.allocMissing == 0 || c.allocKnown > 0)
	r.Unreadable = t.unreadable[p]
	return r
}

// Unreadable records that the source could not read the directory whose
// key prefix is p (ending in '/'), whose row is then marked: the prefixes
// of rows the tally holds at most, those within Depth of the origin, and
// for the view those of every depth (up to MaxPrefixes of them), whose
// lines above them it marks. It is safe for concurrent use.
func (t *Tally) Unreadable(p string) {
	if v := t.view; v != nil && strings.HasPrefix(p, t.opt.Origin) {
		v.mu.Lock()
		if len(v.unreadable) < v.budget {
			v.unreadable[strings.Clone(p)] = true
		}
		v.mu.Unlock()
	}
	if !strings.HasPrefix(p, t.opt.Origin) || strings.Count(p[len(t.opt.Origin):], "/") > t.opt.Depth {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.unreadable == nil {
		t.unreadable = map[string]bool{}
	}
	if len(t.unreadable) < t.opt.MaxPrefixes {
		t.unreadable[strings.Clone(p)] = true
	}
}

// SortBySize reorders rows in Result's order (each prefix before the
// prefixes below it) so that the rows below one parent come by the figure
// primary names (Prefix.Figure), the largest first, the prefix in byte
// order on a tie, and a prefix whose figure is unknown after the others
// (a partial sum ranks by the bytes it has); each row's own rows still
// follow it, and the total stays first.
func SortBySize(rows []Prefix, primary string) {
	kids := make([][]int, len(rows)+1) // by row; the last for the rows without a parent
	top := len(rows)
	var stack []int
	for i := range rows {
		for len(stack) > 0 && rows[stack[len(stack)-1]].Depth >= rows[i].Depth {
			stack = stack[:len(stack)-1]
		}
		parent := top
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		kids[parent] = append(kids[parent], i)
		stack = append(stack, i)
	}
	bigger := func(a, b int) int {
		x, xok := rows[a].Figure(primary)
		y, yok := rows[b].Figure(primary)
		switch {
		case xok != yok:
			if xok {
				return -1
			}
			return 1
		case xok && x != y:
			return cmp.Compare(y, x)
		}
		return cmp.Compare(rows[a].Prefix, rows[b].Prefix)
	}
	out := make([]Prefix, 0, len(rows))
	var emit func(int)
	emit = func(n int) {
		slices.SortFunc(kids[n], bigger)
		for _, i := range kids[n] {
			out = append(out, rows[i])
			emit(i)
		}
	}
	emit(top)
	copy(rows, out)
}

// Stats is the tally's account; like Result, call it after the last
// Flush.
func (t *Tally) Stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.finish()
	s := Stats{Depth: t.opt.Depth, Prefixes: int64(max(1, len(t.cells))), MaxPrefixes: int64(t.opt.MaxPrefixes), Primary: t.opt.Primary, PrimaryReason: t.opt.Reason,
		NotObjects: t.notObjects, LinksOnce: t.opt.LinksOnce && t.unidentified == 0, LinkedRows: t.linkedRows, Inodes: int64(len(t.links)), MaxInodes: int64(t.opt.MaxInodes)}
	c := t.cells[t.opt.Origin]
	if c == nil {
		c = &cell{}
	}
	r := t.prefix(t.opt.Origin, c)
	s.Objects, s.Dirs, s.SizeMissing, s.AllocatedBytesMissing = r.Objects, r.Dirs, r.SizeMissing, r.AllocatedMissing
	if r.HasSize {
		s.Size = &r.Size
	}
	if r.HasAllocated {
		s.AllocatedBytes = &r.Allocated
	}
	if r.HasNewest {
		at := jsontime.Of(time.UnixMicro(r.Newest))
		s.MaxLastModified = &at
	}
	return s
}

// ViewStats is the adaptive view's account (Options.View), nil without
// one: how the view's algorithm ran, for the report's diagnostics.
func (t *Tally) ViewStats() *ViewStats {
	v := t.view
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	s := v.account()
	return &s
}
