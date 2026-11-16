// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package totals

import (
	"cmp"
	"container/heap"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// The adaptive view (Options.View, swath du on a terminal): the largest
// prefixes and objects at whatever depth they are, each exact, in memory
// that does not grow with the listing. The idea of showing a tree of the
// largest entries with their share of the total is dust's and pdu's
// (github.com/bootandy/dust, github.com/KSXGitHub/parallel-disk-usage,
// both Apache-2.0); this is written from their documented behavior, not
// their code, and differs where noted on Select.
//
// A node is a prefix (a key up to a '/' below the origin) or an object's
// key; its figure is the bytes under it in the primary figure
// (Options.Primary). Rows reach the view in runs (Tally.ViewRun): a run is
// one range's rows. In an ordered run they ascend and no row of another
// run sorts between two of its rows (order.RangeSink), so a node the run
// enters after its first row and leaves before its last is complete: no
// other run holds any of it. Such a node is remembered only among the
// viewKeep largest complete nodes of its depth (its bytes are in its
// parent either way): which nodes those are does not depend on the order
// they complete in, so whatever a view shows, from the largest down, is
// there. A node that holds a run's first or
// last row may continue in another run: it is partial, and the runs'
// pieces of it are summed by name and complete when the listing ends. An
// unordered run (a local directory counted unsorted) completes only its
// objects; its prefixes are all partial. So memory is the remembered
// nodes (at most viewKeep a depth, besides those holding a hard link,
// below), the partial ones (each run's two edges, a node a level;
// every directory of a local tree) and each run's open nodes.
//
// A node's children (the prefixes and objects directly under it) are
// counted once: by its run when the child is complete there, else when
// the child is first summed as a partial node.
//
// With Options.LinksOnce a hard-linked object's bytes are added once, at
// the end, to the nodes of its first name (Tally's links): the prefixes
// holding a linked object are therefore kept, whatever their size, until
// then.
//
// Past Options.MaxPrefixes partial and remembered nodes the view keeps
// fewer levels (MaxDepth): the deepest are forgotten, the levels kept stay
// exact, and their nodes at the deepest level kept no longer know how many
// children they have. The view does not fail.

// viewFloor is the share of the total, as a divisor, below which the view
// spends no line on a node: 0.5 %.
const viewFloor = 200

// viewKeep is the complete nodes of each depth remembered, the largest: far
// more than a terminal shows, and each node of 1 % of the total is among
// them (at most 100 of one depth can hold 1 %).
const viewKeep = 100

// floorDepths is the depths whose admission floor (view.floors) a run reads
// without the lock; a deeper node takes the lock.
const floorDepths = 64

// vsum is a node's sums.
type vsum struct {
	bytes    int64 // the primary figure of its objects that had one
	objects  int64
	missing  int64 // objects without the primary figure
	children int64 // distinct child nodes counted so far (counted once, above)
	newest   int64 // the newest modification time of its objects, epoch microseconds; valid with hasNewest
	// hasNewest: an object under it has a time.
	hasNewest bool
	pinned    bool // holds a hard-linked object, whose bytes come at the end
	// recent is its children with the newest times, at most recentKids, the
	// newest first: a fold's newest time is the first one not shown.
	recent []kidTime
}

// recentKids is the children a node remembers the times of, the newest:
// more than a terminal shows of one parent but in a rare view.
const recentKids = 32

// kidTime is a child's newest time.
type kidTime struct {
	name string
	at   int64
}

// noteKid takes a child's newest time (a run's piece of it: the newest
// piece is the child's), name copied if kept.
func (s *vsum) noteKid(name string, at int64) {
	if len(s.recent) == recentKids && at <= s.recent[len(s.recent)-1].at {
		return
	}
	i := slices.IndexFunc(s.recent, func(k kidTime) bool { return k.name == name })
	if i >= 0 {
		if at <= s.recent[i].at {
			return
		}
		s.recent = slices.Delete(s.recent, i, i+1)
	} else {
		name = strings.Clone(name)
		if len(s.recent) == recentKids {
			s.recent = s.recent[:len(s.recent)-1]
		}
	}
	j, _ := slices.BinarySearchFunc(s.recent, at, func(k kidTime, at int64) int { return cmp.Compare(at, k.at) })
	s.recent = slices.Insert(s.recent, j, kidTime{name, at})
}

// add adds o's sums to s, its children included: o is another piece of
// the same node.
func (s *vsum) add(o *vsum) {
	s.include(o)
	s.children += o.children
	for _, k := range o.recent {
		s.noteKid(k.name, k.at)
	}
}

// include adds what o holds to s, a node above it: everything but o's
// children, which are not s's.
func (s *vsum) include(o *vsum) {
	s.bytes += o.bytes
	s.objects += o.objects
	s.missing += o.missing
	if o.hasNewest && (!s.hasNewest || o.newest > s.newest) {
		s.newest, s.hasNewest = o.newest, true
	}
	s.pinned = s.pinned || o.pinned
}

// view is a tally's adaptive view; its runs feed it concurrently.
type view struct {
	origin  string
	primary string
	budget  int // MaxPrefixes: the nodes held (held) before levels are dropped

	// floors are, per depth, the least remembered node once viewKeep are
	// held (nil before): a complete node below it is dropped without the
	// lock.
	floors   [floorDepths]atomic.Pointer[kent]
	maxDepth atomic.Int64 // the deepest level kept (no limit until lowered)

	mu   sync.Mutex
	open map[string]*onode // partial nodes, complete once their subtree or every run has ended
	// byLo and byHi index the stretches of keys whose runs have all ended
	// (cover), by bound: a partial node completes when the stretches
	// around it join.
	byLo, byHi map[string]*ival
	kept       map[string]vsum // complete nodes remembered: the viewKeep largest of their depth
	pins       map[string]vsum // complete nodes holding a hard link, kept to the end
	// largest holds, per depth, kept's nodes by size, the smallest on top:
	// past viewKeep of a depth the smallest is dropped.
	largest []kheap
	// held counts the nodes the budget bounds, those that grow with the
	// listing: the pinned ones, and an unordered run's partial ones (local
	// directories being listed) that hold something. A partial node that
	// holds nothing yet (a directory's row, its listing queued) costs what
	// the queued range already does, an ordered run's partial nodes are its
	// two edges, a node a level, and kept holds at most viewKeep a depth.
	held    int
	lowered bool
	stats   ViewStats
	// unreadable is the directories the source could not read
	// (Tally.Unreadable): the lines above them miss what they hold.
	unreadable map[string]bool
}

// kheap is a depth's remembered nodes, a min-heap by size.
type kheap []kent

type kent struct {
	bytes int64
	name  string
}

// below orders the nodes a depth keeps: by bytes, and of equal bytes the
// one later in byte order below, so which are kept is the same whatever
// the order they complete in.
func (e *kent) below(o *kent) bool {
	return e.bytes < o.bytes || e.bytes == o.bytes && e.name > o.name
}

func (h kheap) Len() int           { return len(h) }
func (h kheap) Less(i, j int) bool { return h[i].below(&h[j]) }
func (h kheap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *kheap) Push(x any)        { *h = append(*h, x.(kent)) }
func (h *kheap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// onode is a partial node.
type onode struct {
	s     vsum
	heavy bool                // holds an object or a child: counted in held
	kids  map[string]struct{} // its partial children, to complete with it
}

// noLimit is maxDepth before the view drops a level: deeper than any key.
const noLimit = 1 << 30

// ViewStats is the view's account for the run report: how many nodes its
// runs completed and remembered or dropped, how many were partial and
// merged across runs, and the depth kept.
type ViewStats struct {
	Runs int64 `json:"runs"` // runs that held rows
	// Completed is the nodes complete inside one run, or when the range
	// owning them ended with its subtree (a local directory).
	Completed int64 `json:"completed"`
	// Remembered and Dropped split Completed: remembered among the
	// viewKeep largest of their depth (or for a hard link), dropped below
	// them. Pruned counts remembered nodes dropped later for larger ones.
	Remembered int64 `json:"remembered"`
	Dropped    int64 `json:"dropped"`
	Pruned     int64 `json:"pruned"`
	// Partial is the distinct nodes held across runs.
	Partial int64 `json:"partial"`
	// DepthKept is the deepest level kept: -1 for every level, else the
	// level the view fell back to past max_prefixes nodes (Lowered).
	DepthKept int  `json:"depth_kept"`
	Lowered   bool `json:"lowered"`
}

func newView(opt *Options) *view {
	v := &view{origin: opt.Origin, primary: opt.Primary, budget: opt.MaxPrefixes, open: map[string]*onode{}, kept: map[string]vsum{}, pins: map[string]vsum{},
		byLo: map[string]*ival{}, byHi: map[string]*ival{}, unreadable: map[string]bool{}}
	v.maxDepth.Store(noLimit)
	v.open[v.origin] = &onode{}
	return v
}

// depthOf is a node's depth: its '/'s below the origin, an object's key
// one more than its prefix.
func (v *view) depthOf(name string) int {
	d := strings.Count(name[len(v.origin):], "/")
	if name != v.origin && !strings.HasSuffix(name, "/") {
		d++
	}
	return d
}

// parentOf is a node's parent's name (depth > 0).
func parentOf(name string) string {
	return name[:strings.LastIndexByte(name[:len(name)-1], '/')+1]
}

// complete takes a complete node (v.mu held): one holding a hard link is
// kept to the end; any other is remembered among the viewKeep largest of
// its depth, the smallest past them dropped, one of no bytes or no larger
// than them all dropped at once. A node is dropped only for viewKeep
// larger ones of its depth, whatever the order the nodes complete in. It
// reports whether the node was dropped.
func (v *view) complete(name string, depth int, s *vsum) bool {
	switch {
	case int64(depth) > v.maxDepth.Load():
		return true // its level was dropped
	case s.pinned:
		v.pins[strings.Clone(name)] = *s
		v.held++
		v.fit()
	case s.bytes == 0:
		return true
	default:
		for len(v.largest) <= depth {
			v.largest = append(v.largest, nil)
		}
		h := &v.largest[depth]
		if h.Len() == viewKeep && !(*h)[0].below(&kent{s.bytes, name}) {
			return true
		}
		name = strings.Clone(name)
		v.kept[name] = *s
		heap.Push(h, kent{s.bytes, name})
		if h.Len() > viewKeep {
			delete(v.kept, heap.Pop(h).(kent).name)
			v.stats.Pruned++
		}
		if h.Len() == viewKeep && depth < floorDepths {
			least := (*h)[0]
			v.floors[depth].Store(&least)
		}
	}
	v.stats.Remembered++
	return false
}

// below reports whether a node is below the least the depth keeps, so
// not kept, from the floor and without the lock.
func (v *view) below(depth int, name string, bytes int64) bool {
	f := v.floors[depth].Load()
	return f != nil && !f.below(&kent{bytes, name})
}

// fit keeps the nodes held within the budget (v.mu held), dropping the
// deepest levels past it.
func (v *view) fit() {
	if v.held > v.budget {
		v.lower()
	}
}

// merge adds a run's piece of a partial node (v.mu held); held says
// whether the node counts against the budget, as an unordered run's does.
func (v *view) merge(name string, depth int, s *vsum, held bool) {
	if int64(depth) > v.maxDepth.Load() {
		return
	}
	n := v.open[name]
	if n == nil {
		n = v.create(name, depth)
	}
	n.s.add(s)
	if held && !n.heavy && (n.s.objects > 0 || n.s.children > 0) {
		n.heavy = true
		v.held++
		v.fit()
	}
}

// create adds a partial node and counts it as its parent's child, the
// parent created too if it is not held yet (v.mu held).
func (v *view) create(name string, depth int) *onode {
	n := &onode{}
	name = strings.Clone(name)
	v.open[name] = n
	v.stats.Partial++
	if depth > 0 {
		pn := parentOf(name)
		p := v.open[pn]
		if p == nil {
			p = v.create(pn, depth-1)
		}
		p.s.children++
		if p.kids == nil {
			p.kids = map[string]struct{}{}
		}
		p.kids[name] = struct{}{}
	}
	return n
}

// subtreeEnd completes the partial node name and the partial nodes below
// it (v.mu held): the range owning name ended with its subtree.
func (v *view) subtreeEnd(name string) {
	n := v.open[name]
	if n == nil || name == v.origin {
		return
	}
	for k := range n.kids {
		v.subtreeEnd(k)
	}
	delete(v.open, name)
	if p := v.open[parentOf(name)]; p != nil {
		delete(p.kids, name)
	}
	if n.heavy {
		v.held--
	}
	v.stats.Completed++
	if v.complete(name, v.depthOf(name), &n.s) {
		v.stats.Dropped++
	}
}

// lower drops the deepest levels so that the nodes held fit in half the
// budget (v.mu held): the levels left keep their sums, their deepest
// nodes stop counting children.
func (v *view) lower() {
	var per []int
	count := func(name string) {
		d := v.depthOf(name)
		for len(per) <= d {
			per = append(per, 0)
		}
		per[d]++
	}
	for k, n := range v.open {
		if n.heavy {
			count(k)
		}
	}
	for k := range v.pins {
		count(k)
	}
	d, held := 0, 0
	if len(per) > 0 {
		held = per[0]
	}
	for d+1 < len(per) && held+per[d+1] <= v.budget/2 {
		d++
		held += per[d]
	}
	if int64(d) >= v.maxDepth.Load() { // one level over the budget alone: keep it
		d--
	}
	d = max(d, 0)
	v.maxDepth.Store(int64(d))
	v.lowered = true
	for k, n := range v.open {
		if v.depthOf(k) > d {
			delete(v.open, k)
			if n.heavy {
				v.held--
			}
		}
	}
	for k := range v.pins {
		if v.depthOf(k) > d {
			delete(v.pins, k)
			v.held--
		}
	}
	for _, h := range v.largest[min(len(v.largest), d+1):] {
		for _, e := range h {
			delete(v.kept, e.name)
		}
	}
	v.largest = v.largest[:min(len(v.largest), d+1)]
}

// frame is a node a run is in.
type frame struct {
	name  string // a view of a row's key until owned
	owned bool
	depth int
	leaf  bool // an object's key
	left  bool // holds the run's first row: rows of it may precede the run
	s     vsum
}

// named is an unordered run's prefix.
type named struct {
	name string
	s    vsum
}

// ViewRun sums one run's rows into the view (Tally.ViewRun). It is used by
// one goroutine: Add each row (after the Batch's Add), Flush when the rows
// handed over are released, End once.
type ViewRun struct {
	v       *view
	t       *Tally
	ordered bool
	max     int               // v.maxDepth at the last Flush
	chain   []string          // the row's nodes, views of its key
	stack   []frame           // ordered: the nodes the run is in, the root first
	cache   []*named          // unordered: the last row's prefixes, in m
	m       map[string]*named // unordered: the run's prefixes
	any     bool              // Add was called: the run held rows
	stats   ViewStats
	// The interval the run covers, (lo, hi] (top: up to the last key),
	// when its source tells it (From, Through): its edge nodes inside it
	// are complete.
	lo, hi        string
	from, through bool
	top           bool
}

// From takes the run's lower bound, exclusive (order.CoveredRun), before
// its first row.
func (r *ViewRun) From(lo string) { r.lo, r.from = lo, true }

// Through takes the run's upper bound, inclusive (top: none), before End.
func (r *ViewRun) Through(hi string, top bool) { r.hi, r.top, r.through = hi, top, true }

// covers reports whether every key under the node name lies in (lo, hi]
// (top: no upper bound): a prefix's keys sort from it to before the first
// string past them all, an object's node is its one key.
func covers(name, lo, hi string, top bool) bool {
	if lo >= name {
		return false
	}
	if top {
		return true
	}
	if strings.HasSuffix(name, "/") {
		return hi > name && !strings.HasPrefix(hi, name)
	}
	return hi >= name
}

// ival is a stretch of keys whose runs have all ended: (lo, hi], top for
// no upper bound.
type ival struct {
	lo, hi string
	top    bool
}

// cover records that the keys (lo, hi] are all summed (v.mu held), joins
// the stretch with those it meets, and completes the partial nodes that
// were cut where they meet and now lie inside: a node cut by a bound is
// a prefix of it.
func (v *view) cover(lo, hi string, top bool, max int) {
	iv := &ival{lo, hi, top}
	var joins []string
	if left := v.byHi[lo]; left != nil {
		delete(v.byHi, lo)
		delete(v.byLo, left.lo)
		iv.lo = left.lo
		joins = append(joins, lo)
	}
	if right := v.byLo[hi]; !top && right != nil {
		delete(v.byLo, hi)
		if !right.top {
			delete(v.byHi, right.hi)
		}
		iv.hi, iv.top = right.hi, right.top
		joins = append(joins, hi)
	}
	v.byLo[iv.lo] = iv
	if !iv.top {
		v.byHi[iv.hi] = iv
	}
	r := ViewRun{v: v, max: max}
	for _, b := range joins {
		r.nodes(&Row{Key: b, Object: true})
		for d := len(r.chain) - 1; d > 0; d-- {
			if name := r.chain[d]; v.open[name] != nil && covers(name, iv.lo, iv.hi, iv.top) {
				v.subtreeEnd(name)
			}
		}
	}
}

// ViewRun starts a run of rows for the view (Options.View): ordered, the
// rows ascend and are a contiguous stretch of the stream (order.RangeSink);
// otherwise they are one range's rows in any order.
func (t *Tally) ViewRun(ordered bool) *ViewRun {
	r := &ViewRun{v: t.view, t: t, ordered: ordered, max: int(t.view.maxDepth.Load())}
	if !ordered {
		r.m = map[string]*named{}
	}
	return r
}

// ViewSubtreeEnd completes the prefix a range owned once it ended with
// its subtree (order.SubtreeSink: a local directory), and the partial
// nodes below it, each as a run completes a node (view.complete).
func (t *Tally) ViewSubtreeEnd(prefix string) {
	v := t.view
	v.mu.Lock()
	v.subtreeEnd(prefix)
	v.mu.Unlock()
}

// figure is a row's primary figure.
func (r *ViewRun) figure(row *Row) (int64, bool) {
	if r.v.primary == AllocatedBytes {
		return row.Allocated, row.HasAllocated
	}
	return row.Size, row.HasSize
}

// nodes sets r.chain to row's nodes from the root down to r.max, and
// reports whether the last is the object itself and whether levels were
// left out.
func (r *ViewRun) nodes(row *Row) (leaf, cut bool) {
	o := r.v.origin
	r.chain = append(r.chain[:0], o)
	key := row.Key
	if !strings.HasPrefix(key, o) {
		return false, false
	}
	end := len(o)
	for {
		i := strings.IndexByte(key[end:], '/')
		if i < 0 && (end == len(key) || !row.Object) {
			return false, false
		}
		if len(r.chain) > r.max {
			return false, true
		}
		if i < 0 {
			r.chain = append(r.chain, key)
			return true, false
		}
		end += i + 1
		r.chain = append(r.chain, key[:end])
	}
}

// Add sums one row; row.Key must stay valid until the next Flush.
func (r *ViewRun) Add(row *Row) {
	leaf, cut := r.nodes(row)
	var c vsum
	link := false
	if row.Object {
		c.objects = 1
		if n, ok := r.figure(row); ok {
			c.bytes = n
		} else {
			c.missing = 1
		}
		if row.HasMtime {
			c.newest, c.hasNewest = row.Mtime, true
		}
		if row.Link && r.t.opt.LinksOnce {
			c = vsum{} // added once, time too, at the end (View), under its first name
			link = true
		}
		// An object with no node of its own (a key ending in '/', outside
		// the origin) is a child of its deepest prefix.
		if !leaf && !cut {
			c.children = 1
		}
	}
	r.any = true
	if r.ordered {
		r.addOrdered(&c, leaf, link)
	} else {
		r.addUnordered(&c, leaf, link)
	}
	if c.children == 1 && c.hasNewest { // its own child, as the deepest node's
		r.deepest().noteKid(row.Key, c.newest)
	}
}

func (r *ViewRun) addOrdered(c *vsum, leaf, link bool) {
	s := 0
	for s < len(r.stack) && s < len(r.chain) && r.stack[s].name == r.chain[s] {
		s++
	}
	r.popTo(s, false)
	first := len(r.stack) == 0 && s == 0
	for d := s; d < len(r.chain); d++ {
		r.stack = append(r.stack, frame{name: r.chain[d], owned: d == 0, depth: d, leaf: leaf && d == len(r.chain)-1, left: first})
	}
	if link {
		for i := range r.stack {
			if !r.stack[i].leaf {
				r.stack[i].s.pinned = true
			}
		}
	}
	r.stack[len(r.stack)-1].s.add(c)
}

// popTo leaves the nodes below the first n of the stack, the deepest
// first, adding each to its parent: a node the run entered after its first
// row is complete, unless the run is ending; any other is partial.
func (r *ViewRun) popTo(n int, ending bool) {
	for len(r.stack) > max(n, 1) || ending && len(r.stack) > n {
		f := &r.stack[len(r.stack)-1]
		if len(r.stack) > 1 {
			p := &r.stack[len(r.stack)-2]
			p.s.include(&f.s)
			if f.s.hasNewest {
				p.s.noteKid(f.name, f.s.newest)
			}
			if !f.left && !ending {
				p.s.children++
			}
		}
		// An edge node is complete too once the run's interval covers it.
		whole := !f.left && !ending ||
			f.depth > 0 && (!f.left || r.from && r.lo < f.name) && (!ending || r.through && covers(f.name, "", r.hi, r.top))
		if len(r.stack) > 1 && whole && (f.left || ending) {
			r.stack[len(r.stack)-2].s.children++ // counted here, never as a partial node
		}
		if whole {
			r.complete(f.name, f.depth, &f.s)
		} else {
			r.partial(f.name, f.depth, &f.s)
		}
		r.stack = r.stack[:len(r.stack)-1]
	}
}

// complete takes a node no other run holds.
func (r *ViewRun) complete(name string, depth int, s *vsum) {
	r.stats.Completed++
	v := r.v
	if int64(depth) > v.maxDepth.Load() {
		return
	}
	if !s.pinned && (s.bytes == 0 || depth < floorDepths && v.below(depth, name, s.bytes)) {
		r.stats.Dropped++ // no larger than the viewKeep largest of its depth: no lock
		return
	}
	v.mu.Lock()
	if v.complete(name, depth, s) {
		r.stats.Dropped++
	}
	v.mu.Unlock()
}

// partial merges a run's piece of a node.
func (r *ViewRun) partial(name string, depth int, s *vsum) {
	r.v.mu.Lock()
	r.v.merge(name, depth, s, false) // a run's edges: at most a node a level at each
	r.v.mu.Unlock()
}

func (r *ViewRun) addUnordered(c *vsum, leaf, link bool) {
	prefixes := len(r.chain)
	if leaf {
		prefixes--
	}
	for d := range prefixes {
		if d < len(r.cache) && r.cache[d].name == r.chain[d] {
			continue
		}
		r.cache = r.cache[:d]
		n := r.m[r.chain[d]]
		if n == nil {
			n = &named{name: strings.Clone(r.chain[d])}
			r.m[n.name] = n
		}
		r.cache = append(r.cache, n)
	}
	for _, n := range r.cache[:prefixes] {
		n.s.include(c)
		n.s.pinned = n.s.pinned || link
	}
	if c.hasNewest {
		for d, n := range r.cache[:prefixes] {
			if d+1 < len(r.chain) {
				n.s.noteKid(r.chain[d+1], c.newest)
			}
		}
	}
	deepest := &r.cache[prefixes-1].s
	deepest.children += c.children
	if leaf {
		deepest.children++
		r.complete(r.chain[len(r.chain)-1], len(r.chain)-1, c)
	}
}

// deepest is the row's deepest prefix's sums in the run.
func (r *ViewRun) deepest() *vsum {
	if r.ordered {
		return &r.stack[len(r.stack)-1].s
	}
	n := len(r.chain)
	if len(r.cache) < n {
		n = len(r.cache)
	}
	return &r.cache[n-1].s
}

// Flush owns the names the run still needs: call it before the rows added
// are released.
func (r *ViewRun) Flush() {
	for i := range r.stack {
		if f := &r.stack[i]; !f.owned {
			f.name, f.owned = strings.Clone(f.name), true
		}
	}
	r.max = int(r.v.maxDepth.Load())
}

// End closes the run: its open nodes are partial.
func (r *ViewRun) End() {
	r.Flush()
	if r.ordered {
		r.popTo(0, true)
		if r.from && r.through {
			r.v.mu.Lock()
			r.v.cover(r.lo, r.hi, r.top, r.max)
			r.v.mu.Unlock()
		}
	} else {
		v := r.v
		v.mu.Lock()
		for k, n := range r.m {
			v.merge(k, v.depthOf(k), &n.s, true)
		}
		v.mu.Unlock()
		r.m = nil
	}
	v := r.v
	v.mu.Lock()
	if r.any {
		v.stats.Runs++
	}
	v.stats.Completed += r.stats.Completed
	v.stats.Dropped += r.stats.Dropped
	v.mu.Unlock()
}

// account is the view's stats (v.mu held).
func (v *view) account() ViewStats {
	s := v.stats
	s.DepthKept, s.Lowered = -1, v.lowered
	if v.lowered {
		s.DepthKept = int(v.maxDepth.Load())
	}
	return s
}

// View is the adaptive view's result: the largest nodes of each depth,
// exact.
type View struct {
	Primary string
	Total   ViewNode
	// Nodes are the nodes but the total, by key: a prefix and every
	// ancestor of a node are present.
	Nodes []ViewNode
	// MaxDepth is the deepest level held: below it the view forgot the
	// nodes (Lowered: more than MaxPrefixes of them).
	MaxDepth int
	Lowered  bool
	Stats    ViewStats
	// Unreadable is the directories the source could not read
	// (Tally.Unreadable), sorted.
	Unreadable []string
}

// ViewNode is a node's sums. Children is the prefixes and objects directly
// under it, -1 when unknown (a node at MaxDepth once Lowered).
type ViewNode struct {
	Key                     string
	Depth                   int
	Bytes, Objects, Missing int64
	Children                int64
	// Newest is the newest modification time of its objects, epoch
	// microseconds, valid with HasNewest (max_last_modified).
	Newest    int64
	HasNewest bool
	// Unreadable: a directory at or below it could not be read, so its
	// sums leave out what that held.
	Unreadable bool
	recent     []kidTime // its children's newest times, the newest first (vsum.recent)
}

// View is the tally's adaptive view (Options.View): call it after the
// last run has ended, once.
func (t *Tally) View() *View {
	t.mu.Lock()
	defer t.mu.Unlock()
	v := t.view
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	maxDepth := int(v.maxDepth.Load())
	all := map[string]*vsum{}
	for k, n := range v.open {
		all[k] = &n.s
	}
	for _, m := range []map[string]vsum{v.kept, v.pins} {
		for k, n := range m {
			all[k] = &n
		}
	}
	// A hard-linked object's bytes, once, under its first name: its
	// prefixes were pinned, so they are held; its own node is added.
	for _, l := range t.links {
		s := vsum{objects: l.c.objects, hasNewest: l.c.hasNewest}
		if s.hasNewest {
			s.newest = l.c.newest
		}
		if t.opt.Primary == AllocatedBytes {
			s.bytes, s.missing = l.c.alloc, l.c.allocMissing
		} else {
			s.bytes, s.missing = l.c.size, l.c.sizeMissing
		}
		r := ViewRun{v: v, t: t, max: maxDepth}
		leaf, _ := r.nodes(&Row{Key: l.key, Object: true})
		for d, name := range r.chain {
			n := all[name]
			if n == nil {
				if !leaf || d != len(r.chain)-1 {
					continue // only past a LimitError, which fails the run
				}
				n = &vsum{}
				all[strings.Clone(name)] = n
			}
			n.include(&s)
			if d > 0 && s.hasNewest {
				if p := all[parentOf(name)]; p != nil {
					p.noteKid(name, s.newest)
				}
			}
		}
	}
	total := all[v.origin]
	out := &View{Primary: v.primary, MaxDepth: maxDepth, Lowered: v.lowered, Stats: v.account()}
	node := func(k string, n *vsum) ViewNode {
		d := v.depthOf(k)
		c := n.children
		if v.lowered && d == maxDepth {
			c = -1
		}
		return ViewNode{Key: k, Depth: d, Bytes: n.bytes, Objects: n.objects, Missing: n.missing, Children: c, Newest: n.newest, HasNewest: n.hasNewest, recent: n.recent}
	}
	out.Unreadable = slices.Sorted(maps.Keys(v.unreadable))
	below := func(k string) bool { // a directory unreadable at or below k
		i, _ := slices.BinarySearch(out.Unreadable, k)
		return i < len(out.Unreadable) && strings.HasPrefix(out.Unreadable[i], k)
	}
	out.Total = node(v.origin, total)
	out.Total.Unreadable = len(out.Unreadable) > 0
	// The viewKeep largest of each depth among the nodes held with bytes
	// (complete ones were kept the same way; partial and pinned ones join
	// them now): the same nodes however the listing ran. Then only those
	// under a parent kept too: a node whose parent fell out of its depth's
	// largest is in that parent's sums only. In byte order a parent comes
	// before its children.
	var depths [][]kent
	for k, n := range all {
		if k != v.origin && n.bytes > 0 {
			d := v.depthOf(k)
			for len(depths) <= d {
				depths = append(depths, nil)
			}
			depths[d] = append(depths[d], kent{n.bytes, k})
		}
	}
	var keys []string
	for _, es := range depths {
		slices.SortFunc(es, func(a, b kent) int {
			switch {
			case b.below(&a):
				return -1
			case a.below(&b):
				return 1
			}
			return 0
		})
		for _, e := range es[:min(len(es), viewKeep)] {
			keys = append(keys, e.name)
		}
	}
	slices.Sort(keys)
	held := map[string]bool{v.origin: true}
	for _, k := range keys {
		if held[parentOf(k)] {
			held[k] = true
			n := node(k, all[k])
			n.Unreadable = strings.HasSuffix(k, "/") && below(k)
			out.Nodes = append(out.Nodes, n)
		}
	}
	return out
}

// ViewRow is a line of the view (View.Select): a node, or the fold of a
// parent's children not shown (Fold), which carries their sums.
type ViewRow struct {
	// Name is the node's key below its parent's ("logs/", "a.bin"); the
	// total's is its key, the origin. A prefix whose one child is shown
	// shares its line: "api/2025/".
	Name string
	// Level is the line's level in the view: the total's 0, a line's
	// children's one more.
	Level                   int
	Bytes, Objects, Missing int64
	// Unreadable: a directory at or below the line's node (a fold: below a
	// child it holds) could not be read.
	Unreadable bool
	// Newest and HasNewest are a node's (ViewNode), or a fold's the newest
	// of what it holds (unknown, in a rare view, when the thirty-two
	// newest children of its parent are all shown).
	Newest    int64
	HasNewest bool
	// Fold: the rest of the parent's children, More of them (-1: an
	// unknown number), Bytes, Objects and Missing their sums.
	Fold bool
	More int64
}

// Select chooses the view's lines, at most rows (the total and every fold
// line included, at least the total and its fold), from the largest down.
// The lines show, besides the total, nodes and the folds of the nodes
// shown with children: a fold holds a parent's children not shown, how
// many and their sums, so each level adds up to its parent (a fold of no
// bytes is left out). Line by line, what is not broken down yet (a fold,
// or a node shown whole) gives its largest child, a node a fold for the
// rest too: the one whose child is the largest. So the largest nodes come
// first and the largest prefixes open, down to a floor (0.5 % of the
// total, more on a short terminal): a long tail of small prefixes stays in
// its fold, and the view ends when nothing above the floor is left.
// Unlike dust, which shows the largest nodes of the whole tree whatever
// their parents, and pdu, which hides what is below 1 % without saying
// so. A prefix with one child shares its line with it (it holds the same
// bytes). The lines come in tree order: a parent first, its children by
// size.
func (v *View) Select(rows int) []ViewRow {
	type sel struct {
		n     *ViewNode
		kids  []int // its children held, by size, the largest first
		next  int   // kids[:next] are shown
		bytes int64 // theirs
	}
	nodes := make([]sel, len(v.Nodes)+1)
	nodes[0] = sel{n: &v.Total}
	index := map[string]int{v.Total.Key: 0}
	for i := range v.Nodes {
		nodes[i+1].n = &v.Nodes[i]
		index[v.Nodes[i].Key] = i + 1
	}
	bigger := func(a, b int) int {
		x, y := nodes[a].n, nodes[b].n
		if c := cmp.Compare(y.Bytes, x.Bytes); c != 0 {
			return c
		}
		return cmp.Compare(x.Key, y.Key)
	}
	for i := 1; i < len(nodes); i++ {
		p := &nodes[index[parentOf(nodes[i].n.Key)]]
		p.kids = append(p.kids, i)
	}
	for i := range nodes {
		slices.SortFunc(nodes[i].kids, bigger)
	}
	shown := func(s *sel) int64 { return int64(s.next) }
	// hiddenUnreadable reports whether a child of s not shown holds a
	// directory that could not be read.
	hiddenUnreadable := func(s *sel) bool {
		if !s.n.Unreadable {
			return false
		}
		shownKey := map[string]bool{}
		for _, k := range s.kids[:s.next] {
			shownKey[nodes[k].n.Key] = true
		}
		i, _ := slices.BinarySearch(v.Unreadable, s.n.Key)
		for ; i < len(v.Unreadable) && strings.HasPrefix(v.Unreadable[i], s.n.Key); i++ {
			u := v.Unreadable[i]
			if u == s.n.Key {
				continue // s itself: its line is marked
			}
			j := strings.IndexByte(u[len(s.n.Key):], '/')
			if !shownKey[u[:len(s.n.Key)+j+1]] {
				return true
			}
		}
		return false
	}
	// folds is the fold lines s needs: one when it lists children (the
	// total always does) and some not shown hold bytes or a directory that
	// could not be read.
	folds := func(s *sel) int {
		if s.next == 0 && s != &nodes[0] {
			return 0
		}
		if (s.n.Children < 0 || shown(s) < s.n.Children) && (s.bytes < s.n.Bytes || hiddenUnreadable(s)) {
			return 1
		}
		return 0
	}
	// joins reports whether s shares its line with its one child: a prefix
	// (not the total) with exactly one child, shown.
	joins := func(s *sel) bool { return s != &nodes[0] && s.n.Children == 1 && s.next == 1 }
	// What is not broken down yet: a shown node not expanded or a fold, by
	// the node it breaks down; the next is the one whose next child is the
	// largest, so the largest prefixes shown open first. No line goes to a
	// node under the floor: a share of the total that one line of the view
	// would barely show (viewFloor, or a quarter of a line's share of the
	// lines), and once none is left above it the view ends, shorter.
	floor := max(v.Total.Bytes/viewFloor, v.Total.Bytes/int64(4*max(rows, 1)))
	type part struct{ i int }
	var parts []part
	if len(nodes[0].kids) > 0 {
		parts = append(parts, part{0})
	}
	used := 1 + folds(&nodes[0])
	for len(parts) > 0 {
		best, worth := -1, int64(-1)
		for j, pt := range parts {
			s := &nodes[pt.i]
			if w := nodes[s.kids[s.next]].n.Bytes; w >= floor && w > worth {
				best, worth = j, w
			}
		}
		if best < 0 {
			break // nothing left above the floor
		}
		pt := parts[best]
		parts = append(parts[:best], parts[best+1:]...)
		s := &nodes[pt.i]
		before := folds(s)
		c := s.kids[s.next]
		s.next++
		s.bytes += nodes[c].n.Bytes
		cost := 1 + folds(s) - before
		if joins(s) {
			cost = 0 // on its parent's line
		}
		if used+cost > rows {
			s.next--
			s.bytes -= nodes[c].n.Bytes
			continue // a cheaper part may still fit
		}
		used += cost
		if len(nodes[c].kids) > 0 {
			parts = append(parts, part{c})
		}
		if s.next < len(s.kids) {
			parts = append(parts, part{pt.i})
		}
	}
	// foldNewest is the newest time among the children of s not shown:
	// s's own when no child shown has it, else the first of its newest
	// children (recent) not shown; unknown when those are all shown.
	foldNewest := func(s *sel, shownNewest int64, shownTimed bool) (int64, bool) {
		if !s.n.HasNewest {
			return 0, false
		}
		if !shownTimed || s.n.Newest > shownNewest {
			return s.n.Newest, true
		}
		names := map[string]bool{}
		for _, i := range s.kids[:s.next] {
			names[nodes[i].n.Key] = true
		}
		for _, k := range s.n.recent {
			if !names[k.name] {
				return k.at, true
			}
		}
		return 0, false
	}
	var out []ViewRow
	var walk func(i, level int)
	walk = func(i, level int) {
		s := &nodes[i]
		name := s.n.Key
		if i != 0 {
			name = name[len(parentOf(name)):]
		}
		for joins(s) {
			s = &nodes[s.kids[0]]
			name += s.n.Key[len(parentOf(s.n.Key)):]
		}
		out = append(out, ViewRow{Name: name, Level: level, Bytes: s.n.Bytes, Objects: s.n.Objects, Missing: s.n.Missing, Newest: s.n.Newest, HasNewest: s.n.HasNewest,
			Unreadable: s.n.Unreadable})
		f := ViewRow{Level: level + 1, Fold: true, Bytes: s.n.Bytes, Objects: s.n.Objects, Missing: s.n.Missing, More: s.n.Children - shown(s)}
		if s.n.Children < 0 {
			f.More = -1
		}
		var shownNewest int64
		shownTimed := false
		for _, k := range s.kids[:s.next] {
			walk(k, level+1)
			f.Bytes -= nodes[k].n.Bytes
			f.Objects -= nodes[k].n.Objects
			f.Missing -= nodes[k].n.Missing
			if kn := nodes[k].n; kn.HasNewest && (!shownTimed || kn.Newest > shownNewest) {
				shownNewest, shownTimed = kn.Newest, true
			}
		}
		if folds(s) > 0 {
			f.Newest, f.HasNewest = foldNewest(s, shownNewest, shownTimed)
			f.Unreadable = hiddenUnreadable(s)
			out = append(out, f)
		}
	}
	walk(0, 0)
	return out
}
