// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package totals

import (
	"cmp"
	"container/heap"
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
}

// add adds o's sums to s, its children included.
func (s *vsum) add(o *vsum) {
	s.include(o)
	s.children += o.children
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
	kept map[string]vsum   // complete nodes remembered: the viewKeep largest of their depth
	pins map[string]vsum   // complete nodes holding a hard link, kept to the end
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
	v := &view{origin: opt.Origin, primary: opt.Primary, budget: opt.MaxPrefixes, open: map[string]*onode{}, kept: map[string]vsum{}, pins: map[string]vsum{}}
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
			if !f.left && !ending {
				p.s.children++
			}
		}
		if f.left || ending {
			r.partial(f.name, f.depth, &f.s)
		} else {
			r.complete(f.name, f.depth, &f.s)
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
	deepest := &r.cache[prefixes-1].s
	deepest.children += c.children
	if leaf {
		deepest.children++
		r.complete(r.chain[len(r.chain)-1], len(r.chain)-1, c)
	}
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
		return ViewNode{Key: k, Depth: d, Bytes: n.bytes, Objects: n.objects, Missing: n.missing, Children: c, Newest: n.newest, HasNewest: n.hasNewest}
	}
	out.Total = node(v.origin, total)
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
			out.Nodes = append(out.Nodes, node(k, all[k]))
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
	// Newest and HasNewest are a node's (ViewNode); a fold line has none.
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
// rest too: the one whose child is the largest, or the fold whose bytes,
// spread over all the lines, would still be. So the largest nodes come first, a
// wide level of small ones is shown as far as it outweighs them, and the
// lines fill down to the nodes held.
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
	// folds is the fold lines s needs: one when it lists children (the
	// total always does) and some not shown hold bytes.
	folds := func(s *sel) int {
		if s.next == 0 && s != &nodes[0] {
			return 0
		}
		if (s.n.Children < 0 || shown(s) < s.n.Children) && s.bytes < s.n.Bytes {
			return 1
		}
		return 0
	}
	// joins reports whether s shares its line with its one child: a prefix
	// (not the total) with exactly one child, shown.
	joins := func(s *sel) bool { return s != &nodes[0] && s.n.Children == 1 && s.next == 1 }
	// What is not broken down yet: a shown node not expanded (its bytes)
	// or a fold (its parent's bytes not shown), each by the node it breaks
	// down. The next is the one whose next line is the largest, or the fold
	// whose bytes, were they spread over every line of the view, would still
	// make a larger line: a fold of many small children is broken down while
	// it outweighs what the other lines would show.
	type part struct {
		bytes int64
		i     int
		fold  bool
	}
	var parts []part
	if len(nodes[0].kids) > 0 {
		parts = append(parts, part{v.Total.Bytes, 0, false})
	}
	used := 1 + folds(&nodes[0])
	for len(parts) > 0 {
		best, worth := -1, int64(-1)
		for j, pt := range parts {
			s := &nodes[pt.i]
			w := nodes[s.kids[s.next]].n.Bytes
			if pt.fold {
				w = max(w, pt.bytes/int64(max(rows, 1)))
			}
			if w > worth {
				best, worth = j, w
			}
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
			parts = append(parts, part{nodes[c].n.Bytes, c, false})
		}
		if s.next < len(s.kids) {
			parts = append(parts, part{s.n.Bytes - s.bytes, pt.i, true})
		}
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
		out = append(out, ViewRow{Name: name, Level: level, Bytes: s.n.Bytes, Objects: s.n.Objects, Missing: s.n.Missing, Newest: s.n.Newest, HasNewest: s.n.HasNewest})
		f := ViewRow{Level: level + 1, Fold: true, Bytes: s.n.Bytes, Objects: s.n.Objects, Missing: s.n.Missing, More: s.n.Children - shown(s)}
		if s.n.Children < 0 {
			f.More = -1
		}
		for _, k := range s.kids[:s.next] {
			walk(k, level+1)
			f.Bytes -= nodes[k].n.Bytes
			f.Objects -= nodes[k].n.Objects
			f.Missing -= nodes[k].n.Missing
		}
		if folds(s) > 0 {
			out = append(out, f)
		}
	}
	walk(0, 0)
	return out
}
