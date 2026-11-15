// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package totals

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
)

// exactNode is a node of the brute-force tree.
type exactNode struct {
	bytes, objects, missing int64
	newest                  int64 // valid with timed
	timed                   bool
	kids                    map[string]bool
	self                    int64 // objects named as the prefix itself (or outside the origin)
	selfNewest              int64 // their newest time, with selfTimed
	selfTimed               bool
}

// exactTree is the view's nodes computed the plain way: every node of every
// key, its sums and its distinct children. linksOnce gives each inode's
// bytes to its smallest key.
func exactTree(origin string, rows []Row, linksOnce bool) map[string]*exactNode {
	first := map[inode]string{}
	if linksOnce {
		for _, r := range rows {
			if r.Object && r.Link {
				id := inode{r.Dev, r.Inode}
				if k, ok := first[id]; !ok || r.Key < k {
					first[id] = r.Key
				}
			}
		}
	}
	seen := map[inode]bool{}
	tree := map[string]*exactNode{}
	get := func(k string) *exactNode {
		n := tree[k]
		if n == nil {
			n = &exactNode{kids: map[string]bool{}}
			tree[k] = n
		}
		return n
	}
	get(origin)
	for _, r := range rows {
		var chain []string
		chain = append(chain, origin)
		leaf := false
		if strings.HasPrefix(r.Key, origin) {
			rest := r.Key[len(origin):]
			end := len(origin)
			for {
				i := strings.IndexByte(rest, '/')
				if i < 0 {
					if rest != "" && r.Object {
						chain = append(chain, r.Key)
						leaf = true
					}
					break
				}
				end += i + 1
				rest = rest[i+1:]
				chain = append(chain, r.Key[:end])
			}
		}
		for d, k := range chain {
			n := get(k)
			if d > 0 {
				get(chain[d-1]).kids[k] = true
			}
			_ = n
		}
		if !r.Object {
			continue
		}
		if !leaf {
			get(chain[len(chain)-1]).self++
		}
		var b, o, m int64 = r.Size, 1, 0
		if !r.HasSize {
			b, m = 0, 1
		}
		if linksOnce && r.Link {
			id := inode{r.Dev, r.Inode}
			if first[id] != r.Key || seen[id] {
				continue
			}
			seen[id] = true
		}
		if d := tree[chain[len(chain)-1]]; !leaf && r.HasMtime && (!d.selfTimed || r.Mtime > d.selfNewest) {
			d.selfNewest, d.selfTimed = r.Mtime, true
		}
		for _, k := range chain {
			n := tree[k]
			n.bytes += b
			n.objects += o
			n.missing += m
			if r.HasMtime && (!n.timed || r.Mtime > n.newest) {
				n.newest, n.timed = r.Mtime, true
			}
		}
	}
	return tree
}

// checkView compares a view with the brute-force tree: the same total, and
// nodes held with their exact sums, children and time, each under a parent
// held: at least every node among the viewKeep largest of its depth (ties
// aside) whose ancestors are, so every node of 1 % of the total.
func checkView(t *testing.T, v *View, origin string, tree map[string]*exactNode) {
	t.Helper()
	total := tree[origin]
	if v.Total.Bytes != total.bytes || v.Total.Objects != total.objects || v.Total.Missing != total.missing || v.Total.Children != int64(len(total.kids))+total.self ||
		v.Total.HasNewest != total.timed || v.Total.Newest != total.newest {
		t.Fatalf("total %+v, want %+v (%d kids, %d self)", v.Total, *total, len(total.kids), total.self)
	}
	depth := func(k string) int {
		d := strings.Count(k[len(origin):], "/")
		if k != origin && !strings.HasSuffix(k, "/") {
			d++
		}
		return d
	}
	// sure: a node no viewKeep others of its depth are as large as, and
	// its ancestors too.
	byDepth := map[int][]int64{}
	for k, n := range tree {
		if k != origin && n.bytes > 0 && strings.HasPrefix(k, origin) {
			byDepth[depth(k)] = append(byDepth[depth(k)], n.bytes)
		}
	}
	for _, bs := range byDepth {
		slices.Sort(bs)
	}
	top := func(k string) bool { // no viewKeep others of its depth as large
		bs := byDepth[depth(k)]
		i, _ := slices.BinarySearch(bs, tree[k].bytes)
		return len(bs)-i <= viewKeep // itself included
	}
	sure := map[string]bool{origin: true}
	var isSure func(k string) bool
	isSure = func(k string) bool {
		if v, ok := sure[k]; ok {
			return v
		}
		v := tree[k].bytes > 0 && top(k) && isSure(parentOf(k))
		sure[k] = v
		return v
	}
	got := map[string]bool{}
	for _, n := range v.Nodes {
		got[n.Key] = true
		e := tree[n.Key]
		if e == nil || n.Bytes != e.bytes || n.Objects != e.objects || n.Missing != e.missing || n.Children != int64(len(e.kids))+e.self ||
			n.HasNewest != e.timed || n.Newest != e.newest || n.Bytes == 0 || parentOf(n.Key) != origin && !got[parentOf(n.Key)] {
			t.Fatalf("node %+v, want %+v", n, e)
		}
	}
	floor := (total.bytes + 99) / 100
	for k, n := range tree {
		if k != origin && strings.HasPrefix(k, origin) && (isSure(k) || n.bytes > 0 && n.bytes >= floor) && !got[k] {
			t.Fatalf("node %q (%d bytes) not held", k, n.bytes)
		}
	}
}

// randomKeys is a sorted key set from a random tree: up to four levels of
// short names, some heavy objects, some keys ending in '/', some objects
// without a size; groups repeats keys (versions).
func randomKeys(rng *rand.Rand, n int, origin string, groups bool) []Row {
	seg := func() string { return string(rune('a' + rng.IntN(4))) }
	keys := map[string]bool{}
	for range n {
		k := origin
		for range rng.IntN(4) {
			k += seg() + "/"
		}
		if rng.IntN(10) > 0 {
			k += seg() + fmt.Sprint(rng.IntN(5))
		}
		if rng.IntN(50) == 0 {
			k = "z" + k // outside the origin, when there is one
		}
		keys[k] = true
	}
	var rows []Row
	for k := range keys {
		size := int64(rng.IntN(100))
		if rng.IntN(20) == 0 {
			size = int64(rng.IntN(1_000_000))
		}
		r := Row{Key: k, Object: true, Size: size, HasSize: rng.IntN(30) > 0, Mtime: rng.Int64N(1_000_000), HasMtime: rng.IntN(20) > 0}
		rows = append(rows, r)
		for groups && rng.IntN(4) == 0 {
			rows = append(rows, r)
		}
	}
	slices.SortStableFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	return rows
}

// feed runs the rows' runs through t's view concurrently, each run's rows in
// order and flushed at random points, as order.Count's emitters would.
func feed(t *Tally, runs [][]Row, ordered bool, rng *rand.Rand) {
	feedRuns(t, runs, ordered, false, rng)
}

// feedRuns is feed, each run told the interval it covers when covered, as
// the range engine's ranges are (order.CoveredRun): from the run before's
// last key, through its own (the last run: to the end).
func feedRuns(t *Tally, runs [][]Row, ordered, covered bool, rng *rand.Rand) {
	var wg sync.WaitGroup
	for n, run := range runs {
		flushes := map[int]bool{}
		for range rng.IntN(3) {
			flushes[rng.IntN(len(run)+1)] = true
		}
		lo := ""
		if n > 0 {
			lo = runs[n-1][len(runs[n-1])-1].Key
		}
		wg.Go(func() {
			r := t.ViewRun(ordered)
			if covered {
				r.From(lo)
				defer r.End()
				defer r.Through(run[len(run)-1].Key, n == len(runs)-1)
			}
			b := t.Batch()
			for i := range run {
				row := run[i]
				row.Key = strings.Clone(row.Key) // a view only until the next Flush
				b.Add(&row)
				r.Add(&row)
				if flushes[i] {
					r.Flush()
				}
			}
			if err := b.Flush(); err != nil {
				panic(err)
			}
			if !covered {
				r.End()
			}
		})
	}
	wg.Wait()
}

// TestViewExactOrdered: on random key sets cut into random contiguous runs
// fed concurrently, the view holds exactly the nodes of at least 1 % of
// the total, with the sums and children counts of the brute-force tree.
func TestViewExactOrdered(t *testing.T) {
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, 1))
		origin := []string{"", "logs/"}[seed%2]
		rows := randomKeys(rng, 1+rng.IntN(400), origin, seed%3 == 0)
		var runs [][]Row
		for len(rows) > 0 {
			n := 1 + rng.IntN(len(rows))
			if rng.IntN(3) > 0 {
				n = 1 + rng.IntN(min(len(rows), 8))
			}
			// A group of one key stays in one run, as the providers keep it.
			for n < len(rows) && rows[n].Key == rows[n-1].Key {
				n++
			}
			runs = append(runs, rows[:n])
			rows = rows[n:]
		}
		var all []Row
		for _, r := range runs {
			all = append(all, r...)
		}
		tl := New(Options{Origin: origin, View: true})
		feedRuns(tl, runs, true, seed%4 < 2, rng) // half the runs told their intervals
		t.Run(fmt.Sprint(seed), func(t *testing.T) { checkView(t, tl.View(), origin, exactTree(origin, all, false)) })
	}
}

// localTree cuts a local tree's rows as the local provider emits them in a
// count: each directory's entries, unsorted, in one or more chunks, with a
// directory row for each subdirectory in its parent's run.
func localTree(rng *rand.Rand, rows []Row) (byDir map[string][][]Row, all []Row) {
	entries := map[string][]Row{"": nil}
	dirs := map[string]bool{}
	for _, r := range rows {
		parent := r.Key[:strings.LastIndexByte(r.Key, '/')+1]
		entries[parent] = append(entries[parent], r)
		for p := parent; p != ""; p = p[:strings.LastIndexByte(p[:len(p)-1], '/')+1] {
			dirs[p] = true
		}
	}
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		parent := d[:strings.LastIndexByte(d[:len(d)-1], '/')+1]
		entries[parent] = append(entries[parent], Row{Key: d, Dir: true})
		entries[d] = append(entries[d], []Row{}...)
	}
	byDir = map[string][][]Row{}
	for _, dir := range slices.Sorted(maps.Keys(entries)) {
		e := entries[dir]
		rng.Shuffle(len(e), func(i, j int) { e[i], e[j] = e[j], e[i] })
		all = append(all, e...)
		byDir[dir] = [][]Row{}
		for len(e) > 0 {
			n := 1 + rng.IntN(len(e))
			byDir[dir] = append(byDir[dir], e[:n])
			e = e[n:]
		}
	}
	return byDir, all
}

// localRuns is localTree's runs, shuffled, without their directories.
func localRuns(rng *rand.Rand, rows []Row) (runs [][]Row, all []Row) {
	byDir, all := localTree(rng, rows)
	for _, dir := range slices.Sorted(maps.Keys(byDir)) {
		runs = append(runs, byDir[dir]...)
	}
	rng.Shuffle(len(runs), func(i, j int) { runs[i], runs[j] = runs[j], runs[i] })
	return runs, all
}

// feedLocal walks byDir as order.Count expands a local tree with eight
// workers: a directory's runs (concurrently), then its subdirectories'
// walks (concurrently, as their ranges are queued once the directory's
// returns), then the end of its subtree (order.SubtreeSink).
func feedLocal(t *Tally, byDir map[string][][]Row, rng *rand.Rand) {
	type job struct {
		dir  string
		seed uint64
	}
	workers := make(chan struct{}, 8)
	var walk func(j job)
	walk = func(j job) {
		rng := rand.New(rand.NewPCG(j.seed, 9))
		var subs []job
		for _, d := range slices.Sorted(maps.Keys(byDir)) {
			if d != j.dir && strings.HasPrefix(d, j.dir) && strings.Count(d[len(j.dir):], "/") == 1 {
				subs = append(subs, job{d, rng.Uint64()})
			}
		}
		workers <- struct{}{}
		feed(t, byDir[j.dir], false, rng)
		if len(subs) == 0 { // the worker settles a leaf before it takes other work
			t.ViewSubtreeEnd(j.dir)
			<-workers
			return
		}
		<-workers
		var wg sync.WaitGroup
		for _, sub := range subs {
			wg.Go(func() { walk(sub) })
		}
		wg.Wait()
		t.ViewSubtreeEnd(j.dir)
	}
	walk(job{"", rng.Uint64()})
}

// localKeys is a random local tree's files: no key ends in '/', and some
// files share an inode (hard links).
func localKeys(rng *rand.Rand, n int) []Row {
	var rows []Row
	for _, r := range randomKeys(rng, n, "", false) {
		if strings.HasSuffix(r.Key, "/") || strings.HasPrefix(r.Key, "z") {
			continue
		}
		if rng.IntN(8) == 0 {
			r.Link, r.Dev, r.Inode = true, 1, int64(rng.IntN(4))
			r.Size, r.HasSize = 1000*(r.Inode+1), true // one inode, one size
			if r.Inode == 3 {
				r.Size = 500_000
			}
		}
		rows = append(rows, r)
	}
	return rows
}

// TestViewExactUnordered: a local tree counted unsorted, directory by
// directory, each completed when its subtree ends, hard links counted once
// under their first name or under each: the view matches the brute-force
// tree.
func TestViewExactUnordered(t *testing.T) {
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, 2))
		rows := localKeys(rng, 1+rng.IntN(400))
		byDir, all := localTree(rng, rows)
		once := seed%2 == 0
		tl := New(Options{View: true, LinksOnce: once})
		feedLocal(tl, byDir, rng)
		t.Run(fmt.Sprint(seed), func(t *testing.T) { checkView(t, tl.View(), "", exactTree("", all, once)) })
	}
}

// TestViewLinksOrdered: hard links counted once in an ordered listing (a
// dataset of a local tree): the prefixes of the first name hold the bytes
// whatever their size when the run closed them.
func TestViewLinksOrdered(t *testing.T) {
	for seed := range uint64(200) {
		rng := rand.New(rand.NewPCG(seed, 3))
		rows := localKeys(rng, 1+rng.IntN(300))
		slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
		var runs [][]Row
		for rest := rows; len(rest) > 0; {
			n := 1 + rng.IntN(len(rest))
			runs = append(runs, rest[:n])
			rest = rest[n:]
		}
		tl := New(Options{View: true, LinksOnce: true})
		feed(tl, runs, true, rng)
		t.Run(fmt.Sprint(seed), func(t *testing.T) { checkView(t, tl.View(), "", exactTree("", rows, true)) })
	}
}

// TestViewMemory: a large flat listing in few runs remembers at most
// viewKeep nodes of a depth beyond those it holds partial: memory does not
// grow with the objects.
func TestViewMemory(t *testing.T) {
	var rows []Row
	for i := range 200_000 {
		rows = append(rows, Row{Key: fmt.Sprintf("d%03d/f%06d", i%500, i), Object: true, Size: int64(i % 1000), HasSize: true})
	}
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	tl := New(Options{View: true})
	feed(tl, [][]Row{rows[:100_000], rows[100_000:]}, true, rand.New(rand.NewPCG(1, 1)))
	v := tl.view
	if len(v.kept) > viewKeep*2 || len(v.open) > 10 {
		t.Fatalf("held %d remembered and %d partial nodes", len(v.kept), len(v.open))
	}
	if s := tl.View().Stats; s.Dropped+s.Pruned < 199_000 || s.Partial == 0 || s.Runs != 2 {
		t.Fatalf("stats %+v", s)
	}
}

// TestViewLowered: past MaxPrefixes nodes the view drops its deepest levels
// rather than fail: the levels kept stay exact, and the deepest kept does
// not claim a count of children.
func TestViewLowered(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	var rows []Row
	for i := range 3000 {
		rows = append(rows, Row{Key: fmt.Sprintf("%c/%04d/f", 'a'+i%3, i), Object: true, Size: int64(1 + rng.IntN(100)), HasSize: true})
	}
	runs, all := localRuns(rng, rows)
	tl := New(Options{View: true, MaxPrefixes: 100})
	feed(tl, runs, false, rng)
	v := tl.View()
	if !v.Lowered || v.MaxDepth != 1 || v.Stats.DepthKept != 1 {
		t.Fatalf("lowered %v to %d (%+v)", v.Lowered, v.MaxDepth, v.Stats)
	}
	tree := exactTree("", all, false)
	if v.Total.Bytes != tree[""].bytes || v.Total.Children != 3 {
		t.Fatalf("total %+v", v.Total)
	}
	for _, n := range v.Nodes {
		if n.Depth != 1 || n.Bytes != tree[n.Key].bytes || n.Children != -1 {
			t.Fatalf("node %+v", n)
		}
	}
	if len(v.Nodes) != 3 {
		t.Fatalf("nodes %+v", v.Nodes)
	}
}

// TestViewSelect: whatever the lines allowed, each parent that lists
// children lists its largest, by size, then one fold line with the number
// and the sums of the rest, so its lines add up to it; the lines fit.
func TestViewSelect(t *testing.T) {
	for seed := range uint64(300) {
		rng := rand.New(rand.NewPCG(seed, 4))
		rows := randomKeys(rng, 1+rng.IntN(300), "", false)
		tl := New(Options{View: true})
		feed(tl, [][]Row{rows}, true, rng)
		v := tl.View()
		limit := 2 + rng.IntN(30)
		lines := v.Select(limit)
		if len(lines) > limit {
			t.Fatalf("seed %d: %d lines over %d", seed, len(lines), limit)
		}
		// Rebuild each parent's lines and check that they add up, and that
		// a fold's time is the newest among the children not shown.
		tree := exactTree("", rows, false)
		type level struct {
			row                ViewRow
			key                string // the line's node (the last of a shared line)
			shown              map[string]bool
			bytes, objs, lines int64
			prev               int64
		}
		var stack []*level
		closeTo := func(depth int) {
			for len(stack) > depth {
				l := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if l.lines > 0 && (l.bytes != l.row.Bytes || l.objs > l.row.Objects) { // a fold of no bytes is left out, its objects with it
					t.Fatalf("seed %d: %q lists %d bytes %d objects, holds %d %d", seed, l.row.Name, l.bytes, l.objs, l.row.Bytes, l.row.Objects)
				}
			}
		}
		for i, r := range lines {
			if i == 0 != (r.Level == 0) || r.Level > len(stack) {
				t.Fatalf("seed %d: line %d %+v out of tree order", seed, i, r)
			}
			closeTo(r.Level)
			if r.Level > 0 {
				p := stack[r.Level-1]
				if p.lines > 0 && r.Bytes > p.prev && !r.Fold {
					t.Fatalf("seed %d: %q after a smaller sibling", seed, r.Name)
				}
				p.bytes += r.Bytes
				p.objs += r.Objects
				p.lines++
				p.prev = r.Bytes
				if r.Fold && (r.More < 1 || r.Bytes == 0 || i+1 < len(lines) && lines[i+1].Level == r.Level) {
					t.Fatalf("seed %d: fold %+v", seed, r)
				}
				if r.Fold {
					e := tree[p.key]
					at, timed := e.selfNewest, e.selfTimed
					for k := range e.kids {
						if n := tree[k]; !p.shown[k] && n.timed && (!timed || n.newest > at) {
							at, timed = n.newest, true
						}
					}
					if r.HasNewest != timed || timed && r.Newest != at {
						t.Fatalf("seed %d: fold under %q has newest %v %d, want %v %d", seed, p.key, r.HasNewest, r.Newest, timed, at)
					}
				} else {
					seg := r.Name
					if i := strings.IndexByte(seg, '/'); i >= 0 {
						seg = seg[:i+1]
					}
					p.shown[p.key+seg] = true
				}
			}
			if !r.Fold {
				key := ""
				if r.Level > 0 {
					key = stack[r.Level-1].key + r.Name
				}
				stack = append(stack, &level{row: r, key: key, shown: map[string]bool{}})
			}
		}
		closeTo(0)
		if v.Total.Children > 0 && len(lines) < 2 {
			t.Fatalf("seed %d: the total lists nothing", seed)
		}
	}
}

// heldNow is the nodes v holds that its budget bounds, counted afresh.
func heldNow(v *view) int {
	n := len(v.pins)
	for _, o := range v.open {
		if o.heavy {
			n++
		}
	}
	return n
}

// TestViewLocalBudget: a local tree completes each directory when its
// subtree ends, through the 1 % rule, so 2,000 small directories beside a
// large file fit a budget of 100 without dropping a level, and give the
// view of the same rows read in order (a dataset of the tree).
func TestViewLocalBudget(t *testing.T) {
	rows := []Row{{Key: "a/big", Object: true, Size: 1_000_000, HasSize: true}}
	for i := range 2000 {
		rows = append(rows, Row{Key: fmt.Sprintf("d%04d/f", i), Object: true, Size: 1, HasSize: true})
	}
	rng := rand.New(rand.NewPCG(5, 5))
	byDir, all := localTree(rng, rows)
	live := New(Options{View: true, MaxPrefixes: 100})
	feedLocal(live, byDir, rng)
	sorted := slices.Clone(all)
	slices.SortFunc(sorted, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	saved := New(Options{View: true, MaxPrefixes: 100})
	feed(saved, [][]Row{sorted}, true, rng)
	lv, sv := live.View(), saved.View()
	if lv.Lowered || sv.Lowered || len(live.view.open) != 1 || heldNow(live.view) != live.view.held {
		t.Fatalf("lowered %v %v, %d partial, held %d (counted %d)", lv.Lowered, sv.Lowered, len(live.view.open), live.view.held, heldNow(live.view))
	}
	if !sameNodes(lv.Nodes, sv.Nodes) || !sameNode(lv.Total, sv.Total) || len(lv.Nodes) != 2*viewKeep || lv.Total.Children != 2001 {
		t.Fatalf("live %+v %+v\nsaved %+v %+v", lv.Total, lv.Nodes, sv.Total, sv.Nodes)
	}
	checkView(t, lv, "", exactTree("", all, false))
}

// TestViewPinnedBudget: prefixes kept for their hard links count against
// the budget: a backup tree whose every file is a link into another tree
// (the other names outside the listing) drops levels rather than hold a
// node per directory.
func TestViewPinnedBudget(t *testing.T) {
	var rows []Row
	for i := range 1000 {
		rows = append(rows, Row{Key: fmt.Sprintf("snap/d%04d/f", i), Object: true, Size: 10, HasSize: true, Link: true, Dev: 1, Inode: int64(i)})
	}
	tl := New(Options{View: true, LinksOnce: true, MaxPrefixes: 10})
	var runs [][]Row
	for i := 0; i < len(rows); i += 7 {
		runs = append(runs, rows[i:min(i+7, len(rows))])
	}
	feed(tl, runs, true, rand.New(rand.NewPCG(3, 3)))
	v := tl.view
	if !v.lowered || heldNow(v) > v.budget || heldNow(v) != v.held {
		t.Fatalf("lowered %v, held %d (counted %d) of %d", v.lowered, v.held, heldNow(v), v.budget)
	}
	if got := tl.View(); got.Total.Bytes != 10_000 || got.Total.Objects != 1000 {
		t.Fatalf("total %+v", got.Total)
	}
}

// TestViewSelectFills: on a wide level (2,173 top-level prefixes, a few
// large, the small ones holding most of the bytes) the lines fill with the
// large prefixes and then the largest of the small ones, the level's rest
// in a fold, rather than one fold of them all.
func TestViewSelectFills(t *testing.T) {
	var rows []Row
	for i := range 2173 {
		size := int64(1000 + i)
		if i%500 == 0 {
			size = 300_000 // a few large; the small ones hold three quarters
		}
		for j := range 3 {
			rows = append(rows, Row{Key: fmt.Sprintf("p%04d/s%d/o", i, j), Object: true, Size: size / 3, HasSize: true})
		}
	}
	slices.SortFunc(rows, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	tl := New(Options{View: true})
	feed(tl, [][]Row{rows[:len(rows)/2], rows[len(rows)/2:]}, true, rand.New(rand.NewPCG(1, 2)))
	v := tl.View()
	lines := v.Select(25)
	top := 0
	for _, l := range lines {
		if l.Level == 1 && !l.Fold {
			top++
		}
	}
	if len(lines) != 25 || top < 20 || !lines[len(lines)-1].Fold {
		t.Fatalf("%d lines, %d top-level prefixes:\n%+v", len(lines), top, lines)
	}
	checkView(t, v, "", exactTree("", rows, false))
}

// TestViewCoveredRuns: runs told the intervals they cover complete their
// edge nodes, and the nodes cut where two runs meet once both have ended:
// 20,000 runs of three objects, each under its own prefix, fed in order
// by eight workers, hold a few partial nodes at any time, not one per run,
// and give the view of the same rows read as one run.
func TestViewCoveredRuns(t *testing.T) {
	var runs [][]Row
	for i := range 20_000 {
		var run []Row
		for j := range 3 {
			run = append(run, Row{Key: fmt.Sprintf("p%05d/%c", i, 'a'+j), Object: true, Size: int64(1 + i%97 + j), HasSize: true})
		}
		runs = append(runs, run)
	}
	tl := New(Options{View: true, MaxPrefixes: 10})
	v := tl.view
	peak := 0
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for n := range jobs {
				run := runs[n]
				r := tl.ViewRun(true)
				lo := ""
				if n > 0 {
					lo = runs[n-1][2].Key
				}
				r.From(lo)
				for i := range run {
					r.Add(&run[i])
				}
				r.Through(run[2].Key, n == len(runs)-1)
				r.End()
				v.mu.Lock()
				peak = max(peak, len(v.open))
				v.mu.Unlock()
			}
		})
	}
	for n := range runs {
		jobs <- n
	}
	close(jobs)
	wg.Wait()
	if len(v.open) != 1 || peak > 8*3 || v.lowered {
		t.Fatalf("%d partial nodes at the end, %d at most, lowered %v", len(v.open), peak, v.lowered)
	}
	var all []Row
	for _, r := range runs {
		all = append(all, r...)
	}
	one := New(Options{View: true, MaxPrefixes: 10})
	feed(one, [][]Row{all}, true, rand.New(rand.NewPCG(1, 1)))
	got, want := tl.View(), one.View()
	if !sameNode(got.Total, want.Total) || !sameNodes(got.Nodes, want.Nodes) {
		t.Fatalf("covered runs %+v %d nodes, one run %+v %d nodes", got.Total, len(got.Nodes), want.Total, len(want.Nodes))
	}
	checkView(t, got, "", exactTree("", all, false))
}

// sameNode compares two nodes' sums, children and time.
func sameNode(a, b ViewNode) bool {
	return a.Key == b.Key && a.Depth == b.Depth && a.Bytes == b.Bytes && a.Objects == b.Objects && a.Missing == b.Missing &&
		a.Children == b.Children && a.Newest == b.Newest && a.HasNewest == b.HasNewest
}

func sameNodes(a, b []ViewNode) bool { return slices.EqualFunc(a, b, sameNode) }

// TestViewCoveredEmptyRuns: intervals that hold no row (a sparse key space,
// or rows all filtered out) still meet their neighbors: 2,001 runs, every
// other one empty and inside the prefix its neighbors share, leave no
// partial node and one stretch once they have all ended.
func TestViewCoveredEmptyRuns(t *testing.T) {
	type cut struct {
		lo, hi string
		top    bool
		rows   []Row
	}
	var cuts []cut
	lo := ""
	for i := range 1000 {
		key := fmt.Sprintf("p%04d/a", i)
		cuts = append(cuts, cut{lo: lo, hi: key, rows: []Row{{Key: key, Object: true, Size: int64(i + 1), HasSize: true}}})
		cuts = append(cuts, cut{lo: key, hi: fmt.Sprintf("p%04d/z", i)}) // inside p<i>/, empty
		lo = fmt.Sprintf("p%04d/z", i)
	}
	cuts = append(cuts, cut{lo: lo, top: true})
	tl := New(Options{View: true})
	rng := rand.New(rand.NewPCG(4, 4))
	rng.Shuffle(len(cuts), func(i, j int) { cuts[i], cuts[j] = cuts[j], cuts[i] })
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for _, c := range cuts {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()
			r := tl.ViewRun(true)
			r.From(c.lo)
			for i := range c.rows {
				r.Add(&c.rows[i])
			}
			r.Flush()
			r.Through(c.hi, c.top)
			r.End()
		})
	}
	wg.Wait()
	v := tl.view
	if len(v.open) != 1 || len(v.byLo) != 1 {
		t.Fatalf("%d partial nodes, %d stretches", len(v.open), len(v.byLo))
	}
	var all []Row
	for _, c := range cuts {
		all = append(all, c.rows...)
	}
	slices.SortFunc(all, func(a, b Row) int { return strings.Compare(a.Key, b.Key) })
	checkView(t, tl.View(), "", exactTree("", all, false))
}
