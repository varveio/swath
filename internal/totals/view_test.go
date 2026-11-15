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
// exactly the nodes of at least 1 % of it (and above 0), with their sums
// and children.
func checkView(t *testing.T, v *View, origin string, tree map[string]*exactNode) {
	t.Helper()
	total := tree[origin]
	if v.Total.Bytes != total.bytes || v.Total.Objects != total.objects || v.Total.Missing != total.missing || v.Total.Children != int64(len(total.kids))+total.self ||
		v.Total.HasNewest != total.timed || v.Total.Newest != total.newest {
		t.Fatalf("total %+v, want %+v (%d kids, %d self)", v.Total, *total, len(total.kids), total.self)
	}
	floor := (total.bytes + viewShare - 1) / viewShare
	var want []string
	for k, n := range tree {
		if k != origin && n.bytes > 0 && n.bytes >= floor {
			want = append(want, k)
		}
	}
	slices.Sort(want)
	var got []string
	for _, n := range v.Nodes {
		got = append(got, n.Key)
		e := tree[n.Key]
		if e == nil || n.Bytes != e.bytes || n.Objects != e.objects || n.Missing != e.missing || n.Children != int64(len(e.kids))+e.self ||
			n.HasNewest != e.timed || n.Newest != e.newest {
			t.Fatalf("node %+v, want %+v", n, e)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("nodes %q, want %q", got, want)
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
	var wg sync.WaitGroup
	for _, run := range runs {
		flushes := map[int]bool{}
		for range rng.IntN(3) {
			flushes[rng.IntN(len(run)+1)] = true
		}
		wg.Go(func() {
			r := t.ViewRun(ordered)
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
			r.End()
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
		feed(tl, runs, true, rng)
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
// viewShare nodes of a depth beyond those it holds partial: memory does not
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
	if len(v.kept) > viewShare*2 || len(v.open) > 10 {
		t.Fatalf("held %d remembered and %d partial nodes", len(v.kept), len(v.open))
	}
	if s := tl.View().Stats; s.Dropped < 199_000 || s.Partial == 0 || s.Runs != 2 {
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
		// Rebuild each parent's lines and check that they add up.
		type level struct {
			row                ViewRow
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
			}
			if !r.Fold {
				stack = append(stack, &level{row: r})
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
	if !slices.Equal(lv.Nodes, sv.Nodes) || lv.Total != sv.Total || len(lv.Nodes) != 2 || lv.Total.Children != 2001 {
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
