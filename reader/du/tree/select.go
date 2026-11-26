// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

// floor is the share of the total, as a divisor, below which Select
// spends no row on a node: 0.5 %.
const floor = 200

// Tree is the totals Select chooses rows from: the total and the nodes
// under it, which add up (the package's documentation, Totals): a node's
// children hold no more than it.
type Tree struct {
	// Total is the whole: its Key is the origin, the key prefix the totals
	// are under ("" for a whole bucket or tree, "logs/" or "logs/2026/0").
	Total Node
	// Nodes are the prefixes and objects under the origin, in any order,
	// each key once. A node at depth 1 (its key's parent prefix no longer
	// than the origin) is the total's child; a deeper one is left out
	// unless its parent prefix is a node too, and so is a key not under the
	// origin or repeated.
	Nodes []Node
	// Unreadable is the directories that could not be read, by key: a
	// node's Unreadable says one is at or below it, and Select marks a
	// fold that holds one.
	Unreadable []string
}

// Node is a prefix's or an object's totals. Its Key is the origin's for
// the total; under it, a prefix's key ends in "/" ("logs/2026/") and an
// object's does not ("logs/2026/a.bin").
type Node struct {
	Key string
	// Bytes, Objects and Missing are the sums under the node: its bytes
	// (one figure, size or space on disk), objects (a local tree's files)
	// and the objects without a value for the bytes.
	Bytes, Objects, Missing int64
	// Children is how many prefixes and objects are directly under the
	// node, -1 when unknown; those not in Nodes are folded.
	Children int64
	// Newest is the newest modification under the node, nil when unknown.
	Newest *time.Time
	// Unreadable: a directory at or below the node could not be read, so
	// its sums leave out what that held.
	Unreadable bool
	// Recent is the newest times of the node's children, the newest first,
	// as many as the caller keeps: when a child Select shows holds the
	// node's Newest, a fold's date is the first of them not shown, unknown
	// past them. A caller with every child in Nodes may leave it empty and
	// see such a fold undated.
	Recent []Child
}

// Child is a child's key and its newest modification (Node.Recent).
type Child struct {
	Key    string
	Newest time.Time
}

// Row is a row of the view (Select): a node, or the fold of a parent's
// children not shown (Fold), which carries their sums.
type Row struct {
	// Name is the node's key below its parent's ("logs/", "a.bin"); the
	// total's is its key, the origin. A prefix whose one child is shown
	// shares its row: "api/2025/".
	Name string
	// Level is the row's level in the view: the total's 0, a row's
	// children's one more.
	Level                   int
	Bytes, Objects, Missing int64
	// Unreadable: a directory at or below the row's node (a fold: below a
	// child it holds) could not be read.
	Unreadable bool
	// Newest is a node's, or a fold's the newest of what it holds (unknown,
	// in a rare view, when the newest children of its parent that Recent
	// holds are all shown); nil when unknown.
	Newest *time.Time
	// Fold: the rest of the parent's children, More of them (-1: an
	// unknown number), Bytes, Objects and Missing their sums.
	Fold bool
	More int64
}

// parentOf is a node's parent prefix (a key below the origin).
func parentOf(name string) string {
	return name[:strings.LastIndexByte(name[:len(name)-1], '/')+1]
}

// The idea of showing a tree of the largest entries with their share of
// the total is dust's and pdu's (github.com/bootandy/dust,
// github.com/KSXGitHub/parallel-disk-usage, both Apache-2.0); Select is
// written from their documented behavior, not their code.

// Select chooses the view's rows of t, at most rows (the total and every
// fold row included, at least the total and its fold), from the largest
// down. The rows show, besides the total, nodes and the folds of the nodes
// shown with children: a fold holds a parent's children not shown, how
// many and their sums, so each level adds up to its parent (a fold of no
// bytes is left out). Row by row, what is not broken down yet (a fold, or
// a node shown whole) gives its largest child, a node a fold for the rest
// too: the one whose child is the largest. So the largest nodes come
// first and the largest prefixes open, down to a floor (0.5 % of the
// total, more on a short terminal): a long tail of small prefixes stays in
// its fold, and the view ends when nothing above the floor is left.
// Unlike dust, which shows the largest nodes of the whole tree whatever
// their parents, and pdu, which hides what is below 1 % without saying
// so. A prefix with one child shares its row with it (it holds the same
// bytes). The rows come in tree order: a parent first, its children by
// size, the larger first, then by key. Select does not change t.
func Select(t *Tree, rows int) []Row {
	type sel struct {
		n     *Node
		kids  []int // its children held, by size, the largest first
		next  int   // kids[:next] are shown
		bytes int64 // theirs
	}
	unreadable := t.Unreadable
	if !slices.IsSorted(unreadable) {
		unreadable = slices.Sorted(slices.Values(unreadable))
	}
	origin := t.Total.Key
	nodes := make([]sel, 1, len(t.Nodes)+1)
	nodes[0] = sel{n: &t.Total}
	index := map[string]int{origin: 0}
	for i := range t.Nodes {
		k := t.Nodes[i].Key
		if _, ok := index[k]; ok || len(k) <= len(origin) || !strings.HasPrefix(k, origin) {
			continue
		}
		index[k] = len(nodes)
		nodes = append(nodes, sel{n: &t.Nodes[i]})
	}
	bigger := func(a, b int) int {
		x, y := nodes[a].n, nodes[b].n
		if c := cmp.Compare(y.Bytes, x.Bytes); c != 0 {
			return c
		}
		return cmp.Compare(x.Key, y.Key)
	}
	for i := 1; i < len(nodes); i++ {
		parent := parentOf(nodes[i].n.Key)
		p, ok := index[parent]
		switch {
		case len(parent) <= len(origin):
			p = 0 // at depth 1, the origin's child
		case !ok:
			continue // under a node not held
		}
		nodes[p].kids = append(nodes[p].kids, i)
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
		i, _ := slices.BinarySearch(unreadable, s.n.Key)
		for ; i < len(unreadable) && strings.HasPrefix(unreadable[i], s.n.Key); i++ {
			u := unreadable[i]
			if u == s.n.Key {
				continue // s itself: its row is marked
			}
			j := strings.IndexByte(u[len(s.n.Key):], '/')
			if !shownKey[u[:len(s.n.Key)+j+1]] {
				return true
			}
		}
		return false
	}
	// folds is the fold rows s needs: one when it lists children (the
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
	// joins reports whether s shares its row with its one child: a prefix
	// (not the total) with exactly one child, shown.
	joins := func(s *sel) bool { return s != &nodes[0] && s.n.Children == 1 && s.next == 1 }
	// What is not broken down yet: a shown node not expanded or a fold, by
	// the node it breaks down; the next is the one whose next child is the
	// largest, so the largest prefixes shown open first. No row goes to a
	// node under the floor: a share of the total that one row of the view
	// would barely show (floor, or a quarter of a row's share of the
	// rows), and once none is left above it the view ends, shorter.
	least := max(t.Total.Bytes/floor, t.Total.Bytes/4/int64(max(rows, 1))) // not 4*rows: it can overflow
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
			if w := nodes[s.kids[s.next]].n.Bytes; w >= least && w > worth {
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
			cost = 0 // on its parent's row
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
	// children (Recent) not shown; unknown when those are all shown.
	foldNewest := func(s *sel, shownNewest *time.Time) *time.Time {
		if s.n.Newest == nil {
			return nil
		}
		if shownNewest == nil || s.n.Newest.After(*shownNewest) {
			return s.n.Newest
		}
		names := map[string]bool{}
		for _, i := range s.kids[:s.next] {
			names[nodes[i].n.Key] = true
		}
		for _, k := range s.n.Recent {
			if !names[k.Key] {
				at := k.Newest
				return &at
			}
		}
		return nil
	}
	var out []Row
	var walk func(i, level int)
	walk = func(i, level int) {
		s := &nodes[i]
		name := s.n.Key
		if i != 0 {
			name = name[len(parentOf(name)):]
		}
		// A shared row gives the first prefix's sums: they hold its one
		// child's and its own blocks (a directory's, under the allocated
		// bytes), so the rows under its parent still add up to it.
		top := s
		for joins(s) {
			s = &nodes[s.kids[0]]
			name += s.n.Key[len(parentOf(s.n.Key)):]
		}
		out = append(out, Row{Name: name, Level: level, Bytes: top.n.Bytes, Objects: top.n.Objects, Missing: top.n.Missing, Newest: s.n.Newest,
			Unreadable: top.n.Unreadable || s.n.Unreadable})
		// The fold is the rest of the row's sums: the shared row's, its
		// joined prefixes' own blocks too.
		f := Row{Level: level + 1, Fold: true, Bytes: top.n.Bytes, Objects: top.n.Objects, Missing: top.n.Missing, More: s.n.Children - shown(s)}
		if s.n.Children < 0 {
			f.More = -1
		}
		var shownNewest *time.Time
		for _, k := range s.kids[:s.next] {
			walk(k, level+1)
			f.Bytes -= nodes[k].n.Bytes
			f.Objects -= nodes[k].n.Objects
			f.Missing -= nodes[k].n.Missing
			if kn := nodes[k].n; kn.Newest != nil && (shownNewest == nil || kn.Newest.After(*shownNewest)) {
				shownNewest = kn.Newest
			}
		}
		if folds(s) > 0 {
			f.Newest = foldNewest(s, shownNewest)
			f.Unreadable = hiddenUnreadable(s)
			out = append(out, f)
		}
	}
	walk(0, 0)
	return out
}
