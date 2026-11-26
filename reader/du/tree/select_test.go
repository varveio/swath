// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// rowsOf is rows one per line: level, name (a fold's "+More"), bytes,
// the newest date and "!" when unreadable.
func rowsOf(rows []Row) string {
	var b strings.Builder
	for _, r := range rows {
		name := r.Name
		if r.Fold {
			name = fmt.Sprintf("+%d", r.More)
		}
		date := "-"
		if r.Newest != nil {
			date = r.Newest.UTC().Format(time.DateOnly)
		}
		mark := ""
		if r.Unreadable {
			mark = " !"
		}
		fmt.Fprintf(&b, "%d %s %d %s%s\n", r.Level, name, r.Bytes, date, mark)
	}
	return b.String()
}

func day(d int) *time.Time {
	t := time.Date(2026, 1, d, 0, 0, 0, 0, time.UTC)
	return &t
}

// TestSelectPlacement: under an origin that is no prefix of its own
// (logs/2026/0), a depth-1 node is the total's child; a deeper node whose
// parent is not given, a key outside the origin, the origin itself and a
// repeated key are left out; the unreadable directories need not be
// sorted; and a prefix with one child shares its row.
func TestSelectPlacement(t *testing.T) {
	tr := &Tree{
		Total: Node{Key: "logs/2026/0", Bytes: 1000, Objects: 4, Children: 3, Newest: day(9), Unreadable: true},
		Nodes: []Node{
			{Key: "logs/2026/02/", Bytes: 400, Objects: 1, Children: 1, Newest: day(2), Unreadable: true},
			{Key: "logs/2026/01/", Bytes: 500, Objects: 2, Children: 1, Newest: day(9)},
			{Key: "logs/2026/01/a/", Bytes: 500, Objects: 2, Children: 2, Newest: day(9)},
			{Key: "logs/2026/01/a/x.bin", Bytes: 300, Objects: 1, Children: 0, Newest: day(9)},
			{Key: "logs/2026/01/a/y.bin", Bytes: 200, Objects: 1, Children: 0, Newest: day(3)},
			{Key: "logs/2026/01/", Bytes: 999, Objects: 9, Children: 9},  // repeated
			{Key: "logs/2026/03/b/c.bin", Bytes: 90, Objects: 1},         // its parent not given
			{Key: "logs/2026/0", Bytes: 1000},                            // the origin
			{Key: "other/z.bin", Bytes: 800, Objects: 1, Newest: day(1)}, // outside
		},
		Unreadable: []string{"logs/2026/02/q/", "logs/2026/02/"},
	}
	got := rowsOf(Select(tr, 20))
	want := "" +
		"0 logs/2026/0 1000 2026-01-09 !\n" +
		"1 01/a/ 500 2026-01-09\n" +
		"2 x.bin 300 2026-01-09\n" +
		"2 y.bin 200 2026-01-03\n" +
		"1 02/ 400 2026-01-02 !\n" +
		"1 +1 100 -\n"
	if got != want {
		t.Errorf("rows:\n%s\nwant\n%s", got, want)
	}
}

// TestSelectFolds: a fold carries the rest of its parent's sums and its
// count; its date is the parent's newest when no row shown has it, else
// the first of the parent's Recent children not shown, and unknown
// without Recent. An unknown count of children folds as -1, and a fold
// that holds an unreadable directory is marked.
func TestSelectFolds(t *testing.T) {
	nodes := []Node{
		{Key: "a/", Bytes: 600, Objects: 1, Children: 1, Newest: day(9)},
		{Key: "b/", Bytes: 300, Objects: 1, Children: 1, Newest: day(5), Unreadable: true},
	}
	total := Node{Key: "", Bytes: 1000, Objects: 5, Children: 4, Newest: day(9), Unreadable: true}
	recent := []Child{{"a/", *day(9)}, {"c/", *day(7)}, {"b/", *day(5)}}
	for _, c := range []struct {
		name   string
		total  Node
		rows   int
		unread []string
		want   string
	}{
		{"recent", Node{Key: "", Bytes: 1000, Objects: 5, Children: 4, Newest: day(9), Recent: recent}, 20, nil,
			"0  1000 2026-01-09\n1 a/ 600 2026-01-09\n1 b/ 300 2026-01-05 !\n1 +2 100 2026-01-07\n"},
		{"no recent", total, 20, nil,
			"0  1000 2026-01-09 !\n1 a/ 600 2026-01-09\n1 b/ 300 2026-01-05 !\n1 +2 100 -\n"},
		{"unknown count", Node{Key: "", Bytes: 1000, Objects: 5, Children: -1, Newest: day(9)}, 20, nil,
			"0  1000 2026-01-09\n1 a/ 600 2026-01-09\n1 b/ 300 2026-01-05 !\n1 +-1 100 -\n"},
		{"newest folded", total, 3, []string{"b/"},
			"0  1000 2026-01-09 !\n1 a/ 600 2026-01-09\n1 +3 400 - !\n"},
	} {
		got := rowsOf(Select(&Tree{Total: c.total, Nodes: nodes, Unreadable: c.unread}, c.rows))
		if got != c.want {
			t.Errorf("%s:\n%s\nwant\n%s", c.name, got, c.want)
		}
	}
}
