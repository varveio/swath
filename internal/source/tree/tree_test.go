// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// memTree is an unordered-mode reader over an in-memory tree: a directory
// is its key prefix, its entries are read whole, keyed, sorted, and the
// directory's rows above chunk are handed out as segments of that size.
type memTree struct {
	dirs     map[string][]string // directory key -> names, a directory's ending in "/"
	chunk    int                 // 0: no segments
	disposed int
}

type seg struct{ rows []schema.Row[int64] }

func (m *memTree) Read(ctx context.Context, r Range[string, *seg], out *Out[string, *seg, int64]) error {
	walk := func(row *schema.Row[int64]) (string, Walk) {
		switch {
		case !strings.HasSuffix(row.Key, "/"):
			return "", Leaf
		case strings.HasSuffix(row.Key, "pruned/"):
			return "", Prune
		case strings.HasSuffix(row.Key, "omitted/"):
			return "", Omit
		}
		return row.Key, Descend
	}
	if r.InSeg {
		return out.Rows(r.Seg.rows, walk)
	}
	var rows []schema.Row[int64]
	for _, n := range m.dirs[r.Dir] {
		name, dir := strings.CutSuffix(n, "/")
		rows = append(rows, schema.Row[int64]{Key: Key(r.Prefix, name, dir), Data: int64(len(name))})
	}
	SortRows(rows)
	if m.chunk == 0 || len(rows) <= m.chunk {
		return out.Rows(rows, walk)
	}
	for i := 0; i < len(rows); i += m.chunk {
		if err := out.Seg(&seg{rows: rows[i:min(i+m.chunk, len(rows))]}); err != nil {
			return err
		}
	}
	return nil
}

func (m *memTree) Dispose(*seg) { m.disposed++ }

// drain expands the tree depth first in emission order, as the order stage
// places children, and returns the keys in output order.
func drain[D, S, P any](t *testing.T, p source.Provider[Range[D, S], P]) []string {
	t.Helper()
	var keys []string
	var walk func(r Range[D, S])
	walk = func(r Range[D, S]) {
		var items []any
		e := &recorder[D, S, P]{items: &items}
		if err := p.Expand(context.Background(), r, e); err != nil {
			t.Fatal(err)
		}
		for _, it := range items {
			switch v := it.(type) {
			case string:
				keys = append(keys, v)
			case Range[D, S]:
				walk(v)
			}
		}
	}
	walk(p.Root())
	return keys
}

type recorder[D, S, P any] struct{ items *[]any }

func (r *recorder[D, S, P]) Rows(rows []schema.Row[P]) error {
	for _, row := range rows {
		*r.items = append(*r.items, row.Key)
	}
	return nil
}

func (r *recorder[D, S, P]) Child(c Range[D, S]) error {
	*r.items = append(*r.items, c)
	return nil
}

func fixture() map[string][]string {
	return map[string][]string{
		"":         {"a0", "a/", "a-b", "a.b", "a.b/", "z", "pruned/", "omitted/", "empty/"},
		"a/":       {"w", "x/", "x-y"},
		"a/x/":     {"deep"},
		"a.b/":     {"f"},
		"empty/":   nil,
		"pruned/":  {"never"},
		"omitted/": {"never"},
	}
}

// TestWalkerGlobalOrder: the walker's output, a directory's row then its
// subtree before the next sibling, is the byte-sorted key set, for any
// chunk size; pruned and omitted directories behave as their Walk says.
func TestWalkerGlobalOrder(t *testing.T) {
	want := []string{"a-b", "a.b", "a.b/", "a.b/f", "a/", "a/w", "a/x-y", "a/x/", "a/x/deep", "a0", "empty/", "pruned/", "z"}
	if !slices.IsSorted(want) {
		t.Fatal("oracle not sorted")
	}
	for _, chunk := range []int{0, 1, 2, 3} {
		m := &memTree{dirs: fixture(), chunk: chunk}
		w := NewWalker(Range[string, *seg]{}, m, Options[int64]{Size: func(n *int64) int64 { return *n }})
		got := drain(t, source.Provider[Range[string, *seg], int64](w))
		if !slices.Equal(got, want) {
			t.Fatalf("chunk %d:\n got %q\nwant %q", chunk, got, want)
		}
		st := w.Stats()
		if st.Files != 8 || st.Dirs != 5 || st.Pruned != 1 || st.Omitted != 1 {
			t.Fatalf("chunk %d: stats %+v", chunk, st)
		}
		if chunk > 0 && st.Segments == 0 {
			t.Fatalf("chunk %d: no segments", chunk)
		}
	}
}

// TestWalkerNoDirs: directories are walked, their rows left out.
func TestWalkerNoDirs(t *testing.T) {
	m := &memTree{dirs: fixture()}
	w := NewWalker(Range[string, *seg]{}, m, Options[int64]{NoDirs: true})
	got := drain(t, source.Provider[Range[string, *seg], int64](w))
	for _, k := range got {
		if strings.HasSuffix(k, "/") {
			t.Fatalf("directory row %q with NoDirs: %q", k, got)
		}
	}
	if !slices.Contains(got, "a/x/deep") || w.Stats().Dirs != 5 {
		t.Fatalf("got %q, stats %+v", got, w.Stats())
	}
}

// TestWalkerCancelDisposes: a segment taken after the run was cancelled is
// not read; the walker releases it, as the order stage releases queued ones.
func TestWalkerCancelDisposes(t *testing.T) {
	m := &memTree{dirs: map[string][]string{"": {"a", "b", "c"}}, chunk: 1}
	w := NewWalker(Range[string, *seg]{}, m, Options[int64]{})
	var items []any
	if err := w.Expand(context.Background(), w.Root(), &recorder[string, *seg, int64]{items: &items}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	segs := 0
	for _, it := range items {
		if r, ok := it.(Range[string, *seg]); ok {
			segs++
			if err := w.Expand(ctx, r, &recorder[string, *seg, int64]{items: new([]any)}); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled segment: %v", err)
			}
		}
	}
	w.Dispose(Range[string, *seg]{Seg: &seg{}, InSeg: true})
	w.Dispose(Range[string, *seg]{}) // a directory holds nothing
	if segs != 3 || m.disposed != 4 {
		t.Fatalf("%d segments, %d disposed", segs, m.disposed)
	}
}

// TestKeyOrder pins the order the package doc states.
func TestKeyOrder(t *testing.T) {
	keys := []string{Key("", "a0", false), Key("", "a", true), Key("", "a.b", false), Key("", "a-b", false), Key("a/", "x", false)}
	rows := make([]schema.Row[int], len(keys))
	for i, k := range keys {
		rows[i].Key = k
	}
	SortRows(rows)
	got := slices.Collect(func(yield func(string) bool) {
		for _, r := range rows {
			if !yield(r.Key) {
				return
			}
		}
	})
	if want := []string{"a-b", "a.b", "a/", "a/x", "a0"}; !slices.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Recursive excludes suppress children in whole directories and segments.
// Reader-owned omissions and device boundaries must win even if the
// predicate would match them; leaf rows must never be offered to it.
func TestWalkerRecursiveExcludes(t *testing.T) {
	for _, chunk := range []int{0, 1, 3} {
		for _, noDirs := range []bool{false, true} {
			m := &memTree{dirs: fixture(), chunk: chunk}
			w := NewWalker(Range[string, *seg]{}, m, Options[int64]{NoDirs: noDirs})
			w.SetPruneDirectories(func(key string) bool {
				if !strings.HasSuffix(key, "/") || key == "omitted/" || key == "pruned/" {
					t.Fatalf("predicate saw reader-owned decision for %q", key)
				}
				return key == "a/"
			})
			want := []string{"a-b", "a.b", "a.b/", "a.b/f", "a/", "a0", "empty/", "pruned/", "z"}
			if noDirs {
				want = slices.DeleteFunc(want, func(k string) bool { return strings.HasSuffix(k, "/") })
			}
			got := drain(t, source.Provider[Range[string, *seg], int64](w))
			if !slices.Equal(got, want) {
				t.Fatalf("chunk=%d noDirs=%v: got %q, want %q", chunk, noDirs, got, want)
			}
			if st := w.Stats(); st.Excluded != 1 || st.Pruned != 1 || st.Omitted != 1 || st.Dirs != 4 || st.Files != 5 {
				t.Fatalf("chunk=%d noDirs=%v: %+v", chunk, noDirs, st)
			}
		}
	}
}
