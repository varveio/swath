// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/keyrange"
)

// obj is the test payload: the entry's name length, so rows can be checked
// to carry their own entry's data.
type obj struct{ N int64 }

var testSchema = schema.Schema[obj]{
	Info:    schema.Info{Name: "tree_test", ID: "swath/tree-test/v1", Key: schema.String, MaxKeyBytes: 4096},
	Columns: []schema.Column[obj]{{Spec: schema.Spec{Name: "n", Type: schema.Int64}, Int: func(o *obj) (int64, bool) { return o.N, true }}},
	Heap:    func(*obj) int64 { return 0 },
	Bytes:   func(*obj) int64 { return 8 },
}

// hns is a fake hierarchical namespace: directories hold names; a
// directory's listing is in byte order of names (the Lister contract),
// after a name, by position tokens bound to their query. Every directory on
// a path exists (as on Azure HNS), empty directories too.
type hns struct {
	dirs     map[string][]string // directory key -> sorted names, a directory's ending in "/"
	requests atomic.Int64
	lookups  atomic.Int64
	// fail, when set, is asked before every request.
	fail func(n int64) error
}

// newHNS builds a namespace of keys: a key ending in '/' is a directory
// (empty unless other keys lie under it), any other a file; parents exist.
func newHNS(keys []string) *hns {
	h := &hns{dirs: map[string][]string{"": nil}}
	seen := map[string]bool{}
	add := func(dir, name string) {
		if !seen[dir+"\x00"+name] {
			seen[dir+"\x00"+name] = true
			h.dirs[dir] = append(h.dirs[dir], name)
		}
	}
	for _, k := range keys {
		parts := strings.Split(strings.TrimSuffix(k, "/"), "/")
		dir := ""
		for i, p := range parts {
			if i < len(parts)-1 || strings.HasSuffix(k, "/") {
				add(dir, p+"/")
				dir += p + "/"
				if _, ok := h.dirs[dir]; !ok {
					h.dirs[dir] = nil
				}
				continue
			}
			add(dir, p)
		}
	}
	for d, names := range h.dirs {
		for _, n := range names {
			if strings.HasSuffix(n, "/") && seen[d+"\x00"+strings.TrimSuffix(n, "/")] {
				panic("a file and a directory named " + d + n)
			}
		}
		slices.SortFunc(h.dirs[d], func(a, b string) int { return strings.Compare(strings.TrimSuffix(a, "/"), strings.TrimSuffix(b, "/")) })
	}
	return h
}

// oracle is every key of the namespace, sorted: the walk's expected output.
func (h *hns) oracle() []string {
	var out []string
	for d, names := range h.dirs {
		for _, n := range names {
			out = append(out, d+n)
		}
	}
	slices.Sort(out)
	return out
}

func entryOf(n string) Entry[obj] {
	name, dir := strings.CutSuffix(n, "/")
	return Entry[obj]{Name: name, Dir: dir, Data: obj{N: int64(len(name))}}
}

func (h *hns) List(ctx context.Context, q *DirQuery) (*DirPage[obj], error) {
	n := h.requests.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.fail != nil {
		if err := h.fail(n); err != nil {
			return nil, err
		}
	}
	names := h.dirs[q.Dir]
	pos := sort.Search(len(names), func(i int) bool { return strings.TrimSuffix(names[i], "/") > q.After })
	if q.Token != "" {
		i := strings.LastIndexByte(q.Token, '@') // the bound may hold '@' (an engine pivot)
		bind, p, ok := q.Token[:max(i, 0)], q.Token[i+1:], i >= 0
		if !ok || bind != q.Dir+"\x00"+q.After {
			return nil, fmt.Errorf("token %q resent with another query", q.Token)
		}
		pos, _ = strconv.Atoi(p)
	}
	end := min(len(names), pos+max(q.Max, 1))
	pg := &DirPage[obj]{}
	for _, nm := range names[pos:end] {
		pg.Entries = append(pg.Entries, entryOf(nm))
	}
	if end < len(names) {
		pg.Truncated, pg.Token = true, q.Dir+"\x00"+q.After+"@"+strconv.Itoa(end)
	}
	return pg, nil
}

func (h *hns) Lookup(ctx context.Context, dir, name string) (Entry[obj], bool, error) {
	h.lookups.Add(1)
	if slices.Contains(h.dirs[dir], name+"/") {
		return entryOf(name + "/"), true, nil
	}
	return Entry[obj]{}, false, nil
}

type collect struct {
	mu   sync.Mutex
	keys []string
	bad  []string
}

func (c *collect) Write(_ context.Context, segs [][]schema.Row[obj]) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, s := range segs {
		for _, r := range s {
			name := strings.TrimSuffix(r.Key, "/")
			name = name[strings.LastIndexByte(name, '/')+1:]
			if r.Data.N != int64(len(name)) {
				c.bad = append(c.bad, r.Key)
			}
			c.keys = append(c.keys, r.Key)
		}
	}
	return nil
}

// walkOrdered lists h through the ordered mode on the order stage.
func walkOrdered(t *testing.T, h *hns, workers, page int, split bool) (*collect, *Walker[struct{}, keyrange.Range, obj], *Ordered[obj], error) {
	t.Helper()
	eo := keyrange.Options{Workers: workers, PageSize: page}
	if split {
		eo.SplitCoin = func() bool { return true }
	}
	o, err := NewOrdered[obj](h, eo, keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWalker[struct{}, keyrange.Range, obj](OrderedRange{}, o, Options[obj]{})
	c := &collect{}
	_, err = order.Run[OrderedRange, obj](context.Background(), w, c, &testSchema, order.Options{Workers: workers, Budget: 1 << 24})
	return c, w, o, err
}

func counter(o *Ordered[obj], key string) int64 {
	for i, d := range OrderedCounters {
		if d.Key == key {
			return o.Engine().Set().Load(o.base + counters.ID(i))
		}
	}
	panic(key)
}

// siblings around '/' and the byte-order edges of UTF-16 names (the
// Lister's contract is byte order: Azure's adapter converts).
var siblingKeys = []string{
	"a/x", "a-b", "a-c/y", "a.b", "a0", "a!b", "a!/z", "a/", "a/b/", "a/b-c", "a/b/c", "b/", "c",
	"\ue000/q", "\U00010000", "\U00010000-x/", "ｚ", "é/é", "e", "e /f/g",
}

func manyKeys(n int) []string {
	keys := []string{"big/part/inner", "big/part-01000/x", "big/part-02500/", "big/part.dir/y", "big/zz/"}
	for i := range n {
		keys = append(keys, fmt.Sprintf("big/part-%05d.parquet", i))
	}
	return keys
}

func deepKeys(depth int) []string {
	var keys []string
	p := ""
	for i := range depth {
		p += fmt.Sprintf("d%d/", i)
		keys = append(keys, p+"f", p+"f-", p)
	}
	return append(keys, p+"empty/")
}

// TestOrderedEqualsOracle: the ordered mode's output is the namespace's
// sorted key set at any worker count and page size, splits forced or not:
// siblings around '/', directories that sort behind their siblings by key,
// UTF-8 edges, a deep tree, empty directories, and a 5,000-file directory
// whose pivots have directory candidates (part before part-0015).
func TestOrderedEqualsOracle(t *testing.T) {
	cases := map[string][]string{"siblings": siblingKeys, "deep": deepKeys(40), "huge": manyKeys(5000)}
	for name, keys := range cases {
		h := newHNS(keys)
		want := h.oracle()
		for _, workers := range []int{1, 3, 16} {
			for _, page := range []int{1, 2, 7, 1000} {
				if name == "huge" && page < 7 {
					continue
				}
				for _, split := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/w%d/p%d/split=%v", name, workers, page, split), func(t *testing.T) {
						c, w, o, err := walkOrdered(t, h, workers, page, split)
						if err != nil {
							t.Fatal(err)
						}
						if !slices.Equal(c.keys, want) {
							t.Fatalf("got %d keys, want %d\n got %q\nwant %q", len(c.keys), len(want), head(c.keys), head(want))
						}
						if len(c.bad) > 0 {
							t.Fatalf("rows with another entry's data: %q", head(c.bad))
						}
						st := w.Stats()
						if st.Files+st.Dirs != int64(len(want)) {
							t.Fatalf("stats %+v for %d keys", st, len(want))
						}
						if name == "huge" && page == 7 && split && counter(o, "tree.dirs_split") == 0 {
							t.Fatal("the huge directory did not split")
						}
					})
				}
			}
		}
	}
}

func head(s []string) []string { return s[:min(len(s), 40)] }

// TestOrderedHeldAndCandidates: the engagement counters see what the
// translation did: a-b emitted before a/ (held), the low-byte siblings of a
// directory key cursor read again (dropped), and a pivot's directory
// candidate resolved.
func TestOrderedHeldAndCandidates(t *testing.T) {
	h := newHNS(manyKeys(3000))
	c, _, o, err := walkOrdered(t, h, 8, 50, true)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, h.oracle()) {
		t.Fatal("output differs from the oracle")
	}
	if counter(o, "tree.held_dirs") == 0 {
		t.Fatal("no directory held: big/part/ sorts after its part-* siblings")
	}
	if counter(o, "tree.candidate_dirs") == 0 {
		t.Fatal("no candidate: pivots part-NNNN have big/part as a candidate")
	}
	t.Logf("held %d, resolve pages %d, dropped %d, candidates %d, lookups %d (requests %d)",
		counter(o, "tree.held_dirs"), counter(o, "tree.resolve_pages"), counter(o, "tree.dropped_rows"),
		counter(o, "tree.candidate_dirs"), counter(o, "tree.lookups"), h.requests.Load())
}

// TestOrderedStart pins start's candidates.
func TestOrderedStart(t *testing.T) {
	for _, c := range []struct {
		dir, a, name string
		cands        []string
		empty        bool
	}{
		{"d/", "", "", nil, false},
		{"d/", "d/", "", nil, false},
		{"d/", "d/part-0015", "part-0015", []string{"part", "part-0015"}, false},
		{"d/", "d/a/", "a", nil, false},
		{"d/", "d/a/b", "a", nil, false},
		{"d/", "d/a b.c", "a b.c", []string{"a", "a b", "a b.c"}, false},
		{"d/", "c", "", nil, false},
		{"d/", "e", "", nil, true},
	} {
		name, cands, empty := start(c.dir, c.a)
		if name != c.name || !slices.Equal(cands, c.cands) || empty != c.empty {
			t.Errorf("start(%q, %q) = %q %q %v, want %q %q %v", c.dir, c.a, name, cands, empty, c.name, c.cands, c.empty)
		}
	}
}

// TestOrderedCancelMidDirectory: a run cancelled while a huge directory's
// segments are listing stops with the cancellation and leaves no request
// running.
func TestOrderedCancelMidDirectory(t *testing.T) {
	h := newHNS(manyKeys(20000))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.fail = func(n int64) error {
		if n == 40 {
			cancel()
		}
		return nil
	}
	o, err := NewOrdered[obj](h, keyrange.Options{Workers: 8, PageSize: 100}, keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWalker[struct{}, keyrange.Range, obj](OrderedRange{}, o, Options[obj]{})
	_, err = order.Run[OrderedRange, obj](ctx, w, &collect{}, &testSchema, order.Options{Workers: 8, Budget: 1 << 24})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err %v, want the cancellation", err)
	}
	if n := h.requests.Load(); n > 200 {
		t.Fatalf("%d requests after cancelling at 40", n)
	}
}

// TestOrderedInvalidPage: a lister whose names are out of order is a
// protocol violation, not a silent misorder.
func TestOrderedInvalidPage(t *testing.T) {
	h := newHNS([]string{"a", "b", "c"})
	h.dirs[""] = []string{"b", "a", "c"}
	_, _, _, err := walkOrdered(t, h, 1, 1000, false)
	if !errors.Is(err, source.ErrProtocol) {
		t.Fatalf("err %v, want a protocol violation", err)
	}
}

// TestOrderedCandidatePastFirstPage: big/part/ lies past its directory's
// first page (2,000 names sort before it), so a segment whose pivot is a
// part-NNNNN name must look the directory up rather than find it in what
// was read; the output is still the oracle's.
func TestOrderedCandidatePastFirstPage(t *testing.T) {
	keys := manyKeys(3000)
	for i := range 2000 {
		keys = append(keys, fmt.Sprintf("big/a-%05d", i))
	}
	h := newHNS(keys)
	c, _, o, err := walkOrdered(t, h, 8, 50, true)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, h.oracle()) {
		t.Fatal("output differs from the oracle")
	}
	if counter(o, "tree.lookups") == 0 {
		t.Fatal("no lookup: big/part is past the first page of big/")
	}
}

// TestOrderedFirstPageRequeued: a directory's first page whose retries were
// exhausted is asked again (the provider's Requeue), as the engine asks its
// own pages, instead of failing the run.
func TestOrderedFirstPageRequeued(t *testing.T) {
	old := requeueFirstWait
	requeueFirstWait = time.Millisecond
	t.Cleanup(func() { requeueFirstWait = old })
	transient := errors.New("exhausted")
	h := newHNS(siblingKeys)
	h.fail = func(n int64) error {
		if n == 1 { // the root's first page
			return transient
		}
		return nil
	}
	o, err := NewOrdered[obj](h, keyrange.Options{Workers: 2, PageSize: 3},
		keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing, Requeue: func(err error) bool { return errors.Is(err, transient) }})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWalker[struct{}, keyrange.Range, obj](OrderedRange{}, o, Options[obj]{})
	c := &collect{}
	if _, err := order.Run[OrderedRange, obj](context.Background(), w, c, &testSchema, order.Options{Workers: 2, Budget: 1 << 24}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, h.oracle()) || counter(o, "tree.first_page_requeues") != 1 {
		t.Fatalf("requeues %d", counter(o, "tree.first_page_requeues"))
	}
}

// TestOrderedRecordCapped: a split directory's record is dropped at its cap
// (later candidates are looked up instead) and the output is unchanged.
func TestOrderedRecordCapped(t *testing.T) {
	old := maxKnownEntries
	maxKnownEntries = 3
	t.Cleanup(func() { maxKnownEntries = old })
	keys := manyKeys(2000)
	for i := range 300 {
		keys = append(keys, fmt.Sprintf("big/d%05d/f", i))
	}
	h := newHNS(keys)
	c, _, o, err := walkOrdered(t, h, 8, 50, true)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, h.oracle()) {
		t.Fatal("output differs from the oracle")
	}
	if counter(o, "tree.record_resets") == 0 {
		t.Fatal("the record was never dropped")
	}
}

// File-only pages, lookup misses and completed split directories all count
// against the optional cache's limits. Eviction must preserve the oracle.
func TestOrderedCacheBounded(t *testing.T) {
	oldEntries, oldRecords := maxKnownEntries, maxKnownRecords
	maxKnownEntries, maxKnownRecords = 8, 3
	t.Cleanup(func() { maxKnownEntries, maxKnownRecords = oldEntries, oldRecords })
	for _, manyDirs := range []bool{false, true} {
		t.Run(fmt.Sprintf("manyDirs=%v", manyDirs), func(t *testing.T) {
			var keys []string
			for i := range 400 {
				prefix := ""
				if manyDirs {
					prefix = fmt.Sprintf("d%03d/", i/20)
				}
				keys = append(keys, fmt.Sprintf("%sf%05d", prefix, i))
			}
			h := newHNS(keys)
			c, _, o, err := walkOrdered(t, h, 4, 1, true)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(c.keys, h.oracle()) {
				t.Fatal("cache eviction changed output")
			}
			if len(o.known) > maxKnownRecords {
				t.Fatalf("retained %d directory records", len(o.known))
			}
			for dir, k := range o.known {
				if n := len(k.spans) + len(k.dirs) + len(k.none); n > maxKnownEntries {
					t.Fatalf("%q retained %d entries", dir, n)
				}
			}
			if manyDirs && counter(o, "tree.record_evictions") == 0 {
				t.Fatal("record cap did not engage")
			}
			if !manyDirs && counter(o, "tree.record_resets") == 0 {
				t.Fatal("file-page span cap did not engage")
			}
		})
	}
	// A cache miss after eviction must still resolve a real directory by
	// Lookup; a long sequence of absent candidates must also remain bounded.
	h := newHNS([]string{"real/child"})
	o, err := NewOrdered[obj](h, keyrange.Options{Workers: 1}, keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing})
	if err != nil {
		t.Fatal(err)
	}
	k := o.know("")
	for i := range 200 {
		if _, _, err := o.lookup(context.Background(), k, "", fmt.Sprintf("missing%d", i)); err != nil {
			t.Fatal(err)
		}
		if len(k.none) > maxKnownEntries {
			t.Fatalf("retained %d negative lookups", len(k.none))
		}
	}
	for i := range 10 {
		o.know(fmt.Sprint(i))
	}
	if len(o.known) > maxKnownRecords {
		t.Fatalf("retained %d directory records", len(o.known))
	}
	k = o.know("")
	e, ok, err := o.lookup(context.Background(), k, "", "real")
	if err != nil || !ok || !e.Dir || h.lookups.Load() != 201 {
		t.Fatalf("fallback lookup: entry=%+v ok=%v err=%v requests=%d", e, ok, err, h.lookups.Load())
	}
}

// emptyFirst answers a directory's first page with n empty continuing
// pages before the namespace's own.
type emptyFirst struct {
	*hns
	n int
}

func (e *emptyFirst) List(ctx context.Context, q *DirQuery) (*DirPage[obj], error) {
	k := 0
	if t, ok := strings.CutPrefix(q.Token, "empty:"); ok {
		k, _ = strconv.Atoi(t)
	} else if q.Token != "" {
		return e.hns.List(ctx, q)
	}
	if k < e.n {
		return &DirPage[obj]{Truncated: true, Token: "empty:" + strconv.Itoa(k+1)}, nil
	}
	in := *q
	in.Token = ""
	return e.hns.List(ctx, &in)
}

// TestOrderedFrontier: a directory's empty continuing first pages advance
// the walk only within keyrange.EmptyChainLive of their run; past it they
// are emptyChained, so the watchdog sees a run of them as no progress.
func TestOrderedFrontier(t *testing.T) {
	h := &emptyFirst{hns: newHNS([]string{"a", "b"}), n: keyrange.EmptyChainLive + 5}
	o, err := NewOrdered[obj](h, keyrange.Options{Workers: 1, PageSize: 10}, keyrange.Description[obj]{Target: "hns", Listing: keyrange.Listing})
	if err != nil {
		t.Fatal(err)
	}
	w := NewWalker[struct{}, keyrange.Range, obj](OrderedRange{}, o, Options[obj]{})
	c := &collect{}
	if _, err := order.Run[OrderedRange, obj](context.Background(), w, c, &testSchema, order.Options{Workers: 1, Budget: 1 << 24}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.keys, []string{"a", "b"}) {
		t.Fatalf("keys %q", c.keys)
	}
	adv, empty := o.Frontier()
	if empty != 5 || adv != int64(keyrange.EmptyChainLive)+1 {
		t.Fatalf("frontier %d advanced, %d empty chained", adv, empty)
	}
}
