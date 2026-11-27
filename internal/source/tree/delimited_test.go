// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package tree

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/counters"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
)

// memLister answers delimited listings of keys with S3's grouping, every
// folder's entries in reverse byte order (the reader may not rely on the
// service's order), size entries a page. edit, if set, may rewrite a page
// before it is returned.
type memLister struct {
	keys     []string
	size     int
	calls    int
	prefixes []string // every request's prefix
	edit     func(prefix string, page int, p *FolderPage[int64])
}

func (m *memLister) ListFolder(_ context.Context, prefix, token string) (*FolderPage[int64], error) {
	m.calls++
	m.prefixes = append(m.prefixes, prefix)
	var names []string
	seen := map[string]bool{}
	for _, k := range m.keys {
		rest, ok := strings.CutPrefix(k, prefix)
		if !ok {
			continue
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			k = prefix + rest[:i+1] + "\x00" // a folder
		}
		if !seen[k] {
			seen[k] = true
			names = append(names, k)
		}
	}
	slices.Sort(names)
	slices.Reverse(names)
	at := 0
	if token != "" {
		at, _ = strconv.Atoi(strings.TrimPrefix(token, prefix+"#"))
	}
	end := min(len(names), at+m.size)
	p := &FolderPage[int64]{}
	for _, n := range names[at:end] {
		if f, ok := strings.CutSuffix(n, "\x00"); ok {
			p.Folders = append(p.Folders, f)
		} else {
			p.Objects = append(p.Objects, schema.Row[int64]{Key: n, Data: int64(len(n))})
		}
	}
	if end < len(names) {
		p.Truncated, p.Token = true, prefix+"#"+strconv.Itoa(end)
	}
	if m.edit != nil {
		m.edit(prefix, at/m.size, p)
	}
	return p, nil
}

// walkDelimited lists keys through a Delimited reader under sel, page
// entries a page, and returns the keys in emission order.
func walkDelimited(t *testing.T, l FolderLister[int64], opt DelimitedOptions[int64]) ([]string, *Delimited[int64], *Walker[struct{}, struct{}, int64], error) {
	t.Helper()
	d := NewDelimited(l, opt)
	w := NewWalker(d.Root(), d, Options[int64]{Size: func(n *int64) int64 { return *n }})
	var keys []string
	var walk func(r FolderRange) error
	walk = func(r FolderRange) error {
		var items []any
		if err := w.Expand(context.Background(), r, &recorder[struct{}, struct{}, int64]{items: &items}); err != nil {
			return err
		}
		for _, it := range items {
			switch v := it.(type) {
			case string:
				keys = append(keys, v)
			case FolderRange:
				if err := walk(v); err != nil {
					return err
				}
			}
		}
		return nil
	}
	err := walk(w.Root())
	return keys, d, w, err
}

// delimitedKeys hold every shape the reader must place: markers (a key
// equal to its folder's prefix) that sort after their own children in a
// store's order, empty segments, slash-only keys, keys that are proper
// prefixes of others, a deep chain.
func delimitedKeys() []string {
	keys := []string{"/", "//", "///x", "a", "a-b", "a.b", "a.jpg", "a.jpg.orig", "a/", "a//", "a//b", "a/x", "a/x/", "a/x/y", "a0",
		"d/", "d/0", "d/0/", "é", "e\u0301x", "z~", "\x7f"}
	deep := ""
	for i := range 12 {
		deep += fmt.Sprintf("l%d/", i)
		keys = append(keys, deep, deep+"f")
	}
	slices.Sort(keys)
	return keys
}

// TestDelimitedOrder: whatever order a store sends a folder's entries in,
// and however it pages them, the walk emits every key once in byte order:
// markers once, in their own folder; empty segments walked, never
// cleaned; no start key ever asked for (the lister has no way to take
// one).
func TestDelimitedOrder(t *testing.T) {
	keys := delimitedKeys()
	for _, size := range []int{1, 2, 3, 1000} {
		got, d, w, err := walkDelimited(t, &memLister{keys: keys, size: size}, DelimitedOptions[int64]{})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(got, keys) {
			t.Fatalf("page size %d:\n got %q\nwant %q", size, got, keys)
		}
		if st := w.Stats(); st.Files != int64(len(keys)) || st.Dirs != 0 || st.Folders == 0 {
			t.Fatalf("stats %+v", st)
		}
		s := d.opt.Counters.Snapshot()
		if s.Value("tree.markers") == 0 || s.Value("tree.empty_segments") == 0 || s.Value("tree.folders_started") != s.Value("tree.folders_completed") ||
			s.Value("tree.objects") != int64(len(keys)) || s.Value("tree.non_nfc_names") != 1 || d.Staged() != 0 {
			t.Fatalf("page size %d: counters %s, staged %d", size, mustJSON(s), d.Staged())
		}
	}
}

func mustJSON(s counters.Snapshot) string {
	b, _ := s.MarshalJSON()
	return string(b)
}

// TestDelimitedSelect: a scope that does not end in '/' starts at its
// deepest folder and keeps that folder's entries whose raw bytes start
// with it: "logs" covers logs, logs-old/ and logs/, not log or lo/; below
// a folder, a decomposed spelling is kept by its raw bytes.
func TestDelimitedSelect(t *testing.T) {
	keys := []string{"lo/x", "log", "logs", "logs-old/a", "logs/", "logs/a", "logs/b/c", "logsx", "m", "d/", "d/e\u0301x", "d/\u00e9y", "d/ex", "d/f"}
	slices.Sort(keys)
	for _, sel := range []string{"", "logs", "logs/", "logs/b", "d/e", "d/", "n", "logs-old/a"} {
		l := &memLister{keys: keys, size: 2}
		got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: sel})
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for _, k := range keys {
			if strings.HasPrefix(k, sel) {
				want = append(want, k)
			}
		}
		if !slices.Equal(got, want) {
			t.Fatalf("select %q:\n got %q\nwant %q", sel, got, want)
		}
		if out := d.opt.Counters.Snapshot().Value("tree.out_of_scope"); sel == "logs" && out != 4 {
			t.Fatalf("select %q: %d out of scope, want 4 (d/, lo/, log, m)", sel, out)
		}
	}
}

// TestDelimitedValidation: a page that breaks a folder's structure stops
// the walk as a protocol violation, counted by its kind, nothing of the
// folder emitted, and its staging released.
func TestDelimitedValidation(t *testing.T) {
	keys := []string{"a/b", "a/c", "a/d/x", "e"}
	// Each edit breaks one page only (the folder's first, unless it says
	// otherwise), so that the check of its kind, not a duplicate that a
	// repeated edit would make, is what stops the walk.
	for _, tc := range []struct {
		name, counter string
		edit          func(prefix string, page int, p *FolderPage[int64])
	}{
		{"raw prefix object", "tree.violations.raw_prefix", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Objects = append(p.Objects, schema.Row[int64]{Key: "b/x"})
			}
		}},
		{"raw prefix folder", "tree.violations.raw_prefix", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Folders = append(p.Folders, "e\u0301/")
			}
		}},
		{"slash in object", "tree.violations.delimiter", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Objects = append(p.Objects, schema.Row[int64]{Key: "a/d/y"})
			}
		}},
		{"two segments", "tree.violations.delimiter", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "" {
				p.Folders = append(p.Folders, "a/d/")
			}
		}},
		{"no slash", "tree.violations.delimiter", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Folders = append(p.Folders, "a/q")
			}
		}},
		{"self reference", "tree.violations.self_reference", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Folders = append(p.Folders, "a/")
			}
		}},
		{"empty key", "tree.violations.empty_key", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "" {
				p.Objects = append(p.Objects, schema.Row[int64]{})
			}
		}},
		{"duplicate object", "tree.violations.duplicate", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" {
				p.Objects = append(p.Objects, schema.Row[int64]{Key: "a/b"}) // also on its own page
			}
		}},
		{"duplicate folder", "tree.violations.duplicate", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "" {
				p.Folders = append(p.Folders, "a/")
			}
		}},
		{"no token", "tree.violations.token", func(prefix string, _ int, p *FolderPage[int64]) {
			if prefix == "a/" && p.Truncated {
				p.Token = ""
			}
		}},
		{"token cycle", "tree.violations.token", func(prefix string, page int, p *FolderPage[int64]) {
			if prefix == "a/" && p.Truncated && page > 0 {
				p.Token = prefix + "#1" // the first page's token again
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			edit := func(prefix string, page int, p *FolderPage[int64]) {
				if page == 0 || tc.counter == "tree.violations.token" {
					tc.edit(prefix, page, p)
				}
			}
			l := &memLister{keys: keys, size: 1, edit: edit}
			got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{})
			if !errors.Is(err, source.ErrProtocol) {
				t.Fatalf("err %v, keys %q", err, got)
			}
			if v := d.opt.Counters.Snapshot().Value(tc.counter); v != 1 {
				t.Fatalf("%s = %d (%v)", tc.counter, v, err)
			}
			if d.Staged() != 0 {
				t.Fatalf("staged %d after the stop", d.Staged())
			}
			for _, k := range got {
				if strings.HasPrefix(k, "a/") {
					t.Fatalf("emitted %q of the folder that failed", k)
				}
			}
		})
	}
}

// TestDelimitedTerminalTokenIgnored: a terminal page needs no token, and
// one it carries is not followed.
func TestDelimitedTerminalTokenIgnored(t *testing.T) {
	keys := []string{"a", "b"}
	l := &memLister{keys: keys, size: 1000, edit: func(_ string, _ int, p *FolderPage[int64]) { p.Token = "stale" }}
	got, _, _, err := walkDelimited(t, l, DelimitedOptions[int64]{})
	if err != nil || !slices.Equal(got, keys) || l.calls != 1 {
		t.Fatalf("%q, %v, %d calls", got, err, l.calls)
	}
}

// TestDelimitedBudget: a folder that would stage more than its bound, or
// more than every folder may at once, stops the walk with the resource
// error naming the folder; nothing is waited for, and the staging is
// released.
func TestDelimitedBudget(t *testing.T) {
	var keys []string
	for i := range 200 {
		keys = append(keys, fmt.Sprintf("big/%04d", i))
	}
	keys = append(keys, "small/a")
	slices.Sort(keys)
	per := int64(len("big/0000")) + entryBytes[int64]()
	for _, tc := range []struct {
		name    string
		opt     DelimitedOptions[int64]
		staging bool
	}{
		{"folder", DelimitedOptions[int64]{FolderBytes: 100 * per}, false},
		{"staging", DelimitedOptions[int64]{StagingBytes: 100 * per}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, d, _, err := walkDelimited(t, &memLister{keys: keys, size: 10}, tc.opt)
			var le *FolderLimitError
			if !errors.As(err, &le) || le.Folder != "big/" || le.Staging != tc.staging || le.Class() != "folder_limit" {
				t.Fatalf("err %v", err)
			}
			if !strings.Contains(err.Error(), `"big/"`) {
				t.Fatalf("message %q does not name the folder", err)
			}
			s := d.opt.Counters.Snapshot()
			if s.Value("tree.budget_refusals") != 1 || d.Staged() != 0 || s.Value("tree.peak_staged_bytes") > 100*per {
				t.Fatalf("counters %s, staged %d", mustJSON(s), d.Staged())
			}
		})
	}
	// Under the bounds, the same walk completes.
	got, _, _, err := walkDelimited(t, &memLister{keys: keys, size: 10}, DelimitedOptions[int64]{FolderBytes: 300 * per, StagingBytes: 300 * per})
	if err != nil || !slices.Equal(got, keys) {
		t.Fatalf("%d keys, %v", len(got), err)
	}
}

// failing answers its first n calls with err.
type failing struct {
	FolderLister[int64]
	n   int
	err error
}

func (f *failing) ListFolder(ctx context.Context, prefix, token string) (*FolderPage[int64], error) {
	if f.n > 0 {
		f.n--
		return nil, f.err
	}
	return f.FolderLister.ListFolder(ctx, prefix, token)
}

// TestDelimitedRequeue: a page whose failure the provider requeues is
// asked again with the same token; any other failure stops the walk.
func TestDelimitedRequeue(t *testing.T) {
	keys := []string{"a/b", "c"}
	transient := errors.New("transient")
	l := &failing{FolderLister: &memLister{keys: keys, size: 1}, n: 2, err: transient}
	got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Requeue: func(err error) bool { return errors.Is(err, transient) }})
	if err != nil || !slices.Equal(got, keys) || d.opt.Counters.Snapshot().Value("tree.requeues") != 2 {
		t.Fatalf("%q, %v", got, err)
	}
	fatal := errors.New("fatal")
	if _, _, _, err := walkDelimited(t, &failing{FolderLister: &memLister{keys: keys, size: 1}, n: 1, err: fatal},
		DelimitedOptions[int64]{Requeue: func(err error) bool { return errors.Is(err, transient) }}); !errors.Is(err, fatal) {
		t.Fatalf("%v", err)
	}
}

// TestFolderSegments: Out.Folder takes one segment, possibly empty, and a
// slash below the range's prefix, and refuses anything else as a protocol
// violation, emitting nothing.
func TestFolderSegments(t *testing.T) {
	for _, tc := range []struct {
		parent, child string
		ok            bool
	}{
		{"", "a/", true}, {"", "/", true}, {"a/", "a//", true}, {"a/", "a/b/", true},
		{"a/", "a/", false}, {"a/", "a", false}, {"a/", "a/b", false}, {"a/", "a/b/c/", false}, {"a/", "b/x/", false}, {"", "", false},
	} {
		var items []any
		out := &Out[struct{}, struct{}, int64]{e: &recorder[struct{}, struct{}, int64]{items: &items}, r: FolderRange{Prefix: tc.parent}}
		err := out.Folder(struct{}{}, tc.child)
		if tc.ok != (err == nil) || !tc.ok && !errors.Is(err, source.ErrProtocol) {
			t.Fatalf("%q below %q: %v", tc.child, tc.parent, err)
		}
		if tc.ok && (len(items) != 1 || items[0].(FolderRange).Prefix != tc.child || out.st.Folders != 1) {
			t.Fatalf("%q below %q: emitted %v", tc.child, tc.parent, items)
		}
		if !tc.ok && len(items) != 0 {
			t.Fatalf("%q below %q: emitted %v", tc.child, tc.parent, items)
		}
	}
}

// TestFolderNotPruned: a virtual folder is walked whatever the
// recursive-exclude predicate says, since its marker is not a descendant
// of its key.
func TestFolderNotPruned(t *testing.T) {
	keys := []string{"x/", "x/a"}
	d := NewDelimited[int64](&memLister{keys: keys, size: 10}, DelimitedOptions[int64]{})
	w := NewWalker(d.Root(), d, Options[int64]{})
	w.SetPruneDirectories(func(string) bool { return true })
	if got := drain(t, source.Provider[FolderRange, int64](w)); !slices.Equal(got, keys) {
		t.Fatalf("got %q", got)
	}
}

// aliasLister answers folders from a fixed table, as a store that groups
// names in NFC answers them: a folder's listing may hold another
// spelling's keys.
type aliasLister struct {
	folders  map[string]*FolderPage[int64]
	prefixes []string
}

func (a *aliasLister) ListFolder(_ context.Context, prefix, _ string) (*FolderPage[int64], error) {
	a.prefixes = append(a.prefixes, prefix)
	if p, ok := a.folders[prefix]; ok {
		c := *p
		c.Objects = slices.Clone(p.Objects)
		return &c, nil
	}
	return &FolderPage[int64]{}, nil
}

func objects(keys ...string) []schema.Row[int64] {
	rows := make([]schema.Row[int64], len(keys))
	for i, k := range keys {
		rows[i].Key = k
	}
	return rows
}

// TestDelimitedScopeProbe: a child folder of the start folder outside the
// scope whose name (or the scope's remainder) is not ASCII may hold a key
// of the scope under another spelling (R2 merges e\u0301/ and \u00e9/ into
// one common prefix, spelled as one of them): it is probed, its subtree
// listed and checked, emitting nothing, so that such a key stops the run
// (protocol), however deep it is. ASCII folders outside the scope are left
// out without a request.
func TestDelimitedScopeProbe(t *testing.T) {
	for _, tc := range []struct {
		name, sel string
		folders   map[string]*FolderPage[int64]
		probes    int64
	}{
		{"alias in the folder", "e", map[string]*FolderPage[int64]{
			"":        {Folders: []string{"\u00e9/"}},
			"\u00e9/": {Objects: objects("\u00e9/!", "e\u0301/x")},
		}, 1},
		{"alias below the folder", "e", map[string]*FolderPage[int64]{
			"":          {Folders: []string{"\u00e9/"}},
			"\u00e9/":   {Folders: []string{"\u00e9/a/"}},
			"\u00e9/a/": {Objects: objects("\u00e9/a/1", "e\u0301/a/2")},
		}, 2},
		{"kelvin", "d/K", map[string]*FolderPage[int64]{
			"d/":        {Folders: []string{"d/\u212a/"}},
			"d/\u212a/": {Objects: objects("d/\u212a/a", "d/K/b")},
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &aliasLister{folders: tc.folders}
			got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: tc.sel})
			if !errors.Is(err, source.ErrProtocol) || !strings.Contains(err.Error(), "by its raw bytes") {
				t.Fatalf("err %v, keys %q", err, got)
			}
			s := d.opt.Counters.Snapshot()
			if s.Value("tree.probes") != tc.probes || s.Value("tree.violations.raw_prefix") != 1 || d.Staged() != 0 {
				t.Fatalf("counters %s, staged %d", mustJSON(s), d.Staged())
			}
			if len(got) != 0 {
				t.Fatalf("a probe emitted %q", got)
			}
		})
	}
	// Probed folders that hold nothing of the scope emit nothing, and the
	// walk goes on.
	l := &aliasLister{folders: map[string]*FolderPage[int64]{
		"":          {Objects: objects("e1"), Folders: []string{"\u00e9/", "e/"}},
		"\u00e9/":   {Objects: objects("\u00e9/x"), Folders: []string{"\u00e9/y/"}},
		"\u00e9/y/": {Objects: objects("\u00e9/y/z")},
		"e/":        {Objects: objects("e/a")},
	}}
	got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: "e"})
	if err != nil || !slices.Equal(got, []string{"e/a", "e1"}) || d.opt.Counters.Snapshot().Value("tree.probes") != 2 {
		t.Fatalf("%q, %v, %s", got, err, mustJSON(d.opt.Counters.Snapshot()))
	}
	// ASCII: no request for a folder outside the scope.
	m := &memLister{keys: []string{"lo/x", "logs/a", "lz/y", "m/1"}, size: 10}
	if got, d, _, err := walkDelimited(t, m, DelimitedOptions[int64]{Select: "logs"}); err != nil || !slices.Equal(got, []string{"logs/a"}) ||
		d.opt.Counters.Snapshot().Value("tree.probes") != 0 || !slices.Equal(m.prefixes, []string{"", "logs/"}) {
		t.Fatalf("%q, %v, requests %q", got, err, m.prefixes)
	}
}

// TestDelimitedDuplicateOutOfScope: an entry listed twice is a violation
// whether or not the scope keeps it.
func TestDelimitedDuplicateOutOfScope(t *testing.T) {
	l := &aliasLister{folders: map[string]*FolderPage[int64]{"": {Objects: objects("a", "b", "b")}}}
	if got, _, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: "a"}); !errors.Is(err, source.ErrProtocol) {
		t.Fatalf("%q, %v", got, err)
	}
}

// TestDelimitedRetained: a page that says what its strings retain is
// charged that, not their lengths.
func TestDelimitedRetained(t *testing.T) {
	l := &memLister{keys: []string{"a", "b"}, size: 1, edit: func(_ string, _ int, p *FolderPage[int64]) { p.Retained = 10_000 }}
	_, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{})
	if v := d.opt.Counters.Snapshot().Value("tree.max_folder_bytes"); err != nil || v < 20_000 {
		t.Fatalf("charged %d for two pages retaining 10,000 bytes each (%v)", v, err)
	}
}

// gatedLister holds the first request for folder held until release, and
// records each folder's requests.
type gatedLister struct {
	mu      sync.Mutex
	held    string
	entered chan struct{}
	release chan struct{}
	calls   map[string]int
}

func (g *gatedLister) ListFolder(_ context.Context, prefix, _ string) (*FolderPage[int64], error) {
	g.mu.Lock()
	g.calls[prefix]++
	g.mu.Unlock()
	if prefix == g.held {
		close(g.entered)
		<-g.release
	}
	return &FolderPage[int64]{Objects: objects(prefix + "x")}, nil
}

func (g *gatedLister) count(prefix string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[prefix]
}

// TestDelimitedHeadroom: with room for one page in flight, a second
// folder's page waits until the first is decoded and charged; then both
// complete. The wait is observed by the reader's hook, not by time.
func TestDelimitedHeadroom(t *testing.T) {
	g := &gatedLister{held: "a/", entered: make(chan struct{}), release: make(chan struct{}), calls: map[string]int{}}
	d := NewDelimited[int64](g, DelimitedOptions[int64]{PageHeadroom: 100, HeadroomBytes: 150})
	waiting := make(chan struct{})
	d.waiting = func() { close(waiting) }
	read := func(prefix string, done chan<- error) {
		var items []any
		out := &Out[struct{}, struct{}, int64]{e: &recorder[struct{}, struct{}, int64]{items: &items}, r: FolderRange{Prefix: prefix}}
		done <- d.Read(context.Background(), FolderRange{Prefix: prefix}, out)
	}
	a, b := make(chan error, 1), make(chan error, 1)
	go read("a/", a)
	<-g.entered
	go read("b/", b)
	select {
	case <-waiting:
	case err := <-b:
		t.Fatalf("folder b/ did not wait for the headroom a/'s page held (%v)", err)
	}
	if n := g.count("b/"); n != 0 {
		t.Fatalf("folder b/ sent %d requests while a/'s page held the headroom", n)
	}
	close(g.release)
	if err := <-a; err != nil {
		t.Fatal(err)
	}
	if err := <-b; err != nil {
		t.Fatal(err)
	}
	s := d.opt.Counters.Snapshot()
	if s.Value("tree.headroom_waits") != 1 || g.count("b/") != 1 || d.Staged() != 0 {
		t.Fatalf("counters %s", mustJSON(s))
	}
	// A cancelled wait returns.
	g2 := &gatedLister{held: "a/", entered: make(chan struct{}), release: make(chan struct{}), calls: map[string]int{}}
	d2 := NewDelimited[int64](g2, DelimitedOptions[int64]{PageHeadroom: 100, HeadroomBytes: 100})
	waiting2 := make(chan struct{})
	d2.waiting = func() { close(waiting2) }
	go func() {
		var items []any
		out := &Out[struct{}, struct{}, int64]{e: &recorder[struct{}, struct{}, int64]{items: &items}, r: FolderRange{Prefix: "a/"}}
		d2.Read(context.Background(), FolderRange{Prefix: "a/"}, out)
	}()
	<-g2.entered
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var items []any
		out := &Out[struct{}, struct{}, int64]{e: &recorder[struct{}, struct{}, int64]{items: &items}, r: FolderRange{Prefix: "b/"}}
		done <- d2.Read(ctx, FolderRange{Prefix: "b/"}, out)
	}()
	select {
	case <-waiting2:
	case err := <-done:
		t.Fatalf("folder b/ did not wait for the headroom (%v)", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait: %v", err)
	}
	close(g2.release)
}

// TestDelimitedProbeStem: a folder outside the scope is probed only when
// its decomposed segment begins with the scope's remainder decomposed up
// to its last starter (stem); no spelling of any other folder can hold a
// key of the scope.
func TestDelimitedProbeStem(t *testing.T) {
	// a: \u00e9/ (NFD e\u0301) cannot hold a key starting with a: never
	// listed, though it holds another spelling's key.
	l := &aliasLister{folders: map[string]*FolderPage[int64]{
		"":        {Objects: objects("ab"), Folders: []string{"\u00e9/", "a/"}},
		"a/":      {Objects: objects("a/1")},
		"\u00e9/": {Objects: objects("\u00e9/!", "e\u0301/x")},
	}}
	got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: "a"})
	if err != nil || !slices.Equal(got, []string{"a/1", "ab"}) || d.opt.Counters.Snapshot().Value("tree.probes") != 0 || slices.Contains(l.prefixes, "\u00e9/") {
		t.Fatalf("%q, %v, requests %+q", got, err, l.prefixes)
	}
	// Reordering: \u00e9\u0323 (e, acute, dot below) decomposes to e, dot
	// below, acute; its NFC is \u1eb9\u0301, which does not start with the
	// scope \u00e9 in any form but its stem e. Probed, the hidden key stops
	// the run.
	l = &aliasLister{folders: map[string]*FolderPage[int64]{
		"":              {Folders: []string{"\u1eb9\u0301/"}},
		"\u1eb9\u0301/": {Objects: objects("\u1eb9\u0301/!", "\u00e9\u0323/x")},
	}}
	if got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: "\u00e9"}); !errors.Is(err, source.ErrProtocol) || d.opt.Counters.Snapshot().Value("tree.probes") != 1 {
		t.Fatalf("reordering: %q, %v", got, err)
	}
	// A remainder with no starter has an empty stem: every folder outside
	// the scope is probed.
	l = &aliasLister{folders: map[string]*FolderPage[int64]{
		"": {Folders: []string{"a/", "b/"}},
	}}
	if got, d, _, err := walkDelimited(t, l, DelimitedOptions[int64]{Select: "\u0301"}); err != nil || len(got) != 0 || d.opt.Counters.Snapshot().Value("tree.probes") != 2 {
		t.Fatalf("no starter: %q, %v, %s", got, err, mustJSON(d.opt.Counters.Snapshot()))
	}
	for r, want := range map[string]string{"": "", "a": "a", "\u00e9": "e", "e\u0301\u0323": "e", "\u0301": "", "ab\u0301c\u0323": "ab\u0301c", "\u212a": "K"} {
		if got := stem(r); got != want {
			t.Errorf("stem(%+q) = %+q, want %+q", r, got, want)
		}
	}
}

// TestDelimitedHeadroomCoversInFlight: without a bound of its own, the
// headroom is one page for each page the walk can have in flight, so it
// never waits.
func TestDelimitedHeadroomCoversInFlight(t *testing.T) {
	d := NewDelimited[int64](&memLister{}, DelimitedOptions[int64]{PageHeadroom: 4 << 20, InFlight: 256})
	if v := d.opt.Counters.Snapshot().Value("tree.limit.headroom_bytes"); v != 256*4<<20 {
		t.Fatalf("headroom %d, want 256 pages of 4 MiB", v)
	}
}
