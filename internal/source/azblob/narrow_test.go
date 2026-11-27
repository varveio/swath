// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azblob

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/schema"
	"github.com/varveio/swath/internal/source"
	"github.com/varveio/swath/internal/source/azblob/azblobtest"
	"github.com/varveio/swath/internal/source/azblob/client"
	"github.com/varveio/swath/internal/source/keyrange"
)

// TestWithPrefix: the narrowed root keeps the SAS byte for byte and the
// prefix's own bytes ('%', '+', '#', spaces, a '?' without a SAS,
// characters at U+E000 and above), and parses back to the same account,
// container and SAS and to the prefix asked for. A prefix the root cannot
// carry (a '?' beside a SAS, or text after a '?' that would read as a SAS)
// is declined, as is a root that does not parse: the root comes back as
// given, never with an include's text taken as a credential.
func TestWithPrefix(t *testing.T) {
	for _, c := range []struct{ root, prefix, want string }{
		{"az://acct/c", "logs/", "az://acct/c/logs/"},
		{"az://acct/c/", "logs/2026-0", "az://acct/c/logs/2026-0"},
		{"az://acct/c/lo", "logs/2026", "az://acct/c/logs/2026"},
		{"az://acct/c/p?sv=2026-10-06&sig=abc%3D", "p/q 100%+#/", "az://acct/c/p/q 100%+#/?sv=2026-10-06&sig=abc%3D"},
		{"az://acct/c?sp=rl&se=2027&sig=x%2By", "a%2Fb/", "az://acct/c/a%2Fb/?sp=rl&se=2027&sig=x%2By"},
		{"az://acct/c?" + RedactedSAS, "d/", "az://acct/c/d/?" + RedactedSAS},
		{"az://acct/c/what?not=x", "what?not=x/y", "az://acct/c/what?not=x/y"},
		{"az://acct/c", "a?b/", "az://acct/c/a?b/"},
		{"az://acct/c/é", "é\U00010000/", "az://acct/c/é\U00010000/"},
		// Declined: the root as given.
		{"az://acct/c?sv=1&sig=S", "a?b/", "az://acct/c?sv=1&sig=S"},
		{"az://acct/c/a?sv=1&sig=S", "a?sv=1&sig=S/", "az://acct/c/a?sv=1&sig=S"},
		{"az://acct/c", "x?sig=1/", "az://acct/c"},
		{"az://acct/c", "x?sv=1&sig=2", "az://acct/c"},
		{"az://acct/c", "x?%73ig=1", "az://acct/c"},
		{"az://acct/c", "x?SIG=1;y", "az://acct/c"},
		{"az://acct/c", "x?sv=1/", "az://acct/c"},
		{"az://acct", "logs/", "az://acct"},
		{"az:///c", "logs/", "az:///c"},
	} {
		got := WithPrefix(c.root, c.prefix)
		if got != c.want {
			t.Errorf("WithPrefix(%q, %q) = %q, want %q", c.root, c.prefix, got, c.want)
			continue
		}
		if got == c.root {
			continue
		}
		a, ctr, p, sas, err := ParseURL(got)
		ra, rc, _, rsas, _ := ParseURL(c.root)
		if err != nil || a != ra || ctr != rc || sas != rsas || p != c.prefix {
			t.Errorf("%q parses to %q %q %q %q (%v), want %q %q %q %q", got, a, ctr, p, sas, err, ra, rc, c.prefix, rsas)
		}
	}
}

// narrowKeys is a container for the narrowing oracle: blob names equal to
// a prefix, trailing-slash names, names either side of '/' ('.' and '0'),
// characters a URL would escape, characters at U+E000 and above (lead
// bytes EE/EF) and above U+FFFF (F0..F4) inside and after a prefix, names
// over 1,000 bytes, and fillers that put page seams and splits inside
// each prefix.
func narrowKeys() []string {
	longE := "long/" + strings.Repeat("é", 500) + "/"
	longA := "L/" + strings.Repeat("a", 1000) + "/"
	keys := []string{
		"logs", "logs/", "logs/2026", "logs/2026-01/", "logs/2026-01/a", "logs/2026-01/b.tmp", "logs/2026-01//x",
		"logs/2026-02/c", "logs/2026-02/c.tmp", "logs/2026-1x", "logs/2026/z", "logs0", "logs.x",
		"p/a", "p/a.b", "p/a/", "p/a/x", "p/a0", "p/a0/y", "p/b",
		"esc/a*b/1", "esc/a?b/1", "esc/100%/x", "esc/100%25/x", "esc/a+b/1", "esc/a b/1", "esc/a#b/1", "esc/ab/1",
		"esc/a?sig=1/x", "esc/a?sv=1/x",
		"u/a", "u/a/1", "u/a/2", "u/b", "u/\U00010000/1", "u/\U00010000", "u/\U00010000\U00010000",
		"u/�x", "u/ｚ/1", "u/\U0001F600", "u/é/1",
		longE + "a", longE + "b", "long/" + strings.Repeat("é", 499) + "x", longA + "1", longA + "2", "L/" + strings.Repeat("a", 999) + "b",
		"zz",
	}
	for i := range 60 {
		keys = append(keys, fmt.Sprintf("logs/2026-01/f%03d", i), fmt.Sprintf("logs/2026-02/g%03d.tmp", i),
			fmt.Sprintf("u/a/f%03d", i), fmt.Sprintf("u/\U00010000/f%03d", i), fmt.Sprintf("p/a/f%03d", i),
			fmt.Sprintf("esc/a*b/f%03d", i), fmt.Sprintf("%sf%03d", longE, i), fmt.Sprintf("%sf%03d", longA, i))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// narrowCase is one command line's filters over a root's prefix.
type narrowCase struct {
	name                    string
	scope                   string // the root's prefix: az://acct/c/SCOPE
	include, regex, exclude []string
	want                    string // the narrowed prefix ("" for scope)
	reason                  string // NarrowPrefix's engagement
	declined                bool   // WithPrefix declines want: scope is listed
}

func narrowCases() []narrowCase {
	longE := "long/" + strings.Repeat("é", 500) + "/"
	longA := "L/" + strings.Repeat("a", 1000) + "/"
	n := filter.Narrowed
	return []narrowCase{
		{name: "mid-segment", include: []string{"logs/2026-0*/**"}, want: "logs/2026-0", reason: n},
		{name: "a blob's name", include: []string{"logs/2026*"}, want: "logs/2026", reason: n},
		{name: "a blob's name, regex", regex: []string{"^logs/2026($|/)"}, want: "logs/2026", reason: n},
		{name: "trailing slash", include: []string{"logs/2026-01/**"}, want: "logs/2026-01/", reason: n},
		{name: "alternatives with a common prefix", include: []string{"logs/2026-0{1,2}/**"}, want: "logs/2026-0", reason: n},
		{name: "includes with a common prefix", include: []string{"p/a/**", "p/a0"}, regex: []string{"^p/a\\.b$"}, want: "p/a", reason: n},
		{name: "alternatives without one", include: []string{"{logs,p}/**"}, reason: filter.NarrowNoLonger},
		{name: "includes without one", include: []string{"logs/**", "u/**"}, reason: filter.NarrowNoLonger},
		{name: "include and exclude", include: []string{"logs/**"}, exclude: []string{"*.tmp", "logs/2026-01/f00*"}, want: "logs/", reason: n},
		{name: "scoped root", scope: "logs/", include: []string{"2026-01/*"}, want: "logs/2026-01/", reason: n},
		{name: "scoped mid-segment root", scope: "logs/20", include: []string{"2026-0*/**"}, exclude: []string{"*.tmp"}, want: "logs/2026-0", reason: n},
		{name: "scoped root, wider include", scope: "logs/2026-01/", include: []string{"*"}, reason: filter.NarrowUnanchored},
		{name: "escaped *", include: []string{`esc/a\*b/**`}, want: "esc/a*b/", reason: n},
		{name: "escaped ?", include: []string{`esc/a\?b/**`}, want: "esc/a?b/", reason: n},
		{name: "escaped ? that reads as a SAS", include: []string{`esc/a\?sig=1/**`}, want: "esc/a?sig=1/", reason: n, declined: true},
		{name: "escaped ? that names a SAS parameter", include: []string{`esc/a\?sv=1/**`}, want: "esc/a?sv=1/", reason: n, declined: true},
		{name: "%", include: []string{"esc/100%/**"}, want: "esc/100%/", reason: n},
		{name: "%25", include: []string{"esc/100%25/**"}, want: "esc/100%25/", reason: n},
		{name: "+, regex", regex: []string{`^esc/a\+b/`}, want: "esc/a+b/", reason: n},
		{name: "#", include: []string{"esc/a#b/**"}, want: "esc/a#b/", reason: n},
		{name: "space", include: []string{"esc/a b/**"}, want: "esc/a b/", reason: n},
		{name: "escaped * and ?", include: []string{`esc/a\*b/**`, `esc/a\?b/**`}, want: "esc/a", reason: n},
		{name: "beside /", include: []string{"p/a/**"}, want: "p/a/", reason: n},
		{name: "before and after /", include: []string{"p/a*"}, want: "p/a", reason: n},
		{name: "U+E000 inside", include: []string{"u/a/**"}, want: "u/a/", reason: n},
		{name: "U+10000 inside", include: []string{"u/\U00010000/**"}, want: "u/\U00010000/", reason: n},
		{name: "U+10000 mid-segment", include: []string{"u/\U00010000*"}, want: "u/\U00010000", reason: n},
		{name: "U+E000 and above after", include: []string{"u/**"}, want: "u/", reason: n},
		{name: "long two-byte", include: []string{longE + "**"}, want: longE, reason: n},
		{name: "long ASCII", include: []string{longA + "f0*"}, want: longA + "f0", reason: n},
		{name: "empty", include: []string{"nothing/**"}, want: "nothing/", reason: n},
	}
}

// spec is c's filters over a root whose prefix is c.scope, Base set as
// the command sets it (up to the prefix's last '/').
func (c narrowCase) spec(t *testing.T) *filter.Spec {
	t.Helper()
	s := &filter.Spec{Include: c.include, IncludeRegex: c.regex, Exclude: c.exclude, Base: c.scope[:strings.LastIndexByte(c.scope, '/')+1]}
	if err := s.Validate(); err != nil {
		t.Fatalf("%s: %v", c.name, err)
	}
	return s
}

// narrowConfig is an engine configuration the oracle lists under.
type narrowConfig struct {
	name  string
	opt   keyrange.Options
	short bool // responses cut short at 1 to 3 items
	after bool // resumed after the middle selected name
}

func narrowConfigs() []narrowConfig {
	return []narrowConfig{
		{name: "pages of 1, splits", opt: keyrange.Options{Workers: 8, PageSize: 1, SplitCoin: coin(1)}},
		{name: "pages of 1, splits, seed", opt: keyrange.Options{Workers: 8, PageSize: 1, Seed: true, SplitCoin: coin(2)}},
		{name: "pages of 3, short, splits, seed", opt: keyrange.Options{Workers: 8, PageSize: 3, Seed: true, SplitCoin: coin(3)}, short: true},
		{name: "sequential", opt: keyrange.Options{Sequential: true, Workers: 1, PageSize: 2}, short: true},
		{name: "resumed, splits", opt: keyrange.Options{Workers: 8, PageSize: 2, SplitCoin: coin(4)}, short: true, after: true},
	}
}

// listScope lists prefix of a store over keys with the configuration
// (resumed after after, unless ""), and returns the rows and the requests.
func listScope(t *testing.T, keys []string, prefix, after string, c narrowConfig) ([]Row, []azblobtest.Record) {
	t.Helper()
	st := azblobtest.NewStore(keys)
	if c.short {
		st.Short = func(n int) int { return 1 + n%3 }
	}
	opt := c.opt
	opt.Prefix = prefix
	if after != "" {
		opt.Resume = &keyrange.Resume{After: after}
	}
	p := azureEngine(t, st, opt)
	rows := &collect{}
	if err := runRows(t, context.Background(), p, max(1, opt.Workers), rows); err != nil {
		t.Fatalf("%s, prefix %q: %v", c.name, prefix, err)
	}
	return rows.rows, st.Log()
}

func keep[P any](t *testing.T, s *filter.Spec, sc *schema.Schema[P], rows []schema.Row[P]) []schema.Row[P] {
	t.Helper()
	pred, err := filter.Compile(s, sc)
	if err != nil {
		t.Fatal(err)
	}
	return slices.DeleteFunc(slices.Clone(rows), func(r schema.Row[P]) bool { return pred != nil && !pred(&r) })
}

// TestNarrowedListingIsTheFilteredListing is the narrowing's correctness
// condition, end to end through the command's path (filter.NarrowPrefix,
// then WithPrefix, the factory's, then ParseURL as Open reads the root):
// listing the narrowed prefix and filtering gives exactly the rows, in the
// same order, that listing the root's prefix and filtering gives, over
// blob names equal to the prefix, trailing slashes, names beside '/',
// escaped characters, characters at U+E000 and above, long names and an
// empty result; in pages of 1, 2 and 3, short responses, forced splits,
// with and without the seed, sequentially and resumed after a selected
// name. Every request of the narrowed listing (data pages and the seed's
// delimiter listings) asks for the narrowed prefix or one below it, and
// some ask for it exactly; where the includes give no longer prefix every
// request stays at the root's.
func TestNarrowedListingIsTheFilteredListing(t *testing.T) {
	keys := narrowKeys()
	for _, c := range narrowCases() {
		s := c.spec(t)
		root := "az://acct/c/" + c.scope
		n, why := s.NarrowPrefix(c.scope)
		want := c.want
		if want == "" {
			want = c.scope
		}
		if n != want || why != c.reason {
			t.Fatalf("%s: narrowed to %q (%s), want %q (%s)", c.name, n, why, want, c.reason)
		}
		listed := root
		if why == filter.Narrowed {
			if listed = WithPrefix(root, n); (listed == root) != c.declined {
				t.Fatalf("%s: WithPrefix(%q, %q) = %q", c.name, root, n, listed)
			}
		}
		if c.declined {
			want = c.scope
		}
		_, _, prefix, sas, err := ParseURL(listed)
		if err != nil || prefix != want || sas != "" {
			t.Fatalf("%s: %q lists %q, SAS %q (%v), want %q", c.name, listed, prefix, sas, err, want)
		}
		for _, cfg := range narrowConfigs() {
			whole, _ := listScope(t, keys, c.scope, "", cfg)
			wantRows := keep(t, s, &Schema, whole)
			after := ""
			if cfg.after {
				if len(wantRows) < 2 {
					continue
				}
				after = wantRows[len(wantRows)/2].Key
				whole, _ = listScope(t, keys, c.scope, after, cfg)
				wantRows = keep(t, s, &Schema, whole)
			}
			rows, log := listScope(t, keys, prefix, after, cfg)
			got := keep(t, s, &Schema, rows)
			if g, w := keysOf(got), keysOf(wantRows); !slices.Equal(g, w) {
				t.Fatalf("%s, %s: narrowed to %q: %q, want %q", c.name, cfg.name, prefix, g, w)
			}
			for i := range got {
				if got[i].Data.Size != wantRows[i].Data.Size {
					t.Fatalf("%s, %s: row %q differs", c.name, cfg.name, got[i].Key)
				}
			}
			exact := false
			for _, r := range log {
				if !strings.HasPrefix(r.In.Prefix, prefix) {
					t.Fatalf("%s, %s: a request for prefix %q outside %q", c.name, cfg.name, r.In.Prefix, prefix)
				}
				exact = exact || r.In.Prefix == prefix
			}
			if len(log) == 0 || !exact {
				t.Fatalf("%s, %s: %d requests, none for %q", c.name, cfg.name, len(log), prefix)
			}
		}
	}
}

// TestNarrowedVersionsKeepGroups: with --all-versions, a narrowed prefix
// (mid-segment, at U+E000, a name's own) lists every entry of each
// selected name (snapshots, versions, soft-deleted entries) consecutive
// and in Azure's order, the same rows as the root's listing filtered,
// with page seams inside names (pages of 1 and 2), short responses and
// forced splits; every request asks for every entry under the narrowed
// prefix.
func TestNarrowedVersionsKeepGroups(t *testing.T) {
	entries := versionEntries(narrowKeys())
	all := client.Include{Versions: true, Snapshots: true, Deleted: true}
	for _, c := range []narrowCase{
		{name: "mid-segment", include: []string{"logs/2026-0*/**"}, exclude: []string{"*.tmp"}, want: "logs/2026-0"},
		{name: "U+E000", include: []string{"u/a/**"}, want: "u/a/"},
		{name: "a blob's name", include: []string{"logs/2026*"}, want: "logs/2026"},
	} {
		s := c.spec(t)
		if n, why := s.NarrowPrefix(""); n != c.want || why != filter.Narrowed {
			t.Fatalf("%s: %q %s", c.name, n, why)
		}
		for _, page := range []int{1, 2} {
			list := func(prefix string) ([]VersionRow, []azblobtest.Record) {
				st := azblobtest.NewEntryStore(entries)
				st.Short = func(n int) int { return 1 + n%2 }
				p := versionsEngine(t, st, keyrange.Options{Prefix: prefix, Workers: 8, PageSize: page, Seed: page == 2, SplitCoin: coin(int64(page))})
				rows, err := runVersions(t, p, 8, nil)
				if err != nil {
					t.Fatalf("%s, pages of %d, prefix %q: %v", c.name, page, prefix, err)
				}
				return rows, st.Log()
			}
			whole, _ := list("")
			want := keep(t, s, &VersionSchema, whole)
			rows, log := list(c.want)
			got := keep(t, s, &VersionSchema, rows)
			if g, w := versionIDs(got), versionIDs(want); len(want) == 0 || !slices.Equal(g, w) {
				t.Fatalf("%s, pages of %d: %q, want %q", c.name, page, g, w)
			}
			for _, r := range log {
				if !strings.HasPrefix(r.In.Prefix, c.want) || r.In.Delimiter == "" && r.In.Include != all {
					t.Fatalf("%s, pages of %d: a request for %q, include %+v", c.name, page, r.In.Prefix, r.In.Include)
				}
			}
		}
	}
}

// admissionAPI answers Get Account Information as told and records the
// container each call names; no listing is expected.
type admissionAPI struct {
	hns   bool
	err   error
	mu    sync.Mutex
	asked []string
}

func (a *admissionAPI) AccountInfo(_ context.Context, container string) (bool, error) {
	a.mu.Lock()
	a.asked = append(a.asked, container)
	a.mu.Unlock()
	return a.hns, a.err
}

func (a *admissionAPI) List(context.Context, *client.ListInput) (*client.ListOutput, error) {
	return &client.ListOutput{}, nil
}

// TestNarrowedAdmission: namespace admission asks about the container,
// whatever prefix an include narrows the root to, and decides as it does
// for the root: a hierarchical namespace opens as a tree, an unknown one
// refuses unless --azure-assume-flat asserts it, which warns, with the same
// errors and warnings word for word.
func TestNarrowedAdmission(t *testing.T) {
	const root = "az://acct/c"
	narrowed := WithPrefix(root, "logs/2026-0")
	if narrowed == root {
		t.Fatal("not narrowed")
	}
	for _, c := range []struct {
		name string
		hns  bool
		err  error
		flat bool
	}{
		{"hierarchical", true, nil, false},
		{"unknown", false, client.ErrHNSUnknown, false},
		{"unknown, asserted flat", false, client.ErrHNSUnknown, true},
		{"flat", false, nil, false},
	} {
		var outcomes []string
		for _, r := range []string{root, narrowed} {
			api := &admissionAPI{hns: c.hns, err: c.err}
			var warned []string
			var original string
			if r != root {
				original = root
			}
			opened, err := OpenNarrowed(context.Background(), r, original, Options{Client: api, Workers: 1, Flat: c.flat,
				Warn: func(code, msg string) { warned = append(warned, code+": "+msg) }})
			if err == nil {
				_, tree := opened.(*OpenedTree)
				if tree != c.hns {
					t.Fatalf("%s, %s: opened %T, HNS=%v", c.name, r, opened, c.hns)
				}
			}
			if !slices.Equal(api.asked, []string{"c"}) {
				t.Fatalf("%s, %s: admission asked about %q", c.name, r, api.asked)
			}
			outcomes = append(outcomes, fmt.Sprintf("%v | unsupported=%v | %q", err, errors.Is(err, source.ErrUnsupported), warned))
		}
		if outcomes[0] != outcomes[1] {
			t.Fatalf("%s: the root %s, narrowed %s", c.name, outcomes[0], outcomes[1])
		}
		refused := c.err != nil && !c.flat
		if strings.HasPrefix(outcomes[0], "<nil>") == refused || c.flat != strings.Contains(outcomes[0], source.WarnFlatAssumed) {
			t.Fatalf("%s: %s", c.name, outcomes[0])
		}
	}
}

// TestNarrowedVersionsRestart: with --all-versions and --include
// 'logs/2026*', the listing narrows to logs/2026, the name of a blob with
// four entries (a snapshot, a previous version, a soft-deleted previous
// version, the current version). A run interrupted once that group is
// published (its checkpoint the prefix itself, the lower edge of the
// narrowed scope) or after the last selected name, and resumed, lists
// the rest: the published entries and the resumed ones together are the
// uninterrupted narrowed listing, entry for entry and in order, which is
// the whole container's listing filtered; the resume lists nothing of the
// checkpoint's group again and every request asks for the narrowed
// prefix or one below it. Pages of 1 and 2 put seams inside groups.
func TestNarrowedVersionsRestart(t *testing.T) {
	const name = "logs/2026"
	entries := versionEntries(slices.DeleteFunc(narrowKeys(), func(k string) bool { return k == name }))
	for j := range 4 {
		it := client.Item{Name: name, Size: int64(1_000_000 + j), BlobType: "BlockBlob"}
		switch j {
		case 0:
			it.Snapshot = "2026-01-01T00:00:00.0000000Z"
		case 1:
			it.VersionID = "2026-01-02T00:00:00.0000000Z"
		case 2:
			it.VersionID, it.Deleted = "2026-01-02T00:00:01.0000000Z", true
		case 3:
			it.VersionID, it.IsCurrentVersion = "2026-01-04T00:00:00.0000000Z", true
		}
		entries = append(entries, it)
	}
	s := narrowCase{include: []string{"logs/2026*"}}.spec(t)
	prefix, why := s.NarrowPrefix("")
	if prefix != name || why != filter.Narrowed {
		t.Fatalf("narrowed to %q (%s)", prefix, why)
	}
	for _, page := range []int{1, 2, 1000} {
		list := func(scope, after string) []string {
			t.Helper()
			st := azblobtest.NewEntryStore(entries)
			st.Short = func(n int) int { return 1 + n%2 }
			p := versionsEngine(t, st, keyrange.Options{Prefix: scope, Workers: 4, PageSize: page, SplitCoin: coin(int64(page))})
			var from *string
			if after != "" {
				if err := p.Restart(after); err != nil {
					t.Fatal(err)
				}
				from = &after
			}
			rows, err := runVersions(t, p, 4, from)
			if err != nil {
				t.Fatalf("pages of %d, prefix %q, after %q: %v", page, scope, after, err)
			}
			for _, r := range st.Log() {
				if !strings.HasPrefix(r.In.Prefix, scope) {
					t.Fatalf("pages of %d, prefix %q, after %q: a request for %q", page, scope, after, r.In.Prefix)
				}
			}
			return versionIDs(keep(t, s, &VersionSchema, rows))
		}
		whole, narrowed := list("", ""), list(prefix, "")
		if !slices.Equal(narrowed, whole) || len(narrowed) < 5 || !strings.HasPrefix(narrowed[3], fmt.Sprintf("%q#", name)) ||
			strings.HasPrefix(narrowed[4], fmt.Sprintf("%q#", name)) {
			t.Fatalf("pages of %d: narrowed %q, the whole container filtered %q", page, narrowed, whole)
		}
		for _, after := range []string{name, "logs/2026-1x"} {
			n := 0
			for n < len(narrowed) && !strings.HasPrefix(narrowed[n], fmt.Sprintf("%q#", after)) {
				n++
			}
			for n < len(narrowed) && strings.HasPrefix(narrowed[n], fmt.Sprintf("%q#", after)) {
				n++
			}
			if got := append(slices.Clone(narrowed[:n]), list(prefix, after)...); !slices.Equal(got, narrowed) {
				t.Fatalf("pages of %d, resumed after %q: %q, want %q", page, after, got, narrowed)
			}
		}
	}
}
