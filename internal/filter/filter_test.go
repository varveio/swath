// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/internal/journal"
	"github.com/varveio/swath/internal/schema"
)

type obj struct {
	size, mtime int64
	class       string
}

var sch = &schema.Schema[obj]{Info: schema.Info{ID: "t", Key: schema.String, StorageClass: "storage_class"}, Columns: []schema.Column[obj]{
	{Spec: schema.Spec{Name: "size"}, Int: func(o *obj) (int64, bool) { return o.size, true }},
	{Spec: schema.Spec{Name: "last_modified"}, Int: func(o *obj) (int64, bool) { return o.mtime, o.mtime != 0 }},
	{Spec: schema.Spec{Name: "storage_class"}, Str: func(o *obj) (string, bool) { return o.class, o.class != "" }},
}}

func us(t *testing.T, s string) int64 {
	v, err := ParseTime(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// kept is the keys of rows s keeps.
func kept(t *testing.T, s Spec, rows []schema.Row[obj]) []string {
	t.Helper()
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	keep, err := Compile(&s, sch)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for i := range rows {
		if keep(&rows[i]) {
			got = append(got, rows[i].Key)
		}
	}
	return got
}

func keyRows(keys ...string) []schema.Row[obj] {
	rows := make([]schema.Row[obj], len(keys))
	for i, k := range keys {
		rows[i] = schema.Row[obj]{Key: k}
	}
	return rows
}

// TestEachFilter: each filter alone and combined.
func TestEachFilter(t *testing.T) {
	jan, feb := us(t, "2026-01-01"), us(t, "2026-02-01T00:00:00Z")
	rows := []schema.Row[obj]{
		{Key: "logs/a.parquet", Data: obj{100, jan, "STANDARD"}},
		{Key: "logs/b.tmp", Data: obj{2048, feb, "GLACIER"}},
		{Key: "data/c.parquet", Data: obj{1 << 20, feb, "standard"}},
		{Key: "data/d", Data: obj{0, 0, ""}},
	}
	for _, tc := range []struct {
		spec Spec
		want []string
	}{
		{Spec{IncludeRegex: []string{`\.parquet$`}}, []string{"logs/a.parquet", "data/c.parquet"}},
		{Spec{IncludeRegex: []string{`parquet`}}, []string{"logs/a.parquet", "data/c.parquet"}}, // found anywhere
		{Spec{ExcludeRegex: []string{`\.tmp$`}}, []string{"logs/a.parquet", "data/c.parquet", "data/d"}},
		{Spec{Include: []string{"*.parquet"}}, []string{"logs/a.parquet", "data/c.parquet"}},
		{Spec{Exclude: []string{"data/*"}}, []string{"logs/a.parquet", "logs/b.tmp"}},
		{Spec{MinSize: "2K"}, []string{"logs/b.tmp", "data/c.parquet"}},
		{Spec{MaxSize: "2048"}, []string{"logs/a.parquet", "logs/b.tmp", "data/d"}},     // inclusive
		{Spec{Since: "2026-02-01"}, []string{"logs/b.tmp", "data/c.parquet", "data/d"}}, // no time: kept
		{Spec{Until: "2026-01-01T00:00:00"}, []string{"logs/a.parquet", "data/d"}},
		{Spec{StorageClasses: []string{"standard"}}, []string{"logs/a.parquet", "data/c.parquet"}},
		{Spec{IncludeRegex: []string{`^data/`}, MinSize: "1", StorageClasses: []string{"STANDARD", "GLACIER"}}, []string{"data/c.parquet"}},
	} {
		if got := kept(t, tc.spec, rows); !slices.Equal(got, tc.want) {
			t.Errorf("%v: %q, want %q", tc.spec.Descriptions(), got, tc.want)
		}
	}
}

// TestGlob: the glob grammar, matched against the key below Base.
func TestGlob(t *testing.T) {
	for _, tc := range []struct {
		pattern, base string
		match, miss   []string
	}{
		// No '/': the last segment at any depth, the whole of it.
		{"*.gz", "", []string{"a.gz", "x/y/a.gz", "dir.gz/", "line1\nline2.gz", "日本.gz"}, []string{"a.gzx", "a.gz/b", "gz", "x/a.gz.tmp"}},
		{"a", "", []string{"a", "x/a", "x/a/"}, []string{"ab", "ba", "a/x", ""}},
		{"a?c", "", []string{"abc", "aéc", "a\nc", "x/abc"}, []string{"ac", "a/c", "abbc"}},
		// A '/' anchors at the root; a leading one only anchors.
		{"logs/*.gz", "", []string{"logs/a.gz", "logs/.gz"}, []string{"x/logs/a.gz", "logs/x/a.gz", "logsa.gz"}},
		{"/a.txt", "", []string{"a.txt"}, []string{"d/a.txt", "/a.txt"}},
		{"logs/", "", []string{"logs/"}, []string{"logs", "logs/a", "x/logs/"}},
		// ** is whole segments, newlines included.
		{"logs/**", "", []string{"logs/a", "logs/a/b", "logs/a/", "logs/a\n/b\n"}, []string{"logs/", "logs", "logsx/a", "x/logs/a"}},
		{"**/x", "", []string{"x", "a/x", "a/b\n/x", "a/x/"}, []string{"ax", "x/a", "a/bx"}},
		{"a/**/b", "", []string{"a/b", "a/x/b", "a/x/y/b"}, []string{"ab", "a/xb", "x/a/b"}},
		{"**", "", []string{"", "a", "a/b\nc/d"}, nil},
		{"/**/*.d", "", []string{"x.d", "a/b/x.d"}, []string{"x.dd"}},
		// Classes never match '/'.
		{"[abc].txt", "", []string{"a.txt", "x/c.txt"}, []string{"d.txt", "ab.txt"}},
		{"[a-c]x", "", []string{"bx"}, []string{"dx"}},
		{"[!a-c]x", "", []string{"dx", "éx", "\nx"}, []string{"ax", "x"}},
		{"/x[+-0]y", "", []string{"x.y", "x0y", "x+y"}, []string{"x/y", "x1y"}},
		{"[]a]", "", []string{"]", "a"}, []string{"b"}},
		{"[a-]", "", []string{"a", "-"}, []string{"b"}},
		{"[!/]", "", []string{"a"}, []string{"/"}},
		{`[\]]`, "", []string{"]"}, []string{`\`}},
		// Alternatives.
		{"*.{gz,zst}", "", []string{"a.gz", "b/a.zst"}, []string{"a.txt", "a.gzst"}},
		{"{a,b/c}", "", []string{"a", "b/c"}, []string{"x/a", "x/b/c"}}, // a '/' in it anchors
		{"x{,.bak}", "", []string{"x", "x.bak"}, []string{"x.b"}},
		// Escapes and regex metacharacters are literal.
		{`\*.txt`, "", []string{"*.txt"}, []string{"a.txt"}},
		{`a\?`, "", []string{"a?"}, []string{"ab"}},
		{"a.b(c)+$^|", "", []string{"a.b(c)+$^|"}, []string{"axb(c)+$^|", "a.b(cc)$^|"}},
		{"日本/*", "", []string{"日本/a"}, []string{"日本/a/b", "x/日本/a"}},
		// The target is below Base, the source's prefix to its last '/'.
		{"2026/*", "logs/", []string{"logs/2026/a"}, []string{"logs/logs/2026/a", "logs/x/2026/a"}},
		{"*.gz", "logs/", []string{"logs/a.gz", "logs/x/a.gz"}, []string{"logs/a.txt"}},
	} {
		s := Spec{Include: []string{tc.pattern}, Base: tc.base}
		got := kept(t, s, keyRows(append(slices.Clone(tc.match), tc.miss...)...))
		if !slices.Equal(got, tc.match) {
			t.Errorf("%q (base %q) kept %q, want %q", tc.pattern, tc.base, got, tc.match)
		}
	}
	// --exclude dir/** drops what is below dir/ and keeps its row; dir/
	// drops the row.
	for pattern, want := range map[string][]string{"2025/**": {"2025/", "x"}, "2025/": {"2025/a", "2025/b/", "2025/b/c", "x"}} {
		if got := kept(t, Spec{Exclude: []string{pattern}}, keyRows("2025/", "2025/a", "2025/b/", "2025/b/c", "x")); !slices.Equal(got, want) {
			t.Errorf("--exclude %s kept %q, want %q", pattern, got, want)
		}
	}
}

// TestGlobMalformed: a malformed or empty pattern is refused (a usage
// error), naming the flag.
func TestGlobMalformed(t *testing.T) {
	for _, p := range []string{"", "/", "a**b", "***", "**a", "a**", "a/**b", "b**/x", "{a,{b}}", "{a", "a}", "[abc", "[", "[z-a]",
		`a\`, "[/]", "[a-z/]", "[[:alpha:]]", "{**,a}", "x/{a,**}"} {
		err := (&Spec{Include: []string{p}}).Validate()
		if err == nil || !strings.HasPrefix(err.Error(), "--include") {
			t.Errorf("%q: %v", p, err)
		}
		if err := (&Spec{Exclude: []string{p}}).Validate(); err == nil {
			t.Errorf("%q accepted as --exclude", p)
		}
	}
	for _, s := range []Spec{{IncludeRegex: []string{""}}, {ExcludeRegex: []string{"("}}, {IncludeRegex: []string{"a", `(a)\1`}}} {
		if err := s.Validate(); err == nil {
			t.Errorf("%+v accepted", s)
		}
	}
}

// TestGlobLinear: a glob runs in RE2's linear time on a long key that does
// not match (a backtracking matcher takes exponential time here).
func TestGlobLinear(t *testing.T) {
	key := strings.Repeat("a", 100000)
	start := time.Now()
	if got := kept(t, Spec{Include: []string{strings.Repeat("*a", 20) + "b"}}, keyRows(key)); len(got) != 0 {
		t.Fatal("matched")
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("took %v", d)
	}
}

// TestAlgebra: a row is kept when no include is given or any include,
// glob or regex, matches, and no exclude matches; the order rules are
// given in does not matter.
func TestAlgebra(t *testing.T) {
	keys := []string{"a.gz", "a.txt", "b/c.gz", "b/d.txt", "tmp/e.gz", "f.log"}
	for _, tc := range []struct {
		spec Spec
		want []string
	}{
		{Spec{Include: []string{"*.gz"}}, []string{"a.gz", "b/c.gz", "tmp/e.gz"}},
		{Spec{Include: []string{"*.gz", "*.txt"}}, []string{"a.gz", "a.txt", "b/c.gz", "b/d.txt", "tmp/e.gz"}},
		{Spec{Exclude: []string{"*.gz"}}, []string{"a.txt", "b/d.txt", "f.log"}},
		{Spec{Exclude: []string{"*.gz", "tmp/**"}}, []string{"a.txt", "b/d.txt", "f.log"}},
		{Spec{Include: []string{"*.gz"}, Exclude: []string{"tmp/**"}}, []string{"a.gz", "b/c.gz"}},
		{Spec{Include: []string{"*.gz"}, IncludeRegex: []string{`log$`}}, []string{"a.gz", "b/c.gz", "tmp/e.gz", "f.log"}},
		{Spec{Include: []string{"b/*"}, ExcludeRegex: []string{`txt`}}, []string{"b/c.gz"}},
		{Spec{IncludeRegex: []string{"^b/"}, Exclude: []string{"*.gz"}}, []string{"b/d.txt"}},
		{Spec{Include: []string{"*.gz"}, Exclude: []string{"*.gz"}}, []string{}}, // an exclude always wins
	} {
		want := kept(t, tc.spec, keyRows(keys...))
		if !slices.Equal(want, tc.want) {
			t.Errorf("%v: %q, want %q", tc.spec.Descriptions(), want, tc.want)
		}
		r := tc.spec
		r.Include, r.Exclude, r.IncludeRegex, r.ExcludeRegex = reversed(r.Include), reversed(r.Exclude), reversed(r.IncludeRegex), reversed(r.ExcludeRegex)
		if got := kept(t, r, keyRows(keys...)); !slices.Equal(got, want) || !slices.Equal(r.Descriptions(), tc.spec.Descriptions()) {
			t.Errorf("%v reversed: %q, %v", tc.spec.Descriptions(), got, r.Descriptions())
		}
	}
}

func reversed(s []string) []string {
	r := slices.Clone(s)
	slices.Reverse(r)
	return r
}

// TestDescriptions: coverage.filters and the identity hold each rule once,
// in a canonical order: the same rules given in another order, or twice,
// are the same identity; other rules are not.
func TestDescriptions(t *testing.T) {
	s := Spec{Include: []string{"b", "a"}, IncludeRegex: []string{"r"}, Exclude: []string{"x"}, ExcludeRegex: []string{"y"}, MinSize: "1K", MaxSize: "2K",
		Since: "2026-01-01", Until: "2026-02-01", StorageClasses: []string{"STANDARD", "GLACIER"}}
	want := "include=a include=b include_regex=r exclude=x exclude_regex=y min_size=1K max_size=2K modified_after=2026-01-01 modified_before=2026-02-01 storage_class=STANDARD,GLACIER"
	if got := strings.Join(s.Descriptions(), " "); got != want {
		t.Errorf("descriptions %q", got)
	}
	id := func(s Spec) journal.Identity { return journal.Identity{Filters: s.Descriptions()} }
	same := Spec{Include: []string{"a", "b", "a"}, IncludeRegex: []string{"r"}, Exclude: []string{"x"}}
	other := Spec{Include: []string{"b", "a"}, IncludeRegex: []string{"r"}, Exclude: []string{"x"}}
	if err := journal.Check(id(other), id(same)); err != nil {
		t.Errorf("reordered rules refused: %v", err)
	}
	for _, d := range []Spec{{Include: []string{"a"}, IncludeRegex: []string{"r"}, Exclude: []string{"x"}}, {Include: []string{"a", "b"}, Exclude: []string{"r", "x"}},
		{Include: []string{"a", "b"}, IncludeRegex: []string{"r"}, ExcludeRegex: []string{"x"}}} {
		if err := journal.Check(id(other), id(d)); err == nil {
			t.Errorf("%v accepted for %v", d.Descriptions(), other.Descriptions())
		}
	}
	if !(&Spec{}).Empty() || (&Spec{Since: "x"}).Empty() || (&Spec{ExcludeRegex: []string{"x"}}).Empty() {
		t.Error("Empty")
	}
}

// TestNarrowPrefix: the includes' common literal prefix narrows the listed
// prefix when it extends scope; each disabling case says why.
func TestNarrowPrefix(t *testing.T) {
	for _, tc := range []struct {
		spec      Spec
		scope     string
		want, why string
	}{
		{Spec{IncludeRegex: []string{`^logs/2026/`}}, "", "logs/2026/", Narrowed},
		{Spec{IncludeRegex: []string{`^logs/2026/.*\.gz$`}}, "logs/", "logs/2026/", Narrowed},
		{Spec{IncludeRegex: []string{`^logs/`}}, "logs/2026/", "logs/2026/", NarrowNoLonger},
		{Spec{IncludeRegex: []string{`^data/`}}, "logs/", "logs/", NarrowNoLonger},
		{Spec{IncludeRegex: []string{`logs/`}}, "", "", NarrowUnanchored},
		{Spec{IncludeRegex: []string{`^(?i)logs/`}}, "", "", NarrowUnanchored},
		{Spec{IncludeRegex: []string{`^a|^b`}}, "", "", NarrowUnanchored},
		{Spec{IncludeRegex: []string{`^l+`}}, "", "", NarrowUnanchored},
		{Spec{IncludeRegex: []string{`^ab+`}}, "", "a", Narrowed},
		{Spec{Include: []string{"logs/2026/*.gz"}}, "", "logs/2026/", Narrowed},
		{Spec{Include: []string{"/2026/**"}, Base: "logs/"}, "logs/20", "logs/2026/", Narrowed},
		{Spec{Include: []string{"/19/**"}, Base: "logs/"}, "logs/20", "logs/20", NarrowNoLonger},
		{Spec{Include: []string{`a\*b/*`}}, "", "a*b/", Narrowed},
		{Spec{Include: []string{"logs/{2025,2026}/*"}}, "", "logs/", Narrowed},
		{Spec{Include: []string{"logs/2026/*", "logs/2025/*"}}, "", "logs/202", Narrowed},
		{Spec{Include: []string{"logs/2026/*"}, IncludeRegex: []string{"^logs/x"}}, "", "logs/", Narrowed},
		{Spec{Include: []string{"logs/2026/*", "data/*"}}, "", "", NarrowNoLonger},
		{Spec{Include: []string{"logs/2026/*", "*.gz"}}, "", "", NarrowUnanchored}, // a basename glob
		{Spec{Include: []string{"logs/2026/*"}, IncludeRegex: []string{"x"}}, "", "", NarrowUnanchored},
		{Spec{Include: []string{"**/x"}}, "", "", NarrowNoLonger},
		{Spec{Include: []string{"d/é/*", "d/è/*"}}, "", "d/", Narrowed}, // never inside a character
		{Spec{Include: []string{"logs/*"}, Exclude: []string{"logs/x/*"}, ExcludeRegex: []string{"y"}}, "", "logs/", Narrowed},
		{Spec{Exclude: []string{"logs/*"}}, "", "", ""},
		{Spec{}, "x", "x", ""},
	} {
		if err := tc.spec.Validate(); err != nil {
			t.Fatal(err)
		}
		if got, why := tc.spec.NarrowPrefix(tc.scope); got != tc.want || why != tc.why {
			t.Errorf("%v in %q: %q %s, want %q %s", tc.spec.Descriptions(), tc.scope, got, why, tc.want, tc.why)
		}
	}
}

// TestNarrowCoversKept: for random keys and include rules, every key the
// filters keep lies under the narrowed prefix (the narrowing never drops a
// row), and the narrowed prefix lies under the source's.
func TestNarrowCoversKept(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"a", "b", "/", "é", "è", "*", "\n"}
	word := func(n int) string {
		var b strings.Builder
		for range rng.IntN(n) {
			b.WriteString(alphabet[rng.IntN(len(alphabet))])
		}
		return b.String()
	}
	var keys []string
	for range 400 {
		keys = append(keys, word(8))
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	globTail := []string{"", "*", "**", "/**", "?", "{a,b}", "[ab]", "/*", "*b"}
	narrowed := 0
	for range 3000 {
		scope := word(4)
		base := scope[:strings.LastIndexByte(scope, '/')+1]
		var s Spec
		s.Base = base
		for range 1 + rng.IntN(3) {
			lit := strings.NewReplacer("*", `\*`).Replace(word(5))
			switch rng.IntN(3) {
			case 0:
				s.IncludeRegex = append(s.IncludeRegex, "^"+regexp.QuoteMeta(lit)+[]string{"", ".*", "b", "(a|b)"}[rng.IntN(4)])
			default:
				p := "/" + lit + globTail[rng.IntN(len(globTail))]
				if _, err := globRegexp(p); err != nil {
					continue
				}
				s.Include = append(s.Include, p)
			}
		}
		if rng.IntN(4) == 0 {
			s.Exclude = []string{"*b"}
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("%v: %v", s.Descriptions(), err)
		}
		prefix, why := s.NarrowPrefix(scope)
		if !strings.HasPrefix(prefix, scope) || (why == Narrowed) != (prefix != scope) {
			t.Fatalf("%v in %q: %q %s", s.Descriptions(), scope, prefix, why)
		}
		if why == Narrowed {
			narrowed++
		}
		keep, err := Compile(&s, sch)
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			r := schema.Row[obj]{Key: k}
			if strings.HasPrefix(k, scope) && (keep == nil || keep(&r)) && !strings.HasPrefix(k, prefix) {
				t.Fatalf("%v in %q: %q is kept but outside the narrowed prefix %q", s.Descriptions(), scope, k, prefix)
			}
		}
	}
	if narrowed < 300 {
		t.Fatalf("only %d of the rule sets narrowed", narrowed)
	}
}

// TestSize: one size syntax; K, M, G, T and KiB... binary, KB... decimal.
func TestSize(t *testing.T) {
	for in, want := range map[string]int64{"1k": 1 << 10, "1K": 1 << 10, "1KiB": 1 << 10, "1kib": 1 << 10, "1KB": 1000, "1kb": 1000, "256MB": 256e6, "256M": 256 << 20,
		"256MiB": 256 << 20, "1.5GiB": 3 << 29, "1.5GB": 15e8, "2G": 2 << 30, "2T": 2 << 40, "2TiB": 2 << 40, "2TB": 2e12, "0": 0, "7 B": 7, "7b": 7, "7": 7, "0.5K": 512, "1.5KB": 1500} {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "k", "1.1b", "-1", "3x", "99999999999T", "1.0001KB", "1P", "1PB", "1kibb"} {
		if _, err := ParseSize(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if err := (&Spec{MinSize: "64MB", MaxSize: "64000000"}).Validate(); err != nil {
		t.Errorf("64MB is 64000000 bytes: %v", err)
	}
}

// TestTimes: the absolute forms, and a duration before the run's start
// resolved once to the time it names.
func TestTimes(t *testing.T) {
	if a, b := us(t, "2026-01-01T12:00:00+02:00"), us(t, "2026-01-01T10:00"); a != b {
		t.Errorf("offset %d, local %d", a, b)
	}
	now := time.Date(2026, 11, 10, 12, 30, 15, 500, time.UTC)
	for in, want := range map[string]string{"7d": "2026-11-03T12:30:15Z", "1w": "2026-11-03T12:30:15Z", "1d12h": "2026-11-09T00:30:15Z", "90m": "2026-11-10T11:00:15Z",
		"0s": "2026-11-10T12:30:15Z", "2026-01-01": "2026-01-01", " 2026-01-01T10:00 ": " 2026-01-01T10:00 "} {
		s := Spec{Since: in, Until: in}
		s.Resolve(now)
		if s.Since != want || s.Until != want {
			t.Errorf("%q: since %q until %q, want %q", in, s.Since, s.Until, want)
		}
	}
	for _, s := range []Spec{{MinSize: "2k", MaxSize: "1k"}, {Since: "2026-02-01", Until: "2026-01-01"}, {Since: "yesterday"}, {Since: "-7d"}, {Since: "7"},
		{Until: "7x"}, {StorageClasses: []string{" "}}} {
		s.Resolve(now)
		if err := s.Validate(); err == nil {
			t.Errorf("%+v accepted", s)
		}
	}
	s := Spec{Since: "7d"}
	s.Resolve(now)
	if err := s.Validate(); err != nil || s.since != now.Add(-7*24*time.Hour).Truncate(time.Second).UnixMicro() {
		t.Errorf("7d: %v %d", err, s.since)
	}
}

func TestMissingColumn(t *testing.T) {
	s := &Spec{MinSize: "1"}
	if _, err := Compile(s, &schema.Schema[obj]{Info: schema.Info{ID: "bare"}}); err == nil {
		t.Fatal("size filter without a size column")
	}
	if size, mtime, class := (&Spec{MaxSize: "1", StorageClasses: []string{"X"}}).Reads(); !size || mtime || !class {
		t.Errorf("Reads %v %v %v", size, mtime, class)
	}
}
