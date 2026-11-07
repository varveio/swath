// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/varveio/swath/go/internal/replay/s3"
)

// The live modes send the provider conformance matrix: the case
// definitions of scripts/provider-conformance (matrix.py, s3_live.py,
// gcs_live.py), with which the native evidence was captured. Each case
// here is one of those, its query written as a template over the
// campaign corpus's names: bound to the corpus (the run prefix plus each
// name) a template renders the campaign's query byte for byte
// (TestProviderMatrixMatchesTheCampaign); bound to a recorded public
// bucket it renders the same question over names that bucket holds. A
// case whose question needs names only an owned bucket can hold says so
// and is not sent.

// providerCase is one case of the provider matrix.
type providerCase struct {
	ID   string
	Kind string // exchange, walk, token or sdk_run
	// Tmpl is the query: literal text with {name} for a corpus name
	// relative to the run prefix ({} is the run prefix itself), escaped as
	// the campaign escapes values (every byte but RFC 3986's unreserved
	// characters and '/'); {name!} escapes '/' too; {=value} is a value
	// that is not a corpus name (a delimiter), escaped like {name}.
	Tmpl string
	// TokenFrom and Page name the walk page whose next token the case
	// sends; Mutation alters it first (tampered needs no source).
	TokenFrom string
	Page      int
	Mutation  string
	Header    [][2]string
	// Owned, when set, is why the question needs an owned bucket's corpus.
	Owned string
	// Bind maps a template name to another name of the fixture binding for
	// this case alone, where a corpus name plays a different part than its
	// role elsewhere (a landmark before a prefix, a scope a spelling probe
	// can run over).
	Bind map[string]string
}

const (
	ownUnicode  = "names across the surrogate block, the supplementary planes, NFC/NFD and case pairs (the u/ group)"
	ownReserved = "names with reserved and escape-like characters, whose presence the spelling decides (the p/ group)"
	ownNames    = "names with control, XML-invalid, dot-segment, trailing-space and 1,024-byte spellings (the n/ group)"
	ownLimits   = "creation probes at and over the name limits (the q/ group)"
	ownSDK      = "an official client library's paginator (sdk_run); the driver sends raw HTTP"
)

// spellingCases are matrix.py's spelling_probes for the prefix parameter,
// with selector (list-type=2& on S3) before each. The ones a public
// bucket answers the same question for run over the r/ or d/ scope.
func spellingCases(provider, selector string) []providerCase {
	c := func(suffix, tmpl, owned string, bind map[string]string) providerCase {
		return providerCase{ID: provider + "-05" + suffix, Kind: "exchange", Tmpl: selector + tmpl, Owned: owned, Bind: bind}
	}
	onR := map[string]string{"p/": "r/"}
	return []providerCase{
		c("a", "prefix={p/}a+b", ownReserved, nil),
		c("b", "prefix={p/}a%2Bb", ownReserved, nil),
		c("c", "prefix={p/}a%20b", ownReserved, nil),
		c("d", "prefix={p/}a%252Bb", ownReserved, nil),
		// Refused versus literal: a refusal discriminates without the names.
		c("e", "prefix={p/}a%ZZ", "", onR),
		c("f", "prefix={p/}pct%", "", onR),
		c("g", "prefix={p/}%FF", "", onR),
		// Duplicates: two scopes with different listings tell first from last.
		c("h", "prefix={p/a}&prefix={p/a}", "", map[string]string{"p/a": "r/"}),
		c("i", "prefix={p/a}&prefix={p/k}", "", map[string]string{"p/a": "r/", "p/k": "r/k01"}),
		c("j", "prefix={p/}k%26v%3D1", ownReserved, nil),
		c("k", "prefix={p/}q?x", ownReserved, nil),
		c("l", "prefix={p/!}", "", map[string]string{"p/": "d/"}),
		c("m", "prefix={p/}a%2bb", ownReserved, nil),
		c("n", "prefix={p/}&delimiter", "", map[string]string{"p/": "d/"}),
		c("o", "prefix={p/}", ownReserved, nil),
		c("p", "prefix={p/}end%20", ownReserved, nil),
		c("q", "prefix={p/}eq=x", ownReserved, nil),
	}
}

// s3ProviderCases is s3_live.py's matrix.
func s3ProviderCases() []providerCase {
	var out []providerCase
	ex := func(id, tmpl string) {
		out = append(out, providerCase{ID: id, Kind: "exchange", Tmpl: "list-type=2&" + tmpl})
	}
	walk := func(id, tmpl string) {
		out = append(out, providerCase{ID: id, Kind: "walk", Tmpl: "list-type=2&" + tmpl})
	}
	tok := func(id, tmpl, from, mutation string) {
		out = append(out, providerCase{ID: id, Kind: "token", Tmpl: "list-type=2&" + tmpl, TokenFrom: from, Mutation: mutation})
	}
	owned := func(id, kind, tmpl, why string) {
		out = append(out, providerCase{ID: id, Kind: kind, Tmpl: "list-type=2&" + tmpl, Owned: why})
	}
	ex("s3-01a", "prefix={r/}&start-after={r/k02}")
	ex("s3-01b", "prefix={r/}&start-after={r/k04}")
	ex("s3-01c", "prefix={r/}&start-after={}")
	ex("s3-01d", "prefix={r/}&start-after={s}")
	ex("s3-01e", "prefix={r/}&start-after={r/}")
	ex("s3-01f", "start-after={r/k06}&max-keys=3")
	ex("s3-01g", "prefix={r/}&start-after=")
	ex("s3-02a", "prefix={x/a}")
	ex("s3-02b", "prefix={none/}")
	ex("s3-03a", "prefix={d/}&delimiter=/")
	ex("s3-03b", "prefix={d/}&delimiter=/&start-after={d/a/}")
	ex("s3-03c", "prefix={d/}&delimiter=/&start-after={d/a/m}")
	ex("s3-03d", "prefix={d/}&delimiter=/&start-after={d/b/3}")
	ex("s3-03e", "prefix={d/}&delimiter=/&start-after={d/a0}")
	ex("s3-03f", "prefix={d/a/}&delimiter=/")
	ex("s3-03g", "prefix={d/a}&delimiter=/")
	ex("s3-03h", "prefix={d/}&delimiter=%7C")
	ex("s3-03i", "prefix={d/}&delimiter={=/1}")
	ex("s3-03j", "prefix={d/}&delimiter={=-g}")
	ex("s3-03k", "prefix={d/}&delimiter=")
	ex("s3-03l", "delimiter=/&start-after={d/}")
	owned("s3-04a", "exchange", "prefix={u/}", ownUnicode)
	owned("s3-04b", "exchange", "prefix={u/}&start-after={u/\ue000}", ownUnicode)
	owned("s3-04c", "exchange", "prefix={u/é}", ownUnicode)
	owned("s3-04d", "exchange", "prefix={u/E}", ownUnicode)
	owned("s3-04e", "exchange", "prefix={u/}&encoding-type=url", ownUnicode)
	out = append(out, spellingCases("s3", "list-type=2&")...)
	owned("s3-05r", "exchange", "prefix={p/}&start-after={p/}a+b", ownReserved)
	for _, v := range [][2]string{{"a", "0"}, {"f", "-1"}, {"g", "abc"}, {"h", "2147483648"}, {"i", "9223372036854775808"},
		{"j", "1.5"}, {"l", "002"}, {"o", "2147483647"}} {
		ex("s3-06"+v[0], "prefix={s/}&max-keys="+v[1])
	}
	ex("s3-06b", "prefix={b/}&max-keys=0")
	walk("s3-06c", "prefix={s/}&max-keys=1")
	walk("s3-06d", "prefix={b/0}&max-keys=1000")
	ex("s3-06e", "prefix={b/}&max-keys=1001")
	ex("s3-06k", "prefix={s/}&max-keys=+2")
	ex("s3-06m", "prefix={s/}&max-keys=1&max-keys=2")
	ex("s3-06n", "prefix={s/}&max-keys=")
	walk("s3-07a", "prefix={}&delimiter=/&max-keys=1")
	walk("s3-07b", "prefix={b/}")
	walk("s3-07c", "prefix={d/}&delimiter=/&max-keys=2")
	walk("s3-07d", "prefix={d/}&delimiter=/&start-after={d/a/m}&max-keys=1")
	walk("s3-07e", "prefix={r/}&max-keys=4")
	walk("s3-07n", "prefix={d/}&max-keys=2")
	walk("s3-07q", "prefix={r/}&max-keys=2")
	same := "prefix={r/}&max-keys=2"
	tok("s3-07g", same, "", "tampered")
	tok("s3-07h", same, "s3-07q", "")
	tok("s3-07i", "prefix={x/}&max-keys=2", "s3-07q", "")
	tok("s3-07j", "prefix={r/}&max-keys=3", "s3-07q", "")
	tok("s3-07k", "prefix={d/}&delimiter=/&max-keys=2", "s3-07n", "")
	tok("s3-07o", "prefix={d/}&max-keys=2", "s3-07c", "")
	tok("s3-07r", same, "s3-07q", "flip")
	tok("s3-07s", same, "s3-07q", "truncate")
	tok("s3-07x", same, "s3-07q", "flip-decoded")
	tok("s3-07t", "prefix={r/k01}&max-keys=2", "s3-07q", "")
	tok("s3-07u", "prefix={r/}&max-keys=2&start-after={r/k01}", "s3-07q", "")
	tok("s3-07v", "prefix={r/}&max-keys=2&start-after={r/k06}", "s3-07q", "")
	ex("s3-07w", "prefix={r/}&max-keys=2&continuation-token=")
	owned("s3-08a", "exchange", "prefix={n/}", ownNames)
	owned("s3-08b", "exchange", "prefix={n/}&encoding-type=url", ownNames)
	owned("s3-08c", "walk", "prefix={p/}&delimiter=%20&start-after={p/a b}&encoding-type=url&max-keys=2", ownReserved)
	ex("s3-08d", "prefix={s/}&encoding-type=bogus")
	owned("s3-08e", "exchange", "prefix={p/}&encoding-type=url", ownReserved)
	owned("s3-08f", "exchange", "prefix={q/}&encoding-type=url", ownLimits)
	for _, id := range []string{"s3-09a", "s3-09b", "s3-09c"} {
		out = append(out, providerCase{ID: id, Kind: "sdk_run", Tmpl: "list-type=2", Owned: ownSDK})
	}
	ex("s3-11a", "prefix={s/}&fetch-owner=true")
	ex("s3-11b", "prefix={s/}&fetch-owner=false")
	ex("s3-11c", "prefix={s/}&fetch-owner=yes")
	out = append(out, providerCase{ID: "s3-11d", Kind: "exchange", Tmpl: "list-type=2&prefix={s/}",
		Header: [][2]string{{"x-amz-optional-object-attributes", "RestoreStatus"}}})
	out = append(out,
		providerCase{ID: "s3-12a", Kind: "exchange", Tmpl: "prefix={r/}&marker={r/k02}"},
		providerCase{ID: "s3-12b", Kind: "exchange", Tmpl: "prefix={d/}&delimiter=/&max-keys=1"},
		providerCase{ID: "s3-12c", Kind: "exchange", Tmpl: "list-type=3&prefix={s/}"},
		providerCase{ID: "s3-12d", Kind: "exchange", Tmpl: "list-type=2&list-type=2&prefix={s/}"})
	ex("s3-12e", "prefix={r/}&marker={r/k02}")
	return out
}

// gcsProviderCases is gcs_live.py's matrix.
func gcsProviderCases() []providerCase {
	var out []providerCase
	ex := func(id, tmpl string) { out = append(out, providerCase{ID: id, Kind: "exchange", Tmpl: tmpl}) }
	walk := func(id, tmpl string) { out = append(out, providerCase{ID: id, Kind: "walk", Tmpl: tmpl}) }
	tok := func(id, tmpl, from, mutation string) {
		out = append(out, providerCase{ID: id, Kind: "token", Tmpl: tmpl, TokenFrom: from, Mutation: mutation})
	}
	owned := func(id, kind, tmpl, why string) {
		out = append(out, providerCase{ID: id, Kind: kind, Tmpl: tmpl, Owned: why})
	}
	ex("gcs-01a", "prefix={r/}&startOffset={r/k02}")
	ex("gcs-01b", "prefix={r/}&endOffset={r/k05}")
	ex("gcs-01c", "prefix={r/}&startOffset={r/k03}&endOffset={r/k03}")
	ex("gcs-01d", "prefix={r/}&startOffset={r/k04}")
	ex("gcs-01e", "prefix={r/}&endOffset={r/k04}")
	ex("gcs-01f", "prefix={r/}&startOffset={r/k06}&endOffset={r/k02}")
	ex("gcs-01g", "startOffset={r/k02}&endOffset={r/k06}")
	ex("gcs-01h", "prefix={r/}&startOffset={}&endOffset={zzz}")
	ex("gcs-01i", "prefix={r/}&startOffset=&endOffset=")
	ex("gcs-01j", "prefix={r/}&startOffset={r/}")
	ex("gcs-01k", "prefix={r/}&endOffset={r/}")
	ex("gcs-02a", "prefix={x/a}&startOffset={w}&endOffset={x/abc}")
	ex("gcs-02b", "prefix={x/a}&startOffset={x/ab}&endOffset={y}")
	// Both offsets before the prefix: in the corpus r/ and s sort before x/.
	out = append(out, providerCase{ID: "gcs-02c", Kind: "exchange", Tmpl: "prefix={x/a}&startOffset={r/}&endOffset={s}",
		Bind: map[string]string{"r/": "", "s": "x/"}})
	ex("gcs-02d", "prefix={x/a}&startOffset={y}&endOffset={z}")
	ex("gcs-02e", "prefix={x/a}")
	ex("gcs-02f", "prefix={x/ab}&endOffset={x/ab}")
	ex("gcs-03a", "prefix={d/}&delimiter=/")
	ex("gcs-03b", "prefix={d/}&delimiter=/&startOffset={d/a/}")
	ex("gcs-03c", "prefix={d/}&delimiter=/&startOffset={d/a/m}")
	ex("gcs-03d", "prefix={d/}&delimiter=/&startOffset={d/b/3}")
	ex("gcs-03e", "prefix={d/}&delimiter=/&startOffset={d/a0}")
	ex("gcs-03f", "prefix={d/}&delimiter=/&endOffset={d/a/m}")
	ex("gcs-03g", "prefix={d/}&delimiter=/&endOffset={d/a/}")
	ex("gcs-03h", "prefix={d/}&delimiter=/&endOffset={d/a/1}")
	ex("gcs-03i", "prefix={d/a/}&delimiter=/")
	ex("gcs-03j", "prefix={d/a}&delimiter=/")
	ex("gcs-03k", "prefix={d/}&delimiter=%7C")
	ex("gcs-03l", "prefix={d/}&delimiter={=/1}")
	ex("gcs-03m", "prefix={d/}&delimiter={=-g}")
	ex("gcs-03n", "prefix={d/}&delimiter=/&startOffset={d/a/m}&endOffset={d/b/2}")
	ex("gcs-03o", "prefix={d/}&delimiter=")
	ex("gcs-03p", "prefix={d/}&delimiter=/&includeTrailingDelimiter=true")
	ex("gcs-03q", "delimiter=/&startOffset={d/}&endOffset={e}")
	owned("gcs-04a", "exchange", "prefix={u/}", ownUnicode)
	owned("gcs-04b", "exchange", "prefix={u/}&startOffset={u/\ue000}", ownUnicode)
	owned("gcs-04c", "exchange", "prefix={u/}&endOffset={u/\U00010000}", ownUnicode)
	owned("gcs-04d", "exchange", "prefix={u/é}", ownUnicode)
	owned("gcs-04e", "exchange", "prefix={u/E}", ownUnicode)
	owned("gcs-04f", "exchange", "prefix={u/}&startOffset={u/}%f0%9f%98%80", ownUnicode)
	out = append(out, spellingCases("gcs", "")...)
	owned("gcs-05r", "exchange", "prefix={p/}&startOffset={p/}eq=x", ownReserved)
	ex("gcs-06a", "prefix={s/}&maxResults=0")
	walk("gcs-06b", "prefix={b/}&maxResults=0")
	walk("gcs-06c", "prefix={s/}&maxResults=1")
	walk("gcs-06d", "prefix={b/0}&maxResults=1000")
	ex("gcs-06e", "prefix={b/}&maxResults=1001")
	for _, v := range [][2]string{{"f", "-1"}, {"g", "abc"}, {"h", "2147483648"}, {"i", "9223372036854775808"}, {"j", "1.5"}} {
		ex("gcs-06"+v[0], "prefix={s/}&maxResults="+v[1])
	}
	ex("gcs-06k", "prefix={s/}&maxResults=+2")
	ex("gcs-06l", "prefix={s/}&maxResults=002")
	ex("gcs-06m", "prefix={s/}&maxResults=1&maxResults=2")
	ex("gcs-06n", "prefix={s/}&maxResults=")
	ex("gcs-06o", "prefix={s/}&maxResults=2147483647")
	walk("gcs-07a", "prefix={}&delimiter=/&maxResults=1")
	walk("gcs-07b", "prefix={b/}")
	walk("gcs-07c", "prefix={d/}&delimiter=/&maxResults=2")
	walk("gcs-07d", "prefix={d/}&delimiter=/&startOffset={d/a/m}&endOffset={d/e}&maxResults=1")
	walk("gcs-07e", "prefix={r/}&maxResults=4")
	ex("gcs-07f", "prefix={none/}")
	walk("gcs-07n", "prefix={d/}&maxResults=2")
	walk("gcs-07q", "prefix={r/}&maxResults=2")
	same := "prefix={r/}&maxResults=2"
	tok("gcs-07g", same, "", "tampered")
	tok("gcs-07h", same, "gcs-07q", "")
	tok("gcs-07i", "prefix={x/}&maxResults=2", "gcs-07q", "")
	tok("gcs-07j", "prefix={r/}&maxResults=3", "gcs-07q", "")
	tok("gcs-07k", "prefix={d/}&delimiter=/&maxResults=2", "gcs-07n", "")
	tok("gcs-07l", "prefix={r/}&maxResults=2&startOffset={r/k06}", "gcs-07q", "")
	ex("gcs-07m", "prefix={r/}&maxResults=2&pageToken=")
	tok("gcs-07o", "prefix={d/}&maxResults=2", "gcs-07c", "")
	tok("gcs-07r", same, "gcs-07q", "flip")
	tok("gcs-07s", same, "gcs-07q", "truncate")
	tok("gcs-07v", same, "gcs-07q", "flip-decoded")
	tok("gcs-07t", "prefix={r/}&maxResults=2&endOffset={r/k02}", "gcs-07q", "")
	tok("gcs-07u", "prefix={r/k01}&maxResults=2", "gcs-07q", "")
	owned("gcs-08a", "exchange", "prefix={n/}", ownNames)
	owned("gcs-08b", "exchange", "prefix={n/}&delimiter=/", ownNames)
	owned("gcs-08c", "exchange", "prefix={q/}", ownLimits)
	for _, id := range []string{"gcs-09a", "gcs-09b", "gcs-09c"} {
		out = append(out, providerCase{ID: id, Kind: "sdk_run", Owned: ownSDK})
	}
	ex("gcs-11a", "prefix={s/}&projection=full")
	ex("gcs-11b", "prefix={s/}&projection=noAcl")
	ex("gcs-11c", "prefix={s/}&projection=bogus")
	ex("gcs-11d", "prefix={s/}&prettyPrint=false&alt=json")
	ex("gcs-11e", "prefix={s/}&fields=items%28name%29%2CnextPageToken")
	ex("gcs-11f", "prefix={s/}&versions=true")
	ex("gcs-11g", "prefix={s/}&matchGlob={s/*}")
	ex("gcs-11h", "prefix={s/}&softDeleted=true")
	ex("gcs-11i", "prefix={d/}&delimiter=/&includeFoldersAsPrefixes=true")
	ex("gcs-11j", "prefix={s/}&versions=false&includeTrailingDelimiter=false&softDeleted=false")
	return out
}

// campaignEscape is the campaign's value escape (Python's
// urllib.parse.quote with safe set to safe): UTF-8 bytes, every byte but
// ASCII letters, digits, "_.-~" and safe written %XX in upper case.
func campaignEscape(v, safe string) string {
	var b strings.Builder
	for i := 0; i < len(v); i++ {
		c := v[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.IndexByte("_.-~", c) >= 0 ||
			c < utf8.RuneSelf && strings.IndexByte(safe, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// placeholders lists the template's {name} references: the name, whether
// it is a value rather than a corpus name ({=v}), and whether '/' is
// escaped ({name!}).
type placeholder struct {
	name           string
	value, slashes bool
}

func parseTemplate(tmpl string, emit func(literal string, p *placeholder)) {
	for tmpl != "" {
		i := strings.IndexByte(tmpl, '{')
		if i < 0 {
			emit(tmpl, nil)
			return
		}
		emit(tmpl[:i], nil)
		j := strings.IndexByte(tmpl[i:], '}')
		p := placeholder{name: tmpl[i+1 : i+j]}
		if v, ok := strings.CutPrefix(p.name, "="); ok {
			p.name, p.value = v, true
		} else if v, ok := strings.CutSuffix(p.name, "!"); ok {
			p.name, p.slashes = v, true
		}
		emit("", &p)
		tmpl = tmpl[i+j+1:]
	}
}

// render fills the template from bind, which maps a corpus name (or a
// value placeholder's spelling) to its string. A name bind cannot fill
// is returned as missing.
func (c *providerCase) render(bind func(name string, value bool) (string, bool)) (query string, missing []string) {
	var b strings.Builder
	parseTemplate(c.Tmpl, func(literal string, p *placeholder) {
		if p == nil {
			b.WriteString(literal)
			return
		}
		v, ok := bind(p.name, p.value)
		if !ok {
			missing = append(missing, p.name)
			return
		}
		safe := "/"
		if p.slashes {
			safe = ""
		}
		b.WriteString(campaignEscape(v, safe))
	})
	return b.String(), missing
}

// corpusBinding binds every name to the campaign's corpus under the run
// prefix: what the campaign itself sent.
func corpusBinding(runPrefix string) func(string, bool) (string, bool) {
	return func(name string, value bool) (string, bool) {
		if value {
			return name, true
		}
		return runPrefix + name, true
	}
}

// fixtureRoles binds the corpus names a public bucket can stand in for to
// names under one scope of the fixture, each chosen to keep the corpus
// name's part in its questions (an existing name, a gap between two, a
// subtree with descendants after a point, a name after a subtree), and
// says for each role it could not find why.
type fixtureRoles struct {
	names   map[string]string
	missing map[string]string // role -> why the fixture has none
}

func (f *fixtureRoles) bind(name string, value bool) (string, bool) {
	if value {
		v, ok := f.names["="+name]
		return v, ok
	}
	v, ok := f.names[name]
	return v, ok
}

// roleOf is the role a corpus name belongs to, for the missing-role
// report: its first segment ("r/k02" is r/), or the name itself.
func roleOf(name string) string {
	if i := strings.IndexByte(name, '/'); i >= 0 {
		return name[:i+1]
	}
	return name
}

// directory is one directory prefix of the scope: its direct objects
// and subdirectories, and how many names lie under it.
type directory struct {
	prefix  string
	objects []string
	subdirs []string
	total   int
}

func scanDirectories(keys []string, scope string) map[string]*directory {
	dirs := map[string]*directory{scope: {prefix: scope}}
	for _, k := range keys {
		if !strings.HasPrefix(k, scope) {
			continue
		}
		parent := dirs[scope]
		parent.total++
		at := len(scope)
		for {
			i := strings.IndexByte(k[at:], '/')
			if i < 0 {
				break
			}
			p := k[:at+i+1]
			d, ok := dirs[p]
			if !ok {
				d = &directory{prefix: p}
				dirs[p] = d
				parent.subdirs = append(parent.subdirs, p)
			}
			d.total++
			parent = d
			at += i + 1
			if at == len(k) {
				break // a folder marker: the directory itself
			}
		}
		if at < len(k) {
			parent.objects = append(parent.objects, k)
		}
	}
	return dirs
}

// under is the keys with prefix p (keys sorted).
func under(keys []string, p string) []string {
	lo, _ := slices.BinarySearch(keys, p)
	hi := lo
	for hi < len(keys) && strings.HasPrefix(keys[hi], p) {
		hi++
	}
	return keys[lo:hi]
}

func successor(p string) string {
	next, _ := s3.Successor([]byte(p))
	return string(next)
}

// deriveRoles binds the roles over the sorted keys under scope.
func deriveRoles(keys []string, scope string) *fixtureRoles {
	f := &fixtureRoles{names: map[string]string{"": scope}, missing: map[string]string{}}
	dirs := scanDirectories(keys, scope)
	sorted := slices.SortedFunc(func(yield func(*directory) bool) {
		for _, d := range dirs {
			if !yield(d) {
				return
			}
		}
	}, func(a, b *directory) int { return strings.Compare(a.prefix, b.prefix) })
	smallest := func(ok func(*directory) bool) *directory {
		var best *directory
		for _, d := range sorted {
			if ok(d) && (best == nil || d.total < best.total) {
				best = d
			}
		}
		return best
	}

	// r/: a directory of at least six objects (no subdirectories when one
	// exists), k01..k07 its first six and the gap k04 between the third
	// and the fourth.
	gapAfter := func(d *directory) (string, bool) {
		if len(d.objects) < 6 {
			return "", false
		}
		g := d.objects[2] + " "
		return g, g < d.objects[3]
	}
	r := smallest(func(d *directory) bool { _, ok := gapAfter(d); return ok && len(d.subdirs) == 0 })
	if r == nil {
		r = smallest(func(d *directory) bool { _, ok := gapAfter(d); return ok })
	}
	if r == nil {
		f.missing["r/"] = "no directory holds six objects with a gap between the third and fourth"
	} else {
		g, _ := gapAfter(r)
		o := r.objects
		for k, v := range map[string]string{"r/": r.prefix, "r/k01": o[0], "r/k02": o[1], "r/k03": o[2], "r/k04": g,
			"r/k05": o[3], "r/k06": o[4], "r/k07": o[5], "s": successor(r.prefix), "zzz": successor(r.prefix)} {
			f.names[k] = v
		}
	}

	// s/: the smallest directory of at least three names.
	if s := smallest(func(d *directory) bool { return d.total >= 3 }); s != nil {
		f.names["s/"], f.names["s/*"] = s.prefix, s.prefix+"*"
	} else {
		f.missing["s/"] = "no directory holds three names"
	}

	// x/: a name that is a byte prefix of the next name (a chain of three
	// when there is one), or else the common start of two names of a
	// directory that does not end at a '/'.
	inScope := under(keys, scope)
	xa, xab, xabc := "", "", ""
	for i := 0; i+1 < len(inScope) && xa == ""; i++ {
		a, b := inScope[i], inScope[i+1]
		if strings.HasPrefix(b, a) && !strings.HasSuffix(a, "/") {
			xa, xab, xabc = a, a, b
			if i+2 < len(inScope) && strings.HasPrefix(inScope[i+2], b) {
				xab, xabc = b, inScope[i+2]
			}
		}
	}
	if xa == "" {
		for _, d := range sorted {
			if len(d.objects) < 2 {
				continue
			}
			a, b := d.objects[0], d.objects[1]
			n := 0
			for n < len(a) && n < len(b) && a[n] == b[n] {
				n++
			}
			for n > 0 && !utf8.ValidString(a[:n]) {
				n--
			}
			if n > len(d.prefix) && a[n-1] != '/' {
				xa, xab, xabc = a[:n], a, b
				break
			}
		}
	}
	if xa == "" {
		f.missing["x/"] = "no name is a byte prefix of others beyond a directory"
	} else {
		dir := xa[:strings.LastIndexByte(xa, '/')+1]
		if len(dir) < len(scope) {
			dir = scope
		}
		y := successor(xa)
		for k, v := range map[string]string{"x/": dir, "x/a": xa, "x/ab": xab, "x/abc": xabc, "w": dir, "y": y, "z": y + "z"} {
			f.names[k] = v
		}
	}

	// d/: a directory with two subdirectories, the first holding at least
	// two names: the smallest that also holds objects of its own (when
	// one of at most 20,000 names does), else the smallest.
	var d, a, b *directory
	for _, pass := range []bool{true, false} {
		for _, cand := range sorted {
			if pass && (len(cand.objects) == 0 || cand.total > 20000) || len(cand.subdirs) < 2 || d != nil && d.total <= cand.total {
				continue
			}
			for i, sub := range cand.subdirs[:len(cand.subdirs)-1] {
				if dirs[sub].total >= 2 {
					d, a, b = cand, dirs[sub], dirs[cand.subdirs[i+1]]
					break
				}
			}
		}
		if d != nil {
			break
		}
	}
	if d == nil {
		f.missing["d/"] = "no directory holds two subdirectories, the first with two names"
	} else {
		da, db := under(keys, a.prefix), under(keys, b.prefix)
		first := da[0]
		if first == a.prefix {
			first = da[1] // the subtree's folder marker comes first
		}
		after := ""
		for _, k := range under(keys, d.prefix) {
			if k >= successor(a.prefix) {
				after = k
				break
			}
		}
		rest := []rune(first[len(a.prefix):])
		f.names["=/1"] = "/" + string(rest[0])
		base := []rune(first[strings.LastIndexByte(first, '/')+1:])
		if len(base) >= 3 {
			f.names["=-g"] = string(base[1:3])
		} else if len(base) == 2 {
			f.names["=-g"] = string(base)
		}
		for k, v := range map[string]string{"d/": d.prefix, "d/a/": a.prefix, "d/a": strings.TrimSuffix(a.prefix, "/"),
			"d/a/m": da[len(da)/2], "d/a/1": first, "d/b/3": db[len(db)-1] + "~", "d/b/2": db[len(db)/2],
			"d/e": successor(b.prefix), "e": successor(d.prefix)} {
			f.names[k] = v
		}
		if after != "" {
			f.names["d/a0"] = after
		}
	}

	// b/: the smallest directory of more than 1,000 names; b/0 a directory
	// of exactly 1,000.
	if big := smallest(func(d *directory) bool { return d.total > 1000 }); big != nil {
		f.names["b/"] = big.prefix
	} else {
		f.missing["b/"] = "no directory holds more than 1,000 names"
	}
	if exact := smallest(func(d *directory) bool { return d.total == 1000 }); exact != nil {
		f.names["b/0"] = exact.prefix
	} else {
		f.missing["b/0"] = "no directory holds exactly 1,000 names"
	}
	for _, none := range []string{"none/", "zz-no-such-prefix/", "zz-no-such-prefix-2/"} {
		if len(under(keys, scope+none)) == 0 {
			f.names["none/"] = scope + none
			break
		}
	}
	return f
}

// instantiate renders c over the fixture's roles: the query, or why it
// cannot be sent.
func (f *fixtureRoles) instantiate(c *providerCase) (string, string) {
	if c.Owned != "" {
		return "", "needs an owned bucket: " + c.Owned
	}
	q, missing := c.render(func(name string, value bool) (string, bool) {
		if alias, ok := c.Bind[name]; ok && !value {
			name = alias
		}
		return f.bind(name, value)
	})
	if len(missing) > 0 {
		var why []string
		for _, m := range missing {
			role := roleOf(m)
			if reason, ok := f.missing[role]; ok {
				why = append(why, m+": "+reason)
			} else if reason, ok := f.missing[m]; ok {
				why = append(why, m+": "+reason)
			} else {
				why = append(why, m+": the fixture has no name for it")
			}
		}
		return "", "not instantiable on this fixture (" + strings.Join(slices.Compact(why), "; ") + ")"
	}
	return q, ""
}
