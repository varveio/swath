// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func pagerOver(rollup bool, keys ...string) *Pager {
	return NewPager(memstore.Keys(memstore.Options{Rollup: rollup}, keys...), "fixture-1")
}

func list(t *testing.T, p *Pager, query string) *Page {
	t.Helper()
	r, err := Parse("bkt", query)
	if err != nil {
		t.Fatal(err)
	}
	page, err := p.List(context.Background(), r)
	if err != nil {
		t.Fatal(err)
	}
	return page
}

// entries spells a page as "name" for items and "name|" for prefixes, in
// key order.
func entries(page *Page) []string {
	var out []string
	for _, it := range page.Items {
		out = append(out, string(it.Key))
	}
	for _, p := range page.Prefixes {
		out = append(out, string(p)+"|")
	}
	slices.SortFunc(out, func(a, b string) int { return strings.Compare(strings.TrimSuffix(a, "|"), strings.TrimSuffix(b, "|")) })
	return out
}

func TestFlatBoundsAreInclusiveThenExclusiveAndTokenSurvivesPageSizeChange(t *testing.T) {
	p := pagerOver(false, "a", "b", "c", "d", "e")
	page := list(t, p, "startOffset=b&endOffset=e&maxResults=2")
	if got := entries(page); !slices.Equal(got, []string{"b", "c"}) || page.Token == "" {
		t.Fatalf("%v %q", got, page.Token)
	}
	next := list(t, p, "startOffset=b&endOffset=e&maxResults=7&pageToken="+page.Token)
	if got := entries(next); !slices.Equal(got, []string{"d"}) || next.Token != "" {
		t.Fatalf("%v %q", got, next.Token)
	}
	if page := list(t, p, "startOffset=b&endOffset=d&maxResults=2"); page.Token != "" {
		t.Error("a page that reaches endOffset exactly carries a token")
	}
}

// TestDelimiterOffsetInsideSubtreeEmitsPrefix: a startOffset inside a
// subtree still emits its prefix (S3's start-after would suppress it), and
// the token after it resumes past the whole subtree.
func TestDelimiterOffsetInsideSubtreeEmitsPrefix(t *testing.T) {
	for _, rollup := range []bool{false, true} {
		p := pagerOver(rollup, "d/a/k", "d/a/x", "d/a/y", "d/a0", "d/b/z", "d/c")
		page := list(t, p, "prefix=d/&delimiter=/&startOffset=d/a/m&maxResults=1")
		if got := entries(page); !slices.Equal(got, []string{"d/a/|"}) || page.Token == "" {
			t.Fatalf("rollup %v: %v", rollup, got)
		}
		rest := list(t, p, "prefix=d/&delimiter=/&startOffset=d/a/m&pageToken="+page.Token)
		if got := entries(rest); !slices.Equal(got, []string{"d/a0", "d/b/|", "d/c"}) {
			t.Fatalf("rollup %v: %v", rollup, got)
		}
	}
}

func TestTokenRejectsAnotherScopeOrFixture(t *testing.T) {
	keys := []string{"a/1", "a/2", "a/3"}
	page := list(t, pagerOver(false, keys...), "prefix=a/&maxResults=1")
	for _, c := range []struct {
		pager *Pager
		query string
	}{
		{pagerOver(false, keys...), "prefix=a/x&pageToken="},
		{pagerOver(false, keys...), "prefix=a/&delimiter=/&pageToken="},
		{pagerOver(false, keys...), "prefix=a/&startOffset=a/1&pageToken="},
		{pagerOver(false, keys...), "prefix=a/&endOffset=a/3&pageToken="},
		{NewPager(memstore.Keys(memstore.Options{}, keys...), "fixture-2"), "prefix=a/&pageToken="},
	} {
		r, err := Parse("bkt", c.query+page.Token)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.pager.ValidateToken(r); err == nil {
			t.Errorf("%s: accepted", c.query)
		}
		if _, err := c.pager.List(context.Background(), r); err == nil {
			t.Errorf("%s: listed", c.query)
		}
	}
	r, _ := Parse("bkt", "prefix=a/&maxResults=50&pageToken="+page.Token)
	if err := pagerOver(false, keys...).ValidateToken(r); err != nil {
		t.Errorf("same scope, another page size: %v", err)
	}
}

func TestEmptyIntersection(t *testing.T) {
	p := pagerOver(false, "a", "b", "c")
	if page := list(t, p, "startOffset=b&endOffset=b"); len(entries(page)) != 0 || page.Token != "" {
		t.Errorf("%v", entries(page))
	}
	if page := list(t, p, "prefix=a&startOffset=z"); len(entries(page)) != 0 {
		t.Errorf("%v", entries(page))
	}
	if page := list(t, p, "startOffset=c&endOffset=a&delimiter=/"); len(entries(page)) != 0 {
		t.Errorf("%v", entries(page))
	}
}

// reference is the listing a request scope means, computed directly:
// keys in [max(prefix, startOffset), min(prefixUpper, endOffset)), each
// rolled up at the first delimiter after the prefix, a prefix once.
func reference(keys []string, prefix, delimiter, start, end string) []string {
	var out []string
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) || k < start || (end != "" && k >= end) {
			continue
		}
		e := k
		if delimiter != "" {
			if i := strings.Index(k[len(prefix):], delimiter); i >= 0 {
				e = k[:len(prefix)+i+len(delimiter)] + "|"
			}
		}
		if len(out) == 0 || out[len(out)-1] != e {
			out = append(out, e)
		}
	}
	return out
}

// TestRollupMatchesWalkAndReference walks random keyspaces (bytes around
// '/', 0xFF and multi-byte delimiters) over every prefix, offset pair and
// page size, with a store that answers the rollup and one that declines
// it: the two must give identical pages and tokens, and each full walk
// must equal the reference listing.
func TestRollupMatchesWalkAndReference(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	alphabet := []string{"a", "b", "/", "//", "\U0010FFFF", "-", "a/", "é", "\U0001F600"}
	for round := range 30 {
		set := map[string]bool{}
		for range 20 + rng.IntN(40) {
			var k strings.Builder
			for range 1 + rng.IntN(5) {
				k.WriteString(alphabet[rng.IntN(len(alphabet))])
			}
			set[k.String()] = true
		}
		var keys []string
		for k := range set {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		walkStore, rollupStore := pagerOver(false, keys...), pagerOver(true, keys...)
		offsets := []string{"", keys[rng.IntN(len(keys))], keys[rng.IntN(len(keys))] + "\x00", "a/"}
		for _, prefix := range []string{"", "a", "a/", "b/", "\U0010FFFF"} {
			for _, delimiter := range []string{"", "/", "//", "a/"} {
				for _, start := range offsets {
					for _, end := range offsets {
						for _, size := range []int{1, 2, 3, 1000} {
							q := fmt.Sprintf("maxResults=%d", size)
							for _, kv := range [][2]string{{"prefix", prefix}, {"delimiter", delimiter}, {"startOffset", start}, {"endOffset", end}} {
								if kv[1] != "" {
									q += "&" + kv[0] + "=" + percent(kv[1])
								}
							}
							var walked []string
							token := ""
							for pages := 0; ; pages++ {
								query := q
								if token != "" {
									query += "&pageToken=" + token
								}
								a, b := list(t, walkStore, query), list(t, rollupStore, query)
								if !slices.Equal(entries(a), entries(b)) || a.Token != b.Token {
									t.Fatalf("round %d %q: walk %q %q, rollup %q %q", round, query, entries(a), a.Token, entries(b), b.Token)
								}
								if len(entries(a)) > size {
									t.Fatalf("%q: %d entries", query, len(entries(a)))
								}
								walked = append(walked, entries(a)...)
								if token = a.Token; token == "" || pages > len(keys)+2 {
									break
								}
							}
							want := reference(keys, prefix, delimiter, max(start, prefix), end)
							if !slices.Equal(walked, want) {
								t.Fatalf("round %d %q over %q:\n walked %q\n want   %q", round, q, keys, walked, want)
							}
						}
					}
				}
			}
		}
	}
}

func percent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		fmt.Fprintf(&b, "%%%02X", s[i])
	}
	return b.String()
}

func TestTokenShapes(t *testing.T) {
	binding := bytes.Repeat([]byte{7}, bindingBytes)
	tok := encodeToken(true, []byte("k\xff"), binding, 0)
	c, err := decodeToken(tok, binding)
	if err != nil || !c.inclusive || string(c.boundary) != "k\xff" {
		t.Fatalf("%+v %v", c, err)
	}
	body := strings.TrimPrefix(tok, tokenPrefix)
	for _, bad := range []string{
		"", "gcs2." + body, tokenPrefix, tokenPrefix + body + "==", tokenPrefix + body[:len(body)-1] + "\n" + body[len(body)-1:],
		tokenPrefix + "Ag" + body[2:], tokenPrefix + body[:1] + "=" + body[1:], tokenPrefix + body[:len(body)-2],
		encodeToken(false, bytes.Repeat([]byte("k"), maxFieldBytes+1), binding, 0),
		encodeToken(false, []byte("k"), binding, 2),
		encodeToken(false, nil, bytes.Repeat([]byte{8}, bindingBytes), 0),
	} {
		if _, err := decodeToken(bad, binding); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	// Padding is optional but must be right where present.
	for _, c := range []struct {
		boundary, pad string
	}{{"ab", "="}, {"a", "=="}} {
		raw := encodeToken(false, []byte(c.boundary), binding, 0)
		if _, err := decodeToken(raw+c.pad, binding); err != nil {
			t.Errorf("%q: correct padding refused: %v", c.boundary, err)
		}
		if _, err := decodeToken(raw+c.pad[1:]+"==", binding); err == nil {
			t.Errorf("%q: wrong padding accepted", c.boundary)
		}
	}
	if _, err := decodeToken(encodeToken(false, []byte("abc"), binding, 0)+"=", binding); err == nil {
		t.Error("padding on a whole unit accepted")
	}
}

// TestTokenProgressIsRetained resumes from a token whose progress byte
// is 1: the next token keeps it, on the flat, rolled-up and walked paths,
// as the Java server's does (the bytes are the ones it answered for this
// request over the same keys and fixture identity).
func TestTokenProgressIsRetained(t *testing.T) {
	const in, want = "gcs1.AAGVQQoGK9iXFfts8fbwln-xYQ", "gcs1.AAGVQQoGK9iXFfts8fbwln-xYg"
	page := list(t, pagerOver(false, "a", "b", "c"), "maxResults=1&pageToken="+in)
	if got := entries(page); !slices.Equal(got, []string{"b"}) || page.Token != want {
		t.Fatalf("%v %q, want [b] %q", got, page.Token, want)
	}
	for _, rollup := range []bool{false, true} {
		p := pagerOver(rollup, "a/1", "b/1", "c/1", "d")
		first := list(t, p, "delimiter=/&maxResults=1")
		r, _ := Parse("bkt", "delimiter=/")
		c, err := decodeToken(first.Token, tokenBinding(r, "fixture-1"))
		if err != nil {
			t.Fatal(err)
		}
		progressed := encodeToken(c.inclusive, c.boundary, tokenBinding(r, "fixture-1"), 1)
		next := list(t, p, "delimiter=/&maxResults=1&pageToken="+progressed)
		got, err := decodeToken(next.Token, tokenBinding(r, "fixture-1"))
		if err != nil || got.progress != 1 || !slices.Equal(entries(next), []string{"b/|"}) {
			t.Errorf("rollup %v: %v %+v %v", rollup, entries(next), got, err)
		}
	}
}
