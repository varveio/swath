// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package scalefake

import (
	"math/rand/v2"
	"slices"
	"sort"
	"strings"
	"testing"
)

// shapes cover every option on keyspaces small enough to materialize.
func shapes() map[string]Shape {
	return map[string]Shape{
		"flat":     {Keys: 5000, Seed: 1},
		"deep":     {Keys: 20000, Fanout: []int{4, 7, 3}, Seed: 2},
		"skewed":   {Keys: 20000, Fanout: []int{6, 6, 6}, Skew: 1.5, Seed: 3},
		"shared":   {Keys: 8000, Fanout: []int{40}, SharedPrefix: 300, SegmentPad: 200, Seed: 4},
		"keylen":   {Keys: 4000, Fanout: []int{5, 5}, KeyLen: 1024, Seed: 5},
		"wide":     {Keys: 3000, Fanout: []int{37, 2}, Skew: 3, Seed: 6}, // empty directories, two-digit segments
		"empty":    {Keys: 0, Fanout: []int{3}},
		"one":      {Keys: 1},
		"tinydirs": {Keys: 10, Fanout: []int{100}}, // more directories than keys
	}
}

func materialize(t *testing.T, k *Keyspace) []string {
	t.Helper()
	keys := make([]string, k.Len())
	for i := range keys {
		keys[i] = k.Key(int64(i))
	}
	return keys
}

// TestKeyspaceOrderAndCount: keys are strictly ascending, exactly Len of
// them, of the requested minimum length, deterministic in the seed, and
// Search and Count agree with the materialized list.
func TestKeyspaceOrderAndCount(t *testing.T) {
	for name, s := range shapes() {
		t.Run(name, func(t *testing.T) {
			k, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			keys := materialize(t, k)
			if int64(len(keys)) != s.Keys {
				t.Fatalf("%d keys, want %d", len(keys), s.Keys)
			}
			for i := 1; i < len(keys); i++ {
				if keys[i-1] >= keys[i] {
					t.Fatalf("keys %d, %d out of order: %q >= %q", i-1, i, keys[i-1], keys[i])
				}
			}
			for _, key := range keys {
				if len(key) < s.KeyLen || strings.HasSuffix(key, "/") || strings.Contains(key, "//") {
					t.Fatalf("key %q (len %d)", key, len(key))
				}
			}
			k2, _ := New(s)
			if len(keys) > 0 && k2.Key(int64(len(keys)/2)) != keys[len(keys)/2] {
				t.Fatal("not deterministic")
			}
			if len(keys) > 1 {
				other := s
				other.Seed++
				k3, _ := New(other)
				if s.Skew != 0 && slices.Equal(materialize(t, k3), keys) {
					t.Fatal("seed changes nothing")
				}
			}
			r := rand.New(rand.NewPCG(s.Seed, 7))
			for range 300 {
				q := probe(r, keys)
				want := int64(sort.SearchStrings(keys, q))
				if got := k.Search(q, false, 0); got != want {
					t.Fatalf("Search(%q) = %d, want %d", q, got, want)
				}
				n := int64(0)
				for _, key := range keys {
					if strings.HasPrefix(key, q) {
						n++
					}
				}
				if got := k.Count(q); got != n {
					t.Fatalf("Count(%q) = %d, want %d", q, got, n)
				}
			}
		})
	}
}

// TestSkewIsSkewed: a skewed keyspace's directories differ in density by
// orders of magnitude; a uniform one's by at most one key.
func TestSkewIsSkewed(t *testing.T) {
	for _, c := range []struct {
		skew   float64
		minMax float64 // the densest directory over the mean, at least
		maxMax float64
	}{{0, 1, 1.01}, {2, 20, 1e9}} {
		k, err := New(Shape{Keys: 1_000_000, Fanout: []int{16, 16, 16}, Skew: c.skew, Seed: 9})
		if err != nil {
			t.Fatal(err)
		}
		most := int64(0)
		for j := range k.Dirs() {
			most = max(most, k.cum[j+1]-k.cum[j])
		}
		ratio := float64(most) / (1e6 / float64(k.Dirs()))
		if ratio < c.minMax || ratio > c.maxMax {
			t.Fatalf("skew %g: densest directory %.2fx the mean", c.skew, ratio)
		}
	}
}

// probe draws a query string near the keyspace: a key, a cut of one, a key
// with a byte bumped or appended, a directory prefix, or noise.
func probe(r *rand.Rand, keys []string) string {
	if len(keys) == 0 || r.IntN(10) == 0 {
		return []string{"", "0", "zzz", "\xff", "/", "a"}[r.IntN(6)]
	}
	k := keys[r.IntN(len(keys))]
	switch r.IntN(6) {
	case 0:
		return k
	case 1:
		return k[:r.IntN(len(k)+1)]
	case 2:
		b := []byte(k)
		i := r.IntN(len(b))
		b[i] += byte(r.IntN(3)) - 1
		return string(b)
	case 3:
		return k + string(rune('!'+r.IntN(90)))
	case 4:
		if i := strings.LastIndex(k, "/"); i >= 0 {
			return k[:i+1]
		}
		return k
	default:
		if i := strings.Index(k, "/"); i >= 0 {
			return k[:i+1] + k[i+1:i+1+r.IntN(len(k)-i)]
		}
		return k
	}
}

// reference answers q over the materialized keys, every page at once.
func reference(keys []string, q Query) []Entry {
	var out []Entry
	for i, k := range keys {
		if !strings.HasPrefix(k, q.Prefix) || (q.End != "" && k >= q.End) {
			continue
		}
		if q.After != "" && (k < q.After || (k == q.After && !q.Inclusive)) {
			continue
		}
		if q.Delimiter != "" {
			if d := strings.Index(k[len(q.Prefix):], q.Delimiter); d >= 0 {
				cp := k[:len(q.Prefix)+d+len(q.Delimiter)]
				if !q.RollUpBelow && q.After != "" && cp <= q.After {
					continue
				}
				if n := len(out); n > 0 && out[n-1].Prefix && out[n-1].Name == cp {
					continue
				}
				out = append(out, Entry{Name: cp, Index: -1, Prefix: true})
				continue
			}
		}
		out = append(out, Entry{Name: k, Index: int64(i)})
	}
	return out
}

// TestListMatchesReference: random queries (prefix, bound, inclusive or
// not, end, delimiter, page size), paged through to the end with the
// continuation token, give exactly the reference's entries, every page
// full but the last, and Next empty exactly on the last.
func TestListMatchesReference(t *testing.T) {
	for name, s := range shapes() {
		t.Run(name, func(t *testing.T) {
			k, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			keys := materialize(t, k)
			r := rand.New(rand.NewPCG(s.Seed, 11))
			for n := range 200 {
				q := Query{Max: []int{1, 2, 3, 7, 100, 1000}[r.IntN(6)]}
				if r.IntN(2) == 0 {
					q.Prefix = probe(r, keys)
					if r.IntN(2) == 0 {
						if i := strings.LastIndex(q.Prefix, "/"); i >= 0 {
							q.Prefix = q.Prefix[:i+1]
						}
					}
				}
				if r.IntN(3) > 0 {
					q.After = probe(r, keys)
				}
				q.Inclusive, q.RollUpBelow = r.IntN(2) == 0, r.IntN(2) == 0
				if r.IntN(3) == 0 {
					q.End = probe(r, keys)
				}
				if r.IntN(2) == 0 {
					q.Delimiter = "/"
				}
				want := reference(keys, q)
				if len(want)/q.Max > 200 { // small pages are for short listings
					q.Max = 1000
				}
				var got []Entry
				for pages := 0; ; pages++ {
					p, err := k.List(q)
					if err != nil {
						t.Fatal(err)
					}
					if p.Next != "" && len(p.Entries) != q.Max {
						t.Fatalf("query %d %+v: a page of %d with a next token", n, q, len(p.Entries))
					}
					got = append(got, p.Entries...)
					if p.Next == "" {
						break
					}
					if pages > len(keys)+2 {
						t.Fatalf("query %d %+v: no end", n, q)
					}
					q.Token = p.Next
				}
				if !slices.Equal(got, want) {
					t.Fatalf("query %d %+v:\n got %d entries %.300v\nwant %d entries %.300v", n, q, len(got), got, len(want), want)
				}
			}
		})
	}
}

func TestListBadToken(t *testing.T) {
	k, _ := New(Shape{Keys: 10})
	for _, tok := range []string{"!!", "eA", ""} {
		if _, err := k.List(Query{Token: tok}); (err != nil) != (tok != "") {
			t.Fatalf("token %q: %v", tok, err)
		}
	}
}

// TestLargeKeyspaceIsLazy: a billion-key keyspace builds in bounded memory
// and answers a deep seek with a full page.
func TestLargeKeyspaceIsLazy(t *testing.T) {
	k, err := New(Shape{Keys: 1_000_000_000, Fanout: []int{32, 32, 32, 8}, Skew: 1, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	mid := k.Key(500_000_000)
	p, err := k.List(Query{After: mid, Max: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Entries) != 1000 || p.Entries[0].Index != 500_000_001 || p.Next == "" {
		t.Fatalf("page after the middle: %d entries, first %+v", len(p.Entries), p.Entries[0])
	}
	if last := k.Key(k.Len() - 1); k.Search(last, true, 0) != k.Len() {
		t.Fatal("past the last key")
	}
}
