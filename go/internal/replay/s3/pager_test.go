// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
)

func pager(keys ...string) *Pager { return &Pager{Store: memstore.Keys(memstore.Options{}, keys...)} }

func list(t *testing.T, p *Pager, req *Request) *Result {
	t.Helper()
	res, err := p.List(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func render(res *Result) string {
	var parts []string
	for _, e := range res.Entries {
		if e.Object != nil {
			parts = append(parts, "O:"+string(e.Object.Key))
		} else {
			parts = append(parts, "P:"+string(e.CommonPrefix))
		}
	}
	return strings.Join(parts, " ")
}

func b(s string) []byte {
	if s == "" {
		return nil
	}
	return []byte(s)
}

func TestListsByPrefixAndExclusiveStartAfter(t *testing.T) {
	res := list(t, pager("a/1", "a/2", "a/3", "b/1"), &Request{Prefix: b("a/"), StartAfter: b("a/1"), MaxKeys: 2})
	if render(res) != "O:a/2 O:a/3" || res.Truncated || res.NextToken != "" {
		t.Errorf("%s truncated=%v next=%q", render(res), res.Truncated, res.NextToken)
	}
}

func TestTruncatesAndResumesFromToken(t *testing.T) {
	p := pager("a/1", "a/2", "a/3")
	first := list(t, p, &Request{Prefix: b("a/"), MaxKeys: 2})
	second := list(t, p, &Request{Prefix: b("a/"), MaxKeys: 2, Token: first.NextToken, TokenSet: true})
	if render(first) != "O:a/1 O:a/2" || !first.Truncated || !strings.HasPrefix(first.NextToken, "v2:") {
		t.Errorf("first %s %v %q", render(first), first.Truncated, first.NextToken)
	}
	if render(second) != "O:a/3" || second.Truncated {
		t.Errorf("second %s %v", render(second), second.Truncated)
	}
}

func TestMaxKeysZeroIsEmptyAndNotTruncated(t *testing.T) {
	res := list(t, pager("a/1", "a/2"), &Request{MaxKeys: 0})
	if len(res.Entries) != 0 || res.Truncated || res.NextToken != "" {
		t.Errorf("%+v", res)
	}
	// The token is still validated.
	if _, err := pager("a").List(context.Background(), &Request{MaxKeys: 0, Token: "bad", TokenSet: true}); err == nil {
		t.Error("a malformed token with max-keys=0 was accepted")
	}
}

func TestRollsUpAndResumesPastARolledUpPrefix(t *testing.T) {
	p := pager("a/0.txt", "a/dir/1.txt", "a/dir/2.txt", "a/dir0", "a/dir2/1.txt", "a/z.txt")
	first := list(t, p, &Request{Prefix: b("a/"), Delimiter: b("/"), MaxKeys: 2})
	second := list(t, p, &Request{Prefix: b("a/"), Delimiter: b("/"), MaxKeys: 10, Token: first.NextToken, TokenSet: true})
	if render(first) != "O:a/0.txt P:a/dir/" || !first.Truncated {
		t.Errorf("first %s %v", render(first), first.Truncated)
	}
	if render(second) != "O:a/dir0 P:a/dir2/ O:a/z.txt" || second.Truncated {
		t.Errorf("second %s %v", render(second), second.Truncated)
	}
}

func TestStartAfterFiltersCommonPrefixes(t *testing.T) {
	res := list(t, pager("a/a", "a/c", "b"), &Request{Delimiter: b("/"), StartAfter: b("a/b"), MaxKeys: 10})
	if render(res) != "O:b" || res.Truncated {
		t.Errorf("%s %v", render(res), res.Truncated)
	}
}

func TestBadTokenIsRejectedEvenWithAStartAfter(t *testing.T) {
	for _, sa := range []string{"", "a/1"} {
		_, err := pager("a/1").List(context.Background(), &Request{StartAfter: b(sa), Token: "not-a-token", TokenSet: true, MaxKeys: 1000})
		var e *Error
		if !errors.As(err, &e) || e.Message != "invalid continuation-token" {
			t.Errorf("start-after %q: %v", sa, err)
		}
	}
}

// The token wins over a start-after on either side of it: honouring a later
// one would drop a/3, an earlier one would repeat a/2.
func TestTokenTakesPrecedenceOverStartAfter(t *testing.T) {
	p := pager("a/1", "a/2", "a/3", "a/4")
	first := list(t, p, &Request{MaxKeys: 2})
	for _, sa := range []string{"a/3", "a/1"} {
		res := list(t, p, &Request{StartAfter: b(sa), Token: first.NextToken, TokenSet: true, MaxKeys: 1000})
		if render(res) != "O:a/3 O:a/4" {
			t.Errorf("start-after %s: %s", sa, render(res))
		}
	}
	p = pager("d/a1/c1", "d/a1/c2", "d/a2/c1", "d/a3/c1")
	first = list(t, p, &Request{Prefix: b("d/"), Delimiter: b("/"), MaxKeys: 1})
	if render(first) != "P:d/a1/" {
		t.Fatalf("first %s", render(first))
	}
	res := list(t, p, &Request{Prefix: b("d/"), Delimiter: b("/"), StartAfter: b("d/a3/"), Token: first.NextToken, TokenSet: true, MaxKeys: 1000})
	if render(res) != "P:d/a2/ P:d/a3/" {
		t.Errorf("delimiter path: %s", render(res))
	}
}

func TestBlankTokenLeavesStartAfterAsTheBoundary(t *testing.T) {
	p := pager("a/1", "a/2", "a/3", "a/4")
	for _, blank := range []string{"", "   "} {
		res := list(t, p, &Request{StartAfter: b("a/2"), Token: blank, TokenSet: true, MaxKeys: 1000})
		if render(res) != "O:a/3 O:a/4" {
			t.Errorf("blank %q: %s", blank, render(res))
		}
	}
}

// The hybrid walk scans short runs in place and seeks past long ones; the
// output is the same and only the number of store reads differs.
func TestShortRunScansLongRunSeeksSameOutput(t *testing.T) {
	keys := []string{"d/00", "d/01", "d/02", "d/03", "d/04", "e"}
	scanStore := memstore.Keys(memstore.Options{}, keys...)
	seekStore := memstore.Keys(memstore.Options{}, keys...)
	req := &Request{Delimiter: b("/"), MaxKeys: 10}
	scan := list(t, &Pager{Store: scanStore, SeekScanThreshold: 32}, req)
	seek := list(t, &Pager{Store: seekStore, SeekScanThreshold: 1}, req)
	if render(scan) != "P:d/ O:e" || render(seek) != render(scan) || scan.Truncated != seek.Truncated {
		t.Errorf("scan %s, seek %s", render(scan), render(seek))
	}
	if seekStore.Calls() <= scanStore.Calls() {
		t.Errorf("seek path read %d times, scan path %d", seekStore.Calls(), scanStore.Calls())
	}
}

func TestPrefixResumeIsInclusiveAtSuccessor(t *testing.T) {
	p := pager("a/1", "a/2", "b/1", "b/2")
	first := list(t, p, &Request{Delimiter: b("/"), MaxKeys: 1})
	if render(first) != "P:a/" || !first.Truncated {
		t.Fatalf("first %s", render(first))
	}
	tok, _, _ := DecodeToken(first.NextToken)
	if string(tok.Boundary) != "a0" || !tok.Inclusive {
		t.Errorf("token resumes at %q inclusive=%v, want a0 inclusive", tok.Boundary, tok.Inclusive)
	}
	second := list(t, p, &Request{Delimiter: b("/"), MaxKeys: 10, Token: first.NextToken, TokenSet: true})
	if render(second) != "P:b/" || second.Truncated {
		t.Errorf("second %s", render(second))
	}
	// Successor("a/") is "a0", a real key here: it is listed after the
	// prefix.
	res := list(t, pager("a/1", "a/2", "a0", "b"), &Request{Delimiter: b("/"), MaxKeys: 10})
	if render(res) != "P:a/ O:a0 O:b" {
		t.Errorf("%s", render(res))
	}
}

func TestAllFFPrefixEndsTheListing(t *testing.T) {
	s, _ := memstore.New([]store.Object{{Key: []byte("A")}, {Key: []byte{0xff, 1}}, {Key: []byte{0xff, 2}}}, memstore.Options{})
	res := list(t, &Pager{Store: s}, &Request{Delimiter: []byte{0xff}, MaxKeys: 10})
	if len(res.Entries) != 2 || res.Entries[0].Object == nil || !bytes.Equal(res.Entries[1].CommonPrefix, []byte{0xff}) ||
		res.Truncated || res.NextToken != "" {
		t.Errorf("%s truncated=%v next=%q", render(res), res.Truncated, res.NextToken)
	}
}

// walk follows tokens to the end and returns every page.
func walk(t *testing.T, p *Pager, req Request) []*Result {
	t.Helper()
	var pages []*Result
	for {
		r := req
		res := list(t, p, &r)
		pages = append(pages, res)
		if !res.Truncated {
			return pages
		}
		if len(pages) > 100000 {
			t.Fatal("walk does not end")
		}
		req.Token, req.TokenSet = res.NextToken, true
	}
}

// TestRollupMatchesWalk is the fast path's contract: a store that answers
// the "/" rollup produces the same pages, byte for byte, as the range walk
// over a store that declines it, for every page size and start, and a full
// token walk lists every key exactly once in order.
func TestRollupMatchesWalk(t *testing.T) {
	r := rand.New(rand.NewPCG(7, 8))
	segs := []string{"a", "b", "a/", "/", "//", "x", "ab", "\xff", "0", "a0"}
	for trial := range 60 {
		set := map[string]bool{}
		for range 1 + r.IntN(120) {
			var k strings.Builder
			for range 1 + r.IntN(5) {
				k.WriteString(segs[r.IntN(len(segs))])
			}
			set[k.String()] = true
		}
		keys := make([]string, 0, len(set))
		for k := range set {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		walkP := &Pager{Store: memstore.Keys(memstore.Options{}, keys...), SeekScanThreshold: 1 + r.IntN(3)}
		var paths []string
		rollP := &Pager{Store: memstore.Keys(memstore.Options{Rollup: true}, keys...), OnDelimiterPath: func(p string) { paths = append(paths, p) }}
		for _, prefix := range []string{"", "a", "a/", keys[r.IntN(len(keys))]} {
			for _, sa := range []string{"", keys[r.IntN(len(keys))], "a/"} {
				for _, mk := range []int32{1, 2, 3, 7, 1000} {
					req := Request{Bucket: "b", Prefix: b(prefix), Delimiter: b("/"), StartAfter: b(sa), MaxKeys: mk}
					wp, rp := walk(t, walkP, req), walk(t, rollP, req)
					if len(wp) != len(rp) {
						t.Fatalf("trial %d %+v: %d walk pages, %d rollup pages", trial, req, len(wp), len(rp))
					}
					var seen []string
					for i := range wp {
						a, c := AppendList(nil, wp[i]), AppendList(nil, rp[i])
						if !bytes.Equal(a, c) {
							t.Fatalf("trial %d %+v page %d:\nwalk   %s\nrollup %s", trial, req, i, a, c)
						}
						for _, e := range wp[i].Entries {
							seen = append(seen, string(e.Key()))
						}
					}
					if !slices.IsSorted(seen) || len(slices.Compact(slices.Clone(seen))) != len(seen) {
						t.Fatalf("trial %d %+v: entries not strictly ascending: %q", trial, req, seen)
					}
					if want := expectedEntries(keys, prefix, sa); fmt.Sprint(seen) != fmt.Sprint(want) {
						t.Fatalf("trial %d %+v: walked %q, want %q", trial, req, seen, want)
					}
				}
			}
		}
		if !slices.Contains(paths, DelimiterPathRollup) {
			t.Fatal("the rollup store never served the fast path")
		}
	}
}

// expectedEntries is the delimiter listing computed directly: keys under
// prefix after startAfter, each rolled up to its first "/" after the
// prefix, prefixes kept only when after startAfter, deduplicated.
func expectedEntries(keys []string, prefix, startAfter string) []string {
	var out []string
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		e := k
		if i := strings.Index(k[len(prefix):], "/"); i >= 0 {
			e = k[:len(prefix)+i+1]
		}
		if (startAfter != "" && e <= startAfter) || (len(out) > 0 && out[len(out)-1] == e) {
			continue
		}
		if startAfter != "" && k <= startAfter {
			continue
		}
		out = append(out, e)
	}
	return out
}
