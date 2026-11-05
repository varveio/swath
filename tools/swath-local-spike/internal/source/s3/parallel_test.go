// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/varveio/swath/tools/swath-local-spike/internal/order"
	"github.com/varveio/swath/tools/swath-local-spike/internal/schema"
)

// sorted is a fake ListObjectsV2 over a sorted key slice: Prefix,
// StartAfter (400 for scalars real S3 rejects), ContinuationToken,
// Delimiter, MaxKeys; optional short truncated pages and latency.
type sorted struct {
	keys    []string
	short   bool
	latency time.Duration
	mu      sync.Mutex
	rng     *rand.Rand
	calls   int
}

func newSorted(keys []string, short bool, seed int64) *sorted {
	k := slices.Clone(keys)
	slices.Sort(k)
	return &sorted{keys: slices.Compact(k), short: short, rng: rand.New(rand.NewSource(seed))}
}

var errBadStartAfter = errors.New("400 InvalidArgument: start-after")

func (f *sorted) ListObjectsV2(ctx context.Context, in *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if f.latency > 0 {
		select {
		case <-time.After(f.latency):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	f.calls++
	size := int(aws.ToInt32(in.MaxKeys))
	if f.short {
		size = 1 + f.rng.Intn(size)
	}
	f.mu.Unlock()
	prefix, delim := aws.ToString(in.Prefix), aws.ToString(in.Delimiter)
	i := sort.SearchStrings(f.keys, prefix)
	if tok := aws.ToString(in.ContinuationToken); tok != "" {
		i, _ = strconv.Atoi(tok)
	} else if sa := aws.ToString(in.StartAfter); sa != "" {
		for _, r := range sa {
			if !isSafe(int(r)) && r != '%' {
				return nil, errBadStartAfter
			}
		}
		i = max(i, sort.Search(len(f.keys), func(j int) bool { return f.keys[j] > sa }))
	}
	out := &awss3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}
	n := 0
	for ; i < len(f.keys) && strings.HasPrefix(f.keys[i], prefix); i++ {
		if n == size {
			out.IsTruncated, out.NextContinuationToken = aws.Bool(true), aws.String(strconv.Itoa(i))
			break
		}
		k := f.keys[i]
		if delim != "" {
			if j := strings.Index(k[len(prefix):], delim); j >= 0 {
				cp := k[:len(prefix)+j+len(delim)]
				if m := len(out.CommonPrefixes); m == 0 || aws.ToString(out.CommonPrefixes[m-1].Prefix) != cp {
					out.CommonPrefixes = append(out.CommonPrefixes, types.CommonPrefix{Prefix: aws.String(cp)})
					n++
				}
				continue
			}
		}
		out.Contents = append(out.Contents, types.Object{Key: aws.String(k), Size: aws.Int64(int64(len(k)))})
		n++
	}
	return out, nil
}

// Keyspaces after Swath's testkit Keyspaces, plus markers and keys with '%'
// and control characters.
func keyspaces() map[string][]string {
	rng := rand.New(rand.NewSource(7))
	gen := func(n int, f func(i int) string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = f(i)
		}
		return out
	}
	alnum := "abcdefghijklmnopqrstuvwxyz0123456789"
	randStr := func(n int, alphabet string) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = alphabet[rng.Intn(len(alphabet))]
		}
		return string(b)
	}
	ks := map[string][]string{
		"empty":            nil,
		"oneKey":           {"a"},
		"exactly3000":      gen(3000, func(i int) string { return fmt.Sprintf("k%06d", i) }),
		"flatRandom":       gen(4000, func(int) string { return randStr(1+rng.Intn(16), alnum) }),
		"singlePrefixFlat": gen(4000, func(i int) string { return fmt.Sprintf("p/%07d", i) }),
		"flatUpperHex":     gen(4000, func(int) string { return randStr(8, "0123456789ABCDEF") }),
		"longKeys1024":     gen(1500, func(i int) string { return strings.Repeat("L", 1018) + fmt.Sprintf("%06d", i) }),
		"lastByteDiffer":   gen(2000, func(i int) string { return "same/long/prefix/" + string(rune(0x21+i%90)) + string(rune(0x21+i/90)) }),
		"maxScalarRun":     gen(300, func(i int) string { return strings.Repeat("\U0010FFFF", 1+i%40) + strconv.Itoa(i) }),
		"supplementaryPlane": gen(3000, func(int) string {
			return string(rune(0x10000+rng.Intn(0xFFFFF-0x10000))) + string(rune(0x1F600+rng.Intn(80))) + randStr(3, alnum)
		}),
		"markersAndOddChars": {"a/", "a/x", "a/y/", "a/y/z", "b", "c%20d", "c%", "d\x01e", "d\x7f", "e/\x01/f", "f/%", "zz/"},
	}
	var deep []string
	for a := range 5 {
		for b := range 5 {
			for c := range 5 {
				for d := range 5 {
					for f := range 8 {
						deep = append(deep, fmt.Sprintf("d%d/e%d/f%d/g%d/file%d", a, b, c, d, f))
					}
				}
			}
		}
	}
	ks["deepTree"] = deep
	skew := gen(4000, func(i int) string { return fmt.Sprintf("big/%06d", i) })
	skew = append(skew, gen(50, func(i int) string { return fmt.Sprintf("small%02d/x", i) })...)
	ks["badlySkewed"] = skew
	needle := gen(4000, func(i int) string { return fmt.Sprintf("h/needle/%05d", i) })
	needle = append(needle, "a", "m/x", "z/y")
	ks["hotNeedle"] = needle
	for k, v := range ks {
		for _, s := range v {
			if !utf8.ValidString(s) {
				panic(k)
			}
		}
	}
	return ks
}

type span struct {
	lo, hi string
	open   bool
}

func run(t *testing.T, api *sorted, opt Options, workers int) ([]string, Stats, []span) {
	return runBudget(t, api, opt, workers, 500)
}

// runBudget runs with a reorder budget in rows (the CLI default is 131,072).
func runBudget(t *testing.T, api *sorted, opt Options, workers, budget int) ([]string, Stats, []span) {
	t.Helper()
	var mu sync.Mutex
	var leaves []span
	opt.Bucket, opt.Client, opt.Workers = "b", api, workers
	opt.trace = func(lo, hi string, open bool) {
		mu.Lock()
		leaves = append(leaves, span{lo, hi, open})
		mu.Unlock()
	}
	p, err := New(context.Background(), opt)
	if err != nil {
		t.Fatal(err)
	}
	c := &collect{}
	if _, err := order.Run[Range](context.Background(), p, c, order.Options{Workers: workers, Budget: budget}); err != nil {
		t.Fatal(err)
	}
	return keysOf(c.rows), p.Stats(), leaves
}

// T1: random splits (a seeded coin instead of the demand gate, guard on)
// cover every key exactly once, in order, and the covered intervals tile
// (⊥, ⊤].
func TestT1RandomSplitsCoverEveryKeyOnce(t *testing.T) {
	seeds := 50
	if testing.Short() {
		seeds = 5
	}
	for name, keys := range keyspaces() {
		want := slices.Compact(slices.Sorted(slices.Values(keys)))
		for s := range seeds {
			for _, w := range []int{1, 4, 64} {
				rng := rand.New(rand.NewSource(int64(s*1000 + w)))
				var mu sync.Mutex
				coin := func() bool { mu.Lock(); defer mu.Unlock(); return rng.Intn(3) == 0 }
				got, st, leaves := run(t, newSorted(keys, true, int64(s)), Options{splitCoin: coin, Seed: s%2 == 1}, w)
				if !slices.Equal(got, want) {
					t.Fatalf("%s seed=%d w=%d: %d keys, want %d (splits %d)", name, s, w, len(got), len(want), st.Split.AcceptedBounded+st.Split.AcceptedFrontier)
				}
				checkTiling(t, fmt.Sprintf("%s seed=%d w=%d", name, s, w), leaves)
			}
		}
	}
}

func checkTiling(t *testing.T, what string, leaves []span) {
	t.Helper()
	slices.SortFunc(leaves, func(a, b span) int { return strings.Compare(a.lo, b.lo) })
	prev := ""
	for i, l := range leaves {
		if l.lo != prev {
			t.Fatalf("%s: gap or overlap at leaf %d: lo %q, previous hi %q", what, i, l.lo, prev)
		}
		if l.open {
			if i != len(leaves)-1 {
				t.Fatalf("%s: open leaf %d is not last", what, i)
			}
			return
		}
		if l.hi <= l.lo && l.lo != "" {
			t.Fatalf("%s: empty leaf (%q, %q]", what, l.lo, l.hi)
		}
		prev = l.hi
	}
	t.Fatalf("%s: no open leaf: the frontier (.., ⊤] was not covered", what)
}

// T2: pivot properties.
func TestT2Pivots(t *testing.T) {
	check := func(a, b string) {
		t.Helper()
		m, ok := between(a, b)
		if !ok {
			return
		}
		if !(a < m && m < b) || !utf8.ValidString(m) || len(m) > maxKeyLen {
			t.Fatalf("between(%q, %q) = %q", a, b, m)
		}
	}
	rng := rand.New(rand.NewSource(1))
	alphabet := []rune{0, 1, 0x1F, ' ', '!', '%', 'a', 'z', 0x7F, 0x80, 0x9F, 0xA0, 0xD7FF, 0xE000, 0xFDD0, 0xFFFD, 0xFFFE, 0x10000, 0x10FFFF}
	for range 20000 {
		mk := func() string {
			var b []rune
			for range rng.Intn(6) {
				b = append(b, alphabet[rng.Intn(len(alphabet))])
			}
			return string(b)
		}
		a, b := mk(), mk()
		if a > b {
			a, b = b, a
		}
		if a < b {
			check(a, b)
		}
	}
	for _, c := range [][2]string{
		{"", "b"}, {"a", "ab"}, {"a", "a b"}, {"a", "b"}, {"a\x1f", "a!"}, {"�", "\U00010000"}, {"퟿", ""},
		{"\U0001F600", "\U0001F610"}, {strings.Repeat("x", 1023) + "a", strings.Repeat("x", 1023) + "c"},
		{"A" + strings.Repeat("\x00", 1023), "B"},
	} {
		check(c[0], c[1])
	}
	for _, nilCase := range [][2]string{{"a", "a\x00"}, {"a", "a\x01"}} {
		if m, ok := between(nilCase[0], nilCase[1]); ok {
			t.Fatalf("between(%q, %q) = %q, want none", nilCase[0], nilCase[1], m)
		}
	}
	if m, ok := between("a", "a "); ok {
		t.Fatalf("a < m < 'a ' has no safe pivot, got %q", m)
	}
	for range 5000 {
		lo := string(alphabet[rng.Intn(len(alphabet))]) + "x"
		c := lo + string(rune('a'+rng.Intn(26)))
		if m, ok := forwardReflect(lo, c); ok && (m <= c || !utf8.ValidString(m) || len(m) > maxKeyLen) {
			t.Fatalf("forwardReflect(%q, %q) = %q", lo, c, m)
		}
	}
	if u, ok := prefixCeil("ab\xff\xff"); !ok || u != "ac" {
		t.Fatalf("prefixCeil = %q %v", u, ok)
	}
	if _, ok := prefixCeil("\xff"); ok {
		t.Fatal("prefixCeil of all-0xFF must be open")
	}
}

// T3: one test per guard clause, the progress gate and the demand gate.
func TestT3Guard(t *testing.T) {
	for _, tc := range []struct {
		c, m    string
		found   bool
		ceil    string
		bounded bool
		done    bool
		want    clause
	}{
		{"b", "c", true, "d", true, false, clauseOK},
		{"b", "b", true, "d", true, false, clauseCursor},
		{"b", "a", true, "d", true, false, clauseCursor},
		{"b", "d", true, "d", true, false, clauseBound},
		{"b", "e", true, "d", true, false, clauseBound},
		{"b", "zz", true, "", false, false, clauseOK}, // open frontier, no ceiling
		{"b", "c", true, "d", true, true, clauseDone},
		{"b\x01", "c", true, "d", true, false, clauseUnsafeCursor},
		{"b", "", false, "d", true, false, clauseNoPivot},
	} {
		if got := guard(tc.c, tc.m, tc.found, tc.ceil, tc.bounded, tc.done); got != tc.want {
			t.Errorf("guard(%+v) = %d, want %d", tc, got, tc.want)
		}
	}
	keys := keyspaces()["singlePrefixFlat"]
	// No demand with one worker: never split.
	_, st, _ := run(t, newSorted(keys, false, 1), Options{}, 1)
	if st.Split.AcceptedBounded+st.Split.AcceptedFrontier != 0 || st.Split.Skipped.NoDemand == 0 {
		t.Fatalf("w1 split: %+v", st.Split)
	}
	// Unsafe cursor: keys ending in a control character never become a lower bound.
	odd := make([]string, 3000)
	for i := range odd {
		odd[i] = fmt.Sprintf("k%05d\x01", i)
	}
	got, st, _ := run(t, newSorted(odd, false, 1), Options{}, 8)
	if len(got) != 3000 || st.Split.Rejected.UnsafeCursor == 0 {
		t.Fatalf("unsafe cursor: %d keys, %+v", len(got), st.Split.Rejected)
	}
	// A page that ends a range is never followed by a split (done is checked
	// before the decision), and splits need a full page (progress gate):
	// one page of keys never splits.
	_, st, _ = run(t, newSorted(keys[:900], false, 1), Options{}, 8)
	if st.Split.Considered != 0 {
		t.Fatalf("split considered on a single final page: %+v", st.Split)
	}
}

// T4: paging edges.
func TestT4Paging(t *testing.T) {
	keys := keyspaces()["exactly3000"]
	f := newSorted(keys, false, 1)
	p, _ := New(context.Background(), Options{Bucket: "b", Client: f, Workers: 1})
	c := &collect{}
	r := Range{lo: "", hi: keys[999], id: 1, born: fromLower}
	if err := p.expandRange(context.Background(), r, &direct{c}); err != nil || len(c.rows) != 1000 || f.calls != 1 {
		t.Fatalf("page ending at hi: err=%v rows=%d calls=%d", err, len(c.rows), f.calls)
	}
	// Truncated empty page.
	empty := &stub{pages: []*awss3.ListObjectsV2Output{{IsTruncated: aws.Bool(true), NextContinuationToken: aws.String("1")}, {}}}
	p, _ = New(context.Background(), Options{Bucket: "b", Client: empty, Workers: 1})
	if err := p.expandRange(context.Background(), Range{open: true}, &direct{&collect{}}); err == nil {
		t.Fatal("truncated empty page accepted")
	}
	// Non-ascending keys and keys not after lo.
	bad := &stub{pages: []*awss3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("b")}, {Key: aws.String("a")}}}}}
	p, _ = New(context.Background(), Options{Bucket: "b", Client: bad, Workers: 1})
	if err := p.expandRange(context.Background(), Range{open: true}, &direct{&collect{}}); err == nil {
		t.Fatal("non-ascending page accepted")
	}
	behind := &stub{pages: []*awss3.ListObjectsV2Output{{Contents: []types.Object{{Key: aws.String("a")}}}}}
	p, _ = New(context.Background(), Options{Bucket: "b", Client: behind, Workers: 1})
	if err := p.expandRange(context.Background(), Range{lo: "b", open: true}, &direct{&collect{}}); err == nil {
		t.Fatal("key not after lo accepted")
	}
	// Cancellation mid-request.
	slow := newSorted(keys, false, 1)
	slow.latency = time.Second
	p, _ = New(context.Background(), Options{Bucket: "b", Client: slow, Workers: 4})
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	if _, err := order.Run[Range](ctx, p, &collect{}, order.Options{Workers: 4, Budget: 100}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines left: %d -> %d", before, after)
	}
	// Seed: truncated top level, cap and subsample, sentinel.
	var many []string
	for i := range 1500 {
		many = append(many, fmt.Sprintf("d%04d/x", i))
	}
	_, st, leaves := run(t, newSorted(many, false, 1), Options{Seed: true}, 2)
	if !st.Seed.Truncated || !st.Seed.Subsampled || st.Seed.Cuts != 8 || st.Seed.Sentinel {
		t.Fatalf("seed (truncated, cap 4W=8): %+v", st.Seed)
	}
	checkTiling(t, "seed truncated", leaves)
	_, st, leaves = run(t, newSorted([]string{"a/1", "b/1", "c/1"}, false, 1), Options{Seed: true}, 4)
	if !st.Seed.Sentinel || st.Seed.Cuts != 3 {
		t.Fatalf("seed sentinel: %+v", st.Seed)
	}
	checkTiling(t, "seed sentinel", leaves)
}

// direct is an emitter that collects rows and ignores children.
type direct struct{ c *collect }

func (d *direct) Rows(rows []schema.Row) error {
	return d.c.Write(context.Background(), [][]schema.Row{rows})
}
func (d *direct) Child(Range) error { return nil }

// T6: balance smoke (synthetic: singlePrefixFlat 200k keys, 20 ms per page,
// 16 workers, the default reorder budget). An engagement check, not a
// performance claim.
func TestT6BalanceSmoke(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	keys := make([]string, 200_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("p/%07d", i)
	}
	f := newSorted(keys, false, 1)
	f.latency = 20 * time.Millisecond
	got, st, leaves := runBudget(t, f, Options{}, 16, 131_072)
	if len(got) != len(keys) {
		t.Fatalf("%d keys", len(got))
	}
	checkTiling(t, "T6", leaves)
	accepted := st.Split.AcceptedBounded + st.Split.AcceptedFrontier
	t.Logf("max_active=%d accepted=%d (bounded %d, frontier %d) child_empty=%+v requests=%d", st.MaxActive, accepted,
		st.Split.AcceptedBounded, st.Split.AcceptedFrontier, st.ChildEmpty, st.Requests)
	if st.MaxActive < 12 || accepted == 0 {
		t.Fatalf("parallelism did not engage: max_active=%d accepted=%d", st.MaxActive, accepted)
	}
}
