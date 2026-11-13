// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package azure

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/varveio/swath/internal/replay/latency"
	"github.com/varveio/swath/internal/replay/output"
	"github.com/varveio/swath/internal/replay/store"
	"github.com/varveio/swath/internal/replay/store/memstore"
)

// The Java route's unit tests that have a Go counterpart, ported.

// countingStore records each Rows limit.
type countingStore struct {
	*memstore.Store
	limits []int
}

func (c *countingStore) Rows(ctx context.Context, from []byte, inclusive bool, to []byte, limit int, p store.Projection) ([]store.Object, error) {
	c.limits = append(c.limits, limit)
	return c.Store.Rows(ctx, from, inclusive, to, limit, p)
}

func names(res *Result) []string {
	var out []string
	for _, e := range res.Entries {
		if e.Object != nil {
			out = append(out, "O:"+string(e.Object.Key))
		} else {
			out = append(out, "P:"+string(e.Prefix))
		}
	}
	return out
}

func keyStore(t *testing.T, rollup bool, keys ...string) *memstore.Store {
	t.Helper()
	slices.Sort(keys)
	return newStore(t, rollup, objects(keys...))
}

func req(prefix, delimiter, startFrom, marker string, size int) *Request {
	return &Request{Account: "replay", Container: "c1", Version: Version20260606, Prefix: prefix, Delimiter: delimiter,
		StartFrom: startFrom, Marker: marker, MarkerSupplied: marker != "", PageSize: size}
}

// AzurePagerTest.fallbackBatchesFlatEntriesUnderDelimiter.
func TestFallbackBatchesFlatEntriesUnderDelimiter(t *testing.T) {
	var keys []string
	for i := range 1000 {
		keys = append(keys, fmt.Sprintf("flat-%04d", i))
	}
	st := keyStore(t, false, keys...)
	res, err := (&Pager{Store: st, FixtureIdentity: "f"}).List(context.Background(), parse(t, "delimiter=%2F&maxresults=1000"))
	if err != nil || len(res.Entries) != 1000 || st.Calls() > 2 {
		t.Fatalf("%d entries, %d calls, %v", len(res.Entries), st.Calls(), err)
	}
}

// AzurePagerTest.nativeFiveThousandPageLimitAndMarkerScope.
func TestFiveThousandPageLimitAndMarkerScope(t *testing.T) {
	var keys []string
	for i := range 5001 {
		keys = append(keys, fmt.Sprintf("key-%05d", i))
	}
	p := &Pager{Store: keyStore(t, false, keys...), FixtureIdentity: "f"}
	first := parse(t, "maxresults=5001")
	res, err := p.List(context.Background(), first)
	if err != nil || len(res.Entries) != 5000 || res.NextMarker == "" {
		t.Fatalf("%d entries %q %v", len(res.Entries), res.NextMarker, err)
	}
	next := parse(t, "maxresults=5001&marker="+res.NextMarker)
	if res, err := p.List(context.Background(), next); err != nil || !slices.Equal(names(res), []string{"O:key-05000"}) {
		t.Fatalf("%v %v", names(res), err)
	}
	other := parse(t, "marker="+res.NextMarker)
	other.Version = Version20261006
	if err := p.ValidateMarker(other); err == nil {
		t.Fatal("a marker of another service version was accepted")
	}
}

// AzurePagerTest.injectedLatencyIsKeyedByTheSameRequestShapesAsS3.
func TestInjectedLatencyShapes(t *testing.T) {
	prof, err := latency.Parse("worker_page=100ms,pivot_probe=20ms,structure_probe=50ms+5ms/cp", 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	p := &Pager{Store: keyStore(t, true, "a/1", "a/2", "b/1", "c"), FixtureIdentity: "f"}
	for q, want := range map[string]time.Duration{"": 100 * time.Millisecond, "maxresults=1": 20 * time.Millisecond,
		"delimiter=%2F": 60 * time.Millisecond} {
		r := parse(t, q)
		res, err := p.List(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if got := prof.Delay(r.Shape(), int64(res.Prefixes), r.LatencyKey()); got != want {
			t.Errorf("%q: %v, want %v", q, got, want)
		}
	}
}

// AzurePagerTest.preparedPageReportsExactBlobAndPrefixCounts.
func TestPageReportsBlobAndPrefixCounts(t *testing.T) {
	p := &Pager{Store: keyStore(t, true, "a/1", "b"), FixtureIdentity: "f"}
	for q, want := range map[string][3]int{"delimiter=%2F&maxresults=1": {int(latency.StructureProbe), 0, 1},
		"maxresults=1": {int(latency.PivotProbe), 1, 0}} {
		r := parse(t, q)
		res, err := p.List(context.Background(), r)
		if err != nil {
			t.Fatal(err)
		}
		if got := [3]int{int(r.Shape()), len(res.Entries) - res.Prefixes, res.Prefixes}; got != want {
			t.Errorf("%q: %v, want %v", q, got, want)
		}
	}
}

// AzureProviderOrderTest.comparatorIsUtf16OrderForValidNames.
func TestCompareIsUTF16OrderForValidNames(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	runes := []rune{'a', 'z', 0x7f, 0xe9, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xfffd, 0xffff, 0x10000, 0x1f600, 0x10ffff}
	word := func() string {
		var b strings.Builder
		for range 1 + rng.IntN(3) {
			b.WriteRune(runes[rng.IntN(len(runes))])
		}
		return b.String()
	}
	for range 20_000 {
		a, b := word(), word()
		if got, want := Compare([]byte(a), []byte(b)), utf16Less(a, b); got != want {
			t.Fatalf("%q vs %q: %d, want %d", a, b, got, want)
		}
	}
}

// AzureProviderOrderTest.namesWithoutLeadBytesAtOrAboveEeKeepOneRangeAndOneRead.
func TestNamesWithoutLeadBytesKeepOneRangeAndOneRead(t *testing.T) {
	if n := len(rangesFrom([]byte("root/a/b/c"), false, []byte("root/"))); n != 1 {
		t.Errorf("%d ranges", n)
	}
	if n := len(rangesFrom([]byte("root/\ue000\U0001F600"), false, []byte("root/"))); n != 4 {
		t.Errorf("%d ranges", n)
	}
	var keys []string
	for i := range 100 {
		keys = append(keys, fmt.Sprintf("k%03d", i))
	}
	st := &countingStore{Store: keyStore(t, false, keys...)}
	p := &Pager{Store: st, FixtureIdentity: "f"}
	first, err := p.List(context.Background(), req("", "", "k010", "", 10))
	if err != nil || len(st.limits) != 1 || st.limits[0] != 11 {
		t.Fatalf("reads %v %v", st.limits, err)
	}
	if _, err := p.List(context.Background(), req("", "", "k010", first.NextMarker, 10)); err != nil || len(st.limits) != 2 {
		t.Fatalf("reads %v %v", st.limits, err)
	}
}

// AzureProviderOrderTest.deepLeadByteBoundsRetireEmptyPartsWithAFewProbes.
func TestDeepLeadByteBoundsRetireEmptyPartsWithAFewProbes(t *testing.T) {
	supplementary := strings.Repeat("\U00010000", 1024)
	privateUse := strings.Repeat("\ue000", 1365)
	deep := strings.Repeat("\U00010000", 255) + "a"
	if n := len(rangesFrom([]byte(supplementary), true, nil)); n != 2049 {
		t.Errorf("%d ranges", n)
	}
	if n := len(rangesFrom([]byte(privateUse), true, nil)); n != 1366 {
		t.Errorf("%d ranges", n)
	}
	keys := []string{"a", "a/1", "\u00e9", "\U00010000/x", "\U0001F600", "\ue000x", "\uff21", "\uff21/y", deep,
		strings.Repeat("\ue000", 340) + "z", "\uffff"}
	for _, rollup := range []bool{false, true} {
		for _, delimiter := range []string{"", "/"} {
			for _, start := range []string{supplementary, privateUse, deep} {
				for _, size := range []int{1, 5000} {
					st := keyStore(t, rollup, slices.Clone(keys)...)
					p := &Pager{Store: st, FixtureIdentity: "f"}
					first, err := p.List(context.Background(), req("", delimiter, start, "", size))
					if err != nil || st.Calls() > 12 {
						t.Fatalf("first page %q %d %d: %d calls %v", delimiter, len(start), size, st.Calls(), err)
					}
					calls := st.Calls()
					if first.NextMarker != "" {
						if _, err := p.List(context.Background(), req("", delimiter, start, first.NextMarker, size)); err != nil || st.Calls()-calls > 12 {
							t.Fatalf("continuation %q %d %d: %d calls %v", delimiter, len(start), size, st.Calls()-calls, err)
						}
					}
				}
			}
		}
	}
}

// AzureProviderOrderTest.swappedBranchesReuseRowsAlreadyRead.
func TestSwappedBranchesReuseRowsAlreadyRead(t *testing.T) {
	st := keyStore(t, false, "a", "\ue000", "\uffff", "\U00010000", "z\u00ff")
	res, err := (&Pager{Store: st, FixtureIdentity: "f"}).List(context.Background(), req("", "", "", "", 5000))
	want := []string{"O:a", "O:z\u00ff", "O:\U00010000", "O:\ue000", "O:\uffff"}
	if err != nil || !slices.Equal(names(res), want) || st.Calls() != 1 {
		t.Fatalf("%q, %d calls, %v", names(res), st.Calls(), err)
	}
}

// AzureProviderOrderTest.invalidUtf8NamesKeepATotalOrderWithEveryNameOnce.
func TestInvalidUTF8NamesKeepATotalOrder(t *testing.T) {
	keys := []string{"\xee", "\xefA", "\xf0", "\xf4\xff", "\xf5", "\xff", "A\xee\xf0", "A\xf0\xee", "\xc0\x80", "\xed\xa0\x80"}
	want := slices.Clone(keys)
	slices.SortFunc(want, func(a, b string) int { return Compare([]byte(a), []byte(b)) })
	for _, k := range keys {
		if utf8.ValidString(k) {
			t.Fatalf("%q is valid", k)
		}
	}
	p := &Pager{Store: keyStore(t, false, slices.Clone(keys)...), FixtureIdentity: "f"}
	for _, size := range []int{1, 2, 3, 5000} {
		var got []string
		marker := ""
		for {
			res, err := p.List(context.Background(), req("", "", "", marker, size))
			if err != nil {
				t.Fatal(err)
			}
			for _, e := range res.Entries {
				got = append(got, string(e.Object.Key))
			}
			if marker = res.NextMarker; marker == "" {
				break
			}
		}
		if !slices.Equal(got, want) {
			t.Errorf("size %d: %q, want %q", size, got, want)
		}
	}
}

// AzureXmlTest.rejectsUnrenderableYearWithoutChangingValidNegativeEpoch.
func TestUnrenderableYearsAndNegativeEpoch(t *testing.T) {
	r := parse(t, "")
	first := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	afterLast := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		t  time.Time
		ok bool
	}{
		{first.Add(-time.Second), false}, {afterLast, false},
		{first, true}, {afterLast.Add(-time.Microsecond), true}, {time.UnixMicro(-1).UTC(), true},
	} {
		size := int64(1)
		o := store.Object{Key: []byte("a"), Size: &size, LastModified: &c.t}
		body, err := AppendList(nil, &Result{Request: r, Entries: []Entry{{Object: &o}}}, "http://127.0.0.1/replay/")
		if c.ok != (err == nil) || c.ok && !strings.Contains(string(body), "<Last-Modified>") {
			t.Errorf("%v: %v", c.t, err)
		}
	}
	// One microsecond before the epoch is the last second of 1969.
	size := int64(1)
	tm := time.UnixMicro(-1).UTC()
	o := store.Object{Key: []byte("a"), Size: &size, LastModified: &tm}
	body, _ := AppendList(nil, &Result{Request: r, Entries: []Entry{{Object: &o}}}, "http://127.0.0.1/replay/")
	if !strings.Contains(string(body), "<Last-Modified>Wed, 31 Dec 1969 23:59:59 GMT</Last-Modified>") {
		t.Errorf("%s", body)
	}
}

// AzureWireTest.httpDatePadsSingleDigitDay.
func TestHTTPDatePadsSingleDigitDay(t *testing.T) {
	if got := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Format(HTTPTime); got != "Thu, 01 Oct 2026 00:00:00 GMT" {
		t.Errorf("%s", got)
	}
}

// TestRenderErrorPrecedence pins the order of sink writes and fixture
// checks under a small cap, the replay wire contract's: with a 256-byte
// cap a valid 1,024-byte name is a write the cap refuses
// (response_too_large) before the year-10000 time after it is checked,
// while a negative size, checked before the entry is opened, and a
// control character in a short name, checked before the name is written,
// are fixture refusals; with room for the name the time is the refusal.
func TestRenderErrorPrecedence(t *testing.T) {
	r := parse(t, "")
	r.Container = "bucket"
	year10000 := time.UnixMicro(253402300800000000).UTC()
	ok := time.Unix(1_700_000_000, 0).UTC()
	obj := func(name string, size int64, tm time.Time) store.Object {
		return store.Object{Key: []byte(name), Size: &size, LastModified: &tm}
	}
	for _, c := range []struct {
		name   string
		obj    store.Object
		cap    int
		reason string // "" for the cap's refusal
	}{
		{"long name, bad year, small cap", obj(strings.Repeat("q", 1024), 1, year10000), 256, ""},
		{"long name, bad year, room", obj(strings.Repeat("q", 1024), 1, year10000), 1 << 20, "timestamp_year_out_of_range"},
		{"negative size, small cap", obj("neg", -1, ok), 256, "negative_size"},
		{"control character, small cap", obj("c\x01", 1, ok), 256, "control_character"},
		{"over-long name, small cap", obj(strings.Repeat("r", 1025), 1, ok), 256, "name_too_long"},
	} {
		o, err := output.New(output.NewBudget(1<<30, nil), 64, c.cap)
		if err != nil {
			t.Fatal(err)
		}
		o2 := c.obj
		err = RenderList(o, &Result{Request: r, Entries: []Entry{{Object: &o2}}}, "http://127.0.0.1:1/replay/")
		var fe *Error
		switch {
		case c.reason == "" && (err != nil || o.Err() != output.ErrTooLarge):
			t.Errorf("%s: %v, sink %v; want the cap's refusal", c.name, err, o.Err())
		case c.reason != "" && (!errors.As(err, &fe) || fe.Reason != c.reason):
			t.Errorf("%s: %v, sink %v; want %s", c.name, err, o.Err(), c.reason)
		}
		o.Close()
	}
}
