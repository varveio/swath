// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/varveio/swath/go/internal/replay/store"
	"github.com/varveio/swath/go/internal/replay/store/memstore"
	"github.com/varveio/swath/go/sortedparquet/sortedparquettest"
)

// The wire contract's own unit tests for the GCS route, held here
// against this implementation.

func render(t *testing.T, r *Request, page *Page) string {
	t.Helper()
	b, err := AppendPage(nil, r, page)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func updatedOf(t *testing.T, body string) []string {
	t.Helper()
	var doc struct{ Items []struct{ Updated string } }
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, it := range doc.Items {
		out = append(out, it.Updated)
	}
	return out
}

// independent is the expected updated text: time's own formatter with six
// fractional digits.
func independent(micros int64) string {
	sec, frac := floorDiv(micros, 1_000_000)
	return time.Unix(sec, frac*1000).UTC().Format("2006-01-02T15:04:05.000000Z")
}

// TestTimestampsMatchAnIndependentFormatter renders times at and around
// the epoch, a day boundary, and both ends of years 0 to 9999 (and 500
// random ones), one per page and all in one page (the renderer formats a
// date once per run of a day), and refuses times outside those years.
func TestTimestampsMatchAnIndependentFormatter(t *testing.T) {
	r := &Request{Bucket: "bucket", PageSize: 1}
	lo := time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).Unix() * 1_000_000
	hi := time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() * 1_000_000
	micros := []int64{-1, 0, 1, -1_000_001, 951_782_399_999_999, 951_782_400_000_000, lo, hi - 1}
	rng := rand.New(rand.NewPCG(0x220223, 0))
	for range 500 {
		micros = append(micros, lo+rng.Int64N(hi-lo))
	}
	for _, m := range micros {
		got := updatedOf(t, render(t, r, &Page{Items: []Item{{Key: []byte("x"), Size: 1, Updated: m}}}))
		if got[0] != independent(m) {
			t.Fatalf("%d: %s, want %s", m, got[0], independent(m))
		}
	}
	transitions := []int64{-1, 0, 1, 951_782_399_999_999, 951_782_400_000_000, 951_782_400_000_001}
	page := &Page{}
	for i, m := range transitions {
		page.Items = append(page.Items, Item{Key: []byte(fmt.Sprintf("k%d", i)), Size: 1, Updated: m})
	}
	for i, got := range updatedOf(t, render(t, r, page)) {
		if got != independent(transitions[i]) {
			t.Errorf("%d: %s", transitions[i], got)
		}
	}
	for _, m := range []int64{math.MinInt64, math.MaxInt64, lo - 1, hi} {
		_, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte("x"), Updated: m}}})
		var p *FixtureProblem
		if !errors.As(err, &p) || p.Reason != "timestamp_out_of_range" {
			t.Errorf("%d: %v", m, err)
		}
	}
}

func TestRendersProfileTypes(t *testing.T) {
	body := render(t, &Request{Bucket: "bucket", PageSize: 1000, FullProjection: true},
		&Page{Items: []Item{{Key: []byte("é/\""), Size: 12, Updated: -1}}})
	var doc struct{ Items []map[string]any }
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	it := doc.Items[0]
	if it["name"] != "é/\"" || it["size"] != "12" || it["updated"] != "1969-12-31T23:59:59.999999Z" || it["generation"] != "1" {
		t.Errorf("%v", it)
	}
	if acl, ok := it["acl"].([]any); !ok || len(acl) != 0 || it["owner"] != nil {
		t.Errorf("%v", it)
	}
}

func TestRawEscapesAndOmitsEmptyArrays(t *testing.T) {
	body := render(t, &Request{Bucket: "bucket", PageSize: 1}, &Page{Items: []Item{{Key: []byte("a\\\"\u2028\U0001F600")}}})
	if !strings.Contains(body, `"name":"a\\\"`+"\u2028\U0001F600"+`"`) || !strings.Contains(body, `"updated":"1970-01-01T00:00:00.000000Z"`) ||
		strings.Contains(body, `"prefixes"`) || strings.Contains(body, `"acl"`) {
		t.Errorf("%s", body)
	}
}

func TestCompleteWireGolden(t *testing.T) {
	got := render(t, &Request{Bucket: "bucket", PageSize: 2, FullProjection: true}, &Page{
		Items: []Item{
			{Key: []byte("a"), Size: 12, Updated: 1_767_225_600_000_000},
			{Key: []byte("b"), Size: 34, Updated: 1_767_225_600_123_456},
		},
		Prefixes: [][]byte{[]byte("c/")},
		Token:    "opaque",
	})
	const want = `{"kind":"storage#objects","items":[` +
		`{"kind":"storage#object","bucket":"bucket","name":"a","size":"12","updated":"2026-01-01T00:00:00.000000Z",` +
		`"etag":"swath-gcs-synthetic-v1","generation":"1","metageneration":"1","storageClass":"STANDARD",` +
		`"contentType":"application/octet-stream","acl":[]},` +
		`{"kind":"storage#object","bucket":"bucket","name":"b","size":"34","updated":"2026-01-01T00:00:00.123456Z",` +
		`"etag":"swath-gcs-synthetic-v1","generation":"1","metageneration":"1","storageClass":"STANDARD",` +
		`"contentType":"application/octet-stream","acl":[]}],"prefixes":["c/"],"nextPageToken":"opaque"}`
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestRefusesTooLongAndReservedNamesAndNegativeSize(t *testing.T) {
	r := &Request{Bucket: "bucket", PageSize: 1}
	for name, reason := range map[string]string{
		".": "reserved_name", "..": "reserved_name", ".well-known/acme-challenge/x": "reserved_name",
		strings.Repeat("x", 1025): "name_length", "\xff": "invalid_utf8_name", "a\nb": "name_newline", "a\rb\xff": "name_newline",
		"a\xffb\n": "invalid_utf8_name", "": "name_length",
	} {
		_, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte(name), Size: 1}}})
		var p *FixtureProblem
		if !errors.As(err, &p) || p.Reason != reason {
			t.Errorf("%.20q: %v, want %s", name, err, reason)
		}
	}
	if _, err := AppendPage(nil, r, &Page{Items: []Item{{Key: []byte("ok"), Size: -1}}}); err == nil || err.(*FixtureProblem).Reason != "negative_size" {
		t.Errorf("negative size: %v", err)
	}
	if _, err := AppendPage(nil, r, &Page{Prefixes: [][]byte{[]byte("..")}, Items: nil}); err != nil {
		t.Errorf("a prefix is not checked for reserved names: %v", err)
	}
	render(t, r, &Page{Items: []Item{{Key: []byte(strings.Repeat("x", 1024)), Size: 1}}})
}

func TestClampedPageLimitReportsItsPath(t *testing.T) {
	p := pagerOver(false, "a")
	var paths []string
	p.OnPath = func(path, reason string) { paths = append(paths, path+":"+reason) }
	list(t, p, "maxResults=5000")
	if !slices.Contains(paths, "page_limit_clamped:requested_above_1000") || !slices.Contains(paths, "flat:open_upper") {
		t.Errorf("%v", paths)
	}
}

// countingStore records the limit of every range read.
type countingStore struct {
	store.Store
	limits []int
}

func (c *countingStore) Rows(ctx context.Context, from []byte, inc bool, to []byte, limit int, p store.Projection) ([]store.Object, error) {
	c.limits = append(c.limits, limit)
	return c.Store.Rows(ctx, from, inc, to, limit, p)
}

// TestDelimiterWalkReadsInBatches: the walk (a store that declines the
// rollup) reads a page of direct objects in at most two reads, and a
// one-entry page reads that entry and one look-ahead row, or, when the
// entry is a prefix, the one row that names it.
func TestDelimiterWalkReadsInBatches(t *testing.T) {
	var keys []string
	for i := range 1000 {
		keys = append(keys, fmt.Sprintf("flat-%04d", i))
	}
	cs := &countingStore{Store: memstore.Keys(memstore.Options{}, keys...)}
	page := list(t, NewPager(cs, "fixture"), "delimiter=/&maxResults=1000")
	if len(page.Items) != 1000 || page.Token != "" || len(cs.limits) > 2 {
		t.Errorf("%d items, token %q, reads %v", len(page.Items), page.Token, cs.limits)
	}

	cs = &countingStore{Store: memstore.Keys(memstore.Options{}, "a", "b", "c", "d")}
	p := NewPager(cs, "fixture")
	first := list(t, p, "delimiter=/&maxResults=1")
	if got := entries(first); !slices.Equal(got, []string{"a"}) || first.Token == "" || !slices.Equal(cs.limits, []int{2}) {
		t.Errorf("%v %v", got, cs.limits)
	}
	second := list(t, p, "delimiter=/&maxResults=1&pageToken="+first.Token)
	if got := entries(second); !slices.Equal(got, []string{"b"}) || cs.limits[len(cs.limits)-1] != 2 {
		t.Errorf("%v %v", got, cs.limits)
	}

	cs = &countingStore{Store: memstore.Keys(memstore.Options{}, "a/1", "a/2", "a/3", "b")}
	page = list(t, NewPager(cs, "fixture"), "delimiter=/&maxResults=1")
	if got := entries(page); !slices.Equal(got, []string{"a/|"}) || page.Token == "" || cs.limits[len(cs.limits)-1] != 1 {
		t.Errorf("%v %v", got, cs.limits)
	}
}

// TestIndexedDelimiterMatchesWalkAcrossRowGroups serves a sorted fixture
// of many small row groups (its rollup answers "/") and the same keys from
// a store that walks, and requires the same walks over offset windows,
// including the empty gap just before a row group that starts inside a
// subtree: the rollup must not infer a prefix from bounding keys alone.
func TestIndexedDelimiterMatchesWalkAcrossRowGroups(t *testing.T) {
	var keys []string
	for _, branch := range []string{"a", "b", "c"} {
		for i := range 500 {
			keys = append(keys, fmt.Sprintf("root/%s/%03d", branch, i))
		}
	}
	keys = append(keys, "root/direct", "root/z")
	slices.Sort(keys)
	const groupRows = 37
	var rows []sortedparquettest.Row
	for i, k := range keys {
		rows = append(rows, sortedparquettest.Object(k, i))
	}
	dir := t.TempDir()
	part := sortedparquettest.Part{Rows: rows, Stamps: sortedparquettest.Stamps(1, true), GroupRows: groupRows, PageBytes: 2048}
	if err := sortedparquettest.Write(filepath.Join(dir, "part-00001.parquet"), part); err != nil {
		t.Fatal(err)
	}
	sorted, meta, err := store.Open(context.Background(), dir, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer sorted.Close()
	if meta.RowGroups < 2 {
		t.Fatalf("%+v", meta)
	}
	indexed, walked := NewPager(sorted, "fixture"), NewPager(memstore.Keys(memstore.Options{}, keys...), "fixture")
	walk := func(p *Pager, q string) []string {
		var out []string
		token := ""
		for range 400 {
			query := q
			if token != "" {
				query += "&pageToken=" + token
			}
			page := list(t, p, query)
			out = append(out, entries(page)...)
			if token = page.Token; token == "" {
				return out
			}
		}
		t.Fatalf("%s did not end", q)
		return nil
	}
	gap := false
	for g := groupRows; g < len(keys); g += groupRows {
		if strings.HasPrefix(keys[g], "root/a/") && strings.HasPrefix(keys[g-1], "root/a/") {
			q := "prefix=root/&delimiter=/&maxResults=1&startOffset=" + keys[g-1] + "x&endOffset=" + keys[g]
			a, b := walk(indexed, q), walk(walked, q)
			if len(a) != 0 || len(b) != 0 {
				t.Errorf("the gap before a row group: %v / %v", a, b)
			}
			gap = true
			break
		}
	}
	if !gap {
		t.Fatal("no row group starts inside root/a/")
	}
	for _, start := range []string{"root/", "root/a/", "root/a/040", "root/b/", "root/b/099"} {
		for _, end := range []string{"root/z", "root/b/050", "root/c/090"} {
			for _, size := range []int{1, 2, 5} {
				q := fmt.Sprintf("prefix=root/&delimiter=/&startOffset=%s&endOffset=%s&maxResults=%d", start, end, size)
				if a, b := walk(indexed, q), walk(walked, q); !slices.Equal(a, b) {
					t.Errorf("%s: indexed %v, walked %v", q, a, b)
				}
			}
		}
	}
}
