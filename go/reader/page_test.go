// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package reader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

// pageAll pages through a listing, checking every page's size and Next,
// and returns the entries described and their prefixes' aggregates.
func pageAll(t *testing.T, d *Dataset, req PageRequest) ([]string, []PageEntry) {
	t.Helper()
	var described []string
	var entries []PageEntry
	for {
		page, err := d.Page(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			described = append(described, describe(e.Entry))
			entries = append(entries, e)
		}
		if page.Next == nil {
			return described, entries
		}
		if len(page.Entries) != req.Limit {
			t.Fatalf("a page of %d entries with a next page, limit %d", len(page.Entries), req.Limit)
		}
		req.Cursor = page.Next
	}
}

// TestPage: every page size, across parts and paired prefixes, lists what
// the oracle lists, the last page has no Next, and each prefix, and no
// object, carries its totals.
func TestPage(t *testing.T) {
	dir := listDir(t)
	objects, err := stream(t, openDir(t, dir))
	if err != nil {
		t.Fatal(err)
	}
	d := openDir(t, dir)
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "a/", "c/"} {
		want := listOracle(listKeys, prefix, '/')
		for _, limit := range []int{1, 2, 3, 100} {
			t.Run(fmt.Sprintf("%q/%d", prefix, limit), func(t *testing.T) {
				got, entries := pageAll(t, d, PageRequest{ListRequest: ListRequest{Prefix: []byte(prefix), Delimiter: '/', Limit: limit},
					Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}})
				if strings.Join(got, " ") != strings.Join(want, " ") {
					t.Fatalf("paged %q, want %q", got, want)
				}
				for _, e := range entries {
					if e.CommonPrefix == nil {
						if e.Aggregate != nil {
							t.Fatalf("object %q has an aggregate", e.Object.Key)
						}
						continue
					}
					count, size := prefixTotals(objects, e.CommonPrefix)
					if a := e.Aggregate; a == nil || a.Count == nil || *a.Count != count || a.Bytes == nil || *a.Bytes != size {
						t.Fatalf("prefix %q: %+v, want count %d and bytes %d", e.CommonPrefix, e.Aggregate, count, size)
					}
				}
			})
		}
	}
}

// TestPageAggregatesReturnedPrefixesOnly: a page whose entry beyond the
// limit is a prefix reads what the listing reads and nothing more.
func TestPageAggregatesReturnedPrefixesOnly(t *testing.T) {
	dir := listDir(t)
	d, src := openSource(t, dir)
	src.reset()
	var st ReadStats
	page, err := d.Page(context.Background(), PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 1, Stats: &st},
		Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}})
	if err != nil || len(page.Entries) != 1 || describe(page.Entries[0].Entry) != "o:a" || page.Next == nil {
		t.Fatalf("%+v, %v", page, err)
	}
	_, pageReads := src.taken()

	d, src = openSource(t, dir)
	src.reset()
	var listed ReadStats
	if _, err := d.List(context.Background(), ListRequest{Delimiter: '/', Limit: 2, Stats: &listed}); err != nil {
		t.Fatal(err)
	}
	if _, reads := src.taken(); len(reads) != len(pageReads) || st != listed {
		t.Fatalf("page read %d times (%+v), its listing %d (%+v)", len(pageReads), st, len(reads), listed)
	}
}

// TestPageBudget: a prefix whose sum does not fit the budget has its
// count and no bytes.
func TestPageBudget(t *testing.T) {
	d := openDir(t, listDir(t))
	if err := d.CheckSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	page, err := d.Page(context.Background(), PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 100},
		Aggregates: AggregateOptions{Counts: true, Sums: SumsWithinBudget, SumBudgetBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	prefixes := 0
	for _, e := range page.Entries {
		if a := e.Aggregate; e.CommonPrefix != nil {
			prefixes++
			if a == nil || a.Count == nil || a.Bytes != nil {
				t.Fatalf("prefix %q: %+v", e.CommonPrefix, a)
			}
		}
	}
	if prefixes == 0 || page.Next != nil {
		t.Fatalf("%d prefixes, next %v", prefixes, page.Next)
	}
}

// TestPageStats: the statistics hold the listing and the aggregation.
func TestPageStats(t *testing.T) {
	d, _ := openSource(t, listDir(t))
	var st ReadStats
	req := PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 100, Stats: &st}, Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}}
	if _, err := d.Page(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	d2, _ := openSource(t, listDir(t))
	var listed ReadStats
	if _, err := d2.List(context.Background(), ListRequest{Delimiter: '/', Limit: 101, Stats: &listed}); err != nil {
		t.Fatal(err)
	}
	if st.SourceReads <= listed.SourceReads || st.ValuePagesDecoded <= listed.ValuePagesDecoded {
		t.Fatalf("page %+v, its listing alone %+v", st, listed)
	}
}

// TestPageCancelledBetweenCalls: a context that ends at the listing's last
// read fails the page with its error and no entries, its statistics the
// listing's.
func TestPageCancelledBetweenCalls(t *testing.T) {
	dir := listDir(t)
	req := PageRequest{ListRequest: ListRequest{Prefix: []byte("c/"), Delimiter: '/', Limit: 1}, Aggregates: AggregateOptions{Counts: true, Sums: SumsAlways}}
	d, src := openSource(t, dir)
	src.reset()
	var listed ReadStats
	if _, err := d.List(context.Background(), ListRequest{Prefix: req.Prefix, Delimiter: '/', Limit: 2, Stats: &listed}); err != nil {
		t.Fatal(err)
	}
	_, reads := src.taken()

	d, src = openSource(t, dir)
	src.reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	src.cancelAt, src.cancel = len(reads), cancel
	var st ReadStats
	req.Stats = &st
	page, err := d.Page(ctx, req)
	if !errors.Is(err, context.Canceled) || page.Entries != nil || page.Next != nil {
		t.Fatalf("%+v, %v", page, err)
	}
	if st != listed {
		t.Fatalf("stats %+v, the listing's %+v", st, listed)
	}
	if _, after := src.taken(); len(after) != len(reads) {
		t.Fatalf("%d reads, the listing's %d", len(after), len(reads))
	}
}

// TestPageFailures: invalid requests before any read, with the statistics
// reset; a listing's failure keeps its statistics.
func TestPageFailures(t *testing.T) {
	d, src := openSource(t, listDir(t))
	src.reset()
	for _, req := range []PageRequest{
		{ListRequest: ListRequest{Delimiter: '/'}},
		{ListRequest: ListRequest{Delimiter: '/', Limit: -1}},
		{ListRequest: ListRequest{Delimiter: '/', Limit: math.MaxInt}},
		{ListRequest: ListRequest{Delimiter: '/', Limit: 1}, Aggregates: AggregateOptions{Sums: SumsAlways, SumBudgetBytes: 1}},
	} {
		st := ReadStats{SourceReads: 9}
		req.Stats = &st
		if page, err := d.Page(context.Background(), req); !errors.Is(err, ErrInvalidRequest) || page.Entries != nil || st != (ReadStats{}) {
			t.Fatalf("%+v: %+v, %v, stats %+v", req, page, err, st)
		}
	}
	if _, reads := src.taken(); len(reads) != 0 {
		t.Fatalf("%d reads", len(reads))
	}
	d = openDir(t, writeDataset(t, s3Spec(), []s3Row{{Key: "a", Size: ptr(int64(1))}, {Key: "b"}}))
	var st ReadStats
	if page, err := d.Page(context.Background(), PageRequest{ListRequest: ListRequest{Delimiter: '/', Limit: 10, Stats: &st}}); !errors.Is(err, ErrRow) || page.Entries != nil || st.KeysDecoded == 0 {
		t.Fatalf("%+v, %v, stats %+v", page, err, st)
	}
}
