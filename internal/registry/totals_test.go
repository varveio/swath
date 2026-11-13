// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry_test

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/internal/filter"
	"github.com/varveio/swath/internal/order"
	"github.com/varveio/swath/internal/registry"
	"github.com/varveio/swath/internal/source/keyrange"
	"github.com/varveio/swath/internal/source/s3"
	"github.com/varveio/swath/internal/totals"
)

// versionsSource serves rows through the range engine, pages of page rows.
func versionsSource(t *testing.T, rows []s3.VersionRow, workers, page int) registry.Bound[s3.Version] {
	t.Helper()
	p, err := keyrange.New(keyrange.Options{Workers: workers, PageSize: page}, keyrange.Description[s3.Version]{Target: "test", Listing: keyrange.Listing, Groups: true, Detach: s3.VersionsSchema.Detach},
		func(_ context.Context, q *keyrange.Query) (*keyrange.Page[s3.Version], error) {
			at := 0
			if q.Token != "" {
				at, _ = strconv.Atoi(q.Token)
			} else {
				for at < len(rows) && rows[at].Key <= q.After {
					at++
				}
			}
			end := min(len(rows), at+q.Max)
			out := &keyrange.Page[s3.Version]{Rows: append([]s3.VersionRow(nil), rows[at:end]...)}
			if end < len(rows) {
				out.Truncated, out.Token = true, strconv.Itoa(end)
			}
			return out, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return registry.Bind(p, &s3.VersionsSchema)
}

// TestCountTotals: the totals sink bound to a schema by its columns sums
// every counted row per prefix, through concurrent workers and pages that
// split prefixes: a delete marker is no object, a version is one (a size
// each), the filters apply first, and the versions counts still run beside
// it.
func TestCountTotals(t *testing.T) {
	var rows []s3.VersionRow
	want := map[string][2]int64{} // prefix: objects, bytes, at depth 2
	kept := int64(0)
	for i := range 300 {
		key := fmt.Sprintf("d%d/e%d/f%03d", i%3, i%5, i)
		if i%7 == 0 {
			key = fmt.Sprintf("top%03d", i) // no '/': the total only
		}
		for v := range 2 {
			marker := v == 0 && i%4 == 0
			size := int64(i*10 + v)
			rows = append(rows, s3.VersionRow{Key: key, Data: s3.Version{Object: s3.Object{Size: size, Mtime: int64(i)}, VersionID: strconv.Itoa(v), DeleteMarker: marker, IsLatest: v == 1}})
			if marker || size > 2000 { // the size filter drops markers (no size) and sizes over 2000
				continue
			}
			kept++
			for _, p := range prefixes(key, 2) {
				w := want[p]
				want[p] = [2]int64{w[0] + 1, w[1] + size}
			}
		}
	}
	slices.SortStableFunc(rows, func(a, b s3.VersionRow) int { return strings.Compare(a.Key, b.Key) })
	for _, page := range []int{1, 7, 1000} {
		for _, workers := range []int{1, 8} {
			t.Run(fmt.Sprintf("page%d/workers%d", page, workers), func(t *testing.T) {
				tl := totals.New(totals.Options{Depth: 2})
				vc := &registry.VersionCounts{}
				var dropped atomic.Int64
				st, _, err := versionsSource(t, rows, workers, page).Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: workers},
					Filter: &filter.Spec{MaxSize: "2000"}, Filtered: &dropped, Totals: tl, Versions: vc})
				if err != nil || st.Rows != kept || dropped.Load() != int64(len(rows))-kept {
					t.Fatalf("stats %+v, dropped %d, err %v", st, dropped.Load(), err)
				}
				got := map[string][2]int64{}
				for _, p := range tl.Result() {
					got[p.Prefix] = [2]int64{p.Objects, p.Bytes}
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("got %v\nwant %v", got, want)
				}
				if !vc.Enabled || vc.Versions.Load() != kept || vc.DeleteMarkers.Load() != 0 {
					t.Fatalf("versions counts %d + %d", vc.Versions.Load(), vc.DeleteMarkers.Load())
				}
			})
		}
	}
}

// prefixes is key's prefixes down to depth, the total ("") first.
func prefixes(key string, depth int) []string {
	out := []string{""}
	for end := 0; len(out) <= depth; {
		i := strings.IndexByte(key[end:], '/')
		if i < 0 {
			break
		}
		end += i + 1
		out = append(out, key[:end])
	}
	return out
}

// TestTotalsNeedCountAndColumn: totals are refused on a run that is not a
// count, on a schema without the column they sum, and, counting links
// once, without the columns that identify a file.
func TestTotalsNeedCountAndColumn(t *testing.T) {
	src := versionsSource(t, nil, 1, 10)
	if _, _, err := src.Run(context.Background(), registry.Env{Order: order.Options{Workers: 1}, Totals: totals.New(totals.Options{})}); err == nil || !strings.Contains(err.Error(), "need a count") {
		t.Fatalf("not a count: %v", err)
	}
	_, _, err := src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 1}, Totals: totals.New(totals.Options{BytesColumn: "allocated_bytes"})})
	if err == nil || !strings.Contains(err.Error(), "need the allocated_bytes column") {
		t.Fatalf("no column: %v", err)
	}
	_, _, err = src.Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: 1}, Totals: totals.New(totals.Options{LinksOnce: true})})
	if err == nil || !strings.Contains(err.Error(), "needs the nlink, dev and inode columns") {
		t.Fatalf("no link columns: %v", err)
	}
}
