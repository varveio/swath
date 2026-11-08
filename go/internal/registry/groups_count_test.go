// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package registry_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/varveio/swath/go/internal/filter"
	"github.com/varveio/swath/go/internal/order"
	"github.com/varveio/swath/go/internal/registry"
	"github.com/varveio/swath/go/internal/source/keyrange"
	"github.com/varveio/swath/go/internal/source/s3"
)

// Count must derive distinct names from surviving rows even when filters
// remove group endings, entire later groups, and the final group.
func TestGroupedCountFilteredBoundaries(t *testing.T) {
	for _, page := range []int{1, 2, 3, 1000} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("page%d/workers%d", page, workers), func(t *testing.T) {
				var rows []s3.VersionRow
				for _, key := range []string{"a", "b", "c", "d"} {
					for i := range 4 {
						size := int64(10)
						if (key == "a" || key == "c") && i < 3 {
							size = 0
						}
						rows = append(rows, s3.VersionRow{Key: key, Data: s3.Version{Object: s3.Object{Size: size}, VersionID: fmt.Sprint(i)}})
					}
				}
				var calls atomic.Int64
				p, err := keyrange.New(keyrange.Options{Workers: workers, PageSize: page}, keyrange.Description[s3.Version]{Target: "test", Listing: keyrange.Listing, Groups: true, Detach: s3.VersionsSchema.Detach},
					func(_ context.Context, q *keyrange.Query) (*keyrange.Page[s3.Version], error) {
						calls.Add(1)
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
							out.Truncated = true
							out.Token = strconv.Itoa(end)
						}
						return out, nil
					})
				if err != nil {
					t.Fatal(err)
				}
				var dropped atomic.Int64
				st, _, err := registry.Bind(p, &s3.VersionsSchema).Run(context.Background(), registry.Env{Count: true, Order: order.Options{Workers: workers}, Filter: &filter.Spec{MaxSize: "0"}, Filtered: &dropped})
				if err != nil || st.Rows != 6 || st.DistinctKeys != 2 || dropped.Load() != 10 || calls.Load() == 0 {
					t.Fatalf("stats=%+v dropped=%d calls=%d err=%v", st, dropped.Load(), calls.Load(), err)
				}
			})
		}
	}
}
